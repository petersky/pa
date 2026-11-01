"""Full session evidence keeps matching, freshness and custom-reader behavior."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.modules import agent_chat
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore


def watch(index, *, session="other", card="other", realm="default", retired=False):
    return PRWatch(
        id=f"watch-{index}", realm_id=realm, card_id=card,
        repository="owner/repo", pr_number=index + 1,
        pr_url=f"https://github.com/owner/repo/pull/{index + 1}",
        originating_instance_id="fixture", originating_session_id=session,
        updated_at=datetime(2026, 10, 29, tzinfo=UTC) + timedelta(seconds=index),
        retired_at=datetime(2026, 10, 29, tzinfo=UTC) if retired else None,
        state={"proof": "x" * 65536},
    )


def request_for(store):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        ctx=SimpleNamespace(services={"pr_supervisor_store": store}))))


@pytest.mark.parametrize("card", [None, "", "card"])
def test_full_related_models_match_existing_predicate_and_order(tmp_path, card):
    store = PRSupervisorStore(tmp_path / "watches.db")
    session = SimpleNamespace(id="session", card_id=card, realm_id="default")
    for item in [watch(0), watch(1, session="session"),
                 watch(2, card="card", retired=True),
                 watch(3, session="session", card="card", realm="other")]:
        store.upsert_watch(item, preserve_lease=False)
    expected = [w.model_dump(mode="json") for w in store.list_watches(include_retired=True)
                if w.originating_session_id == session.id or (w.card_id and w.card_id == card)]
    result = agent_chat._session_pr_watches(request_for(store), session)
    assert result == expected
    assert all(len(w["state"]["proof"]) == 65536 for w in result)
    # Native conversion must occur only for matching rows; no proof cache survives.
    converted = []
    original = store._row_to_watch
    def convert(row):
        converted.append(row["id"])
        return original(row)
    with patch.object(store, "_row_to_watch", side_effect=convert):
        assert [w.id for w in store.list_session_watches(session)] == [w["id"] for w in expected]
    assert converted == [w["id"] for w in expected]
    changed = store.get_watch("watch-1")
    changed.state = {"fresh": True}
    store.upsert_watch(changed, preserve_lease=False)
    assert next(w for w in agent_chat._session_pr_watches(request_for(store), session)
                if w["id"] == "watch-1")["state"] == {"fresh": True}


def test_matching_invalid_evidence_still_fails_validation(tmp_path):
    store = PRSupervisorStore(tmp_path / "watches.db")
    store.upsert_watch(watch(0, session="session"), preserve_lease=False)
    with store._conn() as conn:
        conn.execute("UPDATE pr_watches SET state_json='{malformed' WHERE id='watch-0'")
    with pytest.raises(ValueError):
        agent_chat._session_pr_watches(request_for(store), SimpleNamespace(id="session", card_id=None))


@pytest.mark.parametrize("reader", ["list_watches", "_row_to_watch"])
@pytest.mark.parametrize("extension", ["instance", "subclass"])
def test_custom_reader_keeps_existing_full_scan_contract(tmp_path, reader, extension):
    class CustomStore(PRSupervisorStore):
        pass
    store = (CustomStore if extension == "subclass" else PRSupervisorStore)(tmp_path / "watches.db")
    for index in range(3):
        store.upsert_watch(watch(index, session="session"), preserve_lease=False)
    if extension == "subclass":
        calls = []
        native = getattr(PRSupervisorStore, reader)
        def custom_list(self, *args, **kwargs):
            calls.append(True)
            return native(self, *args, **kwargs)
        def custom_row(row):
            calls.append(True)
            return native(row)
        setattr(CustomStore, reader, custom_list if reader == "list_watches" else staticmethod(custom_row))
        with patch.object(store, "list_session_watches", side_effect=AssertionError("bypassed custom reader")):
            result = agent_chat._session_pr_watches(request_for(store), SimpleNamespace(id="session", card_id=None))
        count = len(calls)
    else:
        with patch.object(store, reader, wraps=getattr(store, reader)) as custom, patch.object(
            store, "list_session_watches", side_effect=AssertionError("bypassed custom reader")
        ):
            result = agent_chat._session_pr_watches(request_for(store), SimpleNamespace(id="session", card_id=None))
        count = custom.call_count
    assert len(result) == 3
    assert count == (1 if reader == "list_watches" else 3)


def test_equal_timestamp_matches_native_order_and_summary_unchanged(tmp_path):
    store = PRSupervisorStore(tmp_path / "watches.db")
    for index in range(4):
        item = watch(index, session="session")
        item.updated_at = datetime(2026, 10, 29, tzinfo=UTC)
        store.upsert_watch(item, preserve_lease=False)
    session = SimpleNamespace(id="session", card_id=None, realm_id="default")
    assert store.list_session_watches(session) == store.list_watches(include_retired=True)
    with patch.object(store, "list_session_watches", side_effect=AssertionError("summary used full evidence")):
        assert agent_chat._session_pr_watches(request_for(store), session, summary=True) == store.session_watch_summaries(session)


@pytest.mark.parametrize("card", [None, "", "card"])
def test_full_batch_preserves_complete_cross_realm_retired_and_tied_order(tmp_path, card):
    store = PRSupervisorStore(tmp_path / "watches.db")
    sessions = [SimpleNamespace(id="session", card_id=card),
                SimpleNamespace(id="other", card_id="card")]
    for index, item in enumerate([
        watch(0, session="session", card=None), watch(1, card="card", retired=True),
        watch(2, session="session", card="card", realm="other"), watch(3, card=""),
        watch(4, session="other", card="card"), watch(5, card=None),
    ]):
        # All ties, nonlexical insertion order and both branches of the OR.
        item.id = f"watch-{9 - index}"
        item.updated_at = datetime(2026, 10, 29, tzinfo=UTC)
        store.upsert_watch(item, preserve_lease=False)
    expected = {s.id: [w.model_dump(mode="json") for w in store.list_session_watches(s)] for s in sessions}
    actual = store.session_watch_payloads_for_sessions(sessions)
    assert actual == expected
    shared = next(w for w in actual["session"] if w["card_id"] == "card") if card else None
    if shared:
        other = next(w for w in actual["other"] if w["id"] == shared["id"])
        assert shared is not other and shared["state"] is not other["state"]
        shared["state"]["proof"] = "changed response only"
        assert len(other["state"]["proof"]) == 65536
    changed = store.get_watch("watch-9")
    changed.state = {"fresh": True}
    store.upsert_watch(changed, preserve_lease=False)
    assert next(w for w in store.session_watch_payloads_for_sessions(sessions)["session"]
                if w["id"] == "watch-9")["state"] == {"fresh": True}


def test_full_batch_validates_matching_evidence_once_and_does_not_load_unrelated(tmp_path):
    store = PRSupervisorStore(tmp_path / "watches.db")
    store.upsert_watch(watch(0, card="shared"), preserve_lease=False)
    store.upsert_watch(watch(1), preserve_lease=False)
    with store._conn() as conn:
        conn.execute("UPDATE pr_watches SET state_json='{malformed' WHERE id='watch-1'")
    sessions = [SimpleNamespace(id=f"s-{i}", card_id="shared") for i in range(10)]
    with patch.object(store, "_row_to_watch", wraps=store._row_to_watch) as convert:
        result = store.session_watch_payloads_for_sessions(sessions)
    assert convert.call_count == 1
    assert all(len(result[s.id]) == 1 for s in sessions)
    with store._conn() as conn:
        conn.execute("UPDATE pr_watches SET state_json='{malformed' WHERE id='watch-0'")
    with pytest.raises(ValueError):
        store.session_watch_payloads_for_sessions(sessions)


def test_full_batch_bounds_queries_and_refreshes_at_request_boundary(tmp_path):
    store = PRSupervisorStore(tmp_path / "watches.db")
    sessions = [SimpleNamespace(id=f"s-{i}", card_id=f"card-{i // 2}") for i in range(501)]
    store.upsert_watch(watch(0, session="s-250", card="card-124"), preserve_lease=False)
    store.upsert_watch(watch(1, session="s-500"), preserve_lease=False)
    calls = []
    native = store._conn
    from contextlib import contextmanager
    @contextmanager
    def traced():
        with native() as conn:
            conn.set_trace_callback(calls.append)
            yield conn
    with patch.object(store, "_conn", traced):
        actual = store.session_watch_payloads_for_sessions(sessions)
    queries = [q for q in calls if q.startswith("SELECT * FROM pr_watches")]
    assert len(queries) == 3
    assert actual == {s.id: [w.model_dump(mode="json") for w in store.list_session_watches(s)] for s in sessions}
    assert store.session_watch_payloads_for_sessions([]) == {}
    sessions[249].card_id = "rebound"
    assert store.session_watch_payloads_for_sessions(sessions)["s-249"] == []


def test_full_batch_retains_independent_reader_without_shared_cards(tmp_path):
    store = PRSupervisorStore(tmp_path / "watches.db")
    sessions = [SimpleNamespace(id=f"s-{i}", card_id=None if i == 0 else f"card-{i}") for i in range(3)]
    store.upsert_watch(watch(0, session="s-0", card="card-1"), preserve_lease=False)
    with patch.object(store, "list_session_watches", wraps=store.list_session_watches) as read:
        actual = store.session_watch_payloads_for_sessions(sessions)
    assert [c.args[0] for c in read.call_args_list] == sessions
    assert actual == {s.id: [w.model_dump(mode="json") for w in store.list_session_watches(s)] for s in sessions}


def test_full_batch_concurrent_calls_own_their_connections_and_response_objects(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    store = PRSupervisorStore(tmp_path / "watches.db")
    store.upsert_watch(watch(0, card="card"), preserve_lease=False)
    sessions = [SimpleNamespace(id="a", card_id="card"), SimpleNamespace(id="b", card_id="card")]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: store.session_watch_payloads_for_sessions(sessions), range(2)))
    assert results[0] == results[1]
    assert results[0]["a"][0]["state"] is not results[1]["a"][0]["state"]
