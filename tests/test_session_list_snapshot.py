"""Bound selected JSON hydration while preserving complete sidebar responses."""
from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection, SessionListMetadata
from pa.execution.session_presentation import build_session_presentation
from pa.modules import agent_chat

NOW = datetime(2026, 10, 18, tzinfo=UTC)


@pytest.fixture
def listing(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    for index in range(12):
        store.save_session(AgentSession(
            id=f"s-{index:02d}", agent_name="fixture", title=f"Fixture {index}",
            label="even" if index % 2 == 0 else "odd",
            purpose="automated_run" if index % 4 == 0 else "chat",
            status="closed" if index % 5 == 0 else "idle",
            archived_at=NOW if index % 3 == 0 else None,
            pinned_at=NOW - timedelta(seconds=index) if index in (2, 7) else None,
            human_activity_at=NOW - timedelta(minutes=index % 3),
            updated_at=NOW - timedelta(seconds=index),
            initiating_workflow={"chat_group_id": "group"},
            config_json={"values": {"model": "fixture"},
                         "selection_proof": "retained" * 4096,
                         "durable_runtime": {"last_event_cursor": index}},
            metrics_json={"retained": "metrics" * 4096},
            execution_binding={"version": 1, "source": str(index)},
        ))
    manager = SimpleNamespace(store=store, list_runtimes=lambda: [], get=lambda _: None,
                              startup_complete=True, quiescing=False)
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                        agent_enabled=False, instance_id="fixture", peers=[])
    ctx = SimpleNamespace(store=store, settings=settings, services={}, require_service=lambda _: manager)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
    def presentation(*args, **kwargs):
        return build_session_presentation(*args, **kwargs, now=NOW)
    with patch.object(agent_chat, "_manager", return_value=manager), patch.object(
        agent_chat, "current_instance_name", return_value="Fixture"
    ), patch.object(agent_chat, "build_session_presentation", side_effect=presentation):
        yield store, manager, request
    store.close_thread_connection()


@pytest.mark.parametrize("filters", [
    {}, {"label": "even"}, {"statuses": ("closed",)},
    {"exclude_statuses": ("closed",)}, {"include_archived": False},
    {"archived": True, "purposes": ("chat",)},
    {"session_ids": {"s-01", "s-07"}}, {"session_ids": set()},
    {"chat_group_id": "group"},
])
def test_snapshot_preserves_filters_order_and_complete_row_values(listing, filters):
    store, _, _ = listing
    expected = store.list_sessions(**filters)
    snapshot = store.session_list_snapshot(**filters)
    assert [s.id for s in snapshot.candidates] == [s.id for s in expected]
    assert all(isinstance(s, SessionListMetadata) for s in snapshot.candidates)
    assert all(not hasattr(s, "model_dump") for s in snapshot.candidates)
    assert [snapshot.hydrate(s.id).model_dump() for s in expected] == [s.model_dump() for s in expected]


@pytest.mark.parametrize("view,archived", [("active", False), ("chats", False),
                                         ("chats", True), ("all", False), ("activity", False)])
@pytest.mark.parametrize("detail", ["summary", "full"])
def test_entire_response_matches_legacy_including_metrics_and_selected_item(listing, view, archived, detail):
    store, _, request = listing
    kwargs = dict(view=view, archived=archived, detail=detail, limit=2, selected_session_id="s-11")
    with patch.object(store, "list_sessions", wraps=store.list_sessions):
        expected = agent_chat.list_agent_sessions(request, **kwargs)
    with patch.object(store, "get_session", side_effect=AssertionError("mixed a later snapshot")):
        actual = agent_chat.list_agent_sessions(request, **kwargs)
    assert actual == expected
    assert all(item["metrics_json"] == {"retained": "metrics" * 4096} for item in actual)
    if detail == "full":
        assert all(item["config_json"]["selection_proof"] == "retained" * 4096 for item in actual)


def test_decodes_only_selected_rows_without_a_second_database_read(listing):
    store, _, request = listing
    original = CardProjection._row_to_session
    with patch.object(CardProjection, "_row_to_session", side_effect=original) as decoded:
        result = agent_chat.list_agent_sessions(request, view="all", limit=2, selected_session_id="s-11")
    assert [item["id"] for item in result] == ["s-00", "s-11"]
    assert decoded.call_count == 2
    snapshot = store.session_list_snapshot()
    old = snapshot.hydrate("s-01")
    changed = store.get_session("s-01")
    changed.metrics_json = {"later": True}
    changed.config_json = {"later": True}
    store.save_session(changed)
    assert snapshot.hydrate("s-01").model_dump() == old.model_dump()
    assert store.get_session("s-01").metrics_json == {"later": True}


def test_live_runtime_wins_and_is_not_replaced_with_snapshot_details(listing):
    store, manager, request = listing
    live = store.get_session("s-11")
    live.metrics_json = {"live": True}
    live.updated_at = NOW + timedelta(seconds=1)
    manager.list_runtimes = lambda: [SimpleNamespace(session=live, _closed=False)]
    with patch.object(agent_chat, "_session_list_item", side_effect=lambda request, session, **_: session.model_dump()), patch.object(
        CardProjection, "_row_to_session", side_effect=AssertionError("hydrated durable row over live runtime")
    ):
        result = agent_chat.list_agent_sessions(request, view="all", limit=1)
    assert result == [live.model_dump()]


@pytest.mark.parametrize("raw", ['{"last":1,"last":2}', '{malformed', 'null', '[]'])
def test_selected_details_retain_existing_json_decoder_and_errors(listing, raw):
    store, _, _ = listing
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET metrics_json=? WHERE id='s-01'", (raw,))
    snapshot = store.session_list_snapshot()
    try:
        expected = store.get_session("s-01")
    except ValidationError:
        with pytest.raises(ValidationError):
            snapshot.hydrate("s-01")
    else:
        assert snapshot.hydrate("s-01").model_dump() == expected.model_dump()


def test_activity_and_legacy_overrides_keep_existing_store_contract(listing):
    store, _, request = listing
    with patch.object(store, "session_list_snapshot", side_effect=AssertionError("activity needs workflow config")):
        agent_chat.list_agent_sessions(request, view="activity", activity_filter="completed")
    with patch.object(store, "list_sessions", wraps=store.list_sessions) as legacy, patch.object(
        store, "session_list_snapshot", side_effect=AssertionError("bypassed custom list contract")
    ):
        agent_chat.list_agent_sessions(request, view="chats")
    legacy.assert_called_once_with(purposes=("chat",), archived=False)


@pytest.mark.parametrize("extension", ["instance", "subclass"])
def test_custom_row_decoder_keeps_its_original_selection_and_payload_semantics(listing, extension):
    store, manager, request = listing
    original = CardProjection._row_to_session

    def decode(row):
        result = original(row)
        result.title = "custom " + result.title
        return result

    if extension == "subclass":
        class CustomProjection(CardProjection):
            _row_to_session = staticmethod(decode)
        custom = CustomProjection(store.db_path)
    else:
        custom = store
        custom._row_to_session = decode
    manager.store = request.app.state.ctx.store = custom
    try:
        with patch.object(custom, "session_list_snapshot", side_effect=AssertionError("bypassed custom decoder")):
            result = agent_chat.list_agent_sessions(request, view="all", limit=2)
        assert [item["title"] for item in result] == ["custom Fixture 0", "custom Fixture 1"]
    finally:
        if custom is not store:
            custom.close_thread_connection()


@pytest.mark.parametrize('filters', [
    {}, {'label': 'even'}, {'statuses': ('closed',)},
    {'exclude_statuses': ('closed',)}, {'include_archived': False},
    {'archived': True, 'purposes': ('chat',)},
    {'session_ids': {'s-01', 's-07'}}, {'session_ids': set()},
    {'chat_group_id': 'group'}, {'include_metrics': False},
])
def test_selective_snapshot_preserves_all_filter_and_decoder_contracts(listing, filters):
    store, _, _ = listing
    expected = store.list_sessions(**filters)
    chosen = [row.id for row in expected[::2]]
    snapshot = store.session_list_snapshot(select_ids=lambda _: chosen, **filters)
    assert [row.id for row in snapshot.candidates] == [row.id for row in expected]
    assert set(snapshot._rows) == set(chosen)
    assert [snapshot.hydrate(key).model_dump() for key in chosen] == [
        row.model_dump() for row in expected[::2]
    ]


def test_route_transfers_full_rows_only_after_metadata_selection(listing):
    store, _, request = listing
    original = CardProjection._list_session_rows
    returned = []
    def read(self, **filters):
        rows = original(self, **filters)
        returned.append((len(rows), set(rows[0].keys()) if rows else set()))
        return rows
    with patch.object(CardProjection, '_list_session_rows', new=read):
        result = agent_chat.list_agent_sessions(request, view='all', limit=2, selected_session_id='s-11')
    assert [row['id'] for row in result] == ['s-00', 's-11']
    assert returned[0] == (12, {'id', 'status', 'purpose', 'archived_at', 'pinned_at',
                               'human_activity_at', 'updated_at'})
    assert returned[1][0] == 2 and {'config_json', 'metrics_json', 'execution_binding_json'} <= returned[1][1]


def test_concurrent_writer_cannot_mix_selection_and_selected_details(listing):
    store, _, _ = listing
    old = store.get_session('s-01').model_dump()
    def write():
        with sqlite3.connect(store.db_path) as connection:
            connection.execute('UPDATE agent_sessions SET status=?,updated_at=?,config_json=?,metrics_json=? WHERE id=?',
                               ('closed', (NOW + timedelta(days=1)).isoformat(),
                                '{"later":true}', '{"later":true}', 's-01'))
    with ThreadPoolExecutor(max_workers=1) as executor:
        def choose(candidates):
            assert next(row for row in candidates if row.id == 's-01').updated_at == old['updated_at']
            executor.submit(write).result(timeout=2)
            return ['s-01']
        snapshot = store.session_list_snapshot(select_ids=choose)
    assert snapshot.hydrate('s-01').model_dump() == old
    assert store.get_session('s-01').metrics_json == {'later': True}
    assert store.get_session('s-01').status == 'closed'


def test_selector_failure_and_invalid_ids_release_read_transaction(listing):
    store, _, _ = listing
    failure = ValueError('fixture selection failed')
    def fail(_):
        raise failure
    with pytest.raises(ValueError) as caught:
        store.session_list_snapshot(select_ids=fail)
    assert caught.value is failure
    with pytest.raises(ValueError, match='outside its snapshot'):
        store.session_list_snapshot(label='even', select_ids=lambda _: ['s-01'])
    with store._conn() as connection:
        assert not connection.in_transaction
    assert store.get_session('s-01') is not None


def test_selective_read_does_not_commit_or_discard_outer_transaction(listing):
    store, _, _ = listing
    old_label = store.get_session('s-01').label
    with pytest.raises(RuntimeError, match='outer rollback'):
        with store._conn() as connection:
            connection.execute('UPDATE agent_sessions SET label=? WHERE id=?', ('uncommitted', 's-01'))
            snapshot = store.session_list_snapshot(select_ids=lambda _: ['s-01'])
            assert snapshot.hydrate('s-01').label == 'uncommitted'
            assert connection.in_transaction
            raise RuntimeError('outer rollback')
    assert store.get_session('s-01').label == old_label


@pytest.mark.parametrize('raw', ['{"last":1,"last":2}', '{malformed', 'null', '[]'])
def test_selective_details_preserve_malformed_and_duplicate_json_semantics(listing, raw):
    store, _, _ = listing
    with store._conn() as connection:
        connection.execute('UPDATE agent_sessions SET metrics_json=? WHERE id=?', (raw, 's-01'))
    snapshot = store.session_list_snapshot(select_ids=lambda _: ['s-01'])
    try:
        expected = store.get_session('s-01')
    except ValidationError:
        with pytest.raises(ValidationError):
            snapshot.hydrate('s-01')
    else:
        assert snapshot.hydrate('s-01').model_dump() == expected.model_dump()


@pytest.mark.parametrize('extension', ['snapshot', 'rows'])
def test_custom_snapshot_and_row_readers_keep_preselector_call_contract(listing, extension):
    store, manager, request = listing
    calls = []
    class CustomProjection(CardProjection):
        pass
    if extension == 'snapshot':
        def snapshot(self, **filters):
            assert 'select_ids' not in filters
            calls.append('snapshot')
            return super(CustomProjection, self).session_list_snapshot(**filters)
        CustomProjection.session_list_snapshot = snapshot
    else:
        def rows(self, **filters):
            assert '_columns' not in filters
            calls.append('rows')
            return super(CustomProjection, self)._list_session_rows(**filters)
        CustomProjection._list_session_rows = rows
    custom = CustomProjection(store.db_path)
    manager.store = request.app.state.ctx.store = custom
    try:
        with patch.object(agent_chat, '_session_list_item', side_effect=lambda request, session, **_: session.id):
            assert agent_chat.list_agent_sessions(request, view='all', limit=2) == ['s-00', 's-01']
        assert calls == [extension]
    finally:
        custom.close_thread_connection()


@pytest.mark.parametrize('view', ['active', 'chats', 'all'])
def test_tied_selection_order_and_pins_match_complete_legacy_path(listing, view):
    store, _, request = listing
    with store._conn() as connection:
        connection.execute('UPDATE agent_sessions SET updated_at=?,human_activity_at=?',
                           (NOW.isoformat(), NOW.isoformat()))
        connection.execute('UPDATE agent_sessions SET pinned_at=? WHERE id IN (?,?)',
                           (NOW.isoformat(), 's-02', 's-07'))
    kwargs = dict(view=view, limit=3, selected_session_id='s-11')
    with patch.object(store, 'list_sessions', wraps=store.list_sessions):
        expected = agent_chat.list_agent_sessions(request, **kwargs)
    assert agent_chat.list_agent_sessions(request, **kwargs) == expected
