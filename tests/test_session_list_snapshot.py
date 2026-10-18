"""Bound selected JSON hydration while preserving complete sidebar responses."""
from datetime import UTC, datetime, timedelta
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
