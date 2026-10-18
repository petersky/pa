"""Recovery inventory must omit diagnostics without losing durable obligations."""
import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager
from pa.instance.quiesce import QueuedPrompt, QuiesceSnapshot, SessionSnapshot


def sessions(store):
    now = datetime.now(UTC)
    prompt = QueuedPrompt(id="queued", message="synthetic recovery")
    variants = [
        ("closed", {}),
        ("closed", {"queued_prompts": [prompt.model_dump(mode="json")]}),
        ("closed", {"in_flight": prompt.model_dump(mode="json")}),
        ("closed", {"pending_permissions": [{"id": "permission"}]}),
        ("closed", {"lifecycle": "completion_pending"}),
        ("idle", {}),
        ("prompting", {}),
        ("recovery_blocked", {}),
        ("prompting", {"queue_paused": True}),
    ]
    for i, (status, durable) in enumerate(variants):
        store.save_session(AgentSession(
            id=f"s-{i}", agent_name="fixture", status=status,
            external_session_id=f"external-{i}", card_id=f"card-{i}",
            project_id="project", principal_id="principal", realm_id="default",
            label=f"card:card-{i}", cwd="/synthetic", title="title",
            model_id="model", mode_id="mode", purpose="chat",
            initiating_workflow={"source": "fixture"},
            workflow_state="pending", workflow_outcome={"retained": True},
            recovery_json={"attempts": 2},
            execution_binding={"workspace": "immutable"},
            config_json={"durable_runtime": durable,
                         "configuration": {"effective": {"model_id": "model"}},
                         "provisioning": {"action": "retained action"},
                         "execution_selection": {"proof": "x" * 65536},
                         "future_extension": {"retain_on_resume": True}},
            metrics_json={"diagnostic": "x" * 65536},
            created_at=now, updated_at=now + timedelta(seconds=i),
        ))


def manager(tmp_path, store):
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces")
    value = AgentSessionManager(settings, store)
    value.workspace_manager.reconcile_terminal_state = lambda: {}
    value.workspace_manager.collect_garbage = lambda **kwargs: {}
    value._project_recovery_available = AsyncMock(return_value=False)
    return value


def test_inventory_never_reads_metrics_and_preserves_all_other_fields(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    expected = store.list_sessions()
    original = store._conn

    def deny_metrics(action, table, column, *_):
        if action == sqlite3.SQLITE_READ and table == "agent_sessions" and column == "metrics_json":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    @contextmanager
    def connection():
        with original() as conn:
            conn.set_authorizer(deny_metrics)
            try:
                yield conn
            finally:
                conn.set_authorizer(None)

    with patch.object(store, "_conn", side_effect=connection):
        observed = store.list_sessions(include_metrics=False)
        startup = store.list_sessions_for_startup_inventory()
    assert [s.model_dump(exclude={"metrics_json"}) for s in observed] == [
        s.model_dump(exclude={"metrics_json"}) for s in expected
    ]
    assert all(s.metrics_json == {} for s in observed)
    assert all(s.metrics_json for s in store.list_sessions())
    assert all(store.get_session(s.id).metrics_json for s in observed)
    assert [s.model_dump(exclude={"metrics_json", "config_json"}) for s in startup] == [
        s.model_dump(exclude={"metrics_json", "config_json"}) for s in expected
    ]
    assert all(s.metrics_json == {} for s in startup)
    for lean, full in zip(startup, expected):
        assert lean.config_json == {
            key: full.config_json[key]
            for key in ("configuration", "durable_runtime", "provisioning")
        }
        assert store.get_session(lean.id).config_json == full.config_json


def test_inventory_filters_order_and_empty_selection_are_unchanged(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    for filters in ({}, {"session_ids": set()}, {"statuses": ("closed",)},
                    {"exclude_statuses": ("closed",)}, {"label": "card:card-2"},
                    {"include_archived": False}, {"purposes": ("chat",)},
                    {"session_ids": {"s-0", "s-2", "s-7"}}):
        assert [s.id for s in store.list_sessions(include_metrics=False, **filters)] == [
            s.id for s in store.list_sessions(**filters)
        ]


def test_recovery_decisions_snapshots_and_closed_obligations_are_unchanged(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    value = manager(tmp_path, store)

    async def check():
        full = store.list_sessions()
        lean = store.list_sessions_for_startup_inventory()
        full_decisions = {s.id: await value._automatic_recovery_eligibility(s) for s in full}
        lean_decisions = {s.id: await value._automatic_recovery_eligibility(s) for s in lean}
        assert full_decisions == lean_decisions
        assert all(lean_decisions[f"s-{i}"] == "durable_obligation" for i in range(1, 5))
        assert not lean_decisions["s-0"] and not lean_decisions["s-8"]
        assert [value._snapshot_from_persisted(s).model_dump() for s in lean] == [
            value._snapshot_from_persisted(s).model_dump() for s in full
        ]

    asyncio.run(check())


def test_start_requests_inventory_and_keeps_closed_ids_for_quiesce_matching(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    value = manager(tmp_path, store)
    value._resume_from_snapshot = AsyncMock(return_value=None)
    # A stale shutdown file must not resurrect a durably closed passive ID.
    snapshot = QuiesceSnapshot(reason="fixture", sessions=[
        SessionSnapshot(session_id="s-0", agent_name="fixture", status="prompting")
    ])
    with patch.object(store, "list_sessions_for_startup_inventory", wraps=store.list_sessions_for_startup_inventory) as read, patch(
        "pa.instance.agent_session.load_quiesce_snapshot", return_value=snapshot
    ):
        asyncio.run(value.start(resume=True))
    read.assert_called_once_with()
    recovered = {call.args[0].session_id for call in value._resume_from_snapshot.await_args_list}
    assert recovered == {"s-1", "s-2", "s-3", "s-4", "s-6"}
    # A paused prompting session is retained/deferred even with eager recovery
    # disabled for its queue. The idle session is deferred independently.
    assert value.startup_state()["deferred"] == 2
    assert value.startup_state()["blocked"] == 1
    assert store.get_session("s-0").metrics_json["diagnostic"] == "x" * 65536


def test_resume_refetches_complete_current_session_before_runtime_mutation(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    value = manager(tmp_path, store)
    inventory = next(s for s in store.list_sessions_for_startup_inventory() if s.id == "s-6")
    snapshot = value._snapshot_from_persisted(inventory)
    current = store.get_session(inventory.id)
    current.metrics_json = {"fresh": "must survive recovery"}
    current.config_json["fresh"] = True
    store.save_session(current)
    value._prepare_workspace = AsyncMock(side_effect=RuntimeError("fixture fence"))
    with pytest.raises(RuntimeError, match="fixture fence"):
        asyncio.run(value._resume_from_snapshot(snapshot, QuiesceSnapshot(reason="fixture")))
    actual = value._prepare_workspace.await_args.args[0]
    assert actual.metrics_json == current.metrics_json
    assert actual.config_json["fresh"] is True
    assert actual.config_json["future_extension"] == {"retain_on_resume": True}
    assert actual.config_json["execution_selection"]["proof"] == "x" * 65536
    assert store.get_session(current.id).metrics_json == current.metrics_json


@pytest.mark.parametrize("raw", [
    '{}', '[invalid', 'null', '[]', '3', 'false', '"legacy"',
    '{"configuration":{"model":"first"},"configuration":{"model":"last"}}',
    r'{"config\u0075ration":{"model":"first"},"configuration":{"model":"last"}}',
    '{"durable_runtime":{"queue_paused":true},"durable_runtime":{"queue_paused":false}}',
    '{"configuration":true}',
    '{"configuration":999999999999999999999999999999999}',
    '{"provisioning":0}',
    '{"durable_runtime":[["queue_paused",false]]}',
    '{"configuration":{"model":"first","model":"last"},"unused":"omit"}',
])
def test_startup_projection_preserves_legacy_json_decoding(tmp_path, raw):
    store = CardProjection(tmp_path / "fixture.db")
    store.save_session(AgentSession(id="legacy", agent_name="fixture"))
    # Legacy serialized input belongs only to this disposable test database.
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET config_json=? WHERE id='legacy'", (raw,))
    try:
        full = store.list_sessions(include_metrics=False)[0]
    except ValueError as exc:
        with pytest.raises(type(exc)):
            store.list_sessions_for_startup_inventory()
        return
    lean = store.list_sessions_for_startup_inventory()[0]
    assert lean.model_dump(exclude={"config_json"}) == full.model_dump(exclude={"config_json"})
    for key in ("configuration", "durable_runtime", "provisioning"):
        assert lean.config_json.get(key) == full.config_json.get(key)
        assert type(lean.config_json.get(key)) is type(full.config_json.get(key))


def test_inventory_keeps_archived_and_human_controlled_ids(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    sessions(store)
    original = store.get_session("s-6")
    original.archived_at = datetime.now(UTC)
    store.save_session(original)
    human = store.get_session("s-2")
    human.purpose = "automated_run"
    human.control_mode = "human"
    store.save_session(human)
    value = manager(tmp_path, store)
    async def check():
        full, lean = store.list_sessions(include_metrics=False), store.list_sessions_for_startup_inventory()
        assert [s.id for s in lean] == [s.id for s in full]
        assert {s.id: await value._automatic_recovery_eligibility(s) for s in full} == {
            s.id: await value._automatic_recovery_eligibility(s) for s in lean
        }
        assert not await value._automatic_recovery_eligibility(next(s for s in lean if s.id == original.id))
        assert not await value._automatic_recovery_eligibility(next(s for s in lean if s.id == human.id))
    asyncio.run(check())


def test_legacy_adapter_keeps_existing_startup_read_contract():
    from types import SimpleNamespace
    from unittest.mock import Mock
    read = Mock(return_value=["fixture"])
    value = object.__new__(AgentSessionManager)
    value.store = SimpleNamespace(list_sessions=read)
    assert value._startup_session_inventory() == ["fixture"]
    read.assert_called_once_with(include_metrics=False)


def test_dynamic_legacy_adapter_does_not_advertise_a_new_inventory_reader():
    from unittest.mock import MagicMock
    store = MagicMock()
    store.list_sessions.return_value = ["durable obligation"]
    value = object.__new__(AgentSessionManager)
    value.store = store
    assert value._startup_session_inventory() == ["durable obligation"]
    store.list_sessions.assert_called_once_with(include_metrics=False)
    assert "list_sessions_for_startup_inventory" not in store._mock_children


def test_declared_adapter_inventory_reader_is_used():
    from types import SimpleNamespace
    from unittest.mock import Mock
    inventory = Mock(return_value=["declared inventory"])
    old_read = Mock(side_effect=AssertionError("Should use explicitly declared inventory"))
    value = object.__new__(AgentSessionManager)
    value.store = SimpleNamespace(
        list_sessions_for_startup_inventory=inventory, list_sessions=old_read
    )
    assert value._startup_session_inventory() == ["declared inventory"]
    inventory.assert_called_once_with()
