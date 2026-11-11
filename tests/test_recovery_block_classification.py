"""Deterministic recovery failures block on the first attempt and stay quiet."""
import logging

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager

LEASE = (
    "Card already has a live worktree lease; resume its owner or explicitly "
    "allow concurrent dispatch"
)
DISPATCH = "governed session does not match its durable assigned dispatch binding"
EMPTY = (
    "Empty-index checkout retained; use targeted materialization "
    "preservation/repair before admission"
)


def make(tmp_path, status="recoverable_interrupted"):
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="run", agent_name="codex", status=status, purpose="automated_run")
    )
    return store, session, AgentSessionManager(Settings(data_dir=tmp_path), store)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,code",
    [
        (LEASE, "workspace_lease_conflict"),
        (DISPATCH, "dispatch_binding_mismatch"),
        (EMPTY, "empty_index_checkout"),
    ],
)
async def test_deterministic_failure_blocks_on_first_attempt(tmp_path, message, code):
    store, session, manager = make(tmp_path)
    state = await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(session), RuntimeError(message)
    )
    persisted = store.get_session(session.id)
    assert state == "recovery_blocked"
    assert persisted.status == "recovery_blocked"
    assert persisted.recovery_json["code"] == code
    assert persisted.recovery_json["attempts"] == 1
    assert persisted.recovery_json["blocked"] is True
    assert persisted.recovery_json["exhausted"] is False
    assert persisted.recovery_json["next_retry_at"] is None
    assert persisted.recovery_json["remedy"]


@pytest.mark.asyncio
async def test_authority_mismatch_is_not_misclassified_as_missing_credentials(tmp_path):
    store, session, manager = make(tmp_path)
    state = await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(session),
        RuntimeError("workspace authority mismatch"),
    )
    persisted = store.get_session(session.id)
    assert persisted.recovery_json["code"] != "auth_missing"
    assert state == "recoverable_interrupted"
    assert persisted.recovery_json["next_retry_at"]


@pytest.mark.asyncio
async def test_repeat_of_the_same_block_does_not_grow_attempts(tmp_path):
    store, session, manager = make(tmp_path)
    error = RuntimeError(LEASE)
    await manager._mark_recovery_interrupted(manager._snapshot_from_persisted(session), error)
    first = store.get_session(session.id)
    for _ in range(5):
        # A restart pass re-admits the session (status moves off recovery_blocked)
        # and fails identically.
        again = store.get_session(session.id)
        again.status = "provisioning_failed"
        store.save_session(again)
        state = await manager._mark_recovery_interrupted(
            manager._snapshot_from_persisted(again), error
        )
        assert state == "recovery_blocked"
    last = store.get_session(session.id)
    assert last.status == "recovery_blocked"
    assert last.recovery_json["attempts"] == 1
    assert last.recovery_json["last_attempt_at"] == first.recovery_json["last_attempt_at"]


@pytest.mark.asyncio
async def test_repeat_block_that_changes_reason_is_a_new_attempt(tmp_path):
    store, session, manager = make(tmp_path)
    await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(session), RuntimeError(LEASE)
    )
    current = store.get_session(session.id)
    await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(current), RuntimeError(EMPTY)
    )
    assert store.get_session(session.id).recovery_json["attempts"] == 2


@pytest.mark.asyncio
async def test_unchanged_repeat_does_not_rewrite_the_session(tmp_path):
    store, session, manager = make(tmp_path)
    error = RuntimeError(LEASE)
    await manager._mark_recovery_interrupted(manager._snapshot_from_persisted(session), error)
    blocked = store.get_session(session.id)
    saves = []
    original = store.save_session
    store.save_session = lambda s, **kw: saves.append(s.id) or original(s, **kw)
    state = await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(blocked), error
    )
    assert state == "recovery_blocked"
    assert saves == []


def test_unchanged_block_is_logged_once_per_process(tmp_path, caplog):
    _, _, manager = make(tmp_path)
    with caplog.at_level(logging.DEBUG, logger="pa.instance.agent_session"):
        for _ in range(4):
            manager._log_recovery_block("run", "fix it")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    manager._log_recovery_block("run", "a different reason")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
