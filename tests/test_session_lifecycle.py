from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pa.domain.models import AgentSession, CardLane
from pa.execution.dispatch import DispatchRecord
from pa.instance.session_lifecycle import SessionLifecyclePolicy
from pa.pr_supervisor.models import PRWatchStatus


class _Store:
    def __init__(self, card=None):
        self.card = card

    def get_card(self, *_args, **_kwargs):
        return self.card


class _Workspace:
    def _status(self, _path):
        return False, False


class _Manager:
    def __init__(self, *, runtime=None, card=None):
        self.runtime = runtime
        self.store = _Store(card)
        self.workspace_manager = _Workspace()
        self.settings = SimpleNamespace(
            agent_session_idle_retention_hours=24,
            agent_session_sweep_seconds=30,
        )
        self._runtimes = {}
        self.reconcile_closed_sessions = AsyncMock()

    def get(self, _session_id):
        return self.runtime

    async def _offload(self, _operation, call, *args, **kwargs):
        kwargs.pop("timeout", None)
        return call(*args, **kwargs)


def _session(**values):
    defaults = {
        "id": "session-1",
        "agent_name": "codex",
        "status": "idle",
        "updated_at": datetime.now(UTC),
    }
    defaults.update(values)
    return AgentSession(**defaults)


def _dispatch(**values):
    defaults = {
        "dispatch_id": "dispatch-1",
        "mutation_id": "mutation-1",
        "authority_instance_id": "authority",
        "authority_url": "https://authority.test",
        "target_instance_id": "target",
        "session_id": "session-1",
        "state": "completed",
        "acknowledged_at": datetime.now(UTC),
        "completion_delivery_class": "acknowledged",
        "reconciliation_state": "not_required",
    }
    defaults.update(values)
    return DispatchRecord(**defaults)


class SessionLifecycleDecisionTests(unittest.IsolatedAsyncioTestCase):
    async def decide(
        self,
        session=None,
        *,
        runtime=None,
        card=None,
        sessions=None,
        dispatches=None,
        watches=None,
        leases=None,
    ):
        session = session or _session()
        policy = SessionLifecyclePolicy(_Manager(runtime=runtime, card=card), {})
        return await policy._decision(
            session,
            sessions=sessions or [session],
            dispatches=dispatches or [],
            watches=watches or [],
            leases=leases or [],
            now=datetime.now(UTC),
        )

    async def test_acknowledged_terminal_dispatch_closes(self):
        self.assertEqual(
            await self.decide(dispatches=[_dispatch()]),
            ("close", "dispatch_completed"),
        )

    async def test_completion_delivery_and_reconciliation_are_guards(self):
        pending = _dispatch(acknowledged_at=None, completion_delivery_class="pending")
        self.assertEqual(
            await self.decide(dispatches=[pending]),
            ("deferred", "completion_delivery_pending"),
        )
        reconciling = _dispatch(reconciliation_state="prompted")
        self.assertEqual(
            await self.decide(dispatches=[reconciling]),
            ("retained", "reconciliation_active"),
        )

    async def test_queued_inflight_and_permission_work_are_guards(self):
        queue = SimpleNamespace(empty=lambda: True)
        runtime = SimpleNamespace(
            prompting=True,
            _queue=[],
            _pending_permissions={},
            _transcript_buffer=[],
            _transcript_queue=queue,
        )
        self.assertEqual(
            await self.decide(runtime=runtime), ("retained", "prompt_pending")
        )
        runtime.prompting = False
        runtime._pending_permissions = {"request": object()}
        self.assertEqual(
            await self.decide(runtime=runtime), ("retained", "permission_pending")
        )
        session = _session(
            config_json={"durable_runtime": {"queued_prompts": [{"id": "p"}]}}
        )
        self.assertEqual(
            await self.decide(session), ("retained", "durable_prompt_pending")
        )

    async def test_actionable_watch_retains_and_terminal_watch_closes(self):
        active = SimpleNamespace(
            originating_session_id="session-1",
            card_id=None,
            status=PRWatchStatus.ACTIVE,
        )
        self.assertEqual(
            await self.decide(watches=[active]),
            ("retained", "actionable_pr_watch"),
        )
        active.status = PRWatchStatus.MERGED
        self.assertEqual(
            await self.decide(watches=[active]), ("close", "pr_watch_terminal")
        )

    async def test_dirty_or_unknown_workspace_is_retained(self):
        lease = SimpleNamespace(
            session_id="session-1", state="ready", worktree_path="/workspace"
        )
        manager = _Manager()
        manager.workspace_manager._status = lambda _path: (True, False)
        policy = SessionLifecyclePolicy(manager, {})
        self.assertEqual(
            await policy._decision(
                _session(),
                sessions=[_session()],
                dispatches=[],
                watches=[],
                leases=[lease],
                now=datetime.now(UTC),
            ),
            ("retained", "workspace_uncommitted"),
        )
        manager.workspace_manager._status = lambda _path: (_ for _ in ()).throw(
            OSError("unavailable")
        )
        self.assertEqual(
            (await policy._decision(
                _session(), sessions=[_session()], dispatches=[], watches=[],
                leases=[lease], now=datetime.now(UTC)
            ))[1],
            "workspace_status_unknown",
        )

    async def test_idle_retention_and_followup_window(self):
        expired = _session(updated_at=datetime.now(UTC) - timedelta(hours=25))
        self.assertEqual(
            await self.decide(expired), ("close", "idle_retention_expired")
        )
        self.assertEqual(
            await self.decide(_session()), ("retained", "followup_window")
        )

    async def test_duplicate_replacement_and_single_purpose_close(self):
        old = _session(
            card_id="card-1", updated_at=datetime.now(UTC) - timedelta(minutes=1)
        )
        new = _session(
            id="session-2", card_id="card-1", updated_at=datetime.now(UTC)
        )
        self.assertEqual(
            await self.decide(old, sessions=[old, new]),
            ("close", "superseded_duplicate"),
        )
        self.assertEqual(
            await self.decide(_session(label="advisor")),
            ("close", "single_purpose_finished"),
        )

    async def test_deleted_and_completed_card_close(self):
        linked = _session(card_id="card-1")
        self.assertEqual(
            await self.decide(linked), ("close", "card_deleted")
        )
        card = SimpleNamespace(lane=CardLane.DONE)
        self.assertEqual(
            await self.decide(linked, card=card), ("close", "card_completed")
        )

    async def test_recoverable_terminal_dispatch_is_retained(self):
        failed = _dispatch(state="failed", recoverable=True)
        self.assertEqual(
            await self.decide(dispatches=[failed]),
            ("retained", "dispatch_recoverable"),
        )
        failed.recoverable = False
        self.assertEqual(
            await self.decide(dispatches=[failed]), ("close", "dispatch_terminal")
        )
