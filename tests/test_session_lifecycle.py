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
            await self.decide(
                _session(lifecycle_owner="dispatch"), dispatches=[_dispatch()]
            ),
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
            await self.decide(
                _session(lifecycle_owner="dispatch"), watches=[active]
            ),
            ("close", "pr_watch_terminal"),
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
        self.assertEqual(
            await self.decide(
                _session(
                    label="card-enrichment:card-1",
                    status="disconnected",
                )
            ),
            ("close", "single_purpose_terminal"),
        )

    async def test_deleted_card_closes_but_completed_card_retains_interactive_session(self):
        linked = _session(card_id="card-1")
        self.assertEqual(
            await self.decide(linked), ("close", "card_deleted")
        )
        card = SimpleNamespace(lane=CardLane.DONE)
        self.assertEqual(
            await self.decide(linked, card=card),
            ("retire", "associated_card_terminal"),
        )

    async def test_completed_card_uses_idle_retention_before_closing(self):
        card = SimpleNamespace(lane=CardLane.DONE)
        expired = _session(
            card_id="card-1",
            lifecycle_owner="dispatch",
            updated_at=datetime.now(UTC) - timedelta(hours=25),
        )
        self.assertEqual(
            await self.decide(expired, card=card),
            ("close", "idle_retention_expired"),
        )

    async def test_completed_card_preserves_single_purpose_close_provenance(self):
        card = SimpleNamespace(lane=CardLane.DONE)
        self.assertEqual(
            await self.decide(
                _session(
                    card_id="card-1",
                    label="card-enrichment:card-1",
                    lifecycle_owner="dispatch",
                ),
                card=card,
            ),
            ("close", "single_purpose_finished"),
        )

    async def test_recoverable_terminal_dispatch_is_retained(self):
        failed = _dispatch(state="failed", recoverable=True)
        self.assertEqual(
            await self.decide(dispatches=[failed]),
            ("retained", "dispatch_recoverable"),
        )
        failed.recoverable = False
        self.assertEqual(
            await self.decide(
                _session(lifecycle_owner="dispatch"), dispatches=[failed]
            ),
            ("close", "dispatch_terminal"),
        )

    async def test_run_once_excludes_closed_sessions_from_store_query(self):
        recorded = {}

        class _RecordingStore(_Store):
            def list_sessions(self, **kwargs):
                recorded["kwargs"] = kwargs
                return []

            def close_session(self, *args, **kwargs):
                return None, None

        manager = _Manager()
        manager.store = _RecordingStore()
        manager.workspace_manager.list = lambda: []
        await SessionLifecyclePolicy(manager, {}).run_once()
        self.assertEqual(recorded["kwargs"], {"exclude_statuses": ("closed",)})

    async def test_run_once_updates_runtime_to_remaining_primary_card(self):
        completed = SimpleNamespace(lane=CardLane.DONE)
        original = _session(card_id="card-done", item_id="card-done")
        replacement = original.model_copy(
            update={
                "card_id": "card-active",
                "item_id": "card-active",
                "project_id": "project-active",
            }
        )
        queue = SimpleNamespace(empty=lambda: True)
        runtime = SimpleNamespace(
            session=original.model_copy(deep=True),
            prompting=False,
            _queue=[],
            _pending_permissions={},
            _pending_elicitations={},
            _transcript_buffer=[],
            _transcript_queue=queue,
            _closed=False,
        )

        class _RetiringStore(_Store):
            def list_sessions(self, **_kwargs):
                return [original]

            def unlink_session_card(self, session_id, card_id, **kwargs):
                self.retirement = (session_id, card_id, kwargs)
                return replacement

        manager = _Manager(runtime=runtime, card=completed)
        manager.store = _RetiringStore(completed)
        manager.workspace_manager.list = lambda: []

        metrics = await SessionLifecyclePolicy(manager, {}).run_once()

        self.assertEqual(runtime.session.card_id, "card-active")
        self.assertEqual(runtime.session.item_id, "card-active")
        self.assertEqual(runtime.session.project_id, "project-active")
        self.assertEqual(metrics["association_retired"], 1)
        self.assertEqual(
            manager.store.retirement[:2], ("session-1", "card-done")
        )
