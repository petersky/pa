from __future__ import annotations

import asyncio
import tempfile
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pa.domain.models import AgentSession, CardLane
from pa.core.async_runtime import AsyncRuntime
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord
from pa.instance.session_lifecycle import SessionLifecyclePolicy, _DispatchLinkIndex, _WatchLinkIndex
from pa.pr_supervisor.models import PRWatch, PRWatchStatus
from pa.pr_supervisor.store import PRSupervisorStore


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
    async def test_dispatch_index_preserves_or_order_and_equal_distinct_rows(self):
        records = [_dispatch(session_id="other", dispatch_id="shared"),
                   _dispatch(session_id="session-1", dispatch_id="other"),
                   _dispatch(session_id="session-1", dispatch_id="shared"),
                   _dispatch(session_id="session-1", dispatch_id="shared"),
                   _dispatch(session_id=None, dispatch_id=""),
                   _dispatch(session_id="", dispatch_id="unrelated")]
        index = _DispatchLinkIndex(records)
        for session_id in ("session-1", "other", "", "unrelated"):
            for dispatch_id in ("shared", "other", "", None):
                with self.subTest(session_id=session_id, dispatch_id=dispatch_id):
                    session = _session(id=session_id, dispatch_id=dispatch_id)
                    expected = [r for r in records if r.session_id == session.id
                                or (session.dispatch_id and r.dispatch_id == session.dispatch_id)]
                    self.assertEqual([id(r) for r in index.for_session(session)],
                                     [id(r) for r in expected])
        session = _session(dispatch_id="shared")
        session.dispatch_id = "other"
        self.assertEqual([id(r) for r in index.for_session(session)],
                         [id(r) for r in records[1:4]])

    async def test_sweep_reads_fresh_dispatch_index_once_on_reserved_worker(self):
        records = [_dispatch()]
        calls = []
        loop_thread = threading.get_ident()

        def read(**kwargs):
            self.assertNotEqual(threading.get_ident(), loop_thread)
            calls.append(kwargs)
            return list(records)

        manager = _Manager()
        manager.store.list_sessions = lambda **kwargs: [_session(), _session(id="other")]
        manager.workspace_manager.list = lambda: []
        policy = SessionLifecyclePolicy(manager, {"dispatch_store": SimpleNamespace(list=read)})
        decision = AsyncMock(return_value=("retained", "fixture"))
        try:
            with patch.object(policy, "_decision", decision):
                await policy.run_once()
                self.assertEqual([c.kwargs["dispatches"].for_session(c.args[0])
                                  for c in decision.call_args_list], [[records[0]], []])
                records.clear()
                decision.reset_mock()
                await policy.run_once()
                self.assertEqual([c.kwargs["dispatches"].for_session(c.args[0])
                                  for c in decision.call_args_list], [[], []])
            self.assertEqual(calls, [{"limit": 10000, "deep": False}] * 2)
        finally:
            await policy.close()

    async def test_dispatch_index_lookup_stays_after_early_durable_guards(self):
        cases = [(_session(status="closed"), None, ("skipped", "already_closed")),
                 (_session(), SimpleNamespace(prompting=True, _queue=[]), ("retained", "prompt_pending")),
                 (_session(), SimpleNamespace(prompting=False, _queue=[], _pending_permissions=[True]),
                  ("retained", "permission_pending")),
                 (_session(), SimpleNamespace(prompting=False, _queue=[], _pending_permissions=[],
                                              _pending_elicitations=[True]), ("retained", "elicitation_pending")),
                 (_session(), SimpleNamespace(prompting=False, _queue=[], _pending_permissions=[],
                                              _transcript_buffer=[True]), ("deferred", "transcript_delivery_pending"))]
        for durable, reason in [({"in_flight": True}, "durable_prompt_pending"),
                                ({"queued_prompts": ["fixture"]}, "durable_prompt_pending"),
                                ({"pending_permissions": ["fixture"]}, "durable_interaction_pending"),
                                ({"pending_elicitations": ["fixture"]}, "durable_interaction_pending"),
                                ({"pending_interaction": {"id": "fixture"}}, "requested_input_pending")]:
            cases.append((_session(config_json={"durable_runtime": durable}), None, ("retained", reason)))
        for session, runtime, expected in cases:
            with self.subTest(reason=expected[1]):
                policy = SessionLifecyclePolicy(_Manager(runtime=runtime), {})
                index = _DispatchLinkIndex([])
                try:
                    with patch.object(index, "for_session", side_effect=AssertionError("early dispatch work")):
                        self.assertEqual(await policy._decision(session, sessions=[], dispatches=index,
                            watches=[], leases=[], now=datetime.now(UTC)), expected)
                finally:
                    await policy.close()

    async def test_indexed_dispatches_preserve_obligations_and_terminal_order(self):
        cases = [({"state": "running"}, ("retained", "dispatch_active")),
                 ({"followup_turns": [{"state": "running"}]}, ("retained", "followup_pending")),
                 ({"acknowledged_at": None}, ("deferred", "completion_delivery_pending")),
                 ({"reconciliation_state": "not_requested"}, ("retained", "reconciliation_active")),
                 ({"state": "failed", "recoverable": True}, ("retained", "dispatch_recoverable")),
                 ({"state": "cancelled", "recoverable": True}, ("retained", "dispatch_recoverable"))]
        for values, expected in cases:
            with self.subTest(values=values):
                records = [_dispatch(session_id="unrelated", dispatch_id="d-unrelated", state="running"),
                           _dispatch(**values)]
                session = _session(purpose="automated_run", dispatch_id="dispatch-1")
                policy = SessionLifecyclePolicy(_Manager(), {})
                try:
                    for dispatches in (records, _DispatchLinkIndex(records)):
                        self.assertEqual(await policy._decision(session, sessions=[], dispatches=dispatches,
                            watches=[], leases=[], now=datetime.now(UTC)), expected)
                finally:
                    await policy.close()
        # The last linked terminal outcome is significant; do not group/sort by state.
        records = [_dispatch(state="cancelled", recoverable=False),
                   _dispatch(state="failed", recoverable=False)]
        policy = SessionLifecyclePolicy(_Manager(), {})
        try:
            for dispatches in (records, _DispatchLinkIndex(records)):
                self.assertEqual(await policy._decision(_session(purpose="automated_run"), sessions=[],
                    dispatches=dispatches, watches=[], leases=[], now=datetime.now(UTC)), ("close", "dispatch_failed"))
        finally:
            await policy.close()

    async def test_watch_index_preserves_or_order_and_distinct_equal_rows(self):
        def link(session_id, card_id):
            return SimpleNamespace(originating_session_id=session_id, card_id=card_id,
                                   status=PRWatchStatus.MERGED)

        watches = [link("other", "shared"), link("session-1", "other"),
                   link("session-1", "shared"), link("session-1", "shared"),
                   link(None, None), link("another", ""), link("", "shared")]
        index = _WatchLinkIndex(watches)
        for session_id in ("session-1", "another", "unrelated"):
            for card_id in ("shared", "other", "", None):
                with self.subTest(session_id=session_id, card_id=card_id):
                    session = _session(id=session_id, card_id=card_id)
                    expected = [w for w in watches if w.originating_session_id == session.id
                                or (session.card_id and w.card_id == session.card_id)]
                    self.assertEqual([id(w) for w in index.for_session(session)],
                                     [id(w) for w in expected])
        # Lookups use the session's current keys; there is no session-result cache.
        session = _session(card_id="shared")
        session.card_id = "other"
        self.assertEqual(index.for_session(session), [watches[1], watches[2], watches[3]])

    async def test_sweep_builds_fresh_watch_index_once_on_reserved_worker(self):
        watches = [SimpleNamespace(originating_session_id="session-1", card_id=None,
                                   status=PRWatchStatus.ACTIVE)]
        store = SimpleNamespace(lifecycle_watch_links=lambda: list(watches))
        manager = _Manager()
        manager.store.list_sessions = lambda **kwargs: [_session(), _session(id="unrelated")]
        manager.workspace_manager.list = lambda: []
        policy = SessionLifecyclePolicy(manager, {"pr_supervisor": SimpleNamespace(store=store)})
        loop_thread = threading.get_ident()
        reads = []
        original = _WatchLinkIndex.read

        def read(store):
            self.assertNotEqual(threading.get_ident(), loop_thread)
            reads.append(True)
            return original(store)

        decision = AsyncMock(return_value=("retained", "fixture"))
        try:
            with patch.object(_WatchLinkIndex, "read", read), patch.object(policy, "_decision", decision):
                await policy.run_once()
                self.assertEqual([call.kwargs["watches"].for_session(call.args[0])
                                  for call in decision.call_args_list],
                                 [[watches[0]], []])
                decision.reset_mock()
                watches.clear()
                await policy.run_once()
                self.assertEqual([call.kwargs["watches"].for_session(call.args[0])
                                  for call in decision.call_args_list], [[], []])
            self.assertEqual(len(reads), 2)
        finally:
            await policy.close()

    async def test_pending_prompt_does_not_materialize_indexed_watch_links(self):
        runtime = SimpleNamespace(prompting=True, _queue=[])
        policy = SessionLifecyclePolicy(_Manager(runtime=runtime), {})
        index = _WatchLinkIndex([])
        try:
            with patch.object(index, "for_session", side_effect=AssertionError("early watch work")):
                self.assertEqual(await policy._decision(
                    _session(), sessions=[], dispatches=[], watches=index, leases=[],
                    now=datetime.now(UTC)), ("retained", "prompt_pending"))
        finally:
            await policy.close()

    async def test_watch_metadata_preserves_every_status_retirement_and_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PRSupervisorStore(Path(tmp) / "watches.db")
            for index, status in enumerate(PRWatchStatus):
                for retired in (False, True):
                    with self.subTest(status=status, retired=retired):
                        watch = store.upsert_watch(PRWatch(
                            id="watch", repository="owner/repo", pr_number=1,
                            pr_url="https://example.invalid/1", status=status,
                            originating_session_id="session-1", card_id="shared",
                            realm_id="other", retired_at=datetime.now(UTC) if retired else None,
                            state={"proof": "retained" * 8192},
                        ))
                        links = store.lifecycle_watch_links()
                        self.assertEqual(len(links), 1)
                        self.assertEqual(
                            links[0], (watch.originating_session_id, watch.card_id, watch.status)
                        )
                        for session in (
                            _session(lifecycle_owner="dispatch"),
                            _session(id="another-session", card_id="shared", lifecycle_owner="dispatch"),
                            _session(id="unrelated", card_id="other", lifecycle_owner="dispatch"),
                        ):
                            old = await self.decide(session, watches=[watch])
                            new = await self.decide(session, watches=links)
                            self.assertEqual(new, old)
                        # Retired active/blocked records still guard closure;
                        # this is intentionally not the default live-watch view.
                        linked = await self.decide(_session(lifecycle_owner="dispatch"), watches=links)
                        self.assertEqual(linked, (
                            ("retained", "actionable_pr_watch")
                            if status in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}
                            else ("close", "pr_watch_terminal")
                        ))
                        self.assertEqual(store.get_watch("watch").state, watch.state)

    async def test_sweep_reads_only_watch_links_without_proof_hydration(self):
        import sqlite3
        from contextlib import contextmanager

        with tempfile.TemporaryDirectory() as tmp:
            store = PRSupervisorStore(Path(tmp) / "watches.db")
            store.upsert_watch(PRWatch(
                id="w", repository="owner/repo", pr_number=1,
                pr_url="https://example.invalid/1", originating_session_id="session-1",
                state={"proof": "retained" * 8192},
            ))
            manager = _Manager()
            manager.store.list_sessions = lambda **kwargs: [_session()]
            manager.workspace_manager.list = lambda: []
            policy = SessionLifecyclePolicy(manager, {"pr_supervisor": SimpleNamespace(store=store)})
            original = store._conn

            @contextmanager
            def metadata_only():
                with original() as conn:
                    def authorize(action, table, column, *_rest):
                        if action == sqlite3.SQLITE_READ and table == "pr_watches":
                            return (sqlite3.SQLITE_OK if column in {
                                "originating_session_id", "card_id", "status", "updated_at"
                            } else sqlite3.SQLITE_DENY)
                        return sqlite3.SQLITE_OK
                    conn.set_authorizer(authorize)
                    yield conn

            try:
                with patch.object(store, "_conn", metadata_only), patch.object(
                    store, "_row_to_watch", side_effect=AssertionError("proof hydration")
                ), patch.object(store, "list_watches", side_effect=AssertionError("full watch read")):
                    result = await policy.run_once()
                self.assertEqual(result["retained:actionable_pr_watch"], 1)
                # A later sweep must read fresh status, not cache its guard.
                watch = store.get_watch("w")
                watch.status = PRWatchStatus.MERGED
                store.upsert_watch(watch)
                self.assertEqual(store.lifecycle_watch_links()[0].status, PRWatchStatus.MERGED)
            finally:
                await policy.close()

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

    async def test_close_has_reserved_capacity_when_unrelated_pool_is_saturated(self):
        release = threading.Event()
        started = threading.Event()
        shared = AsyncRuntime(max_workers=1, max_queue=0, default_timeout=None)

        def unrelated_work():
            started.set()
            release.wait(2)

        unrelated = asyncio.create_task(
            shared.run_blocking("sync.projection", unrelated_work)
        )
        await asyncio.to_thread(started.wait, 1)
        expired = _session(updated_at=datetime.now(UTC) - timedelta(hours=25))

        class _ClosingStore(_Store):
            def list_sessions(self, **_kwargs):
                return [expired]

            def close_session(self, _session_id, **kwargs):
                detail = kwargs["diagnostics"]
                detail["lock_wait_ms"] = 0.0
                detail["terminal_result"] = "closed"
                return expired.model_copy(update={"status": "closed"}), "idle"

        manager = _Manager()
        manager.store = _ClosingStore()
        manager.workspace_manager.list = lambda: []
        policy = SessionLifecyclePolicy(manager, {})
        try:
            before = time.monotonic()
            metrics = await asyncio.wait_for(policy.run_once(), timeout=2.0)
            self.assertLess(time.monotonic() - before, 1.5)
            self.assertEqual(metrics["auto_closed"], 1)
            attempt = policy.snapshot()["attempts"][0]
            self.assertEqual(attempt["terminal_result"], "closed")
            self.assertTrue(attempt["worker_owner"].startswith("pa-session-lifecycle"))
            self.assertEqual(shared.snapshot()["executor"]["active"], 1)
        finally:
            release.set()
            await unrelated
            await policy.close()
            await shared.close()

    async def test_concurrent_sweeps_are_coalesced(self):
        manager = _Manager()
        manager.store.list_sessions = lambda **_kwargs: []
        manager.workspace_manager.list = lambda: []
        entered = asyncio.Event()
        release = asyncio.Event()
        policy = SessionLifecyclePolicy(manager, {})
        original = policy._offload

        async def paused_offload(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        policy._offload = paused_offload
        first = asyncio.create_task(policy.run_once())
        await entered.wait()
        second = await policy.run_once()
        self.assertEqual(second["coalesced:sweep_active"], 1)
        release.set()
        await first
        await policy.close()

    async def test_bounded_lock_failure_cannot_close_later(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            session = _session()
            store.save_session(session)
            detail = {}
            store._mutation_lock.acquire()
            try:
                with self.assertRaises(TimeoutError):
                    await asyncio.to_thread(
                        store.close_session,
                        session.id,
                        reason="auto:test",
                        lock_timeout_seconds=0.02,
                        busy_timeout_ms=20,
                        diagnostics=detail,
                    )
                await asyncio.sleep(0.03)
                self.assertEqual(store.get_session(session.id).status, "idle")
                self.assertEqual(store.list_transcript_events(session.id), [])
                self.assertEqual(detail["terminal_result"], "lock_timeout")
                self.assertGreaterEqual(detail["lock_wait_ms"], 15)
            finally:
                store._mutation_lock.release()
