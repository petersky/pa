import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt, QuiesceSnapshot, SessionSnapshot


class _TranscriptStore:
    def __init__(self, events: list[TranscriptEvent]) -> None:
        self.events = events

    def list_transcript_events(
        self, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> list[TranscriptEvent]:
        return [event for event in self.events if event.seq > after_seq][:limit]

    def list_transcript_events_before(
        self, session_id: str, *, before_seq: int | None = None, limit: int = 500
    ) -> list[TranscriptEvent]:
        eligible = [
            event
            for event in self.events
            if before_seq is None or event.seq < before_seq
        ]
        return eligible[-limit:]


class AgentSessionLiveEventTests(unittest.TestCase):
    def test_transcript_flush_falls_back_if_writer_cannot_be_scheduled(self) -> None:
        event = TranscriptEvent(
            session_id="session-shutdown",
            seq=1,
            event_type="turn_completed",
            payload={},
        )
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.async_runtime = MagicMock()
        runtime.store = MagicMock()
        runtime.session = AgentSession(
            id="session-shutdown",
            agent_name="codex",
        )
        runtime._transcript_buffer = [event]
        runtime._transcript_queue = asyncio.Queue(maxsize=128)
        runtime._transcript_writer_task = None

        with patch(
            "pa.instance.agent_session.asyncio.create_task",
            side_effect=RuntimeError("cannot schedule new futures after shutdown"),
        ):
            runtime._flush_transcript()

        runtime.store.append_transcript_events.assert_called_once_with([event])
        self.assertEqual(runtime._transcript_buffer, [])
        self.assertTrue(runtime._transcript_queue.empty())
        self.assertEqual(runtime._transcript_queue._unfinished_tasks, 0)

    def test_stale_default_session_uses_configured_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_provider="codex"), store
            )
            existing = AgentSession(
                id="default-session",
                agent_name="cursor",
                status="disconnected",
                label="default",
                external_session_id=None,
                principal_id="persisted-user",
            )
            resolved = SimpleNamespace(
                provider_id="codex",
                spec=MagicMock(id="codex"),
                source="instance",
            )

            async def run():
                with (
                    patch(
                        "pa.instance.agent_session.resolve_agent_provider",
                        return_value=resolved,
                    ) as resolve_provider,
                    patch.object(AgentSessionRuntime, "start", new=AsyncMock()),
                ):
                    runtime = await manager.create_session(
                        label="default", existing=existing
                    )
                return runtime, resolve_provider

            runtime, resolve_provider = asyncio.run(run())

            self.assertEqual(runtime.session.id, "default-session")
            self.assertEqual(runtime.session.agent_name, "codex")
            self.assertEqual(
                resolve_provider.call_args.args[1].principal_id,
                "persisted-user",
            )
            store.save_session.assert_called_with(existing)

    def test_resumable_default_session_keeps_its_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_provider="codex"), store
            )
            existing = AgentSession(
                id="default-session",
                agent_name="cursor",
                status="disconnected",
                label="default",
                external_session_id="cursor-session",
            )
            cursor_spec = MagicMock(id="cursor")
            provider = MagicMock()
            provider.resolve_spawn.return_value = cursor_spec

            async def run():
                with (
                    patch(
                        "pa.acp.providers.registry.get_provider",
                        return_value=provider,
                    ),
                    patch(
                        "pa.instance.agent_session.resolve_agent_provider"
                    ) as resolve_provider,
                    patch.object(
                        AgentSessionRuntime, "start", new=AsyncMock()
                    ) as start,
                ):
                    runtime = await manager.create_session(
                        label="default",
                        existing=existing,
                        resume_external_id="cursor-session",
                    )
                return runtime, resolve_provider, start

            runtime, resolve_provider, start = asyncio.run(run())

            self.assertEqual(runtime.session.agent_name, "cursor")
            resolve_provider.assert_not_called()
            start.assert_awaited_once_with(
                resume_external_id="cursor-session",
                provider_spec=cursor_spec,
            )

    def test_non_resumable_default_snapshot_uses_configured_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            store.get_session.return_value = AgentSession(
                id="default-session",
                agent_name="cursor",
                status="disconnected",
                label="default",
            )
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_provider="codex"), store
            )
            resolved = SimpleNamespace(
                provider_id="codex",
                spec=MagicMock(id="codex"),
                source="instance",
            )
            snapshot = SessionSnapshot(
                session_id="default-session",
                agent_name="cursor",
                status="disconnected",
                label="default",
            )

            async def run():
                with (
                    patch(
                        "pa.instance.agent_session.resolve_agent_provider",
                        return_value=resolved,
                    ),
                    patch.object(
                        AgentSessionRuntime, "start", new=AsyncMock()
                    ) as start,
                ):
                    runtime = await manager._resume_from_snapshot(
                        snapshot, QuiesceSnapshot()
                    )
                return runtime, start

            runtime, start = asyncio.run(run())

            self.assertEqual(runtime.session.agent_name, "codex")
            store.save_session.assert_called_with(runtime.session)
            start.assert_awaited_once_with(
                resume_external_id=None,
                queued_prompts=[],
                queue_paused=False,
                provider_spec=resolved.spec,
            )

    def test_quiesce_snapshot_does_not_resurrect_closed_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            store.get_session.return_value = AgentSession(
                id="closed-session",
                agent_name="codex",
                status="closed",
                label="card:card-1",
            )
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager._prepare_workspace = AsyncMock(return_value={})
            snapshot = SessionSnapshot(
                session_id="closed-session",
                agent_name="codex",
                status="prompting",
                label="card:card-1",
            )

            async def run():
                with patch.object(
                    AgentSessionRuntime, "start", new=AsyncMock()
                ) as start:
                    runtime = await manager._resume_from_snapshot(
                        snapshot, QuiesceSnapshot()
                    )
                return runtime, start

            runtime, start = asyncio.run(run())

            self.assertIsNone(runtime)
            manager._prepare_workspace.assert_not_awaited()
            start.assert_not_awaited()

    def test_interrupted_snapshot_is_requeued_with_recovery_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            session = AgentSession(
                id="session-recovery",
                agent_name="codex",
                label="card:card-1",
                cwd=str(Path(tmp) / "workspace"),
            )
            store.get_session.return_value = session
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager._prepare_workspace = AsyncMock(return_value={})
            runtime = AgentSessionRuntime(manager, session)
            runtime._queue = []
            runtime._in_flight = QueuedPrompt(
                id="prompt-interrupted",
                message="Continue this work.",
                source="in_flight",
            )
            snapshot = runtime.to_session_snapshot()

            self.assertEqual(snapshot.in_flight.id, "prompt-interrupted")
            self.assertEqual(snapshot.queued_prompts, [])

            async def run():
                with patch.object(
                    AgentSessionRuntime, "start", new=AsyncMock()
                ) as start:
                    await manager._resume_from_snapshot(snapshot, QuiesceSnapshot())
                return start

            start = asyncio.run(run())
            queued = start.await_args.kwargs["queued_prompts"]
            self.assertEqual(queued[0].source, "recovery")
            self.assertIn("PA recovered this queued turn", queued[0].message)
            self.assertIn("Continue this work.", queued[0].message)

            repeated = SessionSnapshot(
                session_id=session.id,
                agent_name="codex",
                label=session.label,
                cwd=session.cwd,
                in_flight=queued[0],
            )

            async def run_again():
                with patch.object(
                    AgentSessionRuntime, "start", new=AsyncMock()
                ) as second_start:
                    await manager._resume_from_snapshot(repeated, QuiesceSnapshot())
                return second_start

            second_start = asyncio.run(run_again())
            recovered_again = second_start.await_args.kwargs["queued_prompts"][0]
            self.assertEqual(recovered_again.source, "recovery")
            self.assertEqual(
                recovered_again.message.count("PA recovered this queued turn"), 1
            )

    def test_concurrent_disconnect_only_exits_transport_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            connection = AgentConnection(Settings(data_dir=Path(tmp)), MagicMock())
            context = MagicMock()
            context.__aexit__ = AsyncMock()
            connection._ctx = context

            async def run() -> None:
                await asyncio.gather(
                    connection.disconnect(),
                    connection.disconnect(),
                )

            asyncio.run(run())

            context.__aexit__.assert_awaited_once_with(None, None, None)

    def test_mark_transport_dead_uses_disconnect_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            connection = AgentConnection(Settings(data_dir=Path(tmp)), MagicMock())
            context = MagicMock()
            context.__aexit__ = AsyncMock()
            connection._ctx = context

            async def run() -> None:
                await connection._disconnect_lock.acquire()
                cleanup = asyncio.create_task(connection._mark_transport_dead())
                await asyncio.sleep(0)
                self.assertFalse(cleanup.done())
                connection._disconnect_lock.release()
                await cleanup

            asyncio.run(run())

            context.__aexit__.assert_awaited_once_with(None, None, None)

    def test_mark_transport_dead_updates_status_before_cleanup_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            connection = AgentConnection(Settings(data_dir=Path(tmp)), store)
            connection.session = AgentSession(agent_name="codex", status="prompting")
            cleanup_started = asyncio.Event()
            allow_cleanup = asyncio.Event()

            async def block_cleanup(*_args) -> None:
                cleanup_started.set()
                await allow_cleanup.wait()

            context = MagicMock()
            context.__aexit__ = AsyncMock(side_effect=block_cleanup)
            connection._ctx = context

            async def run() -> None:
                cleanup = asyncio.create_task(connection._mark_transport_dead())
                await cleanup_started.wait()
                self.assertEqual(connection.session.status, "disconnected")
                store.save_session.assert_called_once_with(connection.session)
                allow_cleanup.set()
                await cleanup

            asyncio.run(run())

    def test_snapshot_restores_bounded_newest_transcript_window(self) -> None:
        events = [
            TranscriptEvent(
                session_id="session-long",
                seq=seq,
                event_type="agent_message_chunk",
                payload={"text": str(seq)},
            )
            for seq in range(1, 6002)
        ]
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.store = _TranscriptStore(events)
        runtime.session = AgentSession(id="session-long", agent_name="codex")
        runtime.connection = None
        runtime._transcript_buffer = []
        runtime._queue_paused = False
        runtime._queue = []
        runtime._in_flight = None
        runtime._turn_started_at = None
        runtime._permission_requests = {}
        runtime._pending_permissions = {}

        snapshot = runtime.snapshot()

        restored = snapshot["transcript"]
        self.assertEqual(len(restored), 1000)
        self.assertEqual(restored[0]["seq"], 5002)
        self.assertEqual(restored[-1]["seq"], 6001)
        self.assertTrue(snapshot["transcript_page"]["has_older"])
        self.assertEqual(snapshot["transcript_page"]["next_before_seq"], 5002)

    def test_prompting_tracks_in_flight_turn_not_connection_or_lock_cleanup(
        self,
    ) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime._in_flight = None
        runtime.connection = MagicMock(prompting=True)
        runtime._prompt_lock = MagicMock()
        runtime._prompt_lock.locked.return_value = True

        self.assertFalse(runtime.prompting)

        runtime._in_flight = MagicMock()
        runtime.connection.prompting = False
        runtime._prompt_lock.locked.return_value = False

        self.assertTrue(runtime.prompting)

    def test_managed_session_rejects_prompt_cwd_override(self) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.session = AgentSession(
            id="session-managed",
            agent_name="codex",
            cwd="/workspace/leased",
            config_json={"execution_context": {"version": 1}},
        )

        self.assertEqual(runtime._validated_cwd(None), "/workspace/leased")
        with self.assertRaisesRegex(RuntimeError, "cannot override"):
            runtime._validated_cwd("/tmp/escape")

    def test_managed_session_environment_cannot_be_overridden_per_turn(self) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.agent_env = {
            "PA_EXECUTION_CONTEXT": '{"version":1}',
            "PA_WORKSPACE_ROOT": "/workspace/leased",
        }

        merged = runtime._merged_agent_env(
            {"TOKEN": "user-secret", "PA_WORKSPACE_ROOT": "/tmp/escape"}
        )

        self.assertEqual(merged["TOKEN"], "user-secret")
        self.assertEqual(merged["PA_WORKSPACE_ROOT"], "/workspace/leased")

    def test_full_queue_keeps_newest_event_and_subscriber(self):
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.session = Mock(id="session-1")
        subscriber = asyncio.Queue(maxsize=2)
        subscriber.put_nowait({"seq": 1})
        subscriber.put_nowait({"seq": 2})
        runtime._subscribers = [subscriber]

        runtime._emit_live({"seq": 3, "type": "turn_completed"})

        self.assertEqual(runtime._subscribers, [subscriber])
        self.assertEqual(subscriber.get_nowait(), {"seq": 2})
        self.assertEqual(subscriber.get_nowait(), {"seq": 3, "type": "turn_completed"})

    def test_prompt_admission_checkpoints_links_queue_and_event_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            store.next_transcript_seq.return_value = 1
            store.get_session.return_value = None
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            session = AgentSession(
                id="session-durable",
                agent_name="codex",
                status="connected",
                card_id="card-1",
                project_id="project-1",
            )
            runtime = AgentSessionRuntime(manager, session)
            runtime._queue_paused = True

            item = runtime.enqueue("keep working")

            durable = session.config_json["durable_runtime"]
            self.assertEqual(durable["lifecycle"], "queued")
            self.assertEqual(durable["last_event_cursor"], 1)
            self.assertEqual(durable["queued_prompts"][0]["id"], item.id)
            self.assertEqual(durable["queued_prompts"][0]["card_id"], "card-1")
            self.assertEqual(durable["queued_prompts"][0]["project_id"], "project-1")
            store.save_session.assert_called_with(session)

    def test_abrupt_restart_recovers_durable_nonterminal_session_without_snapshot(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            queued = QueuedPrompt(
                id="queued-1",
                message="survive restart",
                session_id="session-restart",
                card_id="card-1",
                project_id="project-1",
            )
            session = AgentSession(
                id="session-restart",
                agent_name="codex",
                external_session_id="provider-session-1",
                status="prompting",
                label="card:card-1",
                card_id="card-1",
                project_id="project-1",
                config_json={
                    "durable_runtime": {
                        "version": 1,
                        "lifecycle": "prompting",
                        "queue_paused": False,
                        "queued_prompts": [],
                        "in_flight": queued.model_dump(mode="json"),
                        "last_event_cursor": 41,
                    }
                },
            )
            store = MagicMock()
            store.list_sessions.return_value = [session]
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            manager._resume_from_snapshot = AsyncMock()
            manager.attach_default = AsyncMock()

            asyncio.run(manager.start(resume=True))

            recovered = manager._resume_from_snapshot.await_args.args[0]
            self.assertEqual(recovered.session_id, "session-restart")
            self.assertEqual(recovered.external_session_id, "provider-session-1")
            self.assertEqual(recovered.card_id, "card-1")
            self.assertEqual(recovered.project_id, "project-1")
            self.assertEqual(recovered.in_flight.id, "queued-1")

    def test_wake_reconciliation_marks_resume_failure_recoverable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-sleep",
                agent_name="codex",
                external_session_id="provider-session-sleep",
                status="connected",
                label="card:card-sleep",
                config_json={
                    "durable_runtime": {
                        "version": 1,
                        "lifecycle": "ready",
                        "queued_prompts": [],
                        "last_event_cursor": 12,
                    }
                },
            )
            store = MagicMock()
            store.list_sessions.return_value = [session]
            store.get_session.return_value = session
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            manager._resume_from_snapshot = AsyncMock(
                side_effect=RuntimeError("provider resume unavailable")
            )
            manager.attach_default = AsyncMock()

            asyncio.run(manager.start(resume=True))

            self.assertEqual(session.status, "recoverable_interrupted")
            durable = session.config_json["durable_runtime"]
            self.assertEqual(durable["lifecycle"], "recoverable_interrupted")
            self.assertIn("provider resume unavailable", durable["recovery_error"])
            store.save_session.assert_called_with(session)
