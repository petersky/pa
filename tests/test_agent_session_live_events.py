import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from pa.acp.client import AgentConnection
from pa.acp.startup_trace import SessionStartupTrace
from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import (
    AgentSessionManager,
    AgentSessionRecoveryError,
    AgentSessionRuntime,
    _prompt_authority,
)
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
    def test_provider_config_update_reconciles_model_metadata_and_persists(self) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.session = AgentSession(
            id="session-config-update",
            agent_name="codex",
            model_id="gpt-5.6-sol[high]",
            status="idle",
            config_json={
                "values": {"model": "gpt-5.6-sol", "reasoning_effort": "high"},
                "configuration": {
                    "state": "ready",
                    "requested": {"model_id": "gpt-6-astra"},
                    "effective": {
                        "model_id": "gpt-5.6-sol[high]",
                        "config": {"model": "gpt-5.6-sol"},
                    },
                },
            },
        )
        runtime._turn_streamed = False
        runtime._in_flight = None
        runtime._turn_agent_events = []
        runtime.connection = SimpleNamespace(config_options=None)
        runtime._save_session_preserving_external_browser_async = AsyncMock()
        runtime._append_transcript = MagicMock()
        runtime._report_progress = AsyncMock()

        asyncio.run(
            runtime._on_acp_update(
                "provider-session",
                {
                    "sessionUpdate": "config_options_update",
                    "configOptions": [
                        {
                            "id": "model",
                            "name": "Model",
                            "currentValue": "gpt-6-astra",
                        },
                        {
                            "id": "reasoning_effort",
                            "name": "Reasoning effort",
                            "currentValue": "high",
                        },
                    ],
                },
            )
        )

        self.assertEqual(runtime.session.model_id, "gpt-6-astra")
        self.assertEqual(runtime.session.config_json["values"]["model"], "gpt-6-astra")
        self.assertEqual(
            runtime.session.config_json["configuration"]["effective"]["model_id"],
            "gpt-6-astra",
        )
        self.assertEqual(
            runtime.connection.config_options,
            runtime.session.config_json["options"],
        )
        runtime._save_session_preserving_external_browser_async.assert_awaited_once()

    def test_operator_prompt_precedes_automatic_reconciliation_deterministically(self) -> None:
        automatic = QueuedPrompt(
            id="automatic",
            message="reconcile",
            source="card-reconciliation:dispatch-1",
            priority=_prompt_authority("card-reconciliation:dispatch-1", "prepend")[0],
            turn_reason="automatic_reconciliation",
        )
        operator = QueuedPrompt(
            id="operator",
            message="stop and review",
            source="api",
            priority=_prompt_authority("api", "append")[0],
            turn_reason="operator_input",
            supersedes=[automatic.id],
        )

        restored = sorted([automatic, operator], key=lambda item: item.priority)

        self.assertEqual([item.id for item in restored], ["operator", "automatic"])
        self.assertEqual(operator.public_dict()["supersedes"], ["automatic"])
        self.assertEqual(
            _prompt_authority("api", "interrupt"), (0, "operator_interrupt")
        )

    def test_manager_records_resolution_workspace_and_publication_phases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            store.next_transcript_seq.return_value = 1
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_provider="codex"), store
            )
            spec = MagicMock(id="codex", env={})
            resolved = SimpleNamespace(
                provider_id="codex", spec=spec, source="instance"
            )
            trace = SessionStartupTrace()

            async def run():
                with (
                    patch(
                        "pa.instance.agent_session.resolve_agent_provider",
                        return_value=resolved,
                    ),
                    patch.object(AgentSessionRuntime, "start", new=AsyncMock()),
                ):
                    return await manager.create_session(
                        label="traced", startup_trace=trace
                    )

            runtime = asyncio.run(run())

        phases = runtime.session.config_json["startup_trace"]["phases"]
        self.assertEqual(
            [phase["name"] for phase in phases],
            [
                "provider_resolution",
                "workspace_preparation",
                "persistence_publication",
            ],
        )
        self.assertIs(manager.get(runtime.session_id), runtime)

    def test_live_close_audits_prior_status_and_is_idempotent(self) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.session = AgentSession(
            id="session-live-close",
            agent_name="codex",
            status="prompting",
        )
        runtime._closed = False
        runtime._queue_paused = False
        runtime._queue = []
        runtime._in_flight = None
        runtime._drain_task = None
        runtime._pending_permissions = {}
        runtime._permission_requests = {}
        runtime.connection = None
        runtime.manager = MagicMock()
        runtime._append_transcript = MagicMock()
        runtime._flush_transcript = MagicMock()
        runtime._drain_transcripts = AsyncMock()
        runtime._save_session_preserving_external_browser_async = AsyncMock()

        async def run() -> tuple[bool, bool]:
            first = await runtime.close(
                reason="bulk_user_close",
                reconcile_workspace=False,
            )
            second = await runtime.close(
                reason="bulk_user_close",
                reconcile_workspace=False,
            )
            return first, second

        with patch("pa.instance.agent_session.logger.info") as log_info:
            first, second = asyncio.run(run())

        self.assertTrue(first)
        self.assertFalse(second)
        runtime._append_transcript.assert_called_once_with(
            "session_closed",
            {
                "reason": "bulk_user_close",
                "prior_status": "prompting",
            },
        )
        self.assertEqual(runtime.session.status, "closed")
        structured = [call.kwargs["extra"] for call in log_info.call_args_list]
        self.assertTrue(
            all(
                detail["session_id"] == "session-live-close"
                and detail["prior_status"] == "prompting"
                for detail in structured
            )
        )

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

    def test_transcript_writer_stops_retrying_when_async_runtime_closes(self) -> None:
        from pa.core.async_runtime import AsyncRuntimeClosed

        event = TranscriptEvent(
            session_id="session-closing",
            seq=1,
            event_type="turn_completed",
            payload={},
        )
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        runtime.async_runtime = MagicMock()
        runtime.store = MagicMock()
        runtime.session = AgentSession(
            id="session-closing",
            agent_name="codex",
        )
        runtime._transcript_buffer = []
        runtime._transcript_queue = asyncio.Queue(maxsize=128)
        runtime._transcript_queue.put_nowait([event])
        runtime._transcript_writer_task = None
        runtime._offload = AsyncMock(side_effect=AsyncRuntimeClosed("closing"))

        async def run() -> None:
            await runtime._write_transcripts()

        asyncio.run(run())

        runtime.store.append_transcript_events.assert_called_once_with([event])
        self.assertTrue(runtime._transcript_queue.empty())
        self.assertEqual(runtime._transcript_queue._unfinished_tasks, 0)
        self.assertEqual(runtime._transcript_buffer, [])

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

    def test_forced_disconnect_kills_child_without_waiting_for_transport(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            connection = AgentConnection(Settings(data_dir=Path(tmp)), MagicMock())
            blocked = asyncio.Event()
            context = MagicMock()

            async def wait_forever(*_args) -> None:
                await blocked.wait()

            context.__aexit__ = AsyncMock(side_effect=wait_forever)
            process = MagicMock(returncode=None)
            process.wait = AsyncMock()
            connection._ctx = context
            connection._proc = process

            asyncio.run(connection.disconnect(timeout=0.01, force=True))

            process.kill.assert_called()
            self.assertIsNone(connection._ctx)

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

    def test_startup_defers_fifty_idle_sessions_and_recovers_only_active_turns(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = [
                AgentSession(
                    id=f"idle-{index}",
                    agent_name="codex",
                    status="idle",
                    config_json={
                        "durable_runtime": {
                            "version": 1,
                            "lifecycle": "ready",
                            "queued_prompts": [],
                            "in_flight": None,
                        }
                    },
                )
                for index in range(50)
            ]
            sessions.extend(
                AgentSession(
                    id=f"active-{index}",
                    agent_name="codex",
                    status="prompting",
                    config_json={
                        "durable_runtime": {
                            "version": 1,
                            "lifecycle": "prompting",
                            "queued_prompts": [],
                            "in_flight": QueuedPrompt(
                                message=f"turn {index}"
                            ).model_dump(mode="json"),
                        }
                    },
                )
                for index in range(3)
            )
            store = MagicMock()
            store.list_sessions.return_value = sessions
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            manager._resume_from_snapshot = AsyncMock()

            asyncio.run(manager.start(resume=True))

            recovered = {
                call.args[0].session_id
                for call in manager._resume_from_snapshot.await_args_list
            }
            self.assertEqual(recovered, {"active-0", "active-1", "active-2"})
            self.assertEqual(
                manager.startup_state(),
                {
                    "phase": "ready",
                    "complete": True,
                    "error": None,
                    "total": 3,
                    "eager": 3,
                    "deferred": 50,
                    "blocked": 0,
                    "recovered": 3,
                    "failed": 0,
                    "session_id": None,
                },
            )

    def test_startup_recovery_concurrency_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = [
                AgentSession(
                    id=f"queued-{index}",
                    agent_name="codex",
                    status="idle",
                    config_json={
                        "durable_runtime": {
                            "lifecycle": "queued",
                            "queued_prompts": [
                                QueuedPrompt(message="continue").model_dump(mode="json")
                            ],
                        }
                    },
                )
                for index in range(8)
            ]
            store = MagicMock()
            store.list_sessions.return_value = sessions
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_recovery_concurrency=2), store
            )
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            active = 0
            maximum = 0

            async def recover(_snapshot, _full):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                await asyncio.sleep(0.01)
                active -= 1

            manager._resume_from_snapshot = AsyncMock(side_effect=recover)

            asyncio.run(manager.start(resume=True))

            self.assertEqual(manager._resume_from_snapshot.await_count, 8)
            self.assertEqual(maximum, 2)

    def test_unavailable_project_is_attempted_once_across_restarts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-project-blocked",
                agent_name="codex",
                status="prompting",
                project_id="project-missing",
            )
            store = MagicMock()
            store.list_sessions.return_value = [session]
            store.get_session.return_value = session
            store.get_project.return_value = None
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})

            async def fail_for_missing_project(snap, _full):
                await manager._prepare_workspace(
                    session,
                    requested_cwd=snap.cwd,
                    provider_id=session.agent_name,
                )

            manager._resume_from_snapshot = AsyncMock(
                side_effect=fail_for_missing_project
            )
            manager.attach_default = AsyncMock()

            with (
                patch("pa.instance.agent_session.logger.warning") as warning,
                patch("pa.instance.agent_session.logger.exception") as exception,
                patch("pa.instance.agent_session.logger.info") as info,
            ):
                asyncio.run(manager.start(resume=True))
                asyncio.run(manager.start(resume=True))

            self.assertEqual(manager._resume_from_snapshot.await_count, 1)
            self.assertEqual(session.status, "recovery_blocked")
            self.assertEqual(
                session.config_json["durable_runtime"]["lifecycle"],
                "recovery_blocked",
            )
            self.assertTrue(
                any(
                    "recovery blocked" in str(call.args[0]).lower()
                    for call in warning.call_args_list
                )
            )
            self.assertTrue(
                any(
                    "remains blocked" in str(call.args[0]).lower()
                    for call in info.call_args_list
                )
            )
            exception.assert_not_called()

    def test_blocked_project_retries_after_project_and_links_arrive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-project-arrived",
                agent_name="codex",
                status="recovery_blocked",
                project_id="project-arrived",
                config_json={
                    "provisioning": {
                        "state": "blocked",
                        "action": "Sync the project, then retry",
                    }
                },
            )
            store = MagicMock()
            store.list_sessions.return_value = [session]
            store.get_session.return_value = session
            store.get_project.return_value = SimpleNamespace(realm_id="default")
            store.list_project_repositories.return_value = [
                (SimpleNamespace(id="repo-1"), SimpleNamespace(branch="main"))
            ]
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            manager._resume_from_snapshot = AsyncMock()
            manager.attach_default = AsyncMock()

            asyncio.run(manager.start(resume=True))

            manager._resume_from_snapshot.assert_awaited_once()
            recovered = manager._resume_from_snapshot.await_args.args[0]
            self.assertEqual(recovered.session_id, session.id)
            store.list_project_repositories.assert_called_once_with(
                "project-arrived", realm_id="default"
            )

    def test_project_arrival_exposes_new_failure_as_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-project-new-failure",
                agent_name="codex",
                status="recovery_blocked",
                project_id="project-arrived",
                config_json={
                    "provisioning": {
                        "state": "blocked",
                        "action": "Sync the project, then retry",
                    }
                },
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            snapshot = manager._snapshot_from_persisted(session)

            recovery_state = asyncio.run(
                manager._mark_recovery_interrupted(
                    snapshot, RuntimeError("provider resume unavailable")
                )
            )

            self.assertEqual(recovery_state, "recoverable_interrupted")
            self.assertEqual(session.status, "recoverable_interrupted")

    def test_unknown_nonterminal_status_is_not_automatically_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-unknown",
                agent_name="codex",
                status="future_terminal_state",
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

            manager._resume_from_snapshot.assert_not_awaited()
            manager.workspace_manager.collect_garbage.assert_called_once_with(
                active_session_ids=set()
            )

    def test_explicit_retry_bypasses_blocked_auto_recovery_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-explicit-retry",
                agent_name="codex",
                status="recovery_blocked",
                project_id="project-missing",
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            runtime = MagicMock()
            manager._resume_from_snapshot = AsyncMock(return_value=runtime)

            recovered = asyncio.run(manager.retry_session(session.id))

            self.assertIs(recovered, runtime)
            manager._resume_from_snapshot.assert_awaited_once()
            snapshot, reason = manager._resume_from_snapshot.await_args.args
            self.assertEqual(snapshot.session_id, session.id)
            self.assertEqual(reason.reason, "explicit_retry")

    def test_no_resume_boot_skips_durable_recovery_and_default_attach(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            store.list_sessions.return_value = [
                AgentSession(
                    id="session-paused",
                    agent_name="codex",
                    status="disconnected",
                )
            ]
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            manager._resume_from_snapshot = AsyncMock()
            manager.attach_default = AsyncMock()

            asyncio.run(manager.start(resume=False))

            store.list_sessions.assert_not_called()
            manager._resume_from_snapshot.assert_not_awaited()
            manager.attach_default.assert_not_awaited()
            manager.workspace_manager.collect_garbage.assert_called_once_with(
                active_session_ids=set()
            )

    def test_shutdown_aborts_remaining_durable_resumes(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        with tempfile.TemporaryDirectory() as tmp:
            sessions = [
                AgentSession(
                    id="session-a",
                    agent_name="cursor",
                    external_session_id="ext-a",
                    status="prompting",
                ),
                AgentSession(
                    id="session-b",
                    agent_name="cursor",
                    external_session_id="ext-b",
                    status="prompting",
                ),
            ]
            store = MagicMock()
            store.list_sessions.return_value = sessions
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_recovery_concurrency=1), store
            )
            manager.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            manager.workspace_manager.collect_garbage = MagicMock(return_value={})
            resumed: list[str] = []

            async def resume_one(snap, _full):
                resumed.append(snap.session_id)
                signal_shutdown()
                manager._accepting = False
                manager._quiescing = True

            manager._resume_from_snapshot = AsyncMock(side_effect=resume_one)
            manager.attach_default = AsyncMock()
            manager._mark_recovery_interrupted = AsyncMock()

            async def run() -> None:
                reset_shutdown_event()
                try:
                    await manager.start(resume=True)
                finally:
                    reset_shutdown_event()

            asyncio.run(run())

            self.assertEqual(resumed, ["session-a"])
            manager.attach_default.assert_not_awaited()
            manager._mark_recovery_interrupted.assert_not_awaited()

    def test_wake_reconciliation_defers_passive_connected_session(self) -> None:
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
            manager._resume_from_snapshot = AsyncMock()

            asyncio.run(manager.start(resume=True))

            self.assertEqual(session.status, "connected")
            manager._resume_from_snapshot.assert_not_awaited()
            self.assertEqual(manager.startup_state()["deferred"], 1)

    def test_concurrent_lazy_recovery_of_idle_session_creates_one_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-idle",
                agent_name="codex",
                external_session_id="provider-idle",
                status="idle",
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            runtime = MagicMock()
            runtime._closed = False

            async def create(**_kwargs):
                await asyncio.sleep(0.01)
                manager._runtimes[session.id] = runtime
                return runtime

            manager.create_session = AsyncMock(side_effect=create)

            async def recover_both():
                return await asyncio.gather(
                    manager.recover_session(session.id),
                    manager.recover_session(session.id),
                )

            first, second = asyncio.run(recover_both())

            self.assertIs(first, runtime)
            self.assertIs(second, runtime)
            self.assertEqual(manager.create_session.await_count, 1)

    def test_concurrent_recovery_of_closed_resumable_session_keeps_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-closed",
                agent_name="codex",
                external_session_id="provider-closed",
                origin_instance_id="local",
                status="closed",
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), instance_id="local"), store
            )
            runtime = MagicMock()
            runtime._closed = False

            async def create(**_kwargs):
                await asyncio.sleep(0.01)
                manager._runtimes[session.id] = runtime
                return runtime

            manager.create_session = AsyncMock(side_effect=create)

            async def recover_both():
                return await asyncio.gather(
                    manager.recover_session(session.id),
                    manager.recover_session(session.id),
                )

            first, second = asyncio.run(recover_both())
            self.assertIs(first, runtime)
            self.assertIs(second, runtime)
            self.assertEqual(manager.create_session.await_count, 1)
            kwargs = manager.create_session.await_args.kwargs
            self.assertIs(kwargs["existing"], session)
            self.assertEqual(kwargs["resume_external_id"], "provider-closed")

    def test_cross_instance_session_cannot_be_recovered_locally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-remote",
                agent_name="codex",
                external_session_id="provider-remote",
                origin_instance_id="remote",
                status="closed",
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), instance_id="local"), store
            )
            with self.assertRaisesRegex(
                AgentSessionRecoveryError, "belongs to another instance"
            ):
                asyncio.run(manager.recover_session(session.id))

    def test_prompt_lazily_recovers_deferred_idle_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manager = AgentSessionManager(Settings(data_dir=Path(tmp)), MagicMock())
            runtime = MagicMock()
            runtime.prompt = AsyncMock(return_value="started")
            manager.recover_session = AsyncMock(return_value=runtime)

            result = asyncio.run(
                manager.prompt("follow up", session_id="session-idle", wait=False)
            )

            self.assertEqual(result, "started")
            manager.recover_session.assert_awaited_once_with("session-idle")
            runtime.prompt.assert_awaited_once()

    def test_missing_provider_rollout_recovers_one_stable_pa_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                id="session-rollout",
                agent_name="future-provider",
                external_session_id="provider-thread-old",
                status="recoverable_interrupted",
                label="card:rollout",
            )
            store = MagicMock()
            store.get_session.return_value = session
            manager = AgentSessionManager(
                Settings(data_dir=Path(tmp), agent_provider="codex"), store
            )
            runtime = MagicMock()
            runtime._closed = False
            runtime.session = session

            async def create_replacement(**_kwargs):
                await asyncio.sleep(0)
                manager._runtimes[session.id] = runtime
                return runtime

            manager.create_session = AsyncMock(side_effect=create_replacement)

            async def run() -> tuple[object, object]:
                return await asyncio.gather(
                    manager.recover_session(session.id),
                    manager.recover_session(session.id),
                )

            first, second = asyncio.run(run())

            self.assertIs(first, runtime)
            self.assertIs(second, runtime)
            self.assertEqual(manager.create_session.await_count, 1)
            kwargs = manager.create_session.await_args.kwargs
            self.assertIs(kwargs["existing"], session)
            self.assertEqual(kwargs["resume_external_id"], "provider-thread-old")
            self.assertEqual(kwargs["provider_override"], "codex")


class AgentSessionTurnWaitingTests(unittest.TestCase):
    def test_watch_emits_turn_waiting_until_the_agent_streams(self) -> None:
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        item = QueuedPrompt(id="prompt-1", message="hello")
        runtime._in_flight = item
        runtime._turn_streamed = False
        runtime._pending_permissions = {}
        runtime._append_transcript = MagicMock()
        runtime._flush_transcript = MagicMock()

        async def run() -> None:
            with patch("pa.instance.agent_session.TURN_WAITING_SECONDS", 0.02):
                task = asyncio.create_task(runtime._watch_turn_waiting(item))
                await asyncio.sleep(0.06)
                runtime._turn_streamed = True
                await asyncio.wait_for(task, timeout=1)

        asyncio.run(run())
        runtime._append_transcript.assert_called()
        event_type, payload = runtime._append_transcript.call_args.args
        self.assertEqual(event_type, "turn_waiting")
        self.assertIn("not streamed", payload["message"])
        self.assertFalse(payload["pending_permissions"])
