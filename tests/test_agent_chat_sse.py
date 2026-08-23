"""Regression: agent chat SSE must stream without UnboundLocalError."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.acp.client import normalize_session_update
from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.agent_session import AgentSessionManager
from pa.modules.agent_chat import (
    CreateSessionBody,
    _apply_initial_options,
    _configuration_request,
    _durable_session_state,
    _requested_effort,
    _runtime_or_404,
    create_session,
    get_agent_session_history,
    get_provider_options,
    list_agent_session_history,
    list_agent_sessions,
    multiplexed_session_event_capabilities,
    multiplexed_session_events,
    session_close,
    session_close_all,
    session_events,
    session_retry,
)


class _FakeStore:
    def __init__(self, events: list[Any] | None = None) -> None:
        self._events = list(events or [])
        self.after_calls: list[int] = []

    def list_transcript_events(
        self, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> list[Any]:
        self.after_calls.append(after_seq)
        return [e for e in self._events if e.seq > after_seq][:limit]

    def list_transcript_events_before(
        self, session_id: str, *, before_seq: int | None = None, limit: int = 500
    ) -> list[Any]:
        events = [
            event
            for event in self._events
            if before_seq is None or event.seq < before_seq
        ]
        return events[-limit:]


class _FakeRuntime:
    def __init__(self) -> None:
        self._closed = False
        self.store = _FakeStore()
        self._subscribers: list[asyncio.Queue] = []
        self._flushed = False
        self.queued_on_subscribe: list[dict[str, Any]] = []

    def _flush_transcript(self) -> None:
        self._flushed = True

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        for event in self.queued_on_subscribe:
            q.put_nowait(event)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)


class AgentChatSseTests(unittest.TestCase):
    def test_ended_session_restart_requires_provider_identity_and_capability(self) -> None:
        manager = MagicMock()
        manager.get.return_value = None
        resumable = AgentSession(
            id="same-pa-session",
            agent_name="generic",
            external_session_id="same-provider-session",
            status="closed",
            config_json={
                "provider_session_recovery": {"resume": False, "load": True}
            },
        )
        state = _durable_session_state(manager, resumable)
        self.assertTrue(state["recoverable"])
        self.assertEqual(state["reason"], "session_closed_recoverable")
        self.assertIn("same-pa-session", state["actions"]["recover_url"])

        resumable.config_json["provider_session_recovery"] = {
            "resume": False,
            "load": False,
        }
        self.assertFalse(_durable_session_state(manager, resumable)["recoverable"])
        resumable.external_session_id = None
        self.assertFalse(_durable_session_state(manager, resumable)["recoverable"])

    def test_multiplex_capability_declares_one_dynamic_transport(self) -> None:
        capability = multiplexed_session_event_capabilities()
        self.assertEqual(capability["scope"], "all_live_sessions")
        self.assertEqual(capability["max_browser_connections_per_instance"], 1)
        self.assertTrue(capability["dynamic_membership"])

    def test_multiplex_stream_replays_only_subscribed_live_runtime(self) -> None:
        async def exercise() -> tuple[str, str, int]:
            runtime = _FakeRuntime()
            runtime.session_id = "live-session"
            runtime.store = _FakeStore(
                [
                    TranscriptEvent(
                        session_id="live-session",
                        seq=8,
                        event_type="turn_completed",
                        payload={"stop_reason": "end_turn"},
                    )
                ]
            )
            manager = MagicMock()
            manager.list_runtimes.return_value = [runtime]
            request = MagicMock()
            request.query_params.get.side_effect = lambda name: {
                "after": json.dumps(
                    {"live-session": 7, "closed-history-session": 10471}
                ),
                "client_id": "tab-1",
            }.get(name)
            request.headers.get.return_value = None
            request.is_disconnected = AsyncMock(return_value=False)
            with patch(
                "pa.modules.agent_chat._require_session_traffic_ready",
                return_value=manager,
            ):
                response = await multiplexed_session_events(request)
                iterator = response.body_iterator
                ready = await anext(iterator)
                event = await anext(iterator)
                await iterator.aclose()
            return ready, event, len(runtime._subscribers)

        ready, event, subscriber_count = asyncio.run(exercise())
        self.assertIn("event: ready", ready)
        self.assertIn("id: live-session:8", event)
        self.assertIn('"session_id": "live-session"', event)
        self.assertNotIn("closed-history-session", event)
        self.assertEqual(subscriber_count, 0)

    def test_multiplex_stream_carries_twenty_five_live_sessions_once(self) -> None:
        async def exercise() -> tuple[list[str], list[int]]:
            runtimes = []
            cursors = {}
            for index in range(25):
                session_id = f"live-{index}"
                runtime = _FakeRuntime()
                runtime.session_id = session_id
                runtime.store = _FakeStore(
                    [
                        TranscriptEvent(
                            session_id=session_id,
                            seq=2,
                            event_type="permission_request",
                            payload={"title": f"Permission {index}"},
                        )
                    ]
                )
                runtimes.append(runtime)
                cursors[session_id] = 1
            manager = MagicMock()
            manager.list_runtimes.return_value = runtimes
            request = MagicMock()
            request.query_params.get.side_effect = lambda name: {
                "after": json.dumps(cursors),
                "client_id": "tab-many",
            }.get(name)
            request.headers.get.return_value = None
            request.is_disconnected = AsyncMock(return_value=False)
            with patch(
                "pa.modules.agent_chat._require_session_traffic_ready",
                return_value=manager,
            ):
                response = await multiplexed_session_events(request)
                iterator = response.body_iterator
                await anext(iterator)
                events = [await anext(iterator) for _index in range(25)]
                await iterator.aclose()
            return events, [len(runtime._subscribers) for runtime in runtimes]

        events, subscriber_counts = asyncio.run(exercise())
        self.assertEqual(len(events), 25)
        self.assertEqual(
            {event.split("id: ", 1)[1].split(":", 1)[0] for event in events},
            {f"live-{index}" for index in range(25)},
        )
        self.assertEqual(subscriber_counts, [0] * 25)

    def test_multiplex_stream_exits_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        async def run() -> None:
            reset_shutdown_event()
            manager = MagicMock()
            manager.list_runtimes.return_value = []
            request = MagicMock()
            request.query_params.get.side_effect = lambda _name: None
            request.headers.get.return_value = None
            request.is_disconnected = AsyncMock(return_value=False)
            try:
                with patch(
                    "pa.modules.agent_chat._require_session_traffic_ready",
                    return_value=manager,
                ):
                    response = await multiplexed_session_events(request)
                    iterator = response.body_iterator
                    ready = await anext(iterator)
                    self.assertIn("event: ready", ready)
                    next_chunk = asyncio.create_task(anext(iterator))
                    await asyncio.sleep(0)
                    signal_shutdown()
                    with self.assertRaises(StopAsyncIteration):
                        await asyncio.wait_for(next_chunk, timeout=1.0)
                    await iterator.aclose()
            finally:
                reset_shutdown_event()

        asyncio.run(run())

    def test_restart_ui_race_is_gated_then_distinguishes_durable_loss(self) -> None:
        manager = MagicMock()
        manager.startup_state.return_value = {
            "phase": "recovering",
            "complete": False,
            "total": 2,
            "recovered": 0,
        }
        request = MagicMock()

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            with self.assertRaises(HTTPException) as starting:
                _runtime_or_404(request, "session-race")

        self.assertEqual(starting.exception.status_code, 503)
        self.assertEqual(
            starting.exception.detail["code"], "agent_recovery_in_progress"
        )

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            for operation in (session_retry, session_close):
                with self.subTest(operation=operation.__name__):
                    with self.assertRaises(HTTPException) as gated:
                        asyncio.run(operation(request, "session-race"))
                    self.assertEqual(gated.exception.status_code, 503)
                    self.assertEqual(
                        gated.exception.detail["code"],
                        "agent_recovery_in_progress",
                    )
        manager.retry_session.assert_not_called()
        manager.get.assert_not_called()

        durable = AgentSession(
            id="session-race",
            agent_name="future-provider",
            external_session_id="provider-thread-1",
            status="recoverable_interrupted",
            config_json={
                "durable_runtime": {
                    "recovery_error": "Unknown ACP provider 'future-provider'"
                }
            },
        )
        manager.startup_state.return_value = {"phase": "ready", "complete": True}
        manager.get.return_value = None
        manager.store.get_session.return_value = durable

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            with self.assertRaises(HTTPException) as interrupted:
                _runtime_or_404(request, durable.id)

        detail = interrupted.exception.detail
        self.assertEqual(detail["code"], "session_not_live")
        self.assertTrue(detail["recoverable"])
        self.assertEqual(detail["durable_session"]["reason"], "provider_thread_lost")
        self.assertIn("/history/", detail["history_url"])
        self.assertIn("/recover", detail["recover_url"])

        durable.status = "recovery_blocked"
        with patch("pa.modules.agent_chat._manager", return_value=manager):
            with self.assertRaises(HTTPException) as blocked:
                _runtime_or_404(request, durable.id)
        blocked_detail = blocked.exception.detail
        self.assertEqual(blocked_detail["code"], "session_not_live")
        self.assertFalse(blocked_detail["recoverable"])
        self.assertEqual(
            blocked_detail["durable_session"]["reason"], "recovery_blocked"
        )
        self.assertIsNone(blocked_detail["recover_url"])

        manager.store.get_session.return_value = None
        with patch("pa.modules.agent_chat._manager", return_value=manager):
            with self.assertRaises(HTTPException) as deleted:
                _runtime_or_404(request, durable.id)
        self.assertEqual(deleted.exception.detail["code"], "session_deleted")

    def test_saved_surface_defaults_are_applied_to_a_new_session(self) -> None:
        from pa.core.preferences import SurfaceAgentPrefs

        runtime = MagicMock()
        runtime.connection.config_options = [
            {"id": "reasoningEffort", "name": "Reasoning effort"}
        ]
        runtime.set_model = AsyncMock()
        runtime.set_mode = AsyncMock()
        runtime.set_config = AsyncMock()
        defaults = SurfaceAgentPrefs(
            model_id="gpt-default",
            mode_id="code",
            effort="high",
            config={"sandbox": "workspace"},
        )

        asyncio.run(_apply_initial_options(runtime, CreateSessionBody(), defaults))

        runtime.set_model.assert_awaited_once_with("gpt-default")
        runtime.set_mode.assert_awaited_once_with("code")
        runtime.set_config.assert_any_await("sandbox", "workspace")
        runtime.set_config.assert_any_await("reasoningEffort", "high")

    def test_default_effort_is_treated_as_unset(self) -> None:
        self.assertIsNone(_requested_effort("default"))
        self.assertIsNone(_requested_effort("Default"))
        self.assertIsNone(_requested_effort(""))
        requested = _configuration_request(CreateSessionBody(effort="default"))
        self.assertIsNone(requested.reasoning)
        self.assertTrue(requested.empty)

    def test_provider_options_synthesizes_openinterpreter_catalog(self) -> None:
        manager = MagicMock()
        manager.list_runtimes.return_value = []
        manager.store.list_sessions.return_value = []
        request = MagicMock()
        request.app.state.ctx.settings.auth_required = False
        request.app.state.ctx.settings.data_dir = Path(tempfile.mkdtemp())

        with (
            patch("pa.modules.agent_chat._manager", return_value=manager),
            patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
            patch(
                "pa.acp.providers.registry.get_provider",
                return_value=MagicMock(),
            ),
            patch(
                "pa.acp.providers.openinterpreter.provider_options_snapshot",
                return_value={
                    "provider": "openinterpreter",
                    "model_provider": "minimax-coding-plan",
                    "model_providers": [{"id": "minimax-coding-plan", "name": "MiniMax"}],
                    "models": {"availableModels": [{"modelId": "MiniMax-M2.5"}]},
                    "modes": {"availableModes": [{"id": "workspace-write"}]},
                    "config_options": [{"id": "reasoning_effort"}],
                    "supports_model_provider": True,
                    "cached": True,
                    "source": "openinterpreter_catalog",
                },
            ),
        ):
            result = get_provider_options(request, "openinterpreter")

        self.assertTrue(result["supports_model_provider"])
        self.assertEqual(result["model_provider"], "minimax-coding-plan")
        self.assertEqual(
            result["models"]["availableModels"][0]["modelId"], "MiniMax-M2.5"
        )

    def test_provider_options_fall_back_to_persisted_capability_catalog(self) -> None:
        session = AgentSession(
            agent_name="codex",
            principal_id="user:local",
            config_json={
                "models": {"availableModels": [{"modelId": "gpt-cached"}]},
                "modes": {"availableModes": [{"id": "code"}]},
                "options": [{"id": "reasoningEffort"}],
            },
        )
        manager = MagicMock()
        manager.list_runtimes.return_value = []
        manager.store.list_sessions.return_value = [session]
        request = MagicMock()
        request.app.state.ctx.settings.auth_required = True

        with (
            patch("pa.modules.agent_chat._manager", return_value=manager),
            patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
            patch("pa.acp.providers.registry.get_provider", return_value=MagicMock()),
        ):
            result = get_provider_options(request, "codex")

        self.assertTrue(result["cached"])
        self.assertEqual(
            result["models"]["availableModels"][0]["modelId"], "gpt-cached"
        )

    def test_provider_options_exclude_other_users_sessions(self) -> None:
        other_live = MagicMock()
        other_live._closed = False
        other_live.session = AgentSession(agent_name="codex", principal_id="user:other")
        other_live.connection.models = {"availableModels": [{"modelId": "other-live"}]}
        own_cached = AgentSession(
            agent_name="codex",
            principal_id="user:local",
            config_json={"models": {"availableModels": [{"modelId": "own-cached"}]}},
        )
        manager = MagicMock()
        manager.list_runtimes.return_value = [other_live]
        manager.store.list_sessions.return_value = [
            other_live.session,
            own_cached,
        ]
        request = MagicMock()
        request.app.state.ctx.settings.auth_required = True

        with (
            patch("pa.modules.agent_chat._manager", return_value=manager),
            patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
            patch("pa.acp.providers.registry.get_provider", return_value=MagicMock()),
        ):
            result = get_provider_options(request, "codex")

        self.assertTrue(result["cached"])
        self.assertEqual(
            result["models"]["availableModels"][0]["modelId"], "own-cached"
        )

    def test_new_session_applies_provider_and_initial_options(self) -> None:
        runtime = MagicMock()
        runtime.connection.config_options = [
            {"id": "reasoningEffort", "name": "Reasoning effort"}
        ]
        runtime.set_model = AsyncMock()
        runtime.set_mode = AsyncMock()
        runtime.set_config = AsyncMock()
        runtime.snapshot.return_value = {"session": {"id": "sess-new"}}

        manager = MagicMock()
        manager.create_session = AsyncMock(return_value=runtime)
        request = MagicMock()

        body = CreateSessionBody(
            title="Focused work",
            cwd="/tmp/project",
            provider="codex",
            model_id="gpt-test",
            mode_id="code",
            effort="high",
        )

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:local"
                ),
            ):
                return await create_session(request, body)

        result = asyncio.run(run())

        self.assertEqual(result["session"]["id"], "sess-new")
        manager.create_session.assert_awaited_once()
        create_kwargs = manager.create_session.await_args.kwargs
        self.assertEqual(create_kwargs["provider_override"], "codex")
        self.assertEqual(create_kwargs["cwd"], "/tmp/project")
        runtime.set_model.assert_awaited_once_with("gpt-test")
        runtime.set_mode.assert_awaited_once_with("code")
        runtime.set_config.assert_awaited_once_with("reasoningEffort", "high")

    def test_new_session_response_persists_complete_startup_trace(self) -> None:
        session = AgentSession(id="sess-traced", agent_name="codex")
        runtime = MagicMock()
        runtime.session = session
        runtime.connection.config_options = []
        runtime.snapshot.side_effect = lambda: {
            "session": {
                "id": session.id,
                "config_json": dict(session.config_json or {}),
            }
        }
        manager = MagicMock()
        manager.store.save_session = MagicMock()

        async def create(**kwargs):
            trace = kwargs["startup_trace"]
            trace.attach(session)
            for phase in (
                "provider_resolution",
                "workspace_preparation",
                "provider_launch",
                "provider_initialize",
                "session_creation",
                "session_configuration",
                "persistence_publication",
            ):
                trace.mark(phase)
            return runtime

        manager.create_session = AsyncMock(side_effect=create)
        request = MagicMock()

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id",
                    return_value="user:local",
                ),
            ):
                return await create_session(request, CreateSessionBody(fresh=True))

        result = asyncio.run(run())
        trace = result["session"]["config_json"]["startup_trace"]
        self.assertTrue(trace["complete"])
        self.assertEqual(
            [phase["name"] for phase in trace["phases"]],
            [
                "preference_resolution",
                "provider_resolution",
                "workspace_preparation",
                "provider_launch",
                "provider_initialize",
                "session_creation",
                "session_configuration",
                "persistence_publication",
                "response_readiness",
            ],
        )
        self.assertTrue(
            all(phase["duration_ms"] >= 0 for phase in trace["phases"])
        )
        manager.store.save_session.assert_called()

    def test_new_session_does_not_apply_unscoped_defaults_to_other_provider(
        self,
    ) -> None:
        from pa.core.preferences import SurfaceAgentPrefs

        runtime = MagicMock()
        runtime.session.agent_name = "cursor"
        runtime.connection.config_options = []
        runtime.set_model = AsyncMock()
        runtime.set_mode = AsyncMock()
        runtime.set_config = AsyncMock()
        runtime.snapshot.return_value = {"session": {"id": "sess-cursor"}}

        manager = MagicMock()
        manager.create_session = AsyncMock(return_value=runtime)
        request = MagicMock()
        request.app.state.ctx.settings = SimpleNamespace(data_dir=Path("/tmp"))
        defaults = SurfaceAgentPrefs(
            model_id="gpt-codex", mode_id="code", config={"sandbox": "workspace"}
        )

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:local"
                ),
                patch(
                    "pa.acp.providers.resolve.resolve_surface_preferences",
                    return_value=defaults,
                ),
                patch(
                    "pa.acp.providers.resolve.resolve_provider_id",
                    return_value=("codex", "user"),
                ),
            ):
                return await create_session(request, CreateSessionBody())

        result = asyncio.run(run())

        self.assertEqual(result["session"]["id"], "sess-cursor")
        runtime.set_model.assert_not_awaited()
        runtime.set_mode.assert_not_awaited()
        runtime.set_config.assert_not_awaited()

    def test_new_session_applies_defaults_for_inherited_provider(self) -> None:
        from pa.core.preferences import SurfaceAgentPrefs

        runtime = MagicMock()
        runtime.session.agent_name = "cursor"
        runtime.connection.config_options = []
        runtime.set_model = AsyncMock()
        runtime.set_mode = AsyncMock()
        runtime.set_config = AsyncMock()
        runtime.snapshot.return_value = {"session": {"id": "sess-cursor"}}

        manager = MagicMock()
        manager.create_session = AsyncMock(return_value=runtime)
        request = MagicMock()
        request.app.state.ctx.settings = SimpleNamespace(data_dir=Path("/tmp"))
        defaults = SurfaceAgentPrefs(
            model_id="cursor-model",
            mode_id="agent",
            config={"sandbox": "workspace"},
        )

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:local"
                ),
                patch(
                    "pa.acp.providers.resolve.resolve_surface_preferences",
                    return_value=defaults,
                ),
                patch(
                    "pa.acp.providers.resolve.resolve_provider_id",
                    return_value=("cursor", "user"),
                ),
            ):
                return await create_session(request, CreateSessionBody())

        result = asyncio.run(run())

        self.assertEqual(result["session"]["id"], "sess-cursor")
        runtime.set_model.assert_awaited_once_with("cursor-model")
        runtime.set_mode.assert_awaited_once_with("agent")
        runtime.set_config.assert_awaited_once_with("sandbox", "workspace")

    def test_labeled_session_is_cleaned_up_when_initial_options_fail(self) -> None:
        runtime = MagicMock()
        runtime.session_id = "sess-labeled"
        runtime.set_model = AsyncMock(side_effect=RuntimeError("invalid model"))
        runtime.close = AsyncMock()

        manager = MagicMock()
        manager.list_runtimes.return_value = []
        manager.store.get_session_by_label.return_value = None
        manager.create_session = AsyncMock(return_value=runtime)
        manager._runtimes = {runtime.session_id: runtime}
        request = MagicMock()
        body = CreateSessionBody(label="card:123", model_id="invalid")

        async def run() -> None:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:local"
                ),
            ):
                await create_session(request, body)

        with self.assertRaises(HTTPException) as raised:
            asyncio.run(run())

        self.assertEqual(raised.exception.status_code, 503)
        runtime.close.assert_awaited_once()
        self.assertNotIn(runtime.session_id, manager._runtimes)

    def test_reused_labeled_session_survives_initial_option_failure(self) -> None:
        runtime = MagicMock()
        runtime.session_id = "sess-existing"
        runtime._closed = False
        runtime.session.label = "card:123"
        runtime.set_model = AsyncMock(side_effect=RuntimeError("invalid model"))
        runtime.close = AsyncMock()

        manager = MagicMock()
        manager.list_runtimes.return_value = [runtime]
        manager.create_session = AsyncMock()
        manager._runtimes = {runtime.session_id: runtime}
        request = MagicMock()
        body = CreateSessionBody(label="card:123", model_id="invalid")

        async def run() -> None:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id", return_value="user:local"
                ),
            ):
                await create_session(request, body)

        with self.assertRaises(HTTPException):
            asyncio.run(run())

        manager.create_session.assert_not_awaited()
        runtime.close.assert_not_awaited()
        self.assertIn(runtime.session_id, manager._runtimes)

    def test_duplicate_labeled_session_creation_is_serialized(self) -> None:
        runtime = MagicMock()
        runtime._closed = False
        runtime.session_id = "sess-only"
        runtime.session = AgentSession(
            id="sess-only", agent_name="codex", label="card:123"
        )
        runtime.connection.config_options = []
        runtime.snapshot.return_value = {"session": {"id": "sess-only"}}

        class Manager:
            def __init__(self) -> None:
                self.store = MagicMock()
                self.store.get_session_by_label.return_value = None
                self._runtimes: dict[str, MagicMock] = {}
                self._locks: dict[str, asyncio.Lock] = {}
                self.create_calls = 0

            def label_lock(self, label: str) -> asyncio.Lock:
                return self._locks.setdefault(label, asyncio.Lock())

            def list_runtimes(self) -> list[MagicMock]:
                return list(self._runtimes.values())

            async def create_session(self, **_kwargs) -> MagicMock:
                self.create_calls += 1
                await asyncio.sleep(0.01)
                self._runtimes[runtime.session_id] = runtime
                return runtime

        manager = Manager()
        request = MagicMock()

        async def run() -> list[dict]:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch(
                    "pa.modules.agent_chat.get_principal_id",
                    return_value="user:local",
                ),
            ):
                return await asyncio.gather(
                    create_session(request, CreateSessionBody(label="card:123")),
                    create_session(request, CreateSessionBody(label="card:123")),
                )

        results = asyncio.run(run())

        self.assertEqual(manager.create_calls, 1)
        self.assertEqual(
            [result["session"]["id"] for result in results],
            [
                "sess-only",
                "sess-only",
            ],
        )

    def test_fresh_dispatch_ignores_old_card_label_and_retry_reuses_exact_link(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                card_id="card-1",
                request_payload={
                    "message": "work",
                    "mode_id": "agent-full-access",
                },
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                principal_id="user:dispatch-owner",
                state="materializing",
            )
            ledger.put(record)
            runtime = MagicMock()
            runtime._closed = False
            runtime.session_id = "session-new"
            runtime.session = AgentSession(
                id="session-new",
                agent_name="codex",
                label="card:card-1:dispatch:dispatch-1",
            )
            runtime.connection.config_options = []
            runtime.set_model = AsyncMock()
            runtime.set_mode = AsyncMock()
            runtime.set_config = AsyncMock()
            runtime.snapshot.return_value = {"session": {"id": "session-new"}}
            old_runtime = MagicMock()
            old_runtime._closed = False
            old_runtime.session.label = "card:card-1"

            manager = MagicMock()
            manager.create_session = AsyncMock(return_value=runtime)
            manager.list_runtimes.return_value = [old_runtime]
            manager.get.return_value = None
            manager.store.get_session.return_value = None
            manager.store.save_session = MagicMock()
            request = MagicMock()
            request.app.state.ctx.settings = SimpleNamespace(
                data_dir=None, instance_id="target"
            )
            request.app.state.ctx.services = {"dispatch_store": ledger}
            body = CreateSessionBody(
                label="card:card-1:dispatch:dispatch-1",
                card_id="card-1",
                dispatch_id="dispatch-1",
                mode_id="agent-full-access",
            )

            async def first() -> dict:
                with (
                    patch("pa.modules.agent_chat._manager", return_value=manager),
                    patch("pa.modules.agent_chat.uuid4", return_value="session-new"),
                    patch(
                        "pa.modules.agent_chat.get_principal_id",
                        return_value="user:local",
                    ),
                ):
                    return await create_session(request, body)

            result = asyncio.run(first())
            self.assertEqual(result["session"]["id"], "session-new")
            manager.create_session.assert_awaited_once()
            self.assertEqual(
                manager.create_session.await_args.kwargs["session_id"], "session-new"
            )
            call = manager.create_session.await_args.kwargs
            self.assertEqual(call["principal_id"], "user:dispatch-owner")
            self.assertEqual(call["authority_instance_id"], "authority")
            self.assertEqual(call["dispatch_id"], "dispatch-1")
            self.assertEqual(call["realm_id"], "default")
            self.assertEqual(
                call["execution_context_seed"]["dispatch_id"], "dispatch-1"
            )
            self.assertEqual(
                call["initial_configuration"].mode_id,
                "agent-full-access",
            )
            self.assertEqual(ledger.get(record.dispatch_id).session_id, "session-new")

            manager.create_session.reset_mock()
            manager.get.return_value = runtime
            result = asyncio.run(first())
            self.assertEqual(result["session"]["id"], "session-new")
            manager.create_session.assert_not_awaited()

    def test_resume_requires_exact_session_linked_during_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    request_payload={"message": "resume"},
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-expected",
                    resume_requested=True,
                    resume_session_id="session-expected",
                )
            )
            request = MagicMock()
            request.app.state.ctx.settings = SimpleNamespace(
                data_dir=None, instance_id="target"
            )
            request.app.state.ctx.services = {"dispatch_store": ledger}
            manager = MagicMock()

            async def run() -> None:
                with (
                    patch("pa.modules.agent_chat._manager", return_value=manager),
                    patch(
                        "pa.modules.agent_chat.get_principal_id",
                        return_value="user:local",
                    ),
                ):
                    await create_session(
                        request,
                        CreateSessionBody(
                            dispatch_id="dispatch-1",
                            resume=True,
                            resume_session_id="session-wrong",
                        ),
                    )

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(run())
            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(raised.exception.detail["code"], "resume_session_mismatch")
            manager.create_session.assert_not_called()

    def test_live_resume_attaches_matching_worktree_without_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id="card-1",
                    project_id="project-1",
                    request_payload={"message": "resume"},
                    materialization_plan={"profile": "repository"},
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-live",
                    resume_requested=True,
                    resume_session_id="session-live",
                )
            )
            session = AgentSession(
                id="session-live",
                agent_name="codex",
                cwd="/worktrees/session-live",
                card_id="card-1",
            )
            runtime = MagicMock()
            runtime._closed = False
            runtime.connected = True
            runtime.session_id = "session-live"
            runtime.session = session
            runtime.connection = SimpleNamespace(
                session_cwd="/worktrees/session-live",
                config_options=[],
            )
            runtime.snapshot.return_value = {"session": {"id": "session-live"}}
            runtime.configure = AsyncMock()
            manager = MagicMock()
            manager.get.return_value = runtime
            manager.create_session = AsyncMock()
            manager.store.save_session = MagicMock()

            async def prepare(target, *, requested_cwd, provider_id, mode_id=None):
                target.cwd = "/worktrees/session-live"
                return {}

            manager._prepare_workspace = AsyncMock(side_effect=prepare)
            request = MagicMock()
            request.app.state.ctx.settings = SimpleNamespace(
                data_dir=None, instance_id="target"
            )
            request.app.state.ctx.services = {"dispatch_store": ledger}

            async def run() -> dict:
                with (
                    patch("pa.modules.agent_chat._manager", return_value=manager),
                    patch(
                        "pa.modules.agent_chat.get_principal_id",
                        return_value="user:local",
                    ),
                ):
                    return await create_session(
                        request,
                        CreateSessionBody(
                            dispatch_id="dispatch-1",
                            card_id="card-1",
                            project_id="project-1",
                            resume=True,
                            resume_session_id="session-live",
                        ),
                    )

            result = asyncio.run(run())
            self.assertEqual(result["session"]["id"], "session-live")
            manager.create_session.assert_not_awaited()
            manager._prepare_workspace.assert_awaited_once()
            manager.store.save_session.assert_called()
            self.assertEqual(session.dispatch_id, "dispatch-1")
            self.assertEqual(session.cwd, "/worktrees/session-live")

    def test_live_resume_does_not_spawn_sibling_when_cwd_cannot_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id="card-1",
                    request_payload={"message": "resume"},
                    materialization_plan={"profile": "repository"},
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-live",
                    resume_requested=True,
                    resume_session_id="session-live",
                )
            )
            session = AgentSession(
                id="session-live",
                agent_name="codex",
                cwd="/var/pa-data",
            )
            runtime = MagicMock()
            runtime._closed = False
            runtime.connected = True
            runtime.session_id = "session-live"
            runtime.session = session
            runtime.connection = SimpleNamespace(
                session_cwd="/var/pa-data",
                config_options=[],
            )
            manager = MagicMock()
            manager.get.return_value = runtime
            manager.create_session = AsyncMock()
            manager.store.save_session = MagicMock()

            async def prepare(target, *, requested_cwd, provider_id, mode_id=None):
                target.cwd = "/worktrees/session-live"
                return {}

            manager._prepare_workspace = AsyncMock(side_effect=prepare)
            request = MagicMock()
            request.app.state.ctx.settings = SimpleNamespace(
                data_dir=None, instance_id="target"
            )
            request.app.state.ctx.services = {"dispatch_store": ledger}

            async def run() -> None:
                with (
                    patch("pa.modules.agent_chat._manager", return_value=manager),
                    patch(
                        "pa.modules.agent_chat.get_principal_id",
                        return_value="user:local",
                    ),
                ):
                    await create_session(
                        request,
                        CreateSessionBody(
                            dispatch_id="dispatch-1",
                            card_id="card-1",
                            resume=True,
                            resume_session_id="session-live",
                        ),
                    )

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(run())
            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(
                raised.exception.detail["code"], "live_session_cwd_immutable"
            )
            manager.create_session.assert_not_awaited()
            self.assertEqual(session.cwd, "/var/pa-data")

    def test_session_list_exposes_provider_for_option_lookup(self) -> None:
        runtime = MagicMock()
        runtime._closed = False
        runtime.connected = True
        runtime.prompting = False
        runtime._queue = []
        runtime._seq = 12
        runtime.session.id = "sess-codex"
        runtime.session.title = "Codex session"
        runtime.session.label = None
        runtime.session.agent_name = "codex"
        runtime.session.status = "idle"
        runtime.session.model_id = "gpt-test"
        runtime.session.mode_id = "code"
        runtime.session.card_id = None
        runtime.session.project_id = None
        runtime.session.metrics_json = {
            "turns": 3,
            "last_usage": {"total_tokens": 8400},
        }
        runtime.session.created_at.isoformat.return_value = "2026-07-16T23:00:00Z"
        runtime.session.config_json = {
            "values": {"reasoningEffort": "high", "approvalPolicy": "on-request"},
            "configuration": {
                "state": "ready",
                "requested": {"model_id": "gpt-test", "reasoning": "high"},
                "effective": {"model_id": "gpt-test", "reasoning": "high"},
            },
        }
        runtime.session.updated_at.isoformat.return_value = "2026-07-17T00:00:00Z"

        manager = MagicMock()
        manager.list_runtimes.return_value = [runtime]
        request = MagicMock()

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            sessions = list_agent_sessions(request)

        self.assertEqual(sessions[0]["agent_name"], "codex")
        self.assertEqual(sessions[0]["model_id"], "gpt-test")
        self.assertEqual(sessions[0]["mode_id"], "code")
        self.assertEqual(sessions[0]["requested_model_id"], "gpt-test")
        self.assertEqual(sessions[0]["requested_reasoning"], "high")
        self.assertEqual(sessions[0]["effective_reasoning"], "high")
        self.assertEqual(sessions[0]["configuration_state"], "ready")
        self.assertEqual(
            sessions[0]["config_json"]["values"]["reasoningEffort"], "high"
        )
        self.assertEqual(sessions[0]["last_seq"], 12)
        self.assertEqual(sessions[0]["metrics_json"]["turns"], 3)
        self.assertEqual(
            sessions[0]["metrics_json"]["last_usage"]["total_tokens"], 8400
        )
        self.assertEqual(sessions[0]["created_at"], "2026-07-16T23:00:00Z")
        self.assertTrue(sessions[0]["live"])
        self.assertFalse(sessions[0]["orphan"])

    def test_session_list_exposes_nonterminal_store_only_orphans(self) -> None:
        orphan = AgentSession(
            id="sess-orphan",
            agent_name="codex",
            status="recoverable_interrupted",
            config_json={
                "durable_runtime": {
                    "queued_prompts": [{"id": "queued-1"}],
                    "last_event_cursor": 19,
                }
            },
        )
        closed = AgentSession(
            id="sess-closed",
            agent_name="codex",
            status="closed",
        )
        manager = MagicMock()
        manager.list_runtimes.return_value = []
        manager.store.list_sessions.return_value = [orphan, closed]
        request = MagicMock()

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            sessions = list_agent_sessions(request)

        self.assertEqual([session["id"] for session in sessions], ["sess-orphan"])
        self.assertFalse(sessions[0]["live"])
        self.assertTrue(sessions[0]["orphan"])
        self.assertEqual(sessions[0]["queue_length"], 1)
        self.assertEqual(sessions[0]["last_seq"], 19)

    def test_persisted_history_includes_closed_session_transcript(self) -> None:
        session = AgentSession(
            id="sess-closed",
            agent_name="codex",
            status="closed",
            title="Remote audit",
            card_id="card-1",
        )
        event = TranscriptEvent(
            session_id=session.id,
            seq=4,
            event_type="turn_completed",
            payload={"stop_reason": "end_turn"},
        )
        manager = MagicMock()
        manager.store.list_sessions.return_value = [session]
        manager.store.get_session.return_value = session
        manager.store.list_transcript_events_before.return_value = [event]
        manager.get.return_value = None
        request = MagicMock()
        request.app.state.ctx.settings.instance_id = "mini-1"
        request.app.state.ctx.settings.instance_name = "macmini"

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            rows = list_agent_session_history(request, card_id="card-1")
            audit = asyncio.run(get_agent_session_history(request, session.id))

        self.assertEqual(rows[0]["id"], session.id)
        self.assertFalse(rows[0]["live"])
        self.assertEqual(rows[0]["instance_name"], "macmini")
        self.assertEqual(audit["events"][0]["event_type"], "turn_completed")
        self.assertEqual(audit["instance"]["id"], "mini-1")

    def test_live_and_closed_history_use_same_newest_backward_pages(self) -> None:
        session = AgentSession(id="sess-long", agent_name="codex")
        events = [
            TranscriptEvent(
                session_id=session.id,
                seq=seq,
                event_type="message",
                payload={"text": str(seq)},
            )
            for seq in range(1, 6002)
        ]
        store = _FakeStore(events)

        for live in (False, True):
            with self.subTest(live=live):
                manager = MagicMock()
                manager.store = store
                manager.store.get_session = MagicMock(return_value=session)
                runtime = _FakeRuntime() if live else None
                if runtime:
                    runtime.store = store
                manager.get.return_value = runtime
                request = MagicMock()
                request.app.state.ctx.settings.instance_id = "mini-1"
                request.app.state.ctx.settings.instance_name = "macmini"

                with patch("pa.modules.agent_chat._manager", return_value=manager):
                    newest = asyncio.run(get_agent_session_history(request, session.id))
                    older = asyncio.run(
                        get_agent_session_history(
                            request,
                            session.id,
                            before_seq=5002,
                        )
                    )

                self.assertEqual(
                    [event["seq"] for event in newest["events"]],
                    list(range(5002, 6002)),
                )
                self.assertTrue(newest["page"]["has_older"])
                self.assertEqual(
                    [event["seq"] for event in older["events"]],
                    list(range(4002, 5002)),
                )
                self.assertTrue(older["page"]["has_older"])
                self.assertEqual(older["page"]["next_before_seq"], 4002)
                self.assertEqual(newest["live"], live)
                if runtime:
                    self.assertFalse(runtime._flushed)

    def test_history_reports_exhausted_reverse_page(self) -> None:
        session = AgentSession(id="sess-short", agent_name="codex")
        store = _FakeStore(
            [
                TranscriptEvent(
                    session_id=session.id,
                    seq=seq,
                    event_type="message",
                    payload={"text": str(seq)},
                )
                for seq in range(1, 4)
            ]
        )
        manager = MagicMock()
        manager.store = store
        manager.store.get_session = MagicMock(return_value=session)
        manager.get.return_value = None
        request = MagicMock()
        request.app.state.ctx.settings.instance_id = "mini-1"
        request.app.state.ctx.settings.instance_name = "macmini"

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            page = asyncio.run(
                get_agent_session_history(request, session.id, before_seq=3, limit=2)
            )

        self.assertEqual([event["seq"] for event in page["events"]], [1, 2])
        self.assertFalse(page["page"]["has_older"])
        self.assertIsNone(page["page"]["next_before_seq"])

    def test_history_page_is_bounded_instrumented_and_within_budget(self) -> None:
        session = AgentSession(id="sess-budget", agent_name="codex")
        store = _FakeStore(
            [
                TranscriptEvent(
                    session_id=session.id,
                    seq=seq,
                    event_type="tool_call_update",
                    payload={"tool_call_id": f"tool-{seq}", "text": "x" * 200},
                )
                for seq in range(1, 10_001)
            ]
        )
        manager = MagicMock()
        manager.store = store
        manager.store.get_session = MagicMock(return_value=session)
        manager.get.return_value = None
        request = MagicMock()
        request.app.state.ctx.settings.instance_id = "mini-1"
        request.app.state.ctx.settings.instance_name = "macmini"

        started = perf_counter()
        with patch("pa.modules.agent_chat._manager", return_value=manager):
            page = asyncio.run(
                get_agent_session_history(request, session.id, limit=5000)
            )
        elapsed = perf_counter() - started

        self.assertEqual(len(page["events"]), 1000)
        self.assertEqual(page["page"]["limit"], 1000)
        self.assertEqual(page["diagnostics"]["event_count"], 1000)
        self.assertGreater(page["diagnostics"]["payload_bytes"], 0)
        self.assertIn("query_ms", page["diagnostics"])
        self.assertIn("serialization_ms", page["diagnostics"])
        self.assertLess(elapsed, 2.0)

    def test_codex_message_phase_is_preserved(self) -> None:
        update = {
            "sessionUpdate": "agent_message_chunk",
            "messageId": "message-1",
            "content": {"type": "text", "text": "Still working"},
            "_meta": {"codex": {"phase": "commentary"}},
        }

        normalized = normalize_session_update(update)

        self.assertEqual(normalized["phase"], "commentary")
        self.assertEqual(normalized["text"], "Still working")

    def test_events_stream_replays_without_unbound_error(self) -> None:
        """Previously crashed with UnboundLocalError on after_seq before subscribe."""
        te = MagicMock()
        te.id = "e1"
        te.seq = 3
        te.event_type = "agent_message_chunk"
        te.session_id = "sess-1"
        te.payload = {"text": "hi"}
        te.created_at.isoformat.return_value = "2026-01-01T00:00:00+00:00"

        runtime = _FakeRuntime()
        runtime.store = _FakeStore([te])

        request = MagicMock()
        request.headers = {}
        request.query_params = {"after": "0"}
        request.is_disconnected = AsyncMock(return_value=True)

        async def run() -> str:
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                resp = await session_events(request, "sess-1")
                chunks: list[str] = []
                try:
                    async for chunk in resp.body_iterator:
                        chunks.append(
                            chunk if isinstance(chunk, str) else chunk.decode()
                        )
                        if any("data:" in c for c in chunks):
                            break
                finally:
                    await resp.body_iterator.aclose()
                return "".join(chunks)

        body = asyncio.run(run())
        self.assertIn("event: agent_message_chunk", body)
        self.assertIn('"seq": 3', body)
        self.assertIn('"text": "hi"', body)
        self.assertTrue(runtime._flushed)
        # Generator reached subscribe() then exited on disconnect.
        self.assertEqual(runtime._subscribers, [])

    def test_live_events_yielded_after_subscribe(self) -> None:
        runtime = _FakeRuntime()
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        # Stay connected for the first live event, then disconnect.
        request.is_disconnected = AsyncMock(side_effect=[False, True])

        async def run() -> str:
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                resp = await session_events(request, "sess-1")

                async def emit_soon() -> None:
                    # Wait until subscribe() registers a queue.
                    for _ in range(50):
                        if runtime._subscribers:
                            break
                        await asyncio.sleep(0.01)
                    runtime._subscribers[0].put_nowait(
                        {
                            "id": "live-1",
                            "seq": 7,
                            "type": "agent_thought_chunk",
                            "session_id": "sess-1",
                            "payload": {"text": "thinking…"},
                            "created_at": "2026-01-01T00:00:01+00:00",
                        }
                    )

                emitter = asyncio.create_task(emit_soon())
                chunks: list[str] = []
                try:
                    async for chunk in resp.body_iterator:
                        chunks.append(
                            chunk if isinstance(chunk, str) else chunk.decode()
                        )
                        if any("agent_thought_chunk" in c for c in chunks):
                            break
                finally:
                    emitter.cancel()
                    await resp.body_iterator.aclose()
                return "".join(chunks)

        body = asyncio.run(run())
        self.assertIn("event: agent_thought_chunk", body)
        data = next(
            json.loads(line.removeprefix("data: ").strip())
            for line in body.splitlines()
            if line.startswith("data:")
        )
        self.assertEqual(data["payload"]["text"], "thinking…")

    def test_live_stream_exits_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        runtime = _FakeRuntime()
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        request.is_disconnected = AsyncMock(return_value=False)

        async def run() -> None:
            reset_shutdown_event()
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                response = await session_events(request, "sess-shutdown")
                next_chunk = asyncio.create_task(anext(response.body_iterator))
                for _ in range(50):
                    if runtime._subscribers:
                        break
                    await asyncio.sleep(0.01)
                signal_shutdown()
                with self.assertRaises(StopAsyncIteration):
                    await asyncio.wait_for(next_chunk, timeout=1.0)
                await response.body_iterator.aclose()
            reset_shutdown_event()

        asyncio.run(run())
        self.assertEqual(runtime._subscribers, [])

    def test_durable_catchup_stops_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        events = [
            TranscriptEvent(
                session_id="sess-catchup-shutdown",
                seq=seq,
                event_type="message",
                payload={"text": str(seq)},
            )
            for seq in range(1, 2001)
        ]
        runtime = _FakeRuntime()
        runtime.store = _FakeStore(events)
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        request.is_disconnected = AsyncMock(return_value=False)

        async def run() -> list[int]:
            reset_shutdown_event()
            sequences: list[int] = []
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                response = await session_events(request, "sess-catchup-shutdown")
                async for chunk in response.body_iterator:
                    text = chunk if isinstance(chunk, str) else chunk.decode()
                    data = next(
                        json.loads(line[5:].strip())
                        for line in text.splitlines()
                        if line.startswith("data:")
                    )
                    sequences.append(data["seq"])
                    if len(sequences) == 5:
                        signal_shutdown()
                await response.body_iterator.aclose()
            reset_shutdown_event()
            return sequences

        self.assertEqual(asyncio.run(run()), [1, 2, 3, 4, 5])
        self.assertEqual(runtime._subscribers, [])

    def test_paginated_catchup_is_complete_ordered_and_deduplicates_live_overlap(
        self,
    ) -> None:
        events = [
            TranscriptEvent(
                session_id="sess-long",
                seq=seq,
                event_type="message",
                payload={"text": str(seq)},
            )
            for seq in range(1, 5506)
        ]
        runtime = _FakeRuntime()
        runtime.store = _FakeStore(events)
        runtime.queued_on_subscribe = [
            {
                "id": events[-1].id,
                "seq": 5505,
                "type": "message",
                "session_id": "sess-long",
                "payload": {"text": "5505"},
                "created_at": events[-1].created_at.isoformat(),
            },
            {
                "id": "live-5506",
                "seq": 5506,
                "type": "message",
                "session_id": "sess-long",
                "payload": {"text": "5506"},
                "created_at": events[-1].created_at.isoformat(),
            },
        ]
        request = MagicMock()
        request.headers = {}
        request.query_params = {"after": "0"}
        request.is_disconnected = AsyncMock(return_value=False)

        async def run() -> list[int]:
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                response = await session_events(request, "sess-long")
                sequences: list[int] = []
                try:
                    async for chunk in response.body_iterator:
                        text = chunk if isinstance(chunk, str) else chunk.decode()
                        for line in text.splitlines():
                            if line.startswith("data:"):
                                sequences.append(json.loads(line[5:].strip())["seq"])
                        if sequences and sequences[-1] == 5506:
                            break
                finally:
                    await response.body_iterator.aclose()
                return sequences

        sequences = asyncio.run(run())

        self.assertEqual(sequences, list(range(1, 5507)))
        self.assertEqual(len(sequences), len(set(sequences)))
        self.assertEqual(runtime.store.after_calls, [0, 1000, 2000, 3000, 4000, 5000])
        self.assertEqual(runtime._subscribers, [])

    def test_live_queue_gap_is_filled_from_durable_events(self) -> None:
        events = [
            TranscriptEvent(
                session_id="sess-busy",
                seq=seq,
                event_type="message",
                payload={"text": str(seq)},
            )
            for seq in range(1, 601)
        ]

        class _GrowingStore(_FakeStore):
            def list_transcript_events(
                self, session_id: str, *, after_seq: int = 0, limit: int = 500
            ) -> list[Any]:
                self.after_calls.append(after_seq)
                visible = (
                    self._events[:3] if len(self.after_calls) == 1 else self._events
                )
                return [event for event in visible if event.seq > after_seq][:limit]

        runtime = _FakeRuntime()
        runtime.store = _GrowingStore(events)
        runtime.queued_on_subscribe = [
            {
                "id": events[-1].id,
                "seq": 600,
                "type": "message",
                "session_id": "sess-busy",
                "payload": {"text": "600"},
                "created_at": events[-1].created_at.isoformat(),
            }
        ]
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        request.is_disconnected = AsyncMock(return_value=False)

        async def run() -> list[int]:
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                response = await session_events(request, "sess-busy")
                sequences: list[int] = []
                try:
                    async for chunk in response.body_iterator:
                        text = chunk if isinstance(chunk, str) else chunk.decode()
                        for line in text.splitlines():
                            if line.startswith("data:"):
                                sequences.append(json.loads(line[5:].strip())["seq"])
                        if sequences and sequences[-1] == 600:
                            break
                finally:
                    await response.body_iterator.aclose()
                return sequences

        sequences = asyncio.run(run())

        self.assertEqual(sequences, list(range(1, 601)))
        self.assertEqual(runtime.store.after_calls, [0, 3])
        self.assertEqual(runtime._subscribers, [])

    def test_live_gap_fill_stops_when_server_shutdown_begins(self) -> None:
        from pa.server.shutdown import reset_shutdown_event, signal_shutdown

        events = [
            TranscriptEvent(
                session_id="sess-gap-shutdown",
                seq=seq,
                event_type="message",
                payload={"text": str(seq)},
            )
            for seq in range(1, 601)
        ]

        class _GrowingStore(_FakeStore):
            def list_transcript_events(
                self, session_id: str, *, after_seq: int = 0, limit: int = 500
            ) -> list[Any]:
                self.after_calls.append(after_seq)
                visible = (
                    self._events[:3] if len(self.after_calls) == 1 else self._events
                )
                return [event for event in visible if event.seq > after_seq][:limit]

        runtime = _FakeRuntime()
        runtime.store = _GrowingStore(events)
        runtime.queued_on_subscribe = [
            {
                "id": events[-1].id,
                "seq": 600,
                "type": "message",
                "session_id": "sess-gap-shutdown",
                "payload": {"text": "600"},
                "created_at": events[-1].created_at.isoformat(),
            }
        ]
        request = MagicMock()
        request.headers = {}
        request.query_params = {}
        request.is_disconnected = AsyncMock(return_value=False)

        async def run() -> list[int]:
            reset_shutdown_event()
            sequences: list[int] = []
            with patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime):
                response = await session_events(request, "sess-gap-shutdown")
                async for chunk in response.body_iterator:
                    text = chunk if isinstance(chunk, str) else chunk.decode()
                    data = next(
                        json.loads(line[5:].strip())
                        for line in text.splitlines()
                        if line.startswith("data:")
                    )
                    sequences.append(data["seq"])
                    if data["seq"] == 10:
                        signal_shutdown()
                await response.body_iterator.aclose()
            reset_shutdown_event()
            return sequences

        self.assertEqual(asyncio.run(run()), list(range(1, 11)))
        self.assertEqual(runtime.store.after_calls, [0, 3])
        self.assertEqual(runtime._subscribers, [])

    def test_cursor_assignment_pattern_does_not_unbind(self) -> None:
        """Guard the Python scoping bug that killed the SSE generator."""
        after_seq = 0

        async def event_stream():
            cursor = after_seq
            for te_seq in [1, 2]:
                yield f"id: {te_seq}\n\n"
                cursor = max(cursor, te_seq)
            self.assertEqual(cursor, 2)

        async def run() -> list[str]:
            return [x async for x in event_stream()]

        out = asyncio.run(run())
        self.assertEqual(len(out), 2)

    def test_close_marks_blocked_store_only_sessions_closed(self) -> None:
        orphan = AgentSession(
            id="sess-orphan",
            agent_name="codex",
            status="recovery_blocked",
            title="Make repositories first-class PA resources",
            label="card:4bd6e725",
        )
        store = MagicMock()
        store.get_session.return_value = orphan
        store.next_transcript_seq.return_value = 42
        store.append_transcript_events.return_value = []
        manager = MagicMock()
        manager.get.return_value = None
        manager.store = store
        request = MagicMock()

        async def run() -> dict:
            with (
                patch("pa.modules.agent_chat._manager", return_value=manager),
                patch("pa.modules.agent_chat.logger.info") as log_info,
            ):
                self.orphan_close_log = log_info
                return await session_close(request, "sess-orphan")

        result = asyncio.run(run())
        self.assertTrue(result["ok"])
        self.assertFalse(result["live"])
        self.assertTrue(result["orphan"])
        self.assertFalse(result["recovery"]["recoverable"])
        self.assertEqual(orphan.status, "closed")
        store.save_session.assert_called_once_with(orphan)
        store.append_transcript_events.assert_called_once()
        event = store.append_transcript_events.call_args.args[0][0]
        self.assertEqual(event.event_type, "session_closed")
        self.assertEqual(event.seq, 42)
        self.assertEqual(event.payload["reason"], "orphan_user_close")
        self.assertEqual(event.payload["prior_status"], "recovery_blocked")
        close_log = self.orphan_close_log.call_args.kwargs["extra"]
        self.assertEqual(close_log["session_id"], "sess-orphan")
        self.assertEqual(close_log["prior_status"], "recovery_blocked")

    def test_bulk_close_handles_live_and_orphan_mixture_then_restart(self) -> None:
        live_session = AgentSession(
            id="sess-live",
            agent_name="codex",
            status="prompting",
        )
        interrupted = AgentSession(
            id="sess-interrupted",
            agent_name="codex",
            status="recoverable_interrupted",
        )
        provisioning = AgentSession(
            id="sess-provisioning",
            agent_name="codex",
            status="provisioning_failed",
        )
        already_closed = AgentSession(
            id="sess-closed",
            agent_name="codex",
            status="closed",
        )
        sessions = [live_session, interrupted, provisioning, already_closed]
        store = MagicMock()
        store.list_sessions.return_value = sessions
        store.next_transcript_seq.side_effect = [7, 11]

        runtime = MagicMock()
        runtime._closed = False
        runtime.session_id = live_session.id

        async def close_live(**_kwargs) -> bool:
            runtime._closed = True
            live_session.status = "closed"
            return True

        runtime.close = AsyncMock(side_effect=close_live)
        manager = MagicMock()
        manager.store = store
        manager._runtimes = {live_session.id: runtime}
        manager.list_runtimes.side_effect = lambda: list(manager._runtimes.values())
        request = MagicMock()

        async def close_all() -> dict:
            with patch("pa.modules.agent_chat._manager", return_value=manager):
                return await session_close_all(request)

        first = asyncio.run(close_all())
        second = asyncio.run(close_all())

        self.assertEqual(
            first,
            {
                "ok": True,
                "closed": 3,
                "live_closed": 1,
                "orphan_closed": 2,
                "session_ids": [
                    "sess-live",
                    "sess-interrupted",
                    "sess-provisioning",
                ],
            },
        )
        self.assertEqual(second["closed"], 0)
        runtime.close.assert_awaited_once_with(
            reason="bulk_user_close",
            reconcile_workspace=False,
        )
        self.assertEqual(store.append_transcript_events.call_count, 2)
        events = [
            call.args[0][0] for call in store.append_transcript_events.call_args_list
        ]
        self.assertEqual(
            [event.payload["prior_status"] for event in events],
            ["recoverable_interrupted", "provisioning_failed"],
        )
        self.assertEqual({session.status for session in sessions}, {"closed"})
        self.assertEqual(
            manager.workspace_manager.expire_session.call_count,
            3,
        )

        with tempfile.TemporaryDirectory() as tmp:
            restarted = AgentSessionManager(Settings(data_dir=Path(tmp)), store)
            restarted.workspace_manager.reconcile_terminal_state = MagicMock(
                return_value={}
            )
            restarted.workspace_manager.collect_garbage = MagicMock(return_value={})
            restarted._resume_from_snapshot = AsyncMock()
            restarted.attach_default = AsyncMock()

            asyncio.run(restarted.start(resume=True))

            restarted._resume_from_snapshot.assert_not_awaited()

    def test_durable_close_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            store.save_session(
                AgentSession(
                    id="sess-atomic",
                    agent_name="codex",
                    status="provisioning_failed",
                )
            )

            closed, prior_status = store.close_session(
                "sess-atomic",
                reason="bulk_user_close",
            )
            repeated, repeated_prior = store.close_session(
                "sess-atomic",
                reason="bulk_user_close",
            )
            events = store.list_transcript_events("sess-atomic")

        assert closed is not None
        assert repeated is not None
        self.assertEqual(closed.status, "closed")
        self.assertEqual(repeated.status, "closed")
        self.assertEqual(prior_status, "provisioning_failed")
        self.assertIsNone(repeated_prior)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "session_closed")
        self.assertEqual(events[0].payload["prior_status"], "provisioning_failed")

    def test_explicit_retry_returns_recovered_session_snapshot(self) -> None:
        runtime = MagicMock()
        runtime.snapshot.return_value = {
            "session": {"id": "sess-recovered", "status": "idle"}
        }
        manager = MagicMock()
        manager.retry_session = AsyncMock(return_value=runtime)
        request = MagicMock()

        async def run() -> dict:
            with patch("pa.modules.agent_chat._manager", return_value=manager):
                return await session_retry(request, "sess-recovered")

        result = asyncio.run(run())

        manager.retry_session.assert_awaited_once_with("sess-recovered")
        self.assertEqual(result["session"]["status"], "idle")

    def test_explicit_retry_surfaces_blocked_operator_action(self) -> None:
        blocked = AgentSession(
            id="sess-blocked",
            agent_name="codex",
            status="recovery_blocked",
            config_json={
                "provisioning": {
                    "state": "blocked",
                    "error_code": "project_unavailable_on_instance",
                    "error": "Project is unavailable",
                    "action": "Sync or link the project, retry, or close the session",
                }
            },
        )
        manager = MagicMock()
        manager.retry_session = AsyncMock(
            side_effect=RuntimeError("Project is unavailable")
        )
        manager.store.get_session.return_value = blocked
        request = MagicMock()

        async def run() -> dict:
            with patch("pa.modules.agent_chat._manager", return_value=manager):
                return await session_retry(request, blocked.id)

        with self.assertRaises(HTTPException) as raised:
            asyncio.run(run())

        self.assertEqual(raised.exception.status_code, 409)
        self.assertTrue(raised.exception.detail["blocked"])
        self.assertFalse(raised.exception.detail["retryable"])
        self.assertIn("close", raised.exception.detail["action"])


if __name__ == "__main__":
    unittest.main()
