"""Regression: agent chat SSE must stream without UnboundLocalError."""

from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.acp.client import normalize_session_update
from pa.domain.models import AgentSession, TranscriptEvent
from pa.modules.agent_chat import (
    CreateSessionBody,
    _apply_initial_options,
    create_session,
    get_provider_options,
    get_agent_session_history,
    list_agent_session_history,
    list_agent_sessions,
    session_close,
    session_events,
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
            patch(
                "pa.modules.agent_chat.get_principal_id", return_value="user:local"
            ),
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
        other_live.session = AgentSession(
            agent_name="codex", principal_id="user:other"
        )
        other_live.connection.models = {
            "availableModels": [{"modelId": "other-live"}]
        }
        own_cached = AgentSession(
            agent_name="codex",
            principal_id="user:local",
            config_json={
                "models": {"availableModels": [{"modelId": "own-cached"}]}
            },
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
            patch(
                "pa.modules.agent_chat.get_principal_id", return_value="user:local"
            ),
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
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
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
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
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
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
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
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
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
                patch("pa.modules.agent_chat.get_principal_id", return_value="user:local"),
            ):
                await create_session(request, body)

        with self.assertRaises(HTTPException):
            asyncio.run(run())

        manager.create_session.assert_not_awaited()
        runtime.close.assert_not_awaited()
        self.assertIn(runtime.session_id, manager._runtimes)

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
        runtime.session.config_json = {
            "values": {"reasoningEffort": "high", "approvalPolicy": "on-request"}
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
        self.assertEqual(
            sessions[0]["config_json"]["values"]["reasoningEffort"], "high"
        )
        self.assertEqual(sessions[0]["last_seq"], 12)

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
            audit = get_agent_session_history(request, session.id)

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
                    newest = get_agent_session_history(request, session.id)
                    older = get_agent_session_history(
                        request,
                        session.id,
                        before_seq=5002,
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
            page = get_agent_session_history(
                request, session.id, before_seq=3, limit=2
            )

        self.assertEqual([event["seq"] for event in page["events"]], [1, 2])
        self.assertFalse(page["page"]["has_older"])
        self.assertIsNone(page["page"]["next_before_seq"])

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
                        chunks.append(chunk if isinstance(chunk, str) else chunk.decode())
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
                        chunks.append(chunk if isinstance(chunk, str) else chunk.decode())
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
                visible = self._events[:3] if len(self.after_calls) == 1 else self._events
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

    def test_close_marks_store_only_orphan_sessions_closed(self) -> None:
        orphan = AgentSession(
            id="sess-orphan",
            agent_name="codex",
            status="prompting",
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
            with patch("pa.modules.agent_chat._manager", return_value=manager):
                return await session_close(request, "sess-orphan")

        result = asyncio.run(run())
        self.assertEqual(result, {"ok": True, "live": False, "orphan": True})
        self.assertEqual(orphan.status, "closed")
        store.save_session.assert_called_once_with(orphan)
        store.append_transcript_events.assert_called_once()
        event = store.append_transcript_events.call_args.args[0][0]
        self.assertEqual(event.event_type, "session_closed")
        self.assertEqual(event.seq, 42)


if __name__ == "__main__":
    unittest.main()
