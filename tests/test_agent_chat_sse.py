"""Regression: agent chat SSE must stream without UnboundLocalError."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.acp.client import normalize_session_update
from pa.domain.models import AgentSession, TranscriptEvent
from pa.modules.agent_chat import (
    CreateSessionBody,
    create_session,
    get_agent_session_history,
    list_agent_session_history,
    list_agent_sessions,
    session_events,
)


class _FakeStore:
    def __init__(self, events: list[Any] | None = None) -> None:
        self._events = list(events or [])

    def list_transcript_events(
        self, session_id: str, *, after_seq: int = 0, limit: int = 500
    ) -> list[Any]:
        return [e for e in self._events if e.seq > after_seq][:limit]


class _FakeRuntime:
    def __init__(self) -> None:
        self._closed = False
        self.store = _FakeStore()
        self._subscribers: list[asyncio.Queue] = []
        self._flushed = False

    def _flush_transcript(self) -> None:
        self._flushed = True

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(q)
        return q

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)


class AgentChatSseTests(unittest.TestCase):
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
        runtime.session.updated_at.isoformat.return_value = "2026-07-17T00:00:00Z"

        manager = MagicMock()
        manager.list_runtimes.return_value = [runtime]
        request = MagicMock()

        with patch("pa.modules.agent_chat._manager", return_value=manager):
            sessions = list_agent_sessions(request)

        self.assertEqual(sessions[0]["agent_name"], "codex")
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
        manager.store.list_transcript_events.return_value = [event]
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


if __name__ == "__main__":
    unittest.main()
