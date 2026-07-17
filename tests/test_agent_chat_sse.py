"""Regression: agent chat SSE must stream without UnboundLocalError."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from pa.acp.client import normalize_session_update
from pa.modules.agent_chat import session_events


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
