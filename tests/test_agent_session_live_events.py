import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import AgentSessionRuntime


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
    def test_concurrent_disconnect_only_exits_transport_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            connection = AgentConnection(
                Settings(data_dir=Path(tmp)), MagicMock()
            )
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
            connection = AgentConnection(
                Settings(data_dir=Path(tmp)), MagicMock()
            )
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
            connection.session = AgentSession(
                agent_name="codex", status="prompting"
            )
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
        self.assertEqual(
            subscriber.get_nowait(), {"seq": 3, "type": "turn_completed"}
        )
