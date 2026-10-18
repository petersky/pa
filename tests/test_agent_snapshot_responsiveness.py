from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import MagicMock, patch

from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.agent_chat import get_session_snapshot


class _ContendedTranscriptStore:
    def __init__(self) -> None:
        self.reads = 0

    def list_transcript_events_before(self, *args, **kwargs):
        self.reads += 1
        time.sleep(1)
        return []


class AgentSnapshotResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_metadata_does_not_flush_or_read_large_transcript(self) -> None:
        runtime = object.__new__(AgentSessionRuntime)
        runtime.session = AgentSession(
            id="active-session",
            agent_name="codex",
            status="prompting",
        )
        runtime.connection = None
        runtime._seq = 4096
        runtime._queue_paused = False
        runtime._queue = []
        runtime._in_flight = None
        runtime._turn_started_at = None
        runtime._permission_requests = {}
        runtime._pending_permissions = []
        runtime._transcript_buffer = [object()] * 4096
        runtime.store = _ContendedTranscriptStore()
        request = MagicMock()

        started_cpu = time.thread_time()
        started = time.monotonic()
        with (
            patch("pa.modules.agent_chat._runtime_or_404", return_value=runtime),
            patch(
                "pa.modules.agent_chat._session_reconciliation",
                return_value={"state": "not_requested"},
            ),
            patch(
                "pa.modules.agent_chat._observability",
                return_value={"session_state": "busy"},
            ),
        ):
            configured = time.monotonic()
            snapshot = await asyncio.wait_for(
                get_session_snapshot(request, runtime.session_id),
                timeout=0.2,
            )
            request_finished = time.monotonic()
        elapsed = time.monotonic() - started

        self.assertLess(
            elapsed, 0.2,
            f"setup={configured - started:.6f}s "
            f"request={request_finished - configured:.6f}s "
            f"teardown={elapsed - (request_finished - started):.6f}s "
            f"loop_thread_cpu={time.thread_time() - started_cpu:.6f}s",
        )
        self.assertEqual(runtime.store.reads, 0)
        self.assertEqual(len(runtime._transcript_buffer), 4096)
        self.assertNotIn("transcript", snapshot)
        self.assertNotIn("transcript_page", snapshot)
        self.assertEqual(snapshot["last_event_seq"], 4096)
