"""Exact derived history pages over isolated durable transcripts."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pa.acp.transcript_page import compact_chunks
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.modules.agent_chat import get_agent_session_history

FINAL_TEXT = ("Opening fixture finding. " + "Exact stream: a.B αβ\n" * 100)[:1900] + " Closing fixture executor limitation.".ljust(57)
assert len(FINAL_TEXT) == 1957


def fixture_events(session_id="history-fixture"):
    events = [TranscriptEvent(session_id=session_id, seq=seq,
        event_type="tool_call_update", payload={"tool_call_id": "tool", "status": "completed"})
        for seq in range(1, 3443)]
    for group, start in enumerate((2036, 2170, 2305, 2440, 2580, 2800)):
        for offset in range(53 + group * 13):
            events[start + offset - 1] = TranscriptEvent(session_id=session_id, seq=start + offset,
                event_type="agent_message_chunk", payload={"message_id": f"commentary-{group}",
                "phase": "commentary", "final": False, "content_mode": "delta", "text": f"Progress {group}:{offset}."})
    for index in range(396):
        events[2989 + index] = TranscriptEvent(session_id=session_id, seq=2990 + index,
            event_type="agent_message_chunk", payload={"message_id": "final-396", "phase": "final",
            "final": False, "content_mode": "delta", "text": FINAL_TEXT[index * 1957 // 396:(index + 1) * 1957 // 396]})
    events[3387] = TranscriptEvent(session_id=session_id, seq=3388, event_type="turn_completed", payload={})
    return events


class HistoryReconstructionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = CardProjection(Path(self.tmp.name) / "pa.db")
        self.session = AgentSession(id="history-fixture", agent_name="codex")
        self.events = fixture_events()
        self.store.append_transcript_events(self.events)
        self.manager = MagicMock()
        self.manager.store = self.store
        self.manager.get.return_value = None
        self.request = MagicMock()
        self.request.app.state.ctx.settings.instance_id = "isolated-fixture"
        self.request.app.state.ctx.settings.instance_name = "fixture"

    def page(self, **kwargs):
        with patch("pa.modules.agent_chat._manager", return_value=self.manager), patch.object(self.store, "get_session", return_value=self.session):
            return asyncio.run(get_agent_session_history(self.request, self.session.id, message_boundaries=True, **kwargs))

    def test_396_chunk_final_is_exact_in_first_250_event_page(self):
        page = self.page(limit=250)
        final = [e for e in page["events"] if e["payload"].get("message_id") == "final-396"]
        self.assertEqual(len(final), 1)
        self.assertEqual(final[0]["payload"]["text"], FINAL_TEXT)
        self.assertFalse(final[0]["payload"]["final"])
        self.assertEqual(final[0]["payload"]["phase"], "final")
        self.assertEqual(final[0]["payload"]["source_first_seq"], 2990)
        self.assertEqual(final[0]["seq"], 3385)
        self.assertEqual(page["page"]["oldest_seq"], 2990)
        self.assertTrue(page["page"]["has_older"])
        self.assertFalse(page["page"]["has_newer"])
        # Projection is derived only; immutable source chunks remain unchanged.
        source = self.store.list_transcript_events(self.session.id, after_seq=2989, limit=396)
        self.assertEqual([e.model_dump() for e in source], [e.model_dump() for e in self.events[2989:3385]])

    def test_backward_pages_cover_all_commentary_and_source_sequences(self):
        received = []
        cursor = None
        while True:
            page = self.page(limit=250, before_seq=cursor)
            received = page["events"] + received
            if not page["page"]["has_older"]:
                break
            new_cursor = page["page"]["next_before_seq"]
            self.assertTrue(cursor is None or new_cursor < cursor)
            cursor = new_cursor
        expanded = []
        for event in received:
            expanded.extend(range(event["payload"].get("source_first_seq", event["seq"]), event["seq"] + 1))
        self.assertEqual(expanded, list(range(1, 3443)))
        for group in range(6):
            expected = "".join(e.payload["text"] for e in self.events if e.payload.get("message_id") == f"commentary-{group}")
            actual = "".join(e["payload"]["text"] for e in received if e["payload"].get("message_id") == f"commentary-{group}")
            self.assertEqual(actual, expected)

    def test_forward_cursor_inside_final_returns_complete_snapshot(self):
        page = self.page(after_seq=3200, limit=20)
        self.assertEqual(page["events"][0]["payload"]["text"], FINAL_TEXT)
        self.assertEqual(page["page"]["newest_seq"], 3385)
        self.assertTrue(page["page"]["has_newer"])

    def test_stream_crosses_multiple_1000_event_reads(self):
        events = [TranscriptEvent(session_id="huge", seq=i, event_type="agent_message_chunk",
            payload={"message_id": "huge-final", "phase": "final", "content_mode": "delta", "text": "x"}) for i in range(1, 3002)]
        self.store.append_transcript_events(events)
        self.session = AgentSession(id="huge", agent_name="codex")
        page = self.page(limit=250)
        self.assertEqual(page["events"][0]["payload"]["text"], "x" * 3001)
        self.assertFalse(page["page"]["has_older"])

    def test_intervening_tools_do_not_split_message_at_page_boundary(self):
        events = []
        for index in range(1200):
            events.extend([
                TranscriptEvent(session_id="interleaved", seq=index * 2 + 1, event_type="agent_message_chunk",
                    payload={"message_id": "m", "phase": "final", "content_mode": "delta", "text": "x"}),
                TranscriptEvent(session_id="interleaved", seq=index * 2 + 2, event_type="tool_call_update", payload={}),
            ])
        self.store.append_transcript_events(events)
        self.session = AgentSession(id="interleaved", agent_name="codex")
        page = self.page(limit=250)
        messages = [e for e in page["events"] if e["event_type"] == "agent_message_chunk"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["payload"]["text"], "x" * 1200)
        self.assertFalse(page["page"]["has_older"])

    def test_snapshot_replaces_deltas_without_changing_originals(self):
        events = [TranscriptEvent(session_id="s", seq=i + 1, event_type="agent_message_chunk",
            payload={"message_id": "m", "phase": "final", "content_mode": mode, "text": text})
            for i, (mode, text) in enumerate((("delta", "wrong"), ("snapshot", "Right."), ("delta", "Next")))]
        result = compact_chunks(events)
        self.assertEqual(result[0].payload["text"], "Right.Next")
        self.assertEqual(events[0].payload["text"], "wrong")


class BrowserHistoryLogicTests(unittest.TestCase):
    def test_node_history_regressions(self):
        import json
        import shutil
        import subprocess
        if not shutil.which("node"):
            self.skipTest("node is required")
        result = subprocess.run(["node", "tests/agent_chat_history_node_harness.js"],
            input=json.dumps({"expected": FINAL_TEXT, "events": [e.model_dump(mode="json") for e in fixture_events()]}),
            text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
