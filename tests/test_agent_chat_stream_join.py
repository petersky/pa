"""Regression tests for agent/thought stream joining in the chat UI."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


def _stream_chunk_separator(prev: str, chunk: str) -> str:
    """Python mirror of streamChunkSeparator in agent-chat.js."""
    if not prev or not chunk:
        return ""
    left = prev[-1]
    right = chunk[0]
    if left.isspace() or right.isspace():
        return ""
    if left in ".!?" and (right.isupper() or right in "\"'“‘(["):
        return "\n\n"
    return ""


class AgentChatStreamJoinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server" / "static" / "js"
        cls.script = (root / "agent-chat.js").read_text()

    def test_script_finalizes_streams_on_tool_call(self) -> None:
        self.assertIn("function streamChunkSeparator(prev, chunk)", self.script)
        self.assertRegex(
            self.script,
            re.compile(
                r'case\s+"tool_call":\s*'
                r"(?:\/\/[^\n]*\n\s*)*"
                r"this\.finalizeStreams\(created\);",
                re.MULTILINE,
            ),
        )
        self.assertIn('case "tool_call_update":', self.script)
        self.assertIn(
            "stream.text += streamChunkSeparator(stream.text, next) + next;",
            self.script,
        )

    def test_separator_inserts_break_at_sentence_boundary(self) -> None:
        self.assertEqual(
            _stream_chunk_separator(
                "Will force-kill if needed.",
                "Monica is stuck deactivating and needs SIGKILL.",
            ),
            "\n\n",
        )
        self.assertEqual(
            _stream_chunk_separator(
                "Checking whether we can start the macmini session directly.",
                "Peers are current and both hosts are working again.",
            ),
            "\n\n",
        )
        joined = (
            "Will force-kill if needed."
            + _stream_chunk_separator(
                "Will force-kill if needed.",
                "Monica is stuck.",
            )
            + "Monica is stuck."
        )
        self.assertEqual(joined, "Will force-kill if needed.\n\nMonica is stuck.")

    def test_separator_preserves_normal_token_streams(self) -> None:
        self.assertEqual(_stream_chunk_separator("Hello", " world"), "")
        self.assertEqual(_stream_chunk_separator("Hello ", "world"), "")
        self.assertEqual(_stream_chunk_separator("version 0.", "2"), "")
        self.assertEqual(_stream_chunk_separator("OK.", "updated"), "")
        self.assertEqual(_stream_chunk_separator("", "Monica"), "")
