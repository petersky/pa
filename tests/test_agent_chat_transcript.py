"""Transcript rendering regressions for the agent chat widget (node + fake DOM)."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]
HARNESS = ROOT / "tests/agent_chat_transcript_node_harness.js"
WIDGET = ROOT / "src/pa/server/static/js/agent-chat.js"

# Types the server emits only on the multiplex stream, or never as a named SSE event.
NOT_TRANSCRIPT_TYPES = {"restart_handoff_reconciled"}


def _server_emitted_event_types() -> set[str]:
    """Every literal transcript event type any server module can append."""
    types: set[str] = set()
    append = re.compile(r"_append_transcript\(\s*[\"']([a-z_]+)[\"']")
    constructed = re.compile(r"TranscriptEvent\((?:[^()]|\([^()]*\))*?event_type=[\"']([a-z_]+)[\"']", re.S)
    for path in (ROOT / "src/pa").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        types.update(append.findall(text))
        types.update(constructed.findall(text))
    return types - NOT_TRANSCRIPT_TYPES


class AgentChatTranscriptTests(unittest.TestCase):
    def run_scenario(self, name: str, argument=None) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is required for the transcript harness")
        command = [node, str(HARNESS), str(WIDGET), name]
        with tempfile.TemporaryDirectory() as scratch:
            if argument is not None:
                path = Path(scratch) / "argument.json"
                path.write_text(json.dumps(argument), encoding="utf-8")
                command.append(str(path))
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_queued_prompts_keep_one_bubble_each_in_order(self) -> None:
        self.run_scenario("queued_prompts_keep_one_bubble_each")

    def test_repeated_identical_prompts_are_all_shown(self) -> None:
        self.run_scenario("repeated_identical_prompts_are_all_shown")

    def test_repeated_identical_prompts_survive_replay(self) -> None:
        self.run_scenario("repeated_identical_prompts_survive_replay")

    def test_queue_removed_discards_optimistic_bubble(self) -> None:
        self.run_scenario("queue_removed_discards_optimistic_bubble")

    def test_every_server_event_type_is_registered_on_both_streams(self) -> None:
        types = _server_emitted_event_types()
        # Guard the scan itself: it must keep seeing the types that regressed.
        self.assertTrue({"prompt_rendered", "user_message", "context_boundary",
                         "collaboration_mode_transition", "queue_removed"} <= types, types)
        self.run_scenario("every_server_event_type_is_registered", sorted(types))

    def test_prompt_rendered_does_not_cause_gap_repair(self) -> None:
        self.run_scenario("prompt_rendered_does_not_cause_gap_repair")

    def test_live_mentions_decorate_the_prompt_bubble(self) -> None:
        self.run_scenario("live_mentions_decorate_the_prompt_bubble")

    def test_queued_prompt_does_not_claim_the_agent_is_working(self) -> None:
        self.run_scenario("queued_prompt_does_not_claim_the_agent_is_working")

    def test_queue_events_refresh_while_history_is_held(self) -> None:
        self.run_scenario("queue_events_refresh_while_history_is_held")

    def test_closed_direct_stream_is_reopened_with_backoff(self) -> None:
        self.run_scenario("closed_direct_stream_is_reopened_with_backoff")

    def test_stale_mention_matches_cannot_be_selected(self) -> None:
        self.run_scenario("stale_mention_matches_cannot_be_selected")


if __name__ == "__main__":
    unittest.main()
