"""Tool-activity and Terminal flyouts: last-entry open, live follow, windowed history."""
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]


class FlyoutScrollTests(unittest.TestCase):
    def test_flyouts_open_at_last_entry_follow_live_and_window_history(self) -> None:
        if not shutil.which("node"):
            self.skipTest("node is required")
        result = subprocess.run(
            ["node", "tests/agent_flyout_scroll_node_harness.js"],
            cwd=ROOT, text=True, capture_output=True, timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ok", result.stdout)

    def test_template_exposes_earlier_activity_control(self) -> None:
        template = (ROOT / "src/pa/server/templates/partials/agent/chat-widget.html").read_text()
        self.assertIn("data-acw-tool-earlier", template)
        self.assertIn("scroll up for earlier ones", template)


if __name__ == "__main__":
    unittest.main()
