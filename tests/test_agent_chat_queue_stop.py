"""Browser-side queue behaviour after Stop and on failed queue mutations."""
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(not shutil.which("node"), reason="node is required for the queue UI harness")
def test_stop_shows_paused_queue_and_queue_failures_are_visible() -> None:
    root = Path(__file__).parents[1]
    result = subprocess.run(
        [shutil.which("node"), str(root / "tests/agent_chat_queue_stop_node_harness.js"),
         str(root / "src/pa/server/static/js/agent-chat.js")],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
