"""Node harnesses for the Workshop floor's client model and activity transport."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
JS = ROOT / "src/pa/server/static/js"

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is required")


def _run(harness: str, *args: Path) -> None:
    result = subprocess.run(
        ["node", str(ROOT / "tests" / harness), *map(str, args)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith("ok")


def test_floor_model_moves_stations_on_real_transitions() -> None:
    _run(
        "workshop_floor_model_node_harness.js",
        JS / "workshop-floor-model.js",
        ROOT / "tests/fixtures/workshop_floor_stations.json",
    )


def test_floor_stream_shares_one_lease_owned_connection_per_instance() -> None:
    _run("workshop_floor_stream_node_harness.js", JS / "workshop-floor-stream.js")
