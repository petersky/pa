"""Private alternating comparison of raw flock and its numeric observer."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from pa.config import Settings
from pa.core.performance import OperationObservation, current_observation
from pa.repository.workspace import WorkspaceManager


@contextmanager
def baseline(manager, key):
    path = manager.lock_root / f"{key}.lock"
    manager._assert_managed_path(path)
    with path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def distribution(values):
    ordered = sorted(values)
    return {"samples": len(values), "max_ms": max(values), **{
        f"p{p}_ms": ordered[math.ceil(len(ordered) * p / 100) - 1]
        for p in (50, 95, 99)
    }}


def run():
    samples = {"raw": [], "observed": []}
    with tempfile.TemporaryDirectory(prefix="pa-workspace-lock-") as directory:
        root = Path(directory)
        manager = WorkspaceManager(Settings(data_dir=root / "data",
            workspace_root=root / "workspaces"), SimpleNamespace())
        for iteration in range(50):
            for name in (["raw", "observed"] if iteration % 2 == 0 else ["observed", "raw"]):
                observation = OperationObservation()
                token = current_observation.set(observation)
                started = time.perf_counter_ns()
                try:
                    for _ in range(100):
                        with (baseline(manager, "fixture-private-key") if name == "raw"
                              else manager._repository_lock("fixture-private-key")):
                            pass
                finally:
                    elapsed = (time.perf_counter_ns() - started) / 1e6
                    current_observation.reset(token)
                samples[name].append(elapsed)
                if name == "observed":
                    assert observation.lock_events_total == 100
                    assert len(observation.lock_events) == 64
                    assert observation.lock_depth == 0
                    assert {item["name"] for item in observation.lock_events} == {"workspace.repository_file"}
                    assert "fixture-private-key" not in json.dumps(observation.lock_events)
        return {"contract": "pa.workspace-lock-observation-comparison/v1",
                "pairs": 50, "locks_per_sample": 100, "alternating": True,
                "latency": {key: distribution(value) for key, value in samples.items()},
                "raw_samples_ms": samples, "retained_events_per_invocation": 64,
                "total_lock_count_per_invocation": 100, "numeric_static_name_only": True,
                "source_hashes": {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in map(Path, ["src/pa/repository/workspace.py", "src/pa/core/operation_budget.py"])},
                "scope": "Private uncontended flock overhead only; native wait/scheduler/IO attribution and deployed performance remain unverified."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run()
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["latency"]))
