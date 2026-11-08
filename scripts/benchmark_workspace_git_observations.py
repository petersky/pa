"""Private local Git under the repository lock; no host config or network."""
import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.config import Settings
from pa.core.performance import OperationObservation, current_observation
from pa.repository.workspace import WorkspaceManager


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), "max_ms": max(values), **{
        f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1]
        for p in (50, 95, 99)}}


def run():
    with tempfile.TemporaryDirectory(prefix="pa-git-phases-") as directory:
        root = Path(directory)
        templates = root / "empty-templates"
        templates.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_SYSTEM=os.devnull,
                   GIT_CONFIG_GLOBAL=os.devnull, GIT_TEMPLATE_DIR=str(templates))
        with patch.dict(os.environ, env, clear=True):
            manager = WorkspaceManager(Settings(data_dir=root / "data",
                workspace_root=root / "workspaces"), SimpleNamespace())
            remote, client = root / "remote.git", root / "client"
            manager._git("init", "--bare", str(remote))
            manager._git("init", str(client))
            manager._git("-C", str(client), "-c", "user.name=Fixture", "-c",
                "user.email=fixture@example.test", "commit", "--allow-empty", "-m", "fixture")
            manager._git("-C", str(client), "push", str(remote), "HEAD:refs/heads/fixture")
            samples = {"disabled": [], "enabled": []}
            phases = []
            for iteration in range(30):
                for name in (["disabled", "enabled"] if iteration % 2 == 0 else ["enabled", "disabled"]):
                    observation = OperationObservation() if name == "enabled" else None
                    token = current_observation.set(observation)
                    started = time.perf_counter_ns()
                    try:
                        with manager._repository_lock("fixture"):
                            manager._git("-C", str(client), "fetch", str(remote), "refs/heads/fixture")
                            manager._git("-C", str(client), "rev-parse", "--git-dir")
                    finally:
                        elapsed = (time.perf_counter_ns() - started) / 1e6
                        current_observation.reset(token)
                    samples[name].append(elapsed)
                    if observation:
                        assert observation.git_subprocess_calls == 2
                        assert 0 < observation.git_fetch_ms <= observation.git_subprocess_ms <= observation.file_lock_hold_ms
                        phases.append({k: getattr(observation, k) for k in (
                            "git_subprocess_calls", "git_subprocess_ms", "git_fetch_ms",
                            "file_lock_wait_ms", "file_lock_hold_ms")})
    return {"contract": "pa.workspace-git-observations/v1", "pairs": 30,
            "scope": "Private file-transport fetch and rev-parse; enabled/disabled observer on identical source, not a product speed comparison or deployed network acceptance.",
            "latency": {k: distribution(v) for k, v in samples.items()},
            "phase_distributions": {k: distribution([p[k] for p in phases]) for k in phases[0] if k.endswith("_ms")},
            "raw_samples_ms": samples, "raw_numeric_phases": phases,
            "source_hashes": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in map(Path,
                ["src/pa/core/performance.py", "src/pa/core/async_runtime.py", "src/pa/repository/workspace.py"])}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run()
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"latency": result["latency"], "phases": result["phase_distributions"]}))
