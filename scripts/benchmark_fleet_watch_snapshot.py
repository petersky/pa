"""Compare complete Fleet watch views against an immutable repository baseline.

Uses a disposable SQLite store and no Kernel, provider, peer or live PA state.
Only unrelated local dimensions are stubbed. Both complete watch views and
their proof payloads must compare equal on every alternating sample.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import subprocess
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.config import Settings
from pa.core.performance import distribution
from pa.domain.models import FleetInstance
from pa.fleet import overview
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore

BASELINE = "a90d89d0d62745f6cc5639209d6a412126a9868a"


def run(root: Path, iterations: int) -> dict:
    archived = subprocess.check_output([
        "git", "show", f"{BASELINE}:src/pa/fleet/overview.py",
    ]).decode()
    tree = ast.parse(archived)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == "build_overview")
    namespace = dict(vars(overview))
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<archived-Fleet-overview>", "exec"), namespace)
    baseline = namespace["build_overview"]
    store = PRSupervisorStore(root / "supervisor.db")
    for index in range(100):
        store.upsert_watch(PRWatch(
            repository="example/repo", pr_number=index + 1,
            pr_url=f"https://github.com/example/repo/pull/{index + 1}",
            owner_instance_id="local", originating_instance_id="local",
            status="active" if index % 2 else "blocked",
            state={"proof": ["fixture " * 1000 for _ in range(8)]},
        ))
    ctx = SimpleNamespace(
        settings=Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                          instance_id="local", agent_enabled=False, peers=[]),
        services={"pr_supervisor_store": store},
        store=SimpleNamespace(list_repositories=lambda _: []),
    )
    instances = [FleetInstance(instance_id="local", name="Local", url="http://local")]
    original = store._row_to_watch
    counts = {name: {"hydrated_rows": 0, "state_policy_bytes": 0} for name in ("baseline", "source")}
    samples = {name: [] for name in counts}
    current = "baseline"

    def observed(row):
        counts[current]["hydrated_rows"] += 1
        counts[current]["state_policy_bytes"] += len(row["state_json"]) + len(row["policy_json"])
        return original(row)

    local = overview.local_dimension
    isolated_local = lambda c, d: local(c, d) if d == "supervisor" else {}
    namespace["local_dimension"] = isolated_local
    namespace["_now"] = lambda: "2026-01-01T00:00:00+00:00"
    with patch.object(store, "_row_to_watch", observed), \
         patch.object(overview, "_now", namespace["_now"]), \
         patch.object(overview, "local_dimension", isolated_local):
        for iteration in range(iterations):
            outputs = {}
            for current in (("baseline", "source") if iteration % 2 == 0 else ("source", "baseline")):
                generate = baseline if current == "baseline" else overview.build_overview
                start = time.perf_counter()
                outputs[current] = generate(ctx, instances, [])
                samples[current].append((time.perf_counter() - start) * 1000)
            assert outputs["baseline"] == outputs["source"]
    assert counts["baseline"]["hydrated_rows"] == iterations * 200
    assert counts["source"]["hydrated_rows"] == iterations * 100
    return {
        "scope": "100 active/blocked watches,64KB proof each; alternating sequential complete overview builds, disposable SQLite; other local dimensions stubbed. All fields/proofs equal, both views independently serialized. No JSON transport or deployed acceptance; host load uncontrolled.",
        "baseline_source": BASELINE, "baseline_function_sha256": hashlib.sha256(ast.get_source_segment(archived, node).encode()).hexdigest(),
        "source_file_sha256": hashlib.sha256(Path(overview.__file__).read_bytes()).hexdigest(),
        "samples_ms": samples, "latency_ms": {name: distribution(values) for name, values in samples.items()},
        "hydration": counts, "complete_outputs_equal_every_pair": True,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.iterations <= 100:
        parser.error("iterations must be1..100")
    with tempfile.TemporaryDirectory(prefix="pa-fleet-watch-snapshot-") as tmp:
        result = run(Path(tmp), args.iterations)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
