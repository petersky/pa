"""Private-cache baseline/candidate scheduling probe; never contacts real peers.

Run with uv run python scripts/benchmark_fleet_required_refresh.py --output FILE.
The pinned baseline is the integrated pre-repair loop, not a second installation.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import json
import logging
import os
import subprocess
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BASE = "23de9ebe054b25f6d06d059a6c7b2f164a279d41"


def distribution(values):
    values = sorted(values)
    if not values:
        return {"samples": 0}
    return {"samples": len(values), **{
        label: round(values[min(len(values) - 1, max(0, __import__("math").ceil(len(values) * q) - 1))], 6)
        for label, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


async def scenario(root, loop, seconds, healthy_peers):
    from pa.domain.models import FleetInstance
    from pa.fleet import overview as ov

    names = [f"healthy-{i}" for i in range(healthy_peers)] + ["stalled"]
    instances = [FleetInstance(instance_id=name, name=name, url=f"http://{name}.invalid")
                 for name in names]
    ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=root), services={
        "fleet_registry": SimpleNamespace(list_instances=lambda: instances)})
    cache = ov.cache_for(root)
    stop = asyncio.Event()
    starts, durations, samples, writes, drift = [], [], [], [], []
    original_write = ov.atomic_write_json
    origin, cpu = time.perf_counter(), time.process_time()

    async def wait(timeout):
        try:
            await asyncio.wait_for(stop.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True

    def write(path, payload):
        started = time.perf_counter()
        original_write(path, payload)
        writes.append({"bytes": path.stat().st_size, "wall_ms": (time.perf_counter() - started) * 1000})

    async def probe(_ctx, inst, dimension):
        started = time.perf_counter()
        starts.append({"instance": inst.instance_id, "dimension": dimension,
                       "at_seconds": started - origin,
                       "started_at": datetime.now(UTC).isoformat()})
        stalled = inst.instance_id == "stalled"
        # Only the disposable transport is delayed; real probe budgets are intact.
        await asyncio.sleep(4 if stalled else {"reachability": .02, "status": .04, "sync": .03}[dimension])
        elapsed = (time.perf_counter() - started) * 1000
        durations.append({"instance": inst.instance_id, "dimension": dimension, "ms": elapsed})
        return ov.field("timeout", error="synthetic bounded transport deadline", duration_ms=elapsed) if stalled else ov.field(
            "fresh", {"health": "up"} if dimension == "reachability" else {"consistent": True},
            observed_at=datetime.now(UTC).isoformat(), duration_ms=elapsed)

    with (
        patch("pa.server.shutdown.wait_for_shutdown", side_effect=wait),
        patch("pa.server.shutdown.is_shutting_down", side_effect=stop.is_set),
        patch("pa.fleet.overview._probe", side_effect=probe),
        patch("pa.fleet.overview.atomic_write_json", side_effect=write),
    ):
        background = asyncio.create_task(loop(ctx))
        until = origin + seconds
        expected = time.perf_counter()
        while time.perf_counter() < until:
            expected += .05
            await asyncio.sleep(max(0, expected - time.perf_counter()))
            stamp = datetime.now(UTC)
            drift.append(max(0, (time.perf_counter() - expected) * 1000))
            # Exclude the unchanged one-second startup plus first 40ms response.
            if time.perf_counter() - origin < 1.2:
                continue
            for inst in instances:
                for dimension in ov.REQUIRED_DIMENSIONS:
                    value = cache.get(inst.instance_id, dimension)
                    samples.append({"at_seconds": time.perf_counter() - origin,
                                    "instance": inst.instance_id, "dimension": dimension,
                                    "sampled_at": stamp.isoformat(),
                                    "observed_at": (value or {}).get("observed_at"),
                                    "wall_minus_monotonic_s": time.time() - time.monotonic(),
                                    "fresh": bool(value and ov._cache_is_fresh(value, dimension, now=stamp)),
                                    "attempt": ov.observation_attempt(value)})
        stop.set()
        await asyncio.wait_for(background, 5)
        # Shielded single-flight probes retain ownership through cache publication.
        pending = [task for (data_dir, _, _), (_, task) in list(ov._probe_tasks.items()) if data_dir == str(root)]
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending), 5)
    elapsed = time.perf_counter() - origin
    healthy = [v for v in samples if v["instance"].startswith("healthy")]
    stalled = [v for v in samples if v["instance"] == "stalled"]
    counts = Counter((v["instance"], v["dimension"]) for v in starts)
    return {"measurement_seconds": seconds, "including_cleanup_seconds": elapsed,
            "process_cpu_ms": (time.process_time() - cpu) * 1000,
            "healthy_samples": len(healthy), "healthy_stale_samples": sum(not v["fresh"] for v in healthy),
            "stalled_samples": len(stalled), "stalled_falsely_fresh": sum(v["fresh"] for v in stalled),
            "probe_starts": {f"{a}:{b}": n for (a, b), n in counts.items()},
            "logical_cache_writes": len(writes), "logical_cache_bytes": sum(v["bytes"] for v in writes),
            "write_wall_ms": distribution([v["wall_ms"] for v in writes]),
            "probe_wall_ms": distribution([v["ms"] for v in durations]), "loop_drift_ms": distribution(drift),
            "starts": starts, "durations": durations, "samples": samples, "writes": writes, "loop_drifts_ms": drift}


async def run(root, seconds, healthy_peers):
    from pa.fleet import overview as ov
    from pa.modules.fleet import _overview_refresh_loop

    source = subprocess.check_output(["git", "show", f"{BASE}:src/pa/modules/fleet.py"], text=True)
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_overview_refresh_loop")
    namespace = {"asyncio": asyncio, "logger": logging.getLogger(__name__), "AppContext": object,
                 "BACKGROUND_STARTUP_DELAY": ov.BACKGROUND_STARTUP_DELAY,
                 "BACKGROUND_REFRESH_INTERVAL": ov.BACKGROUND_REFRESH_INTERVAL,
                 "refresh_required_dimensions": ov.refresh_required_dimensions}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "pinned-baseline-loop", "exec"), namespace)
    baseline = await scenario(root / "baseline", namespace["_overview_refresh_loop"], seconds, healthy_peers)
    candidate = await scenario(root / "candidate", _overview_refresh_loop, seconds, healthy_peers)
    return {"contract": "pa.fleet-required-refresh-benchmark/v1", "observed_at": datetime.now(UTC).isoformat(),
            "baseline": BASE, "baseline_loop_ast_sha256": hashlib.sha256(ast.dump(node).encode()).hexdigest(),
            "source_sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (
                Path("src/pa/fleet/overview.py"), Path("src/pa/modules/fleet.py"))},
            "scenario": f"{healthy_peers + 1} synthetic peers/{3 * (healthy_peers + 1)} required fields; {healthy_peers}20-40ms healthy peers, one4s failed transport; unchanged1s startup/3s TTL/15s failure backoff. One sequential baseline then candidate; shared host, no pooled distribution or fleet acceptance.",
            "limits": "Logical dimension-cache writes/bytes only, not physical disk/WAL/fsync amplification. Private temporary caches; no real peer/network/provider/capture/config mutation. First1.2s excluded from freshness samples; all probe/writes retained including shielded cleanup.",
            "results": {"baseline": baseline, "candidate": candidate}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=24)
    parser.add_argument("--healthy-peers", type=int, default=2)
    args = parser.parse_args()
    if not 20 <= args.seconds <= 30:
        parser.error("measurement must be20–30seconds per stage")
    if not 1 <= args.healthy_peers <= 32:
        parser.error("healthy peer count must be1–32")
    if args.output.exists():
        parser.error("output already exists; retain prior evidence")
    with tempfile.TemporaryDirectory(prefix="pa-fleet-renewal-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"), PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        result = asyncio.run(run(root, args.seconds, args.healthy_peers))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "results": {
        stage: {k: v for k, v in value.items() if k not in {"starts", "durations", "samples", "writes", "loop_drifts_ms"}}
        for stage, value in result["results"].items()}}))


if __name__ == "__main__":
    main()
