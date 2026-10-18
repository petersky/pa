"""Alternate full and recovery session inventories in disposable SQLite state.

This measures one startup read, not full Kernel/provider recovery or deployment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path


def run(root: Path, *, sessions: int, iterations: int, metrics_bytes: int) -> dict:
    from pa.core.performance import distribution
    from pa.domain.models import AgentSession
    from pa.domain.projection import CardProjection

    store = CardProjection(root / "fixture.db")
    now = datetime.now(UTC)
    diagnostics = {"synthetic": "x" * metrics_bytes}
    for index in range(sessions):
        store.save_session(AgentSession(
            id=f"session-{index:06}", agent_name="fixture",
            status=("closed", "idle", "recoverable_interrupted")[index % 3],
            config_json={"durable_runtime": {"pending_permissions": [{"id": "p"}]}}
            if index % 5 == 0 else {},
            metrics_json=diagnostics,
            created_at=now, updated_at=now + timedelta(seconds=index),
        ))
    modes = {name: {"wall_ms": [], "thread_cpu_ms": []} for name in ("full", "inventory")}
    expected = None
    for iteration in range(iterations):
        order = ("full", "inventory") if iteration % 2 == 0 else ("inventory", "full")
        for name in order:
            start, cpu_start = time.perf_counter(), time.thread_time()
            rows = store.list_sessions(include_metrics=name == "full")
            modes[name]["thread_cpu_ms"].append(round((time.thread_time() - cpu_start) * 1000, 3))
            modes[name]["wall_ms"].append(round((time.perf_counter() - start) * 1000, 3))
            # Compare identity, order and EVERY remaining model field, including
            # durable closed-session obligations and immutable workspace fields.
            values = [row.model_dump(mode="json", exclude={"metrics_json"}) for row in rows]
            if expected is None:
                expected = values
            assert values == expected
            assert len(rows) == sessions
            assert all(row.metrics_json == (diagnostics if name == "full" else {}) for row in rows)
    assert all(row.metrics_json == diagnostics for row in store.list_sessions())
    return {
        "contract": "pa.startup-inventory-benchmark/v1",
        "scope": "Alternating disposable projection reads on this host; no Kernel/provider/peer/backup/live workload. "
                 "Includes the same retained sessions, statuses, durable permission obligations and order in both modes. "
                 "Wall/CPU arrays are per-read samples; not a full startup distribution or physical I/O measurement.",
        "observed_at": datetime.now(UTC).isoformat(),
        "sessions": sessions, "iterations_each": iterations, "metrics_value_bytes": metrics_bytes,
        "diagnostic_json_bytes_per_full_read": sessions * len(json.dumps(diagnostics).encode()),
        "diagnostic_json_bytes_per_inventory_read": 0,
        "hydrated_models_each_mode": sessions * iterations,
        "all_nonmetrics_fields_and_order_equal": True, "persisted_metrics_unchanged": True,
        "nonmetrics_sha256": hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest(),
        "results": {name: {"raw": values, **{key: distribution(samples) for key, samples in values.items()}}
                    for name, values in modes.items()},
        "source_sha256": {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                          for name, module in (("pa.domain.projection", __import__("pa.domain.projection", fromlist=["_"])),
                                               ("pa.instance.agent_session", __import__("pa.instance.agent_session", fromlist=["_"])))},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=359)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--metrics-bytes", type=int, default=65536)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (1 <= args.sessions <= 1000 and 1 <= args.iterations <= 100 and 0 <= args.metrics_bytes <= 262144):
        parser.error("bounded fixture limits exceeded")
    with tempfile.TemporaryDirectory(prefix="pa-startup-inventory-") as temporary:
        root = Path(temporary)
        os.environ["PA_DATA_DIR"] = str(root / "data")
        os.environ["PA_WORKSPACE_ROOT"] = str(root / "workspaces")
        result = run(root, sessions=args.sessions, iterations=args.iterations, metrics_bytes=args.metrics_bytes)
    data = json.dumps(result, indent=2) + "\n"
    digest = hashlib.sha256(data.encode()).hexdigest()
    archive = args.output.with_name(args.output.stem + "." + digest + args.output.suffix)
    with archive.open("x") as stream:
        stream.write(data)
    args.output.write_text(data)
    print(json.dumps({"sha256": digest, "results": {name: {k: v for k, v in value.items() if k != "raw"}
                                                       for name, value in result["results"].items()}}))
