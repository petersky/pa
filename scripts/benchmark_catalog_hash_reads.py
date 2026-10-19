"""Compare catalog hash readers on a disposable derived index, without GC.

Uses one trusted tracked baseline method with the current supporting code.
Never opens PA's live data directory, object files, refs, or backup storage.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import inspect
import json
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from pa.core.performance import OperationObservation, current_observation, distribution
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.sync import object_catalog

BASELINE = "1a4d01c4cb900d69d661f69ea4a00e2e5c88aebc"


def baseline_reader():
    source = subprocess.check_output(
        ["git", "show", BASELINE + ":src/pa/sync/object_catalog.py"],
        cwd=Path(__file__).resolve().parents[1], text=True,
    )
    cls = next(node for node in ast.parse(source).body
               if isinstance(node, ast.ClassDef) and node.name == "ObjectCatalog")
    method = next(node for node in cls.body
                  if isinstance(node, ast.FunctionDef) and node.name == "iter_hashes")
    namespace = dict(vars(object_catalog))
    exec(compile(ast.Module(body=[method], type_ignores=[]),
                 "<tracked-catalog-baseline>", "exec"), namespace)
    return namespace["iter_hashes"], hashlib.sha256(source.encode()).hexdigest()


def run(root: Path, rows: int, iterations: int) -> dict:
    baseline, baseline_module_hash = baseline_reader()
    source = object_catalog.ObjectCatalog.iter_hashes
    catalog = object_catalog.ObjectCatalog(root / "disposable-derived-catalog.db")
    hashes = [hashlib.sha256(f"catalog-reader-{i}".encode()).hexdigest() for i in range(rows)]
    with catalog._db() as conn:
        conn.executemany(
            "INSERT INTO objects(object_hash,size_bytes,mtime_ns,object_class,realm_id,recorded_at) VALUES(?,?,?,?,?,?)",
            [(h, 1, 1, "unknown", "fixture", "fixture") for h in hashes],
        )
    expected = sorted(hashes)
    trials = []
    for pair in range(iterations):
        methods = [("baseline", baseline), ("source", source)]
        if pair % 2:
            methods.reverse()
        for name, reader in methods:
            observation = OperationObservation()
            token = current_observation.set(observation)
            wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
            try:
                with sqlite_connection_scope():
                    actual = reader(catalog)
            finally:
                current_observation.reset(token)
            wall_ms = (time.perf_counter_ns() - wall) / 1_000_000
            cpu_ms = (time.thread_time_ns() - cpu) / 1_000_000
            assert actual == expected
            trials.append({
                "pair": pair, "method": name, "wall_ms": round(wall_ms, 3),
                "thread_cpu_ms": round(cpu_ms, 3), "sqlite_calls": observation.sqlite_calls,
                "sqlite_rows": observation.sqlite_rows,
                "sqlite_busy_failures": observation.sqlite_busy_failures,
            })
    return {
        "baseline_revision": BASELINE, "baseline_module_sha256": baseline_module_hash,
        "source_module_sha256": hashlib.sha256(Path(object_catalog.__file__).read_bytes()).hexdigest(),
        "source_method_sha256": hashlib.sha256(inspect.getsource(source).encode()).hexdigest(),
        "rows": rows, "iterations_per_method": iterations, "trials": trials,
        "ordered_output_sha256": hashlib.sha256(json.dumps(expected).encode()).hexdigest(),
        "results": {name: {
            "wall_ms": distribution([t["wall_ms"] for t in trials if t["method"] == name]),
            "thread_cpu_ms": distribution([t["thread_cpu_ms"] for t in trials if t["method"] == name]),
            "observed_sqlite_calls_per_trial": sorted({t["sqlite_calls"] for t in trials if t["method"] == name}),
            "observed_rows_per_trial": sorted({t["sqlite_rows"] for t in trials if t["method"] == name}),
        } for name in ("baseline", "source")},
        "scope": "Only the tracked baseline reader differs. Identical ordered SELECT and full output; supporting source shared. Warm filesystem/uncontrolled host; observed execute/fetch calls are not distinct SQL queries or physical disk IO. No GC plan execution, object deletion, live Store/EventLog, backup work, network or deployment acceptance.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, choices=range(1, 10001), default=1000, metavar="1..10000")
    parser.add_argument("--iterations", type=int, choices=range(1, 101), default=20, metavar="1..100")
    parser.add_argument("--fixture-parent", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), "Keep the original artifact; choose a new output path"
    started_at = datetime.now(UTC).isoformat()
    with tempfile.TemporaryDirectory(prefix="pa-catalog-reader-", dir=args.fixture_parent) as root:
        result = run(Path(root), args.rows, args.iterations)
    result.update(started_at=started_at, finished_at=datetime.now(UTC).isoformat())
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "trials"}))
