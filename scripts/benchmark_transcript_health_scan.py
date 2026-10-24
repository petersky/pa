"""Alternating fresh health measurements on disposable synthetic cold objects.

Compare the former per-reference Path.exists expression with directory scans,
keeping SQL, quick_check, event-derived references and returned health identical.
Never opens a running instance's data, starts a provider, or changes retention.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), **{
        name: round(values[max(0, math.ceil(p * len(values)) - 1)], 6)
        for name, p in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def run(objects, pairs):
    started = datetime.now(UTC).isoformat()
    rows = []
    with tempfile.TemporaryDirectory(prefix="pa-health-scan-benchmark-") as temporary:
        root = Path(temporary)
        store = TranscriptStorage(root / "data" / "fixture.db")
        for start in range(0, objects, 256):
            store.append([TranscriptEvent(
                session_id="retained", seq=i + 1, event_type="chunk",
                payload={"text": f"{i}:" + "retained cold fixture " * 300})
                for i in range(start, min(start + 256, objects))])
        assert len(store.referenced_hashes()) == objects
        real_exists, real_scan, real_stat = Path.exists, os.scandir, os.stat
        expected = None
        for pair in range(pairs):
            for mode in (["individual", "directory"] if pair % 2 == 0
                         else ["directory", "individual"]):
                counts = Counter()

                def exists(path, *args, **kwargs):
                    counts["path_exists"] += 1
                    return real_exists(path, *args, **kwargs)

                def scan(path):
                    counts["directory_scans"] += 1
                    return real_scan(path)

                def stat(path, *args, **kwargs):
                    counts["python_stat_calls"] += 1
                    return real_stat(path, *args, **kwargs)

                def individual():
                    return sum(not store._object_path(d).exists()
                               for d in store.referenced_hashes())

                # The full health path still executes every SQL aggregate and
                # quick_check. Only the existence-check implementation differs.
                with (patch.object(Path, "exists", exists), patch.object(os, "scandir", scan),
                      patch.object(os, "stat", stat)):
                    wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                    if mode == "individual":
                        with patch.object(store, "_missing_referenced_objects", individual):
                            health = store.metrics()
                    else:
                        health = store.metrics()
                    elapsed = (time.perf_counter_ns() - wall) / 1_000_000
                    thread_cpu = (time.thread_time_ns() - cpu) / 1_000_000
                if expected is None:
                    expected = health
                assert health == expected
                assert health["events"] == health["objects"] == health["references"] == objects
                assert health["missing_objects"] == 0 and health["integrity"] == "ok"
                rows.append({"pair": pair, "mode": mode, "wall_ms": elapsed,
                             "thread_cpu_ms": thread_cpu, **counts})
    return {"contract": "pa.transcript-health-directory-comparison/v1",
            "started_at": started, "finished_at": datetime.now(UTC).isoformat(),
            "objects": objects, "pairs": pairs,
            "scope": "Alternating full fresh health calls in one disposable synthetic fixture; "
                     "no production data, external provider or peer. Warm filesystem cache and "
                     "uncontrolled host load; samples are separate from mixed/deployed acceptance.",
            "source_sha256": hashlib.sha256(Path(__import__(
                "pa.domain.transcript_storage", fromlist=["__file__"]).__file__).read_bytes()).hexdigest(),
            "health": expected, "rows": rows,
            "summary": {mode: {metric: distribution([r.get(metric, 0) for r in rows if r["mode"] == mode])
                               for metric in ("wall_ms", "thread_cpu_ms", "path_exists", "directory_scans", "python_stat_calls")}
                        for mode in ("individual", "directory")}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--objects", type=int, default=12511)
    parser.add_argument("--pairs", type=int, default=10)
    args = parser.parse_args()
    if not (1 <= args.objects <= 15000 and 1 <= args.pairs <= 30):
        parser.error("objects must be1..15000 and pairs1..30")
    if args.output.exists():
        parser.error("output already exists; preserve earlier evidence")
    result = run(args.objects, args.pairs)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"], indent=2))
