"""Bounded fresh-reference/health comparison; aggregate stdout, disposable data."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from unittest.mock import patch

from pa.core.performance import OperationObservation, current_observation
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.transcript_storage import TranscriptStorage
import pa.domain.transcript_storage as module


def summary(values):
    ordered = sorted(values)
    return {"samples": len(values), "p50_ms": round(statistics.median(values), 3),
            "p95_ms": round(ordered[math.ceil(.95 * len(values)) - 1], 3),
            "p99_ms": round(ordered[math.ceil(.99 * len(values)) - 1], 3),
            "max_ms": round(max(values), 3)}


def benchmark(*, objects=24576, pairs=20):
    if not 1 <= objects <= 32768 or not 1 <= pairs <= 30:
        raise ValueError("fixture bounds: objects1..32768, pairs1..30")
    samples = {128: [], 1024: []}
    with tempfile.TemporaryDirectory(prefix="pa-health-benchmark-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4 * 1024**3:
            raise RuntimeError("At least4GiB free space required before seeding")
        storage = TranscriptStorage(root / "fixture.db")
        hashes = {hashlib.sha256(str(i).encode()).hexdigest() for i in range(objects)}
        for digest in hashes:
            path = storage._object_path(digest)
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"immutable synthetic fixture")
        with storage._conn() as connection:
            connection.executemany(
                """INSERT INTO transcript_objects
                (hash,codec,uncompressed_bytes,compressed_bytes,ref_count,created_at)
                VALUES(?,'zlib',27,27,1,'2026-11-10')""", ((h,) for h in hashes))
            connection.executemany(
                """INSERT INTO transcript_events
                (id,session_id,seq,event_type,hot_payload,payload_hash,cold_hash,created_at)
                VALUES(?,'fixture',?,'output','{}',?,?,'2026-11-10')""",
                ((h, i, h, h) for i, h in enumerate(hashes)))
        expected_health = None
        for pair in range(pairs):
            for size in ([128, 1024] if pair % 2 == 0 else [1024, 128]):
                observation = OperationObservation()
                token = current_observation.set(observation)
                try:
                    with patch.object(module, "_HEALTH_REFERENCE_BATCH", size), sqlite_connection_scope():
                        wall, cpu = time.perf_counter(), time.thread_time()
                        references = storage.referenced_hashes()
                        reference_ms = (time.perf_counter() - wall) * 1000
                        reference_cpu_ms = (time.thread_time() - cpu) * 1000
                        reference_calls = observation.sqlite_calls
                        wall = time.perf_counter()
                        health = storage.metrics()
                        health_ms = (time.perf_counter() - wall) * 1000
                finally:
                    current_observation.reset(token)
                assert references == hashes
                assert health["events"] == health["objects"] == health["references"] == objects
                assert health["integrity"] == "ok" and health["missing_objects"] == 0
                if expected_health is None:
                    expected_health = health
                assert health == expected_health
                samples[size].append((reference_ms, reference_cpu_ms, health_ms, reference_calls))
        for digest in hashes:
            assert storage._object_path(digest).read_bytes() == b"immutable synthetic fixture"
        with storage._conn() as connection:
            assert connection.execute("SELECT COUNT(*),SUM(ref_count) FROM transcript_objects").fetchone()[:] == (objects, objects)
    return {"scope": "Alternating warm private read-page bounds on the final query shape. Synthetic reference catalog/file membership, not payload decoding, real-provider, full maintenance or deployed acceptance.",
            "objects_preserved": objects, "pairs": pairs,
            **{f"page{size}": {"references": summary([r[0] for r in values]),
                               "reference_thread_cpu": summary([r[1] for r in values]),
                               "fresh_full_health": summary([r[2] for r in values]),
                               "observed_reference_sqlite_calls": sorted({r[3] for r in values}),
                               "expected_reference_pages": math.ceil(objects / size) + 1}
               for size, values in samples.items()},
            "all_health_values_equal": True, "temporary_fixture_removed": not root.exists()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=24576)
    parser.add_argument("--pairs", type=int, default=20)
    print(json.dumps(benchmark(**vars(parser.parse_args()))))
