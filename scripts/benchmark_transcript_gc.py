"""Bounded GC probe-size comparison; aggregate stdout, disposable synthetic data.

Run with the project environment. No live PA state, snapshots or raw logs are
read or retained. This measures catalog/file membership, not payload decoding,
full maintenance or deployed acceptance.
"""
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

from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.transcript_storage import TranscriptStorage
import pa.domain.transcript_storage as module


def benchmark(*, objects: int, pairs: int, orphans: int) -> dict:
    if not 1 <= objects <= 32768 or not 1 <= pairs <= 20 or not 0 <= orphans <= 1024:
        raise ValueError("fixture bounds: objects1..32768, pairs1..20, orphans0..1024")
    samples = {128: [], 1024: []}
    with tempfile.TemporaryDirectory(prefix="pa-gc-benchmark-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4 * 1024**3:
            raise RuntimeError("At least4GiB free space required before fixture creation")
        storage = TranscriptStorage(root / "fixture.db")
        rows = []
        for index in range(objects):
            digest = hashlib.sha256(str(index).encode()).hexdigest()
            path = storage._object_path(digest)
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"live fixture")
            rows.append((digest,))
        with storage._conn() as connection:
            connection.executemany(
                """INSERT INTO transcript_objects
                (hash,codec,uncompressed_bytes,compressed_bytes,ref_count,created_at)
                VALUES(?,'zlib',12,12,1,'2026-11-10')""", rows)
        for pair in range(pairs):
            for size in ([128, 1024] if pair % 2 == 0 else [1024, 128]):
                for index in range(orphans):
                    digest = hashlib.sha256(f"orphan:{index}".encode()).hexdigest()
                    path = storage._object_path(digest)
                    path.parent.mkdir(exist_ok=True)
                    path.write_bytes(b"orphan fixture")
                with patch.object(module, "_GC_READ_BATCH", size), sqlite_connection_scope():
                    started = time.perf_counter()
                    removed = storage.collect_unreferenced_objects()
                    elapsed = (time.perf_counter() - started) * 1000
                assert removed == orphans
                samples[size].append(elapsed)
        assert sum(1 for _ in storage.objects_dir.glob("*/*.zlib")) == objects
        for digest, in rows:
            assert storage._object_path(digest).read_bytes() == b"live fixture"
        with storage._conn() as connection:
            assert [tuple(row) for row in connection.execute("PRAGMA quick_check").fetchall()] == [("ok",)]
            assert tuple(connection.execute("SELECT COUNT(*),SUM(ref_count) FROM transcript_objects").fetchone()) == (objects, objects)

    def summary(values):
        ordered = sorted(values)
        return {"samples": len(values), "p50_ms": round(statistics.median(values), 3),
                "p95_ms": round(ordered[math.ceil(.95 * len(values)) - 1], 3),
                "p99_ms": round(ordered[math.ceil(.99 * len(values)) - 1], 3),
                "max_ms": round(max(values), 3)}

    return {"scope": "Alternating warm private GC probe sizes; same final algorithm, deletion proof bound128. Synthetic catalog/file membership, not durable event publication or full maintenance.",
            "objects_preserved": objects, "orphans_removed_per_call": orphans,
            "pairs": pairs, "probe128": summary(samples[128]),
            "probe1024": summary(samples[1024]),
            "expected_probe_queries_per_call": {str(size): math.ceil((objects + orphans) / size) for size in samples},
            "temporary_fixture_removed": not root.exists()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=24576)
    parser.add_argument("--pairs", type=int, default=12)
    parser.add_argument("--orphans", type=int, default=64)
    options = parser.parse_args()
    print(json.dumps(benchmark(**vars(options))))
