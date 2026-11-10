"""Bounded private WAL cancellation probe; aggregate stdout, no retained fixtures."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext
import json
import math
from pathlib import Path
import shutil
import sqlite3
import statistics
import tempfile
import threading
import time
from unittest.mock import patch

from pa.core.sqlite_metrics import ObservedConnection
from pa.core.sqlite_maintenance import DatabaseMaintenanceGate
from pa.instance.maintenance import _maintain_database


def summary(values):
    ordered = sorted(values)
    return {"samples": len(values), "p50_ms": round(statistics.median(values), 3),
            "p95_ms": round(ordered[math.ceil(.95 * len(values)) - 1], 3),
            "p99_ms": round(ordered[math.ceil(.99 * len(values)) - 1], 3),
            "max_ms": round(max(values), 3)}


def benchmark(*, pairs=3, rows=32768, writer_intent=False, writes=1):
    if not 1 <= pairs <= 10 or not 16384 <= rows <= 65536 or not 1 <= writes <= 100:
        raise ValueError("fixture bounds: pairs1..10, rows16384..65536, writes1..100")
    samples = {False: [], True: []}
    all_writes = {False: [], True: []}
    with tempfile.TemporaryDirectory(prefix="pa-maintenance-yield-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4 * 1024**3:
            raise RuntimeError("At least4GiB free space required before seeding")
        for pair in range(pairs):
            for yielding in ([False, True] if pair % 2 == 0 else [True, False]):
                path = root / "fixture.db"
                with closing(sqlite3.connect(path)) as seed, seed:
                    seed.execute("PRAGMA journal_mode=WAL")
                    seed.execute("PRAGMA synchronous=FULL")
                    seed.execute("CREATE TABLE retained(id INTEGER PRIMARY KEY, body BLOB)")
                    seed.executemany("INSERT INTO retained VALUES(?,?)",
                                     ((i, b"x" * 4096) for i in range(rows)))
                    seed.execute("CREATE TABLE foreground(value INTEGER)")
                    seed.execute("INSERT INTO foreground VALUES(0)")
                    seed.execute("DELETE FROM retained WHERE id<64")
                entered, stop = threading.Event(), threading.Event()
                gate = DatabaseMaintenanceGate() if yielding and writer_intent else None
                local = threading.local()
                execute, progress = ObservedConnection.execute, ObservedConnection.set_progress_handler

                def observe_execute(connection, sql, *args):
                    local.in_vacuum = sql == "VACUUM"
                    try:
                        return execute(connection, sql, *args)
                    finally:
                        local.in_vacuum = False

                def observe_progress(connection, callback, interval):
                    def wrapped():
                        if getattr(local, "in_vacuum", False):
                            entered.set()
                        return callback()
                    return progress(connection, wrapped, interval)

                # Open/prepare before maintenance; initialization itself can wait
                # for VACUUM and would hide cancellation responsiveness.
                with closing(sqlite3.connect(path, timeout=10)) as writer:
                    writer.execute("PRAGMA synchronous=FULL")
                    writer.execute("SELECT value FROM foreground").fetchone()
                    with (ThreadPoolExecutor(max_workers=1) as pool,
                          patch.object(ObservedConnection, "execute", observe_execute),
                          patch.object(ObservedConnection, "set_progress_handler", observe_progress)):
                        future = pool.submit(_maintain_database, path, stop=stop, writers=gate)
                        assert entered.wait(10), "No actual VACUUM progress"
                        for index in range(writes):
                            started = time.perf_counter()
                            if yielding and not writer_intent:
                                stop.set()
                            with gate.writer() if gate is not None else nullcontext():
                                writer.execute("UPDATE foreground SET value=value+1")
                                writer.commit()
                            elapsed = (time.perf_counter() - started) * 1000
                            all_writes[yielding].append(elapsed)
                            if index == 0:
                                writer_ms = elapsed
                        result = future.result(timeout=20)
                with closing(sqlite3.connect(path)) as verify:
                    assert verify.execute("PRAGMA quick_check").fetchall() == [("ok",)]
                    assert verify.execute("SELECT COUNT(*),SUM(length(body)),MIN(id),MAX(id),SUM(body!=?) FROM retained", (b"x"*4096,)).fetchone() == (rows-64, (rows-64)*4096, 64, rows-1, 0)
                    assert verify.execute("SELECT value FROM foreground").fetchone() == (writes,)
                # A completed VACUUM can have a busy final checkpoint when the
                # foreground writer proceeds. Always finish a real quiet retry.
                retry = _maintain_database(path)
                assert retry["status"] == "ok" and retry["quick_check"] == "ok"
                assert retry["after"]["free_bytes"] == 0
                assert retry["after"]["bytes"] < result["before"]["bytes"]
                if yielding:
                    # A stop observed only after VACUUM commits is not a prompt
                    # yield. Scheduling can even finish the sweep before the
                    # requesting thread runs; neither counts as an early yield.
                    assert retry["compacted"] or result["compacted"]
                else:
                    assert result["compacted"]
                samples[yielding].append((writer_ms, result["phases"]["vacuum"]["wall_ms"],
                                          retry["phases"]["total"]["wall_ms"], result["compacted"],
                                          result.get("reason") == "stopping",
                                          result.get("reason") == "foreground_write"))
                for file in root.iterdir():
                    file.unlink()  # Only files generated in this disposable root.
    return {"scope": "Private existing WAL, FULL-synchronous retained data and writer. Explicit owner-local writer-intent mode exercises the production maintenance gate; default mode requests shutdown. Neither is deployed or whole-card acceptance.",
            "pairs": pairs, "retained_rows": rows-64, "writer_intent": writer_intent,
            "durable_updates_per_trial": writes,
            "baseline": {"writer": summary([r[0] for r in samples[False]]),
                         "all_writes": summary(all_writes[False]),
                         "vacuum": summary([r[1] for r in samples[False]])},
            "yield_prototype": {"writer": summary([r[0] for r in samples[True]]),
                                "all_writes": summary(all_writes[True]),
                                "vacuum_until_return": summary([r[1] for r in samples[True]]),
                                "complete_quiet_retry": summary([r[2] for r in samples[True]]),
                                "already_compacted_before_return": sum(r[3] for r in samples[True]),
                                "stop_observed": sum(r[4] for r in samples[True]),
                                "foreground_deferred": sum(r[5] for r in samples[True])},
            "quiet_retries_ok": 2*pairs, "retained_data_checks_ok": 2*pairs,
            "temporary_fixture_removed": not root.exists()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--writer-intent", action="store_true")
    parser.add_argument("--writes", type=int, default=1)
    print(json.dumps(benchmark(**vars(parser.parse_args()))))
