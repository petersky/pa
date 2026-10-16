"""Disposable retained-dispatch and mixed SQLite workload; no installed PA data.

Run the identical script with PYTHONPATH pointing to a baseline checkout's src.
"""
from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import statistics
import tempfile
import time
import tracemalloc
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import AgentSession, CardCreate, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchWorker
from pa.modules.instance import _runtime_status_snapshot


class StatementCounts:
    def __init__(self):
        self.connect = sqlite3.connect
        self.counts = Counter()
        self.lock = Lock()

    def traced_connect(self, *args, **kwargs):
        conn = self.connect(*args, **kwargs)
        def trace(sql):
            # Retain only statement verbs; never SQL or fixture payloads.
            verb = sql.lstrip().split(None, 1)[0].upper()
            with self.lock:
                self.counts[verb] += 1
        conn.set_trace_callback(trace)
        return conn

    def take(self):
        with self.lock:
            result = dict(self.counts)
            self.counts.clear()
            return result


def distribution(samples):
    ordered = sorted(samples)
    return {f"p{p}_ms": round(ordered[math.ceil(len(ordered) * p / 100) - 1], 3)
            for p in (50, 95, 99)}


def measure(call, samples=30):
    call()
    timings, sizes, serialization = [], [], []
    tracemalloc.start()
    for _ in range(samples):
        start = time.perf_counter()
        value = call()
        timings.append((time.perf_counter() - start) * 1000)
        encoding = time.perf_counter()
        sizes.append(len(json.dumps(value, default=str).encode()))
        serialization.append((time.perf_counter() - encoding) * 1000)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {**distribution(timings), "json_bytes": int(statistics.median(sizes)),
            "serialization": distribution(serialization), "peak_allocated_bytes": peak}


async def run(root, statements):
    store = CardProjection(root / "pa.db")
    dispatches = DispatchStore(root)
    plan = {"candidates": [{"id": str(i), "evidence": "fixture " * 1000} for i in range(8)]}
    records = [DispatchRecord(
        mutation_id=f"m-{i}", card_id=f"c-{i}", authority_instance_id="local",
        authority_url="http://fixture.invalid", target_instance_id="local",
        state="completed", placement_decision=plan,
        request_payload={"execution_selection": {"placement_decision": plan}},
    ) for i in range(200)]
    # Real ledger writes, including indexes/publication, before measurement.
    for record in records:
        dispatches.put(record)
    runtime = AsyncRuntime()
    worker = DispatchWorker(dispatches, lambda _: asyncio.sleep(0))
    services = {"async_runtime": runtime, "dispatch_worker": worker, "dispatch_store": dispatches}
    ctx = SimpleNamespace(services=services, store=store, require_service=services.__getitem__)
    before = dispatches._commits
    statements.take()
    result = {
        "fixture": {"dispatches": 200, "samples": 30, "mixed_batches": 50},
        "runtime_snapshot": measure(lambda: _runtime_status_snapshot(ctx)),
        "dispatch_full": measure(lambda: dispatches.get(records[0].dispatch_id).public_dict()),
    }
    if hasattr(dispatches, "public_status"):
        result["dispatch_status"] = measure(lambda: dispatches.public_status(records[0].dispatch_id))
    result["status_dispatch_commits"] = dispatches._commits - before
    result["status_sql_statements"] = statements.take()
    public = dispatches.get(records[0].dispatch_id).public_dict()
    result["duplicate_placement_subtree_bytes"] = len(json.dumps(public["placement_decision"]).encode())
    assert public["execution_selection"]["placement_decision"] == public["placement_decision"]
    store.save_session(AgentSession(id="fixture-session", agent_name="fixture"))
    def mixed(kind):
        times = []
        with store._conn() as conn:
            changes_before = conn.total_changes
        for i in range(50):
            start = time.perf_counter()
            if kind == "transcript":
                store.append_transcript_events([TranscriptEvent(
                    session_id="fixture-session", seq=i * 10 + j + 1,
                    event_type="chunk", payload={"text": f"{i}-{j} " + "fixture " * 1000},
                ) for j in range(10)])
            elif kind == "session":
                store.save_session(AgentSession(id="fixture-session", agent_name="fixture", title=str(i)))
            elif kind == "card":
                store.create_card(CardCreate(title=f"fixture card {i}", auto_enrich=False), via_log=False)
            else:
                assert store.get_session("fixture-session")
                _runtime_status_snapshot(ctx)
            times.append((time.perf_counter() - start) * 1000)
        with store._conn() as conn:
            metadata_row_changes = conn.total_changes - changes_before
        store.close_thread_connection()
        return {**distribution(times), "metadata_row_changes": metadata_row_changes}
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {kind: pool.submit(mixed, kind) for kind in ("transcript", "session", "card", "read")}
        result["mixed"] = {kind: job.result() for kind, job in jobs.items()}
    result["mixed_sql_statements"] = statements.take()
    result["transcript_diagnostics"] = store.transcript_storage_metrics()
    if hasattr(store.transcripts, "performance"):
        result["transcript_performance"] = store.transcripts.performance.snapshot()
    dispatches.close()
    store.close_thread_connection()
    await runtime.close()
    if worker._control_runtime:
        await worker._control_runtime.close()
    return result


if __name__ == "__main__":
    statements = StatementCounts()
    with tempfile.TemporaryDirectory(prefix="pa-db-api-benchmark-") as tmp, patch("sqlite3.connect", statements.traced_connect):
        print(json.dumps(asyncio.run(run(Path(tmp), statements)), indent=2))
