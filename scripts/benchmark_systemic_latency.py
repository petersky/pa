"""Disposable mixed-load acceptance inventory, never an installed PA data dir.

Use identical script + PYTHONPATH=<baseline src> for a source comparison.
No provider process, external peer traffic, restart, or production fault injection.
"""
from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import json
import logging
import math
import os
import platform
import resource
import sqlite3
import sys
import tempfile
import time
from collections import Counter, defaultdict, deque
from pathlib import Path
from threading import Lock
from unittest.mock import patch

import httpx

from pa.acp.client import WireJsonlLogger
from pa.acp import transcript_page  # Include lazy history compaction in source attestation.
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.performance import FILESYSTEM_PHASES
from pa.domain.models import AgentSession, CardEvent, EventType, SyncCommit
from pa.execution.dispatch import DispatchRecord
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase
from pa.fleet.overview import FleetOverviewCache, field
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.sync import _apply_sync_push_local
from pa.modules import instance as instance_module  # Attest runtime diagnostics before measurement.
from pa.pr_supervisor.models import PRWatch
from pa.status import info as status_info  # Attest the lazy status count path before measurement.


def distribution(values):
    ordered = sorted(values)
    return {"samples": len(ordered), "max_ms": round(max(ordered), 3) if ordered else None,
            **{f"p{p}_ms": round(ordered[math.ceil(len(ordered) * p / 100) - 1], 3)
               if ordered else None for p in (50, 95, 99)}}


class Counts:
    def __init__(self):
        self.lock = Lock()
        self.statements = Counter()
        self.objects = Counter()
        self.connect = sqlite3.connect

    def connection(self, *args, **kwargs):
        conn = self.connect(*args, **kwargs)
        def trace(sql):
            with self.lock:
                self.statements[sql.lstrip().split(None, 1)[0].upper()] += 1
        conn.set_trace_callback(trace)
        return conn


async def run(root, *, history=1000, iterations=30, inventory_reads=False):
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
        agent_enabled=False, telemetry_enabled=False, auth_required=False)
    app = Kernel.boot(settings=settings).build_app()
    async with app.router.lifespan_context(app):
        return await run_app(app, root, history=history, iterations=iterations, inventory_reads=inventory_reads)


async def run_app(app, root, *, history, iterations, inventory_reads=False):
    ctx = app.state.ctx
    settings = ctx.settings
    runtime = ctx.require_service("async_runtime")
    manager = ctx.require_service("instance_agent")
    manager._startup_complete = True
    store = ctx.store
    log = ctx.require_service("event_log")
    engine = ctx.require_service("sync_engine")
    dispatches = ctx.require_service("dispatch_store")
    watches = ctx.require_service("pr_supervisor_store")
    counts = Counts()
    card_id = "fixture-card"
    def event(i, *, create=False):
        return CardEvent(type=EventType.CARD_CREATED if create else EventType.CARD_UPDATED,
            realm_id="default", card_id=card_id, author_principal="user:fixture",
            author_instance="fixture", payload={"id": card_id, "title": f"Fixture {i}"})
    def seed():
        for i in range(history):
            store.commit_event(event(i, create=i == 0))
        proof = {"candidates": [{"proof": "fixture " * 1000} for _ in range(8)]}
        for i in range(100):
            dispatches.put(DispatchRecord(mutation_id=f"m-{i}", card_id=f"old-{i}",
                authority_instance_id=settings.instance_id, target_instance_id=settings.instance_id,
                authority_url="http://fixture", state="completed", placement_decision=proof))
        sessions = []
        for i in range(30):
            session = AgentSession(id=f"session-{i}", agent_name="codex", purpose="chat",
                status="idle", config_json={"execution_selection": {
                    **proof, "decision_id": f"fixture-selection-{i}",
                    "selected": {"harness": "codex", "model": "fixture"}},
                    "execution_context": {"repositories": [{"repository_url": "https://github.com/example/repo",
                                                              "branch": "fixture"}]},
                    "values": {"model": "fixture", "reasoningEffort": "high"}})
            store.save_session(session)
            sessions.append(session)
            watches.upsert_watch(PRWatch(repository="example/repo", pr_number=i + 1,
                pr_url=f"https://github.com/example/repo/pull/{i+1}", realm_id="default",
                originating_session_id=session.id, status="closed", state={"fixture_observations": proof}))
        active = dispatches.put(DispatchRecord(mutation_id="active", card_id=card_id,
            session_id=sessions[0].id, authority_instance_id=settings.instance_id,
            target_instance_id=settings.instance_id, authority_url="http://fixture",
            state="running", progress_protocol_version=1, placement_decision=proof))
        return sessions, active
    sessions, active = await asyncio.to_thread(seed)
    # Synthetic sessions have no provider; enable only the read/list admission
    # used by this fixture while agent_enabled=False prevents process creation.
    manager._accepting = True
    await asyncio.sleep(0.05)
    cache = FleetOverviewCache(root / "cache")
    wire = WireJsonlLogger(root / "fixture-wire.jsonl")
    transcript_runtimes = [AgentSessionRuntime(manager, s, initial_transcript_seq=0) for s in sessions[:3]]
    samples, sizes = defaultdict(list), defaultdict(list)
    sync_outcomes = Counter()
    gc_started, gc_pauses, gc_counts = {}, deque(maxlen=256), Counter()
    long_gc_pauses = deque(maxlen=64)
    long_gc_count = 0
    def observe_gc(phase, info):
        nonlocal long_gc_count
        generation = info["generation"]
        if phase == "start":
            gc_started[generation] = time.perf_counter()
        elif generation in gc_started:
            gc_counts[generation] += 1
            pause = {"generation": generation,
                "started_at_monotonic": gc_started[generation],
                "finished_at_monotonic": time.perf_counter(),
                "elapsed_ms": (time.perf_counter() - gc_started.pop(generation)) * 1000,
                "collected": info["collected"], "uncollectable": info["uncollectable"]}
            gc_pauses.append(pause)
            # Diagnostic selection only; warning/deadline thresholds stay fixed.
            # Tiny later collections must not erase an earlier correlated pause.
            if pause["elapsed_ms"] >= 10:
                long_gc_count += 1
                long_gc_pauses.append(pause)
    warnings = Counter()
    class WarningCounts(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.WARNING:
                warnings[record.getMessage().split(" operation=")[0]] += 1
    handler = WarningCounts()
    logging.getLogger().addHandler(handler)
    original_get = log.store.get
    def read_object(digest):
        value = original_get(digest)
        with counts.lock:
            counts.objects["reads"] += 1
            counts.objects["bytes"] += len(value or b"")
        return value
    async def measure(name, awaitable):
        started = time.perf_counter()
        value = await awaitable
        samples[name].append((time.perf_counter() - started) * 1000)
        return value
    async def offload(name, call, *args, **kwargs):
        return await measure(name, runtime.run_blocking(name, call, *args, **kwargs))
    async def sync_work():
        for i in range(iterations):
            # Imported objects are canonical and validated by the normal sync
            # transaction; object creation is included in its workload sample.
            def push():
                parent = log.get_head("default")
                digest = log.store.put_json(event(history + 2 * i).model_dump(mode="json"))
                commit = SyncCommit(hash="", realm_id="default", instance_id="peer",
                    parent_hashes=[parent], event_hashes=[digest], author_principal="user:fixture")
                commit.hash = log.store.put_json(commit.model_dump(mode="json"))
                result = _apply_sync_push_local(ctx, "default", commit.hash, {})
                assert log.is_ancestor(commit.hash, result["head"]), "push omitted requested head"
                return result
            result = await offload("sync.push_transaction", push)
            # Real background writers can require a legitimate merge marker as
            # well as the incoming event. Verify inclusion, not an exact count.
            assert result["commits_applied"] >= 1, result
            sync_outcomes[f"push_applied_{result['commits_applied']}"] += 1
            # Exercise the application's actual incoming-head callback as well
            # as HTTP push. Object import precedes this publication scenario.
            def prepare_reconcile():
                parent = log.get_head("default")
                digest = log.store.put_json(event(history + 2 * i + 1).model_dump(mode="json"))
                commit = SyncCommit(hash="", realm_id="default", instance_id="peer",
                    parent_hashes=[parent], event_hashes=[digest], author_principal="user:fixture")
                return log.store.put_json(commit.model_dump(mode="json"))
            target = await asyncio.to_thread(prepare_reconcile)
            if i % 10 == 0:
                # A normal, unrelated canonical writer creates a compatible
                # fork between object import and publication. No faults or
                # external service state are involved in this fixture.
                def concurrent_write():
                    identity = f"concurrent-{i}"
                    store.commit_event(CardEvent(type=EventType.CARD_CREATED,
                        realm_id="default", card_id=identity, author_principal="user:fixture",
                        author_instance="fixture", payload={"id": identity, "title": identity}))
                await offload("sqlite.card_write", concurrent_write)
            def reconcile_checked(realm_id, incoming):
                result = engine._reconcile_remote_head(realm_id, incoming)
                if result.get("advanced"):
                    assert log.is_ancestor(incoming, result["head"]), "reconcile omitted requested head"
                return result
            reconciled = await measure("sync.reconcile_head", engine.apply_realm_head(
                "default", target, "sync.reconcile_head", reconcile_checked,
                "default", target))
            assert reconciled["advanced"], reconciled
            sync_outcomes["reconcile_merged" if reconciled["head"] != target else "reconcile_fast_forward"] += 1
            if inventory_reads and i % 10 == 0:
                # The actual bounded head-first inventory builder used by v3
                # peers; no peer request, publication, or synthetic fault.
                def inventory():
                    head = log.get_head("default")
                    objects, parents = engine._inventory_page([head], set(), [])
                    assert head in objects and head in parents
                    return {"objects": len(objects), "bytes": sum(map(len, objects.values()))}
                page = await offload("sync.object_collect", inventory)
                sync_outcomes["inventory_pages"] += 1
                sync_outcomes["inventory_objects"] += page["objects"]
                sync_outcomes["inventory_bytes"] += page["bytes"]
            await asyncio.sleep(0.005)
    async def startup_work():
        for i in range(iterations):
            def bind():
                session = AgentSession(id=f"cold-{i}", agent_name="codex")
                store.save_session(session)
                return store.set_session_execution_binding(session.id,
                    {"version": 1, "origin_instance_id": settings.instance_id},
                    reason="workspace_binding_initialized", expected_binding={})
            await offload("sqlite.execution_binding_materialize", bind)
            await asyncio.sleep(0.005)
    async def transcript_work(worker):
        for i in range(iterations):
            for j in range(32):
                worker._append_transcript("agent_message_chunk", {"text": f"{i}-{j} " + "fixture " * 100})
            await measure("transcript.drain", worker._drain_transcripts(raise_on_timeout=True))
            await asyncio.sleep(0.005)
    async def progress_work():
        for i in range(iterations):
            seq = await offload("progress.sequence_allocate", dispatches.allocate_progress_sequence, active.dispatch_id)
            progress = DispatchProgressEventV1(card_id=card_id, dispatch_id=active.dispatch_id,
                acp_session_id=sessions[0].id, originating_instance_id=settings.instance_id,
                authority_instance_id=settings.instance_id, sequence=seq, idempotency_key=f"checkpoint-{i}",
                phase=ProgressPhase.TESTING, summary="Fixture checkpoint")
            receipt = await offload("progress.checkpoint_write", dispatches.ingest_progress, progress, delivered=True)
            assert receipt.accepted, receipt.status
            await offload("dispatch.runnable_read", dispatches.runnable)
            from datetime import UTC, datetime
            await offload("reconciliation.dispatch_read", dispatches.reconciliation_candidates,
                          due_at=datetime.now(UTC), limit=100)
            await asyncio.sleep(0.005)
    async def cache_and_wire():
        for i in range(iterations):
            await offload("fleet.overview_cache_write", cache.put, "local", "sync", field("fresh", {"head": str(i)}))
            await offload("fleet.overview_cache_read", cache.get, "local", "sync")
            await offload("acp.wire_append", wire.log, "in", {"type": "fixture", "number": i})
            await offload("sync.commit_read", log.get_commit, log.get_head("default"))
            await asyncio.sleep(0.005)
    async def foreground():
        paths = ["/api/health", "/api/agent/sessions?view=chats", "/agent",
                 "/partials/home/sections", "/api/status", "/api/runtime?section=sse",
                 "/api/fleet/dispatch-jobs"]
        if inventory_reads:
            # Explicit harmless local reads, never execute URLs from findings
            # data. No SSE stream, provider action or external refresh request.
            paths += ["/api/cards", f"/api/cards/{card_id}",
                      f"/api/cards/{card_id}/history", "/api/agent/history/session-0?limit=64",
                      "/api/agent/history/session-0?limit=64&message_boundaries=true",
                      "/api/notifications", "/api/agent/preferences", "/api/execution/defaults",
                      "/api/fleet/dispatch-queue", "/api/pr-supervisor/watches",
                      "/api/pr-supervisor/capabilities", "/api/workspaces",
                      "/api/health-journal/status", "/api/sync/status", "/api/sync/refs",
                      "/settings?section=agent", "/settings?section=backups", "/fleet", "/pull-requests",
                      "/", "/work", "/api/runtime"]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            for _ in range(iterations):
                for path in paths:
                    response = await measure(path, client.get(path))
                    response.raise_for_status()
                    if path == "/agent":
                        assert "PR #1 · closed" in response.text
                    sizes[path].append(len(response.content))
                await asyncio.sleep(0.005)
    await runtime.start()
    runtime.reset_lag_samples()
    runtime_before = runtime.snapshot()
    source_hashes = {name: hashlib.sha256(Path(sys.modules[name].__file__).read_bytes()).hexdigest()
                     for name in ("pa.core.async_runtime", "pa.core.performance", "pa.core.sqlite_metrics",
                                  "pa.core.io", "pa.core.operation_budget", "pa.sync.object_store",
                                  "pa.domain.projection", "pa.domain.transcript_storage", "pa.domain.card_summary_service", "pa.modules.items",
                                  "pa.execution.dispatch", "pa.fleet.overview", "pa.sync.engine",
                                  "pa.sync.event_log", "pa.sync.dag_index", "pa.acp.transcript_page",
                                  "pa.instance.agent_session", "pa.pr_supervisor.store",
                                  "pa.pr_supervisor.service", "pa.status.info", "pa.modules.instance")}
    diagnostics = ctx.services.get("runtime_diagnostics")
    diagnostics_before = diagnostics.snapshot() if diagnostics else {}
    before_io = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    gc.callbacks.append(observe_gc)
    try:
        with patch("sqlite3.connect", counts.connection), patch.object(log.store, "get", side_effect=read_object):
            await asyncio.gather(sync_work(), startup_work(), progress_work(), cache_and_wire(), foreground(),
                                 *(transcript_work(worker) for worker in transcript_runtimes))
        elapsed = time.perf_counter() - started
        after_io = resource.getrusage(resource.RUSAGE_SELF)
        snapshot = runtime.snapshot()
        expected = iterations * 32 * len(transcript_runtimes)
        actual = 0
        for session in sessions[:3]:
            cursor = 0
            while page := store.list_transcript_events(session.id, after_seq=cursor, limit=1000):
                assert [event.seq for event in page] == list(range(cursor + 1, cursor + 1 + len(page)))
                actual += len(page)
                cursor = page[-1].seq
            assert cursor == iterations * 32
        assert actual == expected
        assert store.get_projection_head("default") == log.get_head("default")
        assert store.get_card(card_id).title == f"Fixture {history + 2 * iterations - 1}"
        return {"hardware": {"machine": platform.machine(), "python": platform.python_version(), "cpus": os.cpu_count()},
            "source_module_sha256": source_hashes,
            "measurement_start_monotonic": started, "measurement_finish_monotonic": time.perf_counter(),
            "premeasurement": {"scope": "Kernel boot and synthetic fixture seeding; not part of steady mixed window",
                               "slow_calls": sum(value.get("slow_calls", 0) for value in runtime_before["operations"].values()),
                               "async_backend_initialization": runtime_before.get("async_backend_initialization"),
                               "slow_observations": runtime_before.get("slow_operations", {}),
                               "acceptance": "reported separately; full server cold boot remains unaccepted"},
            "fixture": {"history_commits": history, "retained_dispatches": 101, "sessions": 30,
                "workload_version": 5 if inventory_reads else 3,
                "foreground_inventory_reads": inventory_reads,
                "sync_pushes": iterations, "incoming_reconciliations": iterations,
                "controlled_compatible_forks": (iterations + 9) // 10,
                "bounded_sync_inventory_pages": (iterations + 9) // 10 if inventory_reads else 0,
                "watches": 30, "iterations_per_family": iterations, "transcript_sessions": 3,
                "executor_workers": runtime.max_workers, "duration_seconds": round(elapsed, 3),
                "limits": "single process, local disposable storage, synthetic providers, no external peer/provider time"},
            "latency": {key: distribution(values) for key, values in samples.items()},
            "response_bytes": {key: {"min": min(values), "max": max(values)} for key, values in sizes.items()},
            "warnings": dict(warnings), "sql_statements": dict(counts.statements),
            "sync_outcomes": dict(sync_outcomes),
            "threshold_violations": {key: sum(value >= (2000 if key == "dispatch.runnable_read" else 500)
                                              for value in values)
                                     for key, values in samples.items()
                                     if any(value >= (2000 if key == "dispatch.runnable_read" else 500)
                                            for value in values)},
            "operation_counter_deltas": {name: {key: (value[key] - runtime_before.get("operations", {}).get(name, {}).get(key, 0)
                                                    if key in value else None)
                for key in ("submitted", "completed", "failed", "timed_out", "rejected", "slow_calls")}
                for name, value in snapshot["operations"].items()},
            "diagnostics_runtime": diagnostics.snapshot() if diagnostics else None,
            "diagnostics_counter_deltas": {name: {key: value[key] - diagnostics_before.get("operations", {}).get(name, {}).get(key, 0)
                for key in ("submitted", "completed", "failed", "timed_out", "rejected", "slow_calls")}
                for name, value in (diagnostics.snapshot()["operations"] if diagnostics else {}).items()},
            "filesystem_phase_totals_delta": {
                name: {phase: value["filesystem_totals"][phase] - runtime_before["operations"].get(name, {}).get("filesystem_totals", {}).get(phase, 0)
                       for phase in FILESYSTEM_PHASES}
                for name, value in snapshot["operations"].items()},
            "filesystem_phase_scope": "Exact completed-worker logical counters during measurement, separate from last256 distributions and premeasurement boot/seeding. Wall intervals overlap and are not physical disk bytes or native busy proof.",
            "canonical_object_reads": dict(counts.objects),
            "process_cpu_ms": round((after_io.ru_utime + after_io.ru_stime - before_io.ru_utime - before_io.ru_stime) * 1000, 3),
            "disk_output_blocks": after_io.ru_oublock - before_io.ru_oublock,
            "gc_pauses": {"latency": distribution([item["elapsed_ms"] for item in gc_pauses]),
                          "window": "last_256_collections_during_measurement",
                          "counts_by_generation": dict(gc_counts), "observations": list(gc_pauses),
                          "long_pause_selection_ms": 10, "long_pause_count": long_gc_count,
                          "long_pause_window": "last_64_collections_at_least_10ms_during_measurement",
                          "long_observations": list(long_gc_pauses)},
            "transcript_input_rows": expected, "transcript_retained_rows": actual,
            "runtime": snapshot, "runtime_window_note": "Counters/windows include boot and fixture seeding; counter deltas and explicit latency samples cover only the declared measurement scenario.",
            "transcript_performance": store.transcripts.performance.snapshot(),
            "dispatch_performance": dispatches.performance.snapshot()}
    finally:
        gc.callbacks.remove(observe_gc)
        logging.getLogger().removeHandler(handler)
        store.close_thread_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--history", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--inventory-reads", action="store_true",
                        help="Include 19 additional local read/page paths from the findings inventory")
    args = parser.parse_args()
    if args.history < 1 or args.iterations < 1:
        parser.error("history and iterations must be positive")
    with tempfile.TemporaryDirectory(prefix="pa-systemic-benchmark-") as tmp:
        result = asyncio.run(run(Path(tmp), history=args.history, iterations=args.iterations,
                                 inventory_reads=args.inventory_reads))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "warnings": result["warnings"], "latency": result["latency"]}, indent=2))
