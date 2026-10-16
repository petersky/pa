"""Disposable mixed-load acceptance inventory, never an installed PA data dir.

Use identical script + PYTHONPATH=<baseline src> for a source comparison.
No provider process, external peer traffic, restart, or production fault injection.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import platform
import resource
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from threading import Lock
from unittest.mock import patch

import httpx

from pa.acp.client import WireJsonlLogger
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardEvent, EventType, SyncCommit
from pa.execution.dispatch import DispatchRecord
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase
from pa.fleet.overview import FleetOverviewCache, field
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.sync import _apply_sync_push_local
from pa.pr_supervisor.models import PRWatch


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


async def run(root, *, history=1000, iterations=30):
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
        agent_enabled=False, telemetry_enabled=False, auth_required=False)
    app = Kernel.boot(settings=settings).build_app()
    async with app.router.lifespan_context(app):
        return await run_app(app, root, history=history, iterations=iterations)


async def run_app(app, root, *, history, iterations):
    ctx = app.state.ctx
    settings = ctx.settings
    runtime = ctx.require_service("async_runtime")
    manager = ctx.require_service("instance_agent")
    manager._startup_complete = True
    store = ctx.store
    log = ctx.require_service("event_log")
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
                status="idle", config_json={"execution_selection": proof,
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
                digest = log.store.put_json(event(history + i).model_dump(mode="json"))
                commit = SyncCommit(hash="", realm_id="default", instance_id="peer",
                    parent_hashes=[parent], event_hashes=[digest], author_principal="user:fixture")
                commit.hash = log.store.put_json(commit.model_dump(mode="json"))
                return _apply_sync_push_local(ctx, "default", commit.hash, {})
            result = await offload("sync.push_transaction", push)
            assert result["commits_applied"] == 1
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
            await offload("progress.checkpoint_write", dispatches.ingest_progress, progress, delivered=True)
            await offload("dispatch.runnable_read", dispatches.runnable)
            await offload("reconciliation.dispatch_read", dispatches.list,
                          reconciliation_states={"pending", "prompted", "blocked"}, limit=100)
            await asyncio.sleep(0.005)
    async def cache_and_wire():
        for i in range(iterations):
            await offload("fleet.overview_cache_write", cache.put, "local", "sync", field("fresh", {"head": str(i)}))
            await offload("fleet.overview_cache_read", cache.get, "local", "sync")
            await offload("acp.wire_append", wire.log, "in", {"type": "fixture", "number": i})
            await offload("sync.commit_read", log.get_commit, log.get_head("default"))
            await asyncio.sleep(0.005)
    async def foreground():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            for _ in range(iterations):
                for path in ("/api/health", "/api/agent/sessions?view=chats", "/agent",
                             "/partials/home/sections", "/api/status", "/api/runtime?section=sse"):
                    response = await measure(path, client.get(path))
                    response.raise_for_status()
                    sizes[path].append(len(response.content))
                await asyncio.sleep(0.005)
    await runtime.start()
    runtime.reset_lag_samples()
    runtime_before = runtime.snapshot()
    before_io = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    try:
        with patch("sqlite3.connect", counts.connection), patch.object(log.store, "get", side_effect=read_object):
            await asyncio.gather(sync_work(), startup_work(), progress_work(), cache_and_wire(), foreground(),
                                 *(transcript_work(worker) for worker in transcript_runtimes))
        elapsed = time.perf_counter() - started
        after_io = resource.getrusage(resource.RUSAGE_SELF)
        snapshot = runtime.snapshot()
        expected = iterations * 32 * len(transcript_runtimes)
        actual = sum(len(store.list_transcript_events(s.id, limit=iterations * 32 + 1)) for s in sessions[:3])
        assert actual == expected
        assert store.get_projection_head("default") == log.get_head("default")
        assert store.get_card(card_id).title == f"Fixture {history + iterations - 1}"
        return {"hardware": {"machine": platform.machine(), "python": platform.python_version(), "cpus": os.cpu_count()},
            "fixture": {"history_commits": history, "retained_dispatches": 101, "sessions": 30,
                "watches": 30, "iterations_per_family": iterations, "transcript_sessions": 3,
                "executor_workers": runtime.max_workers, "duration_seconds": round(elapsed, 3),
                "limits": "single process, local disposable storage, synthetic providers, no external peer/provider time"},
            "latency": {key: distribution(values) for key, values in samples.items()},
            "response_bytes": {key: {"min": min(values), "max": max(values)} for key, values in sizes.items()},
            "warnings": dict(warnings), "sql_statements": dict(counts.statements),
            "threshold_violations": {key: sum(value >= (2000 if key == "dispatch.runnable_read" else 500)
                                              for value in values)
                                     for key, values in samples.items()
                                     if any(value >= (2000 if key == "dispatch.runnable_read" else 500)
                                            for value in values)},
            "operation_counter_deltas": {name: {key: value.get(key, 0) - runtime_before.get("operations", {}).get(name, {}).get(key, 0)
                for key in ("submitted", "completed", "failed", "timed_out", "rejected", "slow_calls")}
                for name, value in snapshot["operations"].items()},
            "canonical_object_reads": dict(counts.objects),
            "process_cpu_ms": round((after_io.ru_utime + after_io.ru_stime - before_io.ru_utime - before_io.ru_stime) * 1000, 3),
            "disk_output_blocks": after_io.ru_oublock - before_io.ru_oublock,
            "transcript_input_rows": expected, "transcript_retained_rows": actual,
            "runtime": snapshot, "runtime_window_note": "Counters/windows include boot and fixture seeding; counter deltas and explicit latency samples cover only the declared measurement scenario.",
            "transcript_performance": store.transcripts.performance.snapshot(),
            "dispatch_performance": dispatches.performance.snapshot()}
    finally:
        logging.getLogger().removeHandler(handler)
        store.close_thread_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--history", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=30)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-systemic-benchmark-") as tmp:
        result = asyncio.run(run(Path(tmp), history=args.history, iterations=args.iterations))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "warnings": result["warnings"], "latency": result["latency"]}, indent=2))
