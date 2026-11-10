"""Disposable mixed-load acceptance inventory, never an installed PA data dir.

Emit bounded aggregate JSON to stdout; keep only one current compact report.
No raw samples, diagnostic dumps or failure-history files are written.
No provider process, external peer traffic, restart, or production fault injection.
"""
from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import inspect
import json
import logging
import math
import os
import platform
import resource
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import zlib
from contextlib import contextmanager, redirect_stderr
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock, local
from unittest.mock import patch

import httpx

from pa.acp.client import WireJsonlLogger
from pa.acp import transcript_page  # Include lazy history compaction in source attestation.
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.performance import FILESYSTEM_PHASES
from pa.domain.models import (AgentSession, CardEvent, EventType, SyncCommit, TranscriptEvent,
                              KnowledgeEntry, KnowledgeUpdate, KnowledgeAuditEvent)
from pa.execution.dispatch import DispatchRecord
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase
from pa.fleet.overview import FleetOverviewCache, field
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.sync import _apply_sync_push_local
from pa.modules import instance as instance_module  # Attest runtime diagnostics before measurement.
from pa.pr_supervisor.models import PRWatch
from pa.repository.workspace import WorkspaceManager
from pa.status import info as status_info  # Attest the lazy status count path before measurement.


def distribution(values):
    ordered = sorted(values)
    return {"samples": len(ordered), "max_ms": round(max(ordered), 3) if ordered else None,
            **{f"p{p}_ms": round(ordered[math.ceil(len(ordered) * p / 100) - 1], 3)
               if ordered else None for p in (50, 95, 99)}}


def validate_fixture(*, history, iterations, retention_events, retained_cold_objects, maintenance_rows=0):
    if not 1 <= history <= 1000 or not 1 <= iterations <= 100:
        raise ValueError("history must be1..1000 and iterations1..100")
    if not 0 <= retention_events <= 10000 or not 0 <= retained_cold_objects <= 15000:
        raise ValueError("retention-events0..10000; retained-cold-objects0..15000")
    if retained_cold_objects and not retention_events:
        raise ValueError("retained-cold-objects requires retention-events")
    if (maintenance_rows and not 16384 <= maintenance_rows <= 65536) or (maintenance_rows and not retention_events):
        raise ValueError("maintenance-rows must be0 or16384..65536 and requires retention-events")


def fixture_bytes(root):
    total = files = 0
    for directory, _, names in os.walk(root, followlinks=False):
        for name in names:
            path = Path(directory) / name
            if not path.is_symlink():
                try:
                    total += path.stat().st_size
                    files += 1
                except FileNotFoundError:  # A live SQLite journal may disappear.
                    pass
    if total > 1024**3:
        raise RuntimeError("Private fixture exceeded1GiB logical storage budget")
    return {"logical_bytes": total, "files": files, "budget_bytes": 1024**3,
            "scope": "Postworkload metadata sum, not peak allocation, physical writes or exclusive APFS bytes; inputs also have fixed count bounds."}


def compact_runtime(snapshot):
    """Numeric projections only; never export complete diagnostic snapshots."""
    selected = ("submitted", "completed", "failed", "timed_out", "cancelled", "rejected", "active", "queued",
                "slow_calls", "max_runtime_ms", "runtime_ms", "queue_ms", "worker_service_ms",
                "completion_delivery_ms", "thread_cpu_ms", "sqlite_calls", "sqlite_rows")
    quantiles = ("samples", "p50", "p95", "p99", "max")
    def cell(value):
        return [value.get(key) for key in quantiles] if isinstance(value, dict) else value
    worst = {}
    for item in snapshot.get("slow_operations", {}).get("observations", []):
        name = item["operation"]
        if name not in worst or item.get("elapsed_ms", 0) > worst[name].get("elapsed_ms", 0):
            worst[name] = {key: value for key, value in item.items() if isinstance(value, (int, float)) and value != 0}
    return {"operation_columns": list(selected), "distribution_columns": list(quantiles),
            "operations": {name: [cell(value.get(key)) for key in selected]
                           for name, value in snapshot.get("operations", {}).items()},
            "event_loop": snapshot.get("event_loop", {}),
            "routes": snapshot.get("requests", {}).get("routes", {}),
            "slow_worst_by_operation_in_last64": worst,
            "window_scope": "Operation/route quantiles are bounded runtime windows; maxima/counters are process totals including boot/seed. Explicit workload distributions/deltas are separate."}


def compact_maintenance(result):
    fields = ("status", "reason", "compacted", "quick_check", "before", "after")
    return {key: {**{field: value[field] for field in fields if field in value},
                  "phase_columns": ["calls", "errors", "wall_ms", "thread_cpu_ms"],
                  "phases": {name: [phase.get(field) for field in ("calls", "errors", "wall_ms", "thread_cpu_ms")]
                             for name, phase in value.get("phases", {}).items()}}
            for key in ("sqlite", "transcript_sqlite") if isinstance(value := result.get(key), dict)}


def compact_result(result):
    value = dict(result)
    value.pop("raw_latency_samples_ms", None)
    value.pop("raw_decoded_response_bytes", None)
    for key in ("runtime", "diagnostics_runtime"):
        if value.get(key) is not None:
            value[key] = compact_runtime(value[key])
    gc_data = dict(value.get("gc_pauses", {}))
    observations = gc_data.pop("observations", [])
    gc_data.pop("long_observations", None)
    gc_data["latest_finished_monotonic"] = max((r["finished_at_monotonic"] for r in observations), default=None)
    value["gc_pauses"] = gc_data
    pre = dict(value.get("premeasurement", {}))
    slow = pre.pop("slow_observations", {})
    pre["slow_operation_counts_in_last_window"] = dict(Counter(r["operation"] for r in slow.get("observations", [])))
    pre["slow_worst_by_operation_in_last64"] = compact_runtime({"slow_operations": slow})["slow_worst_by_operation_in_last64"]
    value["premeasurement"] = pre
    fixture = dict(value.get("fixture", {}))
    retention = dict(fixture.get("retention_outcome", {}))
    if "result" in retention:
        retention["database_reports"] = compact_maintenance(retention.pop("result"))
    fixture["retention_outcome"] = retention
    value["fixture"] = fixture
    return value


class WarningCounts(logging.Handler):
    def __init__(self):
        super().__init__()
        self.counts = Counter()

    def emit(self, record):
        if record.levelno >= logging.WARNING:
            operation = re.search(r"\boperation=([A-Za-z0-9_.:-]{1,96})\b", record.getMessage())
            self.counts[record.name + ":" + (operation[1] if operation else record.levelname)] += 1


@contextmanager
def fixture_logging():
    """Aggregate private console output without lowering any logging threshold."""
    import pa.core.kernel as kernel_module
    root = logging.getLogger()
    previous, level = list(root.handlers), root.level
    warnings = WarningCounts()
    configure = kernel_module.configure_logging
    class NumericStream:
        characters = 0
        def write(self, text):
            self.characters += len(text)
            return len(text)
        def flush(self):
            pass
    stream = NumericStream()
    def configured(settings):
        configure(settings)
        root.addHandler(warnings)
    try:
        with redirect_stderr(stream), patch.object(kernel_module, "configure_logging", configured):
            yield warnings, stream
    finally:
        for handler in root.handlers:
            if handler not in previous:
                handler.close()
        root.handlers = previous
        root.setLevel(level)


def minimum_failure(exc):
    import traceback
    frame = traceback.extract_tb(exc.__traceback__)[-1]
    return {"error_type": type(exc).__name__,
            "location": {"file": Path(frame.filename).name, "function": frame.name, "line": frame.lineno},
            "http_status": exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None,
            "route": exc.request.url.path if isinstance(exc, httpx.HTTPStatusError) else None}


def benchmark(**options):
    """Own and remove all private state even if boot, workload or validation fails."""
    validate_fixture(history=options.get("history", 1000), iterations=options.get("iterations", 30),
                     retention_events=options.get("retention_events", 0),
                     retained_cold_objects=options.get("retained_cold_objects", 0),
                     maintenance_rows=options.get("maintenance_rows", 0))
    failure = {}
    with tempfile.TemporaryDirectory(prefix="pa-systemic-benchmark-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4 * 1024**3:
            raise RuntimeError("At least4GiB free required before seeding")
        try:
            result = asyncio.run(run(root, **options, failure_observer=failure.update))
        except BaseException as exc:
            result = {"failed": True, **minimum_failure(exc), "partial": failure,
                      "scope": "Failed private fixture; no acceptance or final durable-data validation."}
    result["temporary_fixture_removed"] = not root.exists()
    encoded = json.dumps(result, separators=(",", ":"))
    if len(encoded.encode()) > 128 * 1024:
        raise RuntimeError("Aggregate report exceeds128KiB; reduce structural duplication, never drop findings")
    return result


class MaintenanceAttempts:
    """Fixture-owner aggregates retain retries that last_result replaces."""
    def __init__(self, service):
        self.service = service
        self.original = service._execute
        self.stage = "steady"
        self.calls = Counter()
        self.databases = {}

    async def execute(self, **kwargs):
        stage = self.stage
        only = kwargs.get("database_only", False)
        keys = tuple(self.service._pending_compactions) if only else ("sqlite", "transcript_sqlite")
        self.calls[stage + (":database_only" if only else ":full")] += 1
        try:
            result = await self.original(**kwargs)
        except BaseException as exc:
            self.calls[stage + ":error:" + type(exc).__name__] += 1
            raise
        for key in keys:
            if key not in result:
                continue
            report = result[key]
            value = self.databases.setdefault(key, {"attempts": 0, "outcomes": {}, "phases": {}})
            value["attempts"] += 1
            outcome = ":".join((stage, self.stage, report.get("status", "unknown"), report.get("reason", "none")))
            value["outcomes"][outcome] = value["outcomes"].get(outcome, 0) + 1
            for name, phase in report.get("phases", {}).items():
                total = value["phases"].setdefault(name, {"calls":0,"errors":0,"wall_ms":0,"thread_cpu_ms":0,"max_wall_ms":0})
                for field in ("calls", "errors", "wall_ms", "thread_cpu_ms"):
                    total[field] += phase.get(field, 0)
                if phase.get("wall_ms", 0) >= total["max_wall_ms"]:
                    total.update(max_wall_ms=phase["wall_ms"],
                                 max_call_thread_cpu_ms=phase.get("thread_cpu_ms"),
                                 max_call_start=phase.get("started_at_monotonic"),
                                 max_call_finish=phase.get("finished_at_monotonic"))
        return result

    def snapshot(self):
        return {"calls":dict(self.calls), "databases":self.databases,
                "scope":"Every owned execute entered after the measured-window boundary, including database-only retries through the quiet barrier. Start/end stages are explicit. Only requested databases counted, never carried-forward stale reports. Inclusive phases overlap; logical totals are not physical I/O."}


class CommitProbe:
    """Private native commit intervals; fixed owner WAL metadata, no SQL probes."""
    def __init__(self, paths):
        self.paths = paths
        self.local = local()
        self.lock = Lock()
        self.operations = {}
        from pa.core.sqlite_metrics import ObservedConnection
        self.original = ObservedConnection.commit

    def wal_sizes(self):
        values = {}
        for key, path in self.paths.items():
            try:
                values[key] = Path(str(path) + "-wal").stat().st_size
            except FileNotFoundError:
                values[key] = 0
        return values

    def commit(self, connection):
        if not connection.in_transaction:
            return self.original(connection)
        name = getattr(self.local, "operation", "other_private_work")
        metadata_started = time.perf_counter()
        before = self.wal_sizes()
        started, cpu = time.perf_counter(), time.thread_time()
        error = False
        try:
            return self.original(connection)
        except BaseException:
            error = True
            raise
        finally:
            finished = time.perf_counter()
            elapsed, cpu_ms = (finished-started)*1000, (time.thread_time()-cpu)*1000
            after = self.wal_sizes()
            overhead = (time.perf_counter()-metadata_started)*1000-elapsed
            with self.lock:
                value = self.operations.setdefault(name, {"calls":0,"errors":0,"wall_ms":0,"thread_cpu_ms":0,"metadata_overhead_ms":0,"ge500ms":0,"max_wall_ms":0})
                value["calls"] += 1
                value["errors"] += int(error)
                value["wall_ms"] += elapsed
                value["thread_cpu_ms"] += cpu_ms
                value["metadata_overhead_ms"] += overhead
                value["ge500ms"] += int(elapsed>=500)
                if elapsed >= value["max_wall_ms"]:
                    value.update(max_wall_ms=elapsed, max_call_thread_cpu_ms=cpu_ms,
                                 max_call_start=started,max_call_finish=finished,
                                 max_call_wal_bytes_before=before,max_call_wal_bytes_after=after)

    def call(self, name, function, *args, **kwargs):
        previous = getattr(self.local, "operation", "other_private_work")
        self.local.operation = name
        try:
            return function(*args, **kwargs)
        finally:
            self.local.operation = previous

    def snapshot(self):
        with self.lock:
            return {"operations":dict(self.operations),
                    "scope":"Actual private ObservedConnection.commit calls with active transactions. Helper operation labels propagate on the owning thread; other work stays explicit. Max-call intervals/WAL stat sizes only; allocated WAL length is not committed frames, checkpoint progress or physical bytes. Metadata overhead is outside native interval. Default checkpoint/synchronous policies unchanged."}


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


async def _run_workload(*awaitables, outcomes=None):
    """Drain this fixture's sibling tasks before teardown after any failure."""
    tasks = [asyncio.create_task(call) for call in awaitables]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if outcomes is not None:
            for index, task in enumerate(tasks):
                error = None if task.cancelled() else task.exception()
                outcomes[str(index)] = (
                    "cancelled" if task.cancelled() else type(error).__name__ if error else "completed"
                )
        raise


async def run(root, *, history=1000, iterations=30, inventory_reads=False, retention_events=0,
              retained_cold_objects=0, maintenance_rows=0, sample_observer=None, failure_observer=None):
    validate_fixture(history=history, iterations=iterations, retention_events=retention_events,
                     retained_cold_objects=retained_cold_objects, maintenance_rows=maintenance_rows)
    root = Path(root).resolve()
    temporary = Path(tempfile.gettempdir()).resolve()
    assert root != temporary and root.is_relative_to(temporary), "benchmark root must be temporary"
    root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(root).free < 4 * 1024**3:
        raise RuntimeError("At least4GiB free required before seeding")
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
        agent_enabled=False, telemetry_enabled=False, auth_required=False)
    original = WorkspaceManager.__init__
    roots = []
    def guarded(manager, settings, store):
        data = Path(settings.data_dir).resolve()
        workspace = Path(settings.workspace_root).expanduser().resolve()
        assert data.is_relative_to(root), "benchmark data root escaped fixture"
        assert workspace.is_relative_to(root), "benchmark workspace root escaped fixture"
        roots.append({"data_dir": str(data), "workspace_root": str(workspace)})
        return original(manager, settings, store)
    # A private fixture must never reuse a manager retained by an earlier
    # embedded Kernel. Restore that process-local binding after our owner closes.
    import pa.instance.agent_session as agent_module
    with patch.object(WorkspaceManager, "__init__", guarded), \
         patch.object(agent_module, "_instance_agent", None), fixture_logging() as (all_warnings, console):
        startup_started = time.perf_counter()
        app = Kernel.boot(settings=settings).build_app()
        async with app.router.lifespan_context(app):
            startup_ms = (time.perf_counter() - startup_started) * 1000
            result = await run_app(app, root, history=history, iterations=iterations,
                                   inventory_reads=inventory_reads, retention_events=retention_events,
                                   retained_cold_objects=retained_cold_objects, sample_observer=sample_observer,
                                   maintenance_rows=maintenance_rows, failure_observer=failure_observer)
    result["workspace_roots_checked"] = len(roots)
    result["all_fixture_warning_counts"] = dict(all_warnings.counts)
    result["console_characters_aggregated_without_capture"] = console.characters
    result["startup_to_lifespan_ready_ms"] = round(startup_ms, 3)
    result["fixture_storage"] = fixture_bytes(root)
    return compact_result(result)


async def run_app(app, root, *, history, iterations, inventory_reads=False, retention_events=0,
                  retained_cold_objects=0, maintenance_rows=0, sample_observer=None, failure_observer=None):
    from pa.server.template_cache import _compatible
    template_cache = app.state.templates.env.bytecode_cache
    template_artifact = {
        "cache_class": type(template_cache).__name__,
        "packaged_entries": len(getattr(template_cache, "entries", {})),
        "environment_compatible": _compatible(app.state.templates.env),
    }
    ctx = app.state.ctx
    settings = ctx.settings
    runtime = ctx.require_service("async_runtime")
    manager = ctx.require_service("instance_agent")
    assert manager.store is ctx.store, "benchmark manager reused a different storage owner"
    assert Path(manager.settings.data_dir).resolve().is_relative_to(root), "benchmark manager escaped fixture"
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
    maintenance = ctx.services.get("instance_maintenance")
    retention_result = {}
    if retention_events:
        # Finish the initial sweep before adding an explicitly expired fixture.
        # The measured sweep below is the real service path, concurrent with the
        # other workloads. No production maintenance or retention change.
        await maintenance.run_once()
        # Fixture-only lifecycle pause: prevent a setup-time sweep from consuming
        # the expired/free-page inputs before the declared measured window.
        # Start the same real service at the window boundary below.
        await maintenance.close()
        def seed_expired():
            expired = AgentSession(id="retention-expired", agent_name="codex", status="closed")
            store.save_session(expired)
            store.append_transcript_events([
                TranscriptEvent(session_id=expired.id, seq=i + 1,
                    event_type="turn_completed" if i % 128 == 0 else "chunk",
                    payload={"text": "expired synthetic fixture"})
                for i in range(retention_events)
            ])
            expired.updated_at = datetime.now(UTC) - timedelta(
                days=int(settings.transcript_retention_days) + 1)
            store.save_session(expired)
            assert store.transcripts.next_seq(expired.id) == retention_events + 1
        await asyncio.to_thread(seed_expired)
    if retained_cold_objects:
        await asyncio.to_thread(store.save_session, AgentSession(
            id="retention-live-cold", agent_name="codex", status="idle"))
        for start in range(0, retained_cold_objects, 256):
            stop = min(start + 256, retained_cold_objects)
            await asyncio.to_thread(store.append_transcript_events, [
                TranscriptEvent(session_id="retention-live-cold", seq=i + 1,
                    event_type="chunk", payload={"text": f"{i}:" + "retained cold fixture " * 300})
                for i in range(start, stop)
            ])
            if sample_observer is not None:
                pending = sample_observer({"kind": "retained_cold_seed", "through_seq": stop,
                                           "expected": retained_cold_objects})
                if inspect.isawaitable(pending):
                    await pending
    reclamation_before = None
    if maintenance_rows:
        def seed_reclamation():
            from contextlib import closing
            with closing(sqlite3.connect(store.db_path)) as connection, connection:
                connection.execute("CREATE TABLE mixed_retained_fixture(id INTEGER PRIMARY KEY, body BLOB)")
                connection.executemany("INSERT INTO mixed_retained_fixture VALUES (?,?)",
                                       ((i, b"x"*4096) for i in range(maintenance_rows)))
                connection.execute("DELETE FROM mixed_retained_fixture WHERE id<?", (maintenance_rows//2,))
                connection.commit()
                page_size = connection.execute("PRAGMA page_size").fetchone()[0]
                return {"bytes": connection.execute("PRAGMA page_count").fetchone()[0]*page_size,
                        "free_bytes": connection.execute("PRAGMA freelist_count").fetchone()[0]*page_size}
        reclamation_before = await asyncio.to_thread(seed_reclamation)
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
    handler = WarningCounts()
    warnings = handler.counts
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
        try:
            value = await awaitable
        except BaseException as exc:
            if sample_observer is not None:
                await observe_sample({"kind": "error", "name": name,
                                      "elapsed_ms": (time.perf_counter() - started) * 1000,
                                      "error_type": type(exc).__name__})
            raise
        elapsed = (time.perf_counter() - started) * 1000
        samples[name].append(elapsed)
        if sample_observer is not None:
            await observe_sample({"kind": "latency", "name": name, "elapsed_ms": elapsed})
        return value
    async def observe_sample(record):
        pending = sample_observer(record)
        if inspect.isawaitable(pending):
            await pending
    async def offload(name, call, *args, **kwargs):
        return await measure(name, runtime.run_blocking(name, commits.call, name, call, *args, **kwargs))
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
    async def retention_work():
        if retention_events:
            result = await measure("maintenance.run", maintenance.run_once())
            assert result["transcript_events_deleted"] == retention_events
            assert not store.list_transcript_events("retention-expired")
            with store.transcripts._conn() as conn:
                proofs = conn.execute(
                    "SELECT COUNT(*) FROM transcript_evidence WHERE session_id=?",
                    ("retention-expired",),
                ).fetchone()[0]
            assert proofs == min(16, (retention_events + 127) // 128)
            retention_result.update(deleted=retention_events, independent_audit_proofs=proofs,
                                    result=result)
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
    async def knowledge_work():
        for i in range(iterations):
            identity = f"fixture-knowledge-{i}"
            await offload("benchmark.knowledge_add", store.add_knowledge,
                          KnowledgeEntry(id=identity, summary=f"Knowledge {i}"))
            await offload("benchmark.knowledge_update", store.update_knowledge, identity,
                          KnowledgeUpdate(summary=f"Updated knowledge {i}"))
            audit = KnowledgeAuditEvent(id=f"audit-{i}", knowledge_id=identity,
                                       action="read", actor="fixture", payload={"retained": True})
            def audit_and_replay():
                store.add_knowledge_audit(audit)
                store.add_knowledge_audit(audit)
            await offload("benchmark.knowledge_audit", audit_and_replay)
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
                    if sample_observer is not None:
                        await observe_sample({"kind": "http_response", "name": path,
                                              "status": response.status_code,
                                              "decoded_response_bytes": len(response.content)})
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
                                  "pa.core.sqlite_maintenance",
                                  "pa.domain.projection", "pa.domain.transcript_storage", "pa.domain.card_summary_service", "pa.modules.items",
                                  "pa.execution.dispatch", "pa.modules.fleet", "pa.fleet.overview", "pa.sync.engine",
                                  "pa.sync.event_log", "pa.sync.dag_index", "pa.acp.transcript_page",
                                  "pa.instance.agent_session", "pa.instance.maintenance", "pa.pr_supervisor.store",
                                  "pa.pr_supervisor.service", "pa.status.info", "pa.modules.instance",
                                  "pa.core.kernel", "pa.openapi", "pa.server.schema_generation",
                                  "pa.server.readiness", "pa.server.template_cache", "pa.modules.ui_shell")}
    diagnostics = ctx.services.get("runtime_diagnostics")
    if "pa.server.fleet_compression" in sys.modules:
        source_hashes["pa.server.fleet_compression"] = hashlib.sha256(
            Path(sys.modules["pa.server.fleet_compression"].__file__).read_bytes()
        ).hexdigest()
    diagnostics_before = diagnostics.snapshot() if diagnostics else {}
    before_io = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    attempts = MaintenanceAttempts(maintenance)
    commits = CommitProbe({"sqlite":store.db_path,"transcript_sqlite":store.transcripts.db_path})
    execute_patch = patch.object(maintenance, "_execute", attempts.execute)
    from pa.core.sqlite_metrics import ObservedConnection
    commit_patch = patch.object(ObservedConnection, "commit", lambda connection: commits.commit(connection))
    execute_patch.start()
    commit_patch.start()
    if retention_events:
        maintenance.start()
    workload_outcomes = {}
    gc.callbacks.append(observe_gc)
    try:
        with patch("sqlite3.connect", counts.connection), patch.object(log.store, "get", side_effect=read_object):
            await _run_workload(sync_work(), startup_work(), progress_work(), cache_and_wire(), foreground(),
                                 retention_work(), knowledge_work(),
                                 *(transcript_work(worker) for worker in transcript_runtimes),
                                 outcomes=workload_outcomes)
        finished = time.perf_counter()
        attempts.stage = "postmeasurement"
        elapsed = finished - started
        after_io = resource.getrusage(resource.RUSAGE_SELF)
        snapshot = runtime.snapshot()
        diagnostic_snapshot = diagnostics.snapshot() if diagnostics else None
        transcript_snapshot = store.transcripts.performance.snapshot()
        dispatch_snapshot = dispatches.performance.snapshot()
        with counts.lock:
            statement_counts = dict(counts.statements)
            object_counts = dict(counts.objects)
        # Close all measurement collectors before verifying durable contents.
        # Snapshot calls are sequential, not an atomic cross-executor read.
        gc.callbacks.remove(observe_gc)
        logging.getLogger().removeHandler(handler)
        collected_warnings = dict(warnings)
        captured = time.perf_counter()
        validation_started = captured
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
        cold_verified = None
        if retained_cold_objects:
            def verify_retained_cold():
                with store.transcripts._conn() as conn:
                    rows = conn.execute(
                        "SELECT seq,cold_hash FROM transcript_events WHERE session_id=? ORDER BY seq",
                        ("retention-live-cold",),
                    ).fetchall()
                assert [row[0] for row in rows] == list(range(1, retained_cold_objects + 1))
                for _, digest in rows:
                    raw = zlib.decompress(store.transcripts._object_path(digest).read_bytes())
                    assert hashlib.sha256(raw).hexdigest() == digest
                return {"events": len(rows), "unique_objects": len({row[1] for row in rows}),
                        "all_compressed_content_hashes_verified": True,
                        "scope": "Postmeasurement verification of every retained cold object; outside workload samples/counter snapshot, adds local read/decompression work."}
            cold_verified = await asyncio.to_thread(verify_retained_cold)
            if sample_observer is not None:
                pending = sample_observer({"kind": "retained_cold_verified", **cold_verified})
                if inspect.isawaitable(pending):
                    await pending
        assert store.get_projection_head("default") == log.get_head("default")
        assert store.get_card(card_id).title == f"Fixture {history + 2 * iterations - 1}"
        quiet = None
        if retention_events:
            quiet_started = time.perf_counter()
            quiet_warnings = WarningCounts()
            logging.getLogger().addHandler(quiet_warnings)
            try:
                for _ in range(3):
                    if not maintenance.snapshot()["pending_database_compactions"]:
                        break
                    await maintenance._run_once(database_only=True)
                quiet = {"duration_ms": round((time.perf_counter() - quiet_started) * 1000, 3),
                         "database_reports": compact_maintenance(maintenance.last_result or {}),
                         "warnings": dict(quiet_warnings.counts),
                         "pending": maintenance.snapshot()["pending_database_compactions"],
                         "scope": "Postvalidation completion barrier and any required database-only retries. Reports may come from an autonomous retry that overlapped the measured window/validation; report phase durations are separate from barrier wall time and must not be added to steady samples."}
                quiet["database_report_phases_ge500ms"] = {
                    name: {phase: values[2] for phase, values in report["phases"].items() if values[2] >= 500}
                    for name, report in quiet["database_reports"].items()
                    if any(values[2] >= 500 for values in report["phases"].values())}
                assert not quiet["pending"], "quiet compaction remained pending"
                assert set(quiet["database_reports"]) == {"sqlite", "transcript_sqlite"}
                assert all(report["status"] == "ok" for report in quiet["database_reports"].values()), "quiet compaction did not complete"
                if maintenance_rows:
                    initial = retention_result.get("result", {})
                    quiet["rewrite_observed"] = any(
                        report.get("compacted") for report in (*quiet["database_reports"].values(),
                        *(initial.get(key, {}) for key in ("sqlite", "transcript_sqlite"))))
                    assert quiet["rewrite_observed"], "fixture did not exercise real reclamation"
                    after = quiet["database_reports"]["sqlite"]["after"]
                    assert after["free_bytes"] == 0 and after["bytes"] < reclamation_before["bytes"]
            finally:
                logging.getLogger().removeHandler(quiet_warnings)
        for i in range(iterations):
            identity = f"fixture-knowledge-{i}"
            assert store.get_knowledge(identity).summary == f"Updated knowledge {i}"
            audits = store.list_knowledge_audit(identity)
            assert len(audits) == 1 and audits[0].id == f"audit-{i}" and audits[0].payload == {"retained": True}
        if maintenance_rows:
            def verify_reclamation_rows():
                with store._conn() as connection:
                    assert tuple(connection.execute(
                        "SELECT COUNT(*),MIN(id),MAX(id),SUM(body!=?) FROM mixed_retained_fixture",
                        (b"x"*4096,),
                    ).fetchone()) == (maintenance_rows-maintenance_rows//2, maintenance_rows//2, maintenance_rows-1, 0)
            await asyncio.to_thread(verify_reclamation_rows)
        validation_finished = time.perf_counter()
        return {"hardware": {"machine": platform.machine(), "python": platform.python_version(), "cpus": os.cpu_count()},
            "source_module_sha256": source_hashes,
            "benchmark_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "measurement_start_monotonic": started, "measurement_finish_monotonic": finished,
            "measurement_capture": {"finished_monotonic": captured,
                "duration_ms": (captured - finished) * 1000,
                "scope": "Sequential resource/executor/storage snapshots after workload completion, before validation; not an atomic read. GC and warning collectors close at capture end."},
            "validation": {"started_monotonic": validation_started,
                "finished_monotonic": validation_finished,
                "duration_ms": (validation_finished - validation_started) * 1000,
                "scope": "Durable order, retained cold content hashes, projection head and card checks; includes awaited validation observer persistence. Excluded from workload duration and captured counters."},
            "premeasurement": {"scope": "Kernel boot and synthetic fixture seeding; not part of steady mixed window",
                               "slow_calls": sum(value.get("slow_calls", 0) for value in runtime_before["operations"].values()),
                               "async_backend_initialization": runtime_before.get("async_backend_initialization"),
                               "startup_openapi": runtime_before["operations"].get("startup.openapi"),
                               "slow_observations": runtime_before.get("slow_operations", {}),
                               "acceptance": "reported separately; full server cold boot remains unaccepted"},
            "fixture": {"history_commits": history, "retained_dispatches": 101, "sessions": 30,
                "workload_version": 8,
                "knowledge_entries": iterations, "knowledge_audits": iterations, "knowledge_audit_replays": iterations,
                "knowledge_operation_label_scope": "benchmark.* labels identify explicit helper invocations, not previously registered production route/operation names",
                "expired_retention_events": retention_events, "retention_outcome": retention_result,
                "retained_cold_objects": retained_cold_objects, "retained_cold_verification": cold_verified,
                "reclamation_seed_rows": maintenance_rows, "reclamation_retained_rows": maintenance_rows-maintenance_rows//2,
                "reclamation_before": reclamation_before,
                "retention_fixture_sessions": int(bool(retention_events)) + int(bool(retained_cold_objects)),
                "foreground_inventory_reads": inventory_reads,
                "sync_pushes": iterations, "incoming_reconciliations": iterations,
                "controlled_compatible_forks": (iterations + 9) // 10,
                "bounded_sync_inventory_pages": (iterations + 9) // 10 if inventory_reads else 0,
                "watches": 30, "iterations_per_family": iterations, "transcript_sessions": 3,
                "executor_workers": runtime.max_workers, "duration_seconds": round(elapsed, 3),
                "limits": "single process, local disposable storage, synthetic providers, no external peer/provider time"},
            "latency": {key: distribution(values) for key, values in samples.items()},
            "threshold_policy_ms": {"default": 500, "dispatch.runnable_read": 2000},
            "universal_500ms_observations": {
                key: sum(value >= 500 for value in values)
                for key, values in samples.items() if any(value >= 500 for value in values)
            },
            "response_bytes": {key: {"count": len(values), "total": sum(values), "min": min(values), "max": max(values)} for key, values in sizes.items()},
            "warnings": collected_warnings, "sql_statements": statement_counts,
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
            "diagnostics_runtime": diagnostic_snapshot,
            "diagnostics_counter_deltas": {name: {key: value[key] - diagnostics_before.get("operations", {}).get(name, {}).get(key, 0)
                for key in ("submitted", "completed", "failed", "timed_out", "rejected", "slow_calls")}
                for name, value in (diagnostic_snapshot["operations"] if diagnostic_snapshot else {}).items()},
            "filesystem_phase_totals_delta": {
                name: {phase: value["filesystem_totals"][phase] - runtime_before["operations"].get(name, {}).get("filesystem_totals", {}).get(phase, 0)
                       for phase in FILESYSTEM_PHASES
                       if value["filesystem_totals"][phase] != runtime_before["operations"].get(name, {}).get("filesystem_totals", {}).get(phase, 0)}
                for name, value in snapshot["operations"].items()},
            "filesystem_phase_scope": "Exact completed-worker logical counters during measurement; omitted phase values are zero. Separate from last256 distributions and premeasurement boot/seeding. Wall intervals overlap and are not physical disk bytes or native busy proof.",
            "canonical_object_reads": object_counts,
            "process_cpu_ms": round((after_io.ru_utime + after_io.ru_stime - before_io.ru_utime - before_io.ru_stime) * 1000, 3),
            "disk_output_blocks": after_io.ru_oublock - before_io.ru_oublock,
            "disk_output_blocks_scope": "Native getrusage ru_oublock delta; platform accounting, not physical device bytes or exclusive write amplification.",
            "gc_pauses": {"latency": distribution([item["elapsed_ms"] for item in gc_pauses]),
                          "window": "last_256_collections_during_measurement",
                          "counts_by_generation": dict(gc_counts), "observations": list(gc_pauses),
                          "long_pause_selection_ms": 10, "long_pause_count": long_gc_count,
                          "long_pause_window": "last_64_collections_at_least_10ms_during_measurement",
                          "long_observations": list(long_gc_pauses)},
            "template_artifact": template_artifact,
            "maintenance_attempts": attempts.snapshot(),
            "commit_probe": commits.snapshot(),
            "quiet_completion": quiet,
            "transcript_input_rows": expected, "transcript_retained_rows": actual,
            "runtime": snapshot, "runtime_window_note": "Counters/windows include boot and fixture seeding; counter deltas and explicit latency samples cover only the declared measurement scenario.",
            "transcript_performance": transcript_snapshot,
            "dispatch_performance": dispatch_snapshot}
    except BaseException as exc:
        if failure_observer is not None:
            failure = {
                "contract": "pa.private-mixed-failure/v1", "error_type": type(exc).__name__,
                "http_status": exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None,
                "route": exc.request.url.path if isinstance(exc, httpx.HTTPStatusError) else None,
                "measurement_start_monotonic": started, "captured_monotonic": time.perf_counter(),
                "source_module_sha256": source_hashes, "workload_outcomes": workload_outcomes,
                "warnings": dict(warnings), "runtime": compact_runtime(runtime.snapshot()),
                "completed_latency": {name: distribution(values) for name, values in samples.items()},
                "scope": "Partial failed fixture after sibling task drain, before Kernel teardown. No final order/data validation or performance acceptance; running native workers may remain. No exception messages, SQL or response bodies.",
            }
            pending = failure_observer(failure)
            if inspect.isawaitable(pending):
                await pending
        raise
    finally:
        execute_patch.stop()
        commit_patch.stop()
        if observe_gc in gc.callbacks:
            gc.callbacks.remove(observe_gc)
        logging.getLogger().removeHandler(handler)
        store.close_thread_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--inventory-reads", action="store_true",
                        help="Include 19 additional local read/page paths from the findings inventory")
    parser.add_argument("--retention-events", type=int, default=0,
                        help="Seed up to10000expired events and measure one real concurrent retention sweep")
    parser.add_argument("--retained-cold-objects", type=int, default=0,
                        help="Seed up to15000retained cold objects for the concurrent retention traversal")
    parser.add_argument("--maintenance-rows", type=int, default=0,
                        help="Seed up to65536private4KiB rows, delete half, require actual full reclamation")
    args = parser.parse_args()
    result = benchmark(**vars(args))
    print(json.dumps(result, separators=(",", ":")))
    raise SystemExit(int(result.get("failed", False)))
