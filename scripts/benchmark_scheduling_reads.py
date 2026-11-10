"""Bounded actual scheduling read-router inventory; no consumer or delivery."""
import asyncio
from collections import Counter, defaultdict
from contextlib import ExitStack
from contextvars import ContextVar
from functools import wraps
import hashlib
import importlib.util
import json
import logging
from pathlib import Path
import shutil
import tempfile
import threading
import time
from types import MethodType, SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
import httpx
from starlette.responses import JSONResponse

from pa.core.async_runtime import AsyncRuntime
from pa.modules.scheduling import router
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger, Schedule

spec = importlib.util.spec_from_file_location("schedule_read_support",
    Path(__file__).with_name("benchmark_systemic_latency.py"))
support = importlib.util.module_from_spec(spec); spec.loader.exec_module(support)
DISTRIBUTION_COLUMNS = ["samples", "p50", "p95", "p99", "max"]


def distribution(values):
    result = support.distribution(values)
    return [result["samples"], *[result[name+"_ms"] for name in ("p50", "p95", "p99", "max")]]


def seed(folder, *, population, payload_bytes):
    ledger = Ledger(folder / "scheduling.db")
    records, histories = {}, {}
    base_time = time.time()-3600
    for index in range(population):
        owner = "owner-b" if index % 4 == 0 else "owner-a"
        context = {"session" if index % 5 == 0 else "session_id": "synthetic-session"} if index % 5 in (0, 3) else {}
        destination = {"kind": "agent_session", "target": "synthetic-session"} if index % 5 == 1 else {"kind": "inbox", "target": owner}
        extra = [{"kind": "agent_session", "target": "synthetic-session"}] if index % 5 == 2 else []
        spec = Schedule(name=f"Synthetic {index}", trigger={"kind": "manual"}, destination=destination,
            additional_destinations=extra, context_refs=context,
            payload={"index": index, "synthetic": "x"*payload_bytes})
        record = ledger.create(owner, f"create-{index}", spec, base_time)
        records[record["id"]] = record
        if index % 3 == 0:
            ledger.fire(owner, record["id"], f"fire-{index}", base_time+index/1000)
            record["occurrence_count"] += 1
    target = sorted(sid for sid, record in records.items() if record["owner"] == "owner-a" and record["occurrence_count"])[0]
    for index in range(12):
        ledger.fire("owner-a", target, f"history-{index}", base_time+index/100)
        records[target]["occurrence_count"] += 1
    for sid, record in records.items():
        history = ledger.history(record["owner"], sid) if record["occurrence_count"] else []
        assert len(history) < 100, "Oracle needs the complete bounded private history"
        histories[sid] = history
        latest = history[0] if history else None
        envelope = json.loads(latest["envelope"]) if latest else {}
        record.update(pending=len(history), failed=0 if history else None,
            last_successful_delivery=None, next_retry=min((x["next_retry"] for x in history), default=None),
            last_occurrence={**{k: latest[k] for k in ("id", "state", "attempts", "delivered_at", "error", "next_retry")},
                **{k: envelope.get(k) for k in ("due_at", "missed_count", "coalesced_count")}} if latest else None,
            state="pending" if history else "idle")
        assert all(item["state"] == "pending" and item["attempts"] == 0 for item in history)
    return ledger, records, histories, target


def expected_page(records, histories, owner, params):
    selected = []
    for sid, record in sorted(records.items()):
        if record["owner"] != owner or sid <= params.get("cursor", ""): continue
        session = params.get("session_id")
        destinations = [record["destination"], *record.get("additional_destinations", [])]
        if session and not (session in [record["context_refs"].get("session"), record["context_refs"].get("session_id")]
                or any(x["kind"] == "agent_session" and x["target"] == session for x in destinations)): continue
        if params.get("pending_only") == "true" and record["state"] not in {"active", "pending", "failed"}: continue
        selected.append(record)
    limit = int(params.get("limit", 50)); page = selected[:limit]
    due = [item["next_retry"] for sid, history in histories.items() if records[sid]["owner"] == owner for item in history]
    return {"schedules": page, "next_cursor": page[-1]["id"] if len(selected)>limit else None,
        "health": {"running": False, "last_tick": None, "event_error": None, "error": None,
            "worker_overdue": True, "overdue": True, "overdue_delivery_count": len(due),
            "oldest_overdue_due_at": min(due) if due else None},
        "provider_discovery": {"status": "unsupported", "reason": "Integrated ACP providers expose no automation discovery/control capability"}}


async def profile(folder, *, population, payload_bytes, iterations, progress):
    folder.mkdir(); started = time.perf_counter()
    ledger, records, histories, target = seed(folder, population=population, payload_bytes=payload_bytes)
    setup_ms = (time.perf_counter()-started)*1000
    runtime = AsyncRuntime(); ledger.runtime = runtime
    service = Scheduler(None, None, ledger)  # Deliberately no consumer/delivery.
    app = FastAPI(); app.state.ctx = SimpleNamespace(services={"scheduling": service, "async_runtime": runtime})
    selected = ContextVar("private_read_request", default=None)
    counts, phases = Counter(), defaultdict(list)
    lock = threading.Lock(); samples, sizes, hashes = defaultdict(list), defaultdict(list), {}
    statuses = Counter(); oracle_ms, oracle_cpu = [], []
    progress.update(phase="setup", statuses=statuses, samples=samples, counts=counts)
    original_connection = ledger._new_connection
    def connection():
        assert ledger.path.resolve() == (folder/"scheduling.db").resolve()
        db = original_connection()
        def trace(sql):
            label = selected.get()
            if label and sql.lstrip().upper().startswith("SELECT"):
                with lock: counts[label+":selects"] += 1
        db.set_trace_callback(trace)
        return db
    def observed(name, function):
        @wraps(function)
        def call(self, *args, **kwargs):
            wall, cpu = time.perf_counter(), time.thread_time()
            try: return function(*args, **kwargs)
            finally:
                label = selected.get()
                if label:
                    with lock:
                        counts[label+":"+name] += 1
                        phases[name+":wall_ms"].append((time.perf_counter()-wall)*1000)
                        phases[name+":thread_cpu_ms"].append((time.thread_time()-cpu)*1000)
        return MethodType(call, ledger)
    original_render = JSONResponse.render
    def render(response, content):
        wall, cpu = time.perf_counter(), time.thread_time()
        try: return original_render(response, content)
        finally:
            if selected.get():
                phases["json_render:wall_ms"].append((time.perf_counter()-wall)*1000)
                phases["json_render:thread_cpu_ms"].append((time.thread_time()-cpu)*1000)
    @app.middleware("http")
    async def auth_and_metrics(request, next):
        principal = request.headers.get("test-user")
        if principal:
            request.state.user = SimpleNamespace(role="viewer")
            request.state.principal_id = principal
        wall = time.perf_counter()
        response = await next(request)
        runtime.record_request(getattr(request.scope.get("route"), "path", "unknown"), response.status_code,
            (time.perf_counter()-wall)*1000, response_bytes=int(response.headers.get("content-length", 0)))
        return response
    app.include_router(router, prefix="/api")
    own = sorted(sid for sid, record in records.items() if record["owner"] == "owner-a")
    foreign = next(sid for sid, record in records.items() if record["owner"] == "owner-b")
    variants = {
        "default": ("/api/schedules", {}, "owner-a", 200),
        "page200": ("/api/schedules", {"limit": 200}, "owner-a", 200),
        "cursor": ("/api/schedules", {"limit": 17, "cursor": own[len(own)//2]}, "owner-a", 200),
        "session": ("/api/schedules", {"session_id": "synthetic-session", "limit": 200}, "owner-a", 200),
        "session_pending": ("/api/schedules", {"session_id": "synthetic-session", "pending_only": "true"}, "owner-a", 200),
        "pending": ("/api/schedules", {"pending_only": "true", "limit": 200}, "owner-a", 200),
        "pending_one": ("/api/schedules", {"pending_only": "true", "limit": 1}, "owner-a", 200),
        "other_owner": ("/api/schedules", {}, "owner-b", 200),
        "empty_owner": ("/api/schedules", {}, "owner-c", 200),
        "detail": ("/api/schedules/"+target, {}, "owner-a", 200),
        "history": ("/api/schedules/"+target+"/history", {}, "owner-a", 200),
        "foreign_detail": ("/api/schedules/"+foreign, {}, "owner-a", 404),
        "foreign_history": ("/api/schedules/"+foreign+"/history", {}, "owner-a", 404),
        "missing": ("/api/schedules/missing", {}, "owner-a", 404),
        "unauthenticated": ("/api/schedules", {}, None, 401),
        "limit201": ("/api/schedules", {"limit": 201}, "owner-a", 422),
        "limit0": ("/api/schedules", {"limit": 0}, "owner-a", 422),
    }
    warnings, warning_counts = [], Counter()
    class NumericWarnings(logging.Handler):
        def emit(self, record):
            if record.levelno < logging.WARNING: return
            warning_counts[record.name+":"+record.levelname] += 1
            value = getattr(record, "performance_phases", None)
            if isinstance(value, dict):
                warnings.append({k: v for k, v in value.items() if isinstance(v, (int, float)) or k == "operation"})
    progress.update(warnings=warnings, warning_counts=warning_counts)
    handler = NumericWarnings(); logger = logging.getLogger(); logger.addHandler(handler)
    try:
        await runtime.start()
        with ExitStack() as scopes:
            scopes.enter_context(patch.object(ledger, "_new_connection", connection))
            for name in ["list_page", "_get", "_summary", "delivery_health", "history"]:
                scopes.enter_context(patch.object(ledger, name, observed(name, getattr(ledger, name))))
            scopes.enter_context(patch.object(JSONResponse, "render", render))
            progress["phase"] = "requests"
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                for _ in range(iterations):
                    for name, (path, params, owner, status) in variants.items():
                        async def foreground():
                            token = selected.set(name); wall = time.perf_counter()
                            try: return await client.get(path, params=params, headers={"test-user": owner} if owner else {})
                            finally:
                                samples[name].append((time.perf_counter()-wall)*1000)
                                selected.reset(token)
                        async def background():
                            token = selected.set("background"); wall = time.perf_counter()
                            try: return await runtime.run_blocking("scheduling.pending", ledger.pending, time.time(), limit=4)
                            finally:
                                samples["background"].append((time.perf_counter()-wall)*1000)
                                selected.reset(token)
                        response, pending = await asyncio.gather(foreground(), background())
                        # All concurrent server work is awaited before client/oracle CPU.
                        statuses[name+":"+str(response.status_code)] += 1; sizes[name].append(len(response.content))
                        assert response.status_code == status and len(response.content) < 16*1024**2
                        wall, cpu = time.perf_counter(), time.thread_time()
                        assert all(item["state"] == "pending" for item in pending)
                        if status == 200:
                            expected = histories[target] if name == "history" else records[target] if name == "detail" else expected_page(records, histories, owner, params)
                            assert response.json() == expected, "complete public response oracle mismatch"
                        oracle_ms.append((time.perf_counter()-wall)*1000); oracle_cpu.append((time.thread_time()-cpu)*1000)
                        digest = hashlib.sha256(response.content).hexdigest()
                        assert hashes.setdefault(name, digest) == digest
            snapshot = runtime.snapshot()
    finally:
        progress["phase"] = "shutdown"
        await runtime.close(); logger.removeHandler(handler)
    assert not service.task and not service.last_tick
    return {"population": population, "payload_bytes": payload_bytes, "setup_ms": round(setup_ms, 3),
        "iterations": iterations, "request_ms": {k: distribution(v) for k, v in samples.items()},
        "response_bytes": {k: distribution(v) for k, v in sizes.items()},
        "response_byte_totals": {k: sum(v) for k, v in sizes.items()}, "response_sha256": hashes,
        "status_counts": dict(statuses), "counts": dict(counts), "phase_ms": {k: distribution(v) for k, v in phases.items()},
        "oracle_ms": distribution(oracle_ms), "oracle_thread_cpu_ms": distribution(oracle_cpu),
        "explicit_ge500ms": {k: sum(x>=500 for x in v) for k, v in samples.items()},
        "runtime": support.compact_runtime(snapshot), "warnings": warnings, "warning_counts": dict(warning_counts),
        "fixture_rows": {"schedules": len(records), "occurrences": sum(len(x) for x in histories.values())},
        "write_scope": "Only ordinary private Ledger.create/fire during seeding. No consumer/claim/finish/fake acceptance/delivery/remote provider/PAqueue control. Measured calls read only."}


def benchmark(*, population=480, iterations=6, payloads=(1024, 12288)):
    if not 24 <= population <= 480 or not 1 <= iterations <= 6 or any(not 1 <= n <= 12288 for n in payloads):
        raise ValueError("bounded population/iterations/valid payload")
    result = {"contract": "pa.scheduling-read-api-inventory/v1", "distribution_columns": DISTRIBUTION_COLUMNS, "profiles": []}
    with tempfile.TemporaryDirectory(prefix="pa-scheduling-read-api-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        progress = {}
        try:
            for payload in payloads:
                result["profiles"].append(asyncio.run(profile(root/str(payload), population=population,
                    payload_bytes=payload, iterations=iterations, progress=progress)))
        except Exception as exc:
            result.update(failed=True, **support.minimum_failure(exc), failed_phase=progress.get("phase"),
                partial_status_counts=dict(progress.get("statuses", {})), partial_counts=dict(progress.get("counts", {})),
                partial_request_ms={k: distribution(v) for k, v in progress.get("samples", {}).items()},
                partial_warning_counts=dict(progress.get("warning_counts", {})), partial_warnings=progress.get("warnings", []))
        result["fixture_storage"] = support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Actual FastAPI scheduling router/Ledger/default AsyncRuntime, isolated component. Test-user middleware supplies viewer principal, not native credentials/global CSRF/full Kernel/readiness/peer/provider coverage. Synthetic normal manual schedules and pending occurrences, no consumer/delivery/fake receipts. Default50/max200/cursor/session/pending/otherowner/detail/history/health and expected401/404/422. Full public response and owner/page/filter/history/health oracle AFTER awaited foreground+background work; no inline oracle CPU contender. Query trace only counts SELECTs on exact private ledger; numeric timing observer adds overhead. Runtime route samples and process operation windows include this component startup and all calls; explicit request samples separate. No source fix/deployed/whole-card acceptance, no physical IO/peakRAM claim.")
    return result


if __name__ == "__main__":
    print(json.dumps(benchmark(), separators=(",", ":")))
