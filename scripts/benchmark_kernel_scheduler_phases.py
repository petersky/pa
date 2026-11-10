"""Private full-Kernel scheduling attribution, bounded aggregate stdout only."""
import asyncio
from collections import Counter
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
import hashlib
import importlib.util
import json
import itertools
from pathlib import Path
import resource
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from unittest.mock import patch

from pa.auth.users import UserDirectory
from pa.core.performance import OperationObservation, current_observation
from pa.core.sqlite_metrics import ObservedConnection, ObservedCursor
from pa.domain.models import CardEvent, EventType
from pa.scheduling import events, store as ledger_module
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("kernel_scheduler_inbox_support",
    Path(__file__).with_name("benchmark_notification_inbox.py"))
inbox = importlib.util.module_from_spec(spec); spec.loader.exec_module(inbox)
SQL_FIELDS = ("sqlite_calls", "sqlite_rows", "sqlite_statement_ms", "sqlite_statement_thread_cpu_ms",
    "sqlite_commit_ms", "sqlite_commit_thread_cpu_ms", "sqlite_busy_failures",
    "object_get_calls", "object_get_bytes", "object_get_ms")


class Collector:
    """One worker's numeric counters; inclusive phases must not be summed."""
    def __init__(self):
        self.active = ContextVar("private_scheduler_phase", default=None)
        self.completed = []
        self.overflow = 0

    def invoke(self, name, call, *args, **kwargs):
        current = self.active.get()
        if current is None or current["thread_id"] != threading.get_ident():
            return call(*args, **kwargs)
        wall, cpu = time.perf_counter(), time.thread_time()
        error, rows, size = 0, 0, 0
        try:
            value = call(*args, **kwargs)
            if isinstance(value, bytes): size = len(value)
            if name.startswith("fetch"):
                rows = len(value) if isinstance(value, list) else int(value is not None)
            return value
        except BaseException as exc:
            error = int(not isinstance(exc, StopIteration))
            if isinstance(exc, sqlite3.OperationalError):
                code = getattr(exc, "sqlite_errorcode", 0) & 0xFF
                if code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}: current["busy_failures"] += 1
            raise
        finally:
            elapsed, used = (time.perf_counter()-wall)*1000, (time.thread_time()-cpu)*1000
            cell = current["phases"].setdefault(name, [0, 0., 0., 0., 0., 0, 0, 0])
            cell[0] += 1; cell[1] += elapsed; cell[2] += used
            cell[3] = max(cell[3], elapsed); cell[4] = max(cell[4], used)
            cell[5] += error; cell[6] += rows; cell[7] += size

    def wrap(self, name, call):
        @wraps(call)
        def observed(*args, **kwargs): return self.invoke(name, call, *args, **kwargs)
        return observed


def measured_poll(root, collector, progress, *, detailed):
    native = events.poll_events
    expected = root/"data"/"scheduling.db"
    class Cursor(ObservedCursor):
        def execute(self, sql, *args, **kwargs):
            token = sql.lstrip().split(None, 1)[0].lower()
            operation = token if token in {"begin", "select", "insert", "update", "pragma", "delete"} else "other"
            before = self.connection.total_changes
            try: return collector.invoke("sql_"+operation, super().execute, sql, *args, **kwargs)
            finally:
                current = collector.active.get()
                if current is not None: current["changes"] += self.connection.total_changes-before
        def fetchone(self): return collector.invoke("fetch_one", super().fetchone)
        def fetchall(self): return collector.invoke("fetch_all", super().fetchall)
        def fetchmany(self, size=None):
            return collector.invoke("fetch_many", super().fetchmany, *(() if size is None else (size,)))
        def __next__(self): return collector.invoke("fetch_next", super().__next__)
    class Connection(ObservedConnection):
        def cursor(self, factory=None): return super().cursor(factory or Cursor)
        def execute(self, sql, *args, **kwargs): return self.cursor().execute(sql, *args, **kwargs)
        def commit(self): return collector.invoke("commit", super().commit)
        def rollback(self): return collector.invoke("rollback", super().rollback)
        def close(self): return collector.invoke("close", super().close)
    class PrivateSQLite:
        """Only the private ledger module sees this; global sqlite3 is untouched."""
        def __getattr__(self, key): return getattr(sqlite3, key)
        def connect(self, database, *args, **kwargs):
            assert Path(database) == expected
            assert kwargs.get("factory", sqlite3.Connection) in {sqlite3.Connection, ObservedConnection}
            return collector.invoke("connect", sqlite3.connect, database, *args, **(kwargs|{"factory": Connection}))
    @wraps(native)
    def poll(ctx, ledger):
        assert Path(ctx.settings.data_dir).resolve() == (root/"data").resolve()
        assert ledger.path.resolve() == expected.resolve()
        record = {"thread_id": threading.get_ident(), "start_phase": progress.get("phase"),
            "phases": {}, "changes": 0, "busy_failures": 0}
        token = collector.active.set(record)
        started, cpu = time.perf_counter(), time.thread_time()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        try:
            with ExitStack() as scopes:
                if detailed:
                    log = ctx.services["event_log"]
                    scopes.enter_context(patch.object(ledger_module, "sqlite3", PrivateSQLite()))
                    for obj, attr, name in ((log, "get_head", "head"), (log, "get_commit", "commit_object"),
                            (log, "get_event", "event_object"), (log.store, "get", "object_bytes"),
                            (events, "_records", "owners"), (events, "event_fields", "event_fields"),
                            (UserDirectory, "__init__", "users"), (ledger, "event", "ledger_event")):
                        scopes.enter_context(patch.object(obj, attr, collector.wrap(name, getattr(obj, attr))))
                return native(ctx, ledger)
        except BaseException as exc:
            record["error_type"] = type(exc).__name__
            raise
        finally:
            record.update(wall_ms=round((time.perf_counter()-started)*1000, 3),
                thread_cpu_ms=round((time.thread_time()-cpu)*1000, 3), end_phase=progress.get("phase"))
            after = resource.getrusage(resource.RUSAGE_SELF)
            record["process_context_switch_delta"] = [after.ru_nvcsw-usage.ru_nvcsw, after.ru_nivcsw-usage.ru_nivcsw]
            record["process_io_block_delta"] = [after.ru_inblock-usage.ru_inblock, after.ru_oublock-usage.ru_oublock]
            observation = current_observation.get()
            if observation is not None:
                record["sql_observation"] = [round(getattr(observation, key), 3) for key in SQL_FIELDS]
            record.pop("thread_id")
            if not detailed:
                record.pop("changes")
                record.pop("busy_failures")
            record["phases"] = {k: [round(v, 3) if isinstance(v, float) else v for v in values]
                for k, values in record["phases"].items()}
            if len(collector.completed) < 16: collector.completed.append(record)
            else: collector.overflow += 1
            collector.active.reset(token)
    assert poll.__name__ == "poll_events"
    return poll


def one(root, *, mode, population, iterations):
    root.mkdir(); progress = {}; collector = Collector()
    with patch.object(events, "poll_events", measured_poll(root, collector, progress, detailed=mode == "observed")):
        try: result = asyncio.run(inbox.run(root, population=population, iterations=iterations, progress=progress))
        except Exception as exc:
            result = {"failed": True, **inbox.shared.support.minimum_failure(exc),
                "partial_status_counts": dict(progress.get("statuses", {})), "failed_phase": progress.get("phase")}
    keys = ("failed", "error_type", "location", "partial_status_counts", "failed_phase", "startup_ms", "seed_ms",
        "request_ms", "response_bytes", "status_counts", "operation_deltas", "expiry_passes", "detail_and_auth_probes",
        "explicit_ge500ms", "warning_counts", "warning_phase_counts", "warning_observations",
        "private_response_retained", "public_redaction_verified")
    return {**{k: result[k] for k in keys if k in result}, "mode": mode,
        "poll_calls": collector.completed, "poll_overflow": collector.overflow,
        "event_loop": result.get("runtime", {}).get("event_loop"),
        "console_characters": result.get("console_characters")}


def benchmark(*, pairs=2, population=480, iterations=4):
    if not 1 <= pairs <= 2 or not 30 <= population <= 480 or not 1 <= iterations <= 4:
        raise ValueError("bounded population")
    result = {"contract": "pa.kernel-scheduler-phases/v1", "population": population,
        "iterations": iterations, "pairs": pairs, "runs": [],
        "phase_columns": ["calls", "wall_total_ms", "thread_cpu_total_ms", "wall_max_ms", "thread_cpu_max_ms", "errors", "rows", "bytes"], "sql_observation_columns": SQL_FIELDS}
    with tempfile.TemporaryDirectory(prefix="pa-kernel-scheduler-phases-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        try:
            for pair in range(pairs):
                for mode in (("control", "observed") if pair % 2 == 0 else ("observed", "control")):
                    run = one(root/f"{pair}-{mode}", mode=mode, population=population, iterations=iterations)
                    run["pair"] = pair; result["runs"].append(run)
                    if run.get("failed"): break
                if any(run.get("failed") for run in result["runs"]): break
        except Exception as exc: result.update(failed=True, **inbox.shared.support.minimum_failure(exc))
        result["fixture_storage"] = inbox.shared.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Alternating independent full private Kernels, original inbox creation/auth/oracle/cadence/shutdown. Sole normal Scheduler consumer, no manual polling/delivery handlers/provider/peer/native/live state. Original poll_events name and100commit bound preserved. Observer replaces only private ledger-module sqlite binding guarded to exact new root/data/scheduling.db; global sqlite3.connect untouched. Both modes have minimal poll wall/CPU and process rusage samples; observed adds numeric inclusive phases/calls/errors/rows/bytes, never SQL/parameters/results. Phases overlap and cannot be summed. Per-process context switches/IO blocks include all private foreground/background work; neither wall-minusCPU nor block counts prove GIL/physicalIO/fsync. First Kernel startup/caches differ; two pairs are small, observer overhead/control tails must remain explicit. Runtime windows include boot/warmup; request/deltas/lifecycle warnings distinct. No acceptance claim.")
    assert len(json.dumps(result).encode()) <= 128*1024
    return result


def factory_overhead(*, pairs=3, commits=100):
    """Same real DAG and complete logical output; only connection observer differs."""
    if not 1 <= pairs <= 3 or not 1 <= commits <= 100: raise ValueError("bounded population")
    result = {"contract": "pa.scheduler-sql-observer-overhead/v1", "pairs": pairs,
        "commits": commits, "runs": []}
    fields = SQL_FIELDS
    with tempfile.TemporaryDirectory(prefix="pa-scheduler-sql-observer-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        try:
            for pair in range(pairs):
                folder = root/str(pair); folder.mkdir()
                user = UserDirectory(folder).ensure_default_user(); owner = "user:"+user.id
                log = EventLog(ObjectStore(folder/"objects"), folder, "synthetic")
                ctx = SimpleNamespace(settings=SimpleNamespace(subscribed_realms=["default"], data_dir=folder),
                    services={"event_log": log})
                def append(index):
                    event = CardEvent(id=f"event-{index}", type=EventType.CARD_CREATED, realm_id="default",
                        card_id=f"card-{index}", author_principal="owner", author_instance="synthetic",
                        timestamp=datetime(2026, 11, 10, tzinfo=UTC), payload={"title": "Synthetic", "body": "x"*8192})
                    log.append_event(event)
                append("baseline")
                ledgers = {}
                for mode in ("plain", "numeric"):
                    ids = itertools.count()
                    with patch.object(ledger_module, "uuid4", lambda: f"synthetic-{next(ids):04d}"):
                        ledger = ledger_module.Ledger(folder/(mode+".db"))
                        ledger.create(owner, "create", ledger_module.Schedule(name="Synthetic", payload={"fixture": True},
                            trigger={"kind": "event", "event_type": EventType.CARD_CREATED.value},
                            destination={"kind": "inbox", "target": owner}), 0)
                    events.poll_events(ctx, ledger); ledgers[mode] = ledger
                for index in range(commits): append(index)
                digests = []
                for mode in (("plain", "numeric") if pair % 2 == 0 else ("numeric", "plain")):
                    ledger = ledgers[mode]; ids = itertools.count(1)
                    class PrivateFactory:
                        def __getattr__(self, key): return getattr(sqlite3, key)
                        def connect(self, database, *args, **kwargs):
                            assert Path(database) == ledger.path
                            assert kwargs.get("factory") is ObservedConnection
                            return sqlite3.connect(database, *args, **(kwargs|{"factory":
                                sqlite3.Connection if mode == "plain" else ObservedConnection}))
                    observation = OperationObservation(); token = current_observation.set(observation)
                    wall, cpu = time.perf_counter(), time.thread_time()
                    try:
                        with (patch.object(ledger_module, "sqlite3", PrivateFactory()),
                              patch.object(ledger_module, "uuid4", lambda: f"synthetic-{next(ids):04d}"),
                              patch.object(events.time, "time", lambda: 1794312000.0)):
                            events.poll_events(ctx, ledger)
                    finally:
                        elapsed, used = (time.perf_counter()-wall)*1000, (time.thread_time()-cpu)*1000
                        current_observation.reset(token)
                    with patch.object(events.time, "time", lambda: 1794312000.0):
                        page = ledger.list_page(owner)
                        state = {"page": page, "pending": ledger.pending(1794312000.),
                            "get": ledger.get(owner, "synthetic-0000"), "history": ledger.history(owner, "synthetic-0000"),
                            "status": ledger.status(owner, "synthetic-0000"), "health": ledger.delivery_health(owner, 1794312000.)}
                        encoded = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
                    before = len(state["pending"])
                    events.poll_events(ctx, ledger_module.Ledger(ledger.path))
                    assert len(ledger.pending(1794312000.)) == before == commits
                    digest = hashlib.sha256(encoded).hexdigest(); digests.append(digest)
                    result["runs"].append({"pair": pair, "mode": mode, "wall_ms": round(elapsed, 3),
                        "thread_cpu_ms": round(used, 3), "observation": {k: round(getattr(observation, k), 3) for k in fields},
                        "complete_read_state_bytes": len(encoded), "complete_read_state_sha256": digest})
                assert digests[0] == digests[1]
        except Exception as exc: result.update(failed=True, **inbox.shared.support.minimum_failure(exc))
        result["fixture_storage"] = inbox.shared.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        scope=f"{pairs} alternating actual private same-DAG{commits}commit/{commits}occurrence polls, no Kernel/delivery/network. Original connection vs existing numeric observer under same OperationObservation context. Exact complete final get/list/pending/history/status/health bytes and reopened replay counts. Fixture-only stable IDs/clock; no copied databases. Module-local guarded factory, global sqlite3.connect unchanged. Wall/threadCPU/calls/rows/bytes; no raw SQL/results. Direct-component overhead is not a bound on contended Kernel/native overhead or acceptance.",
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    return result


@contextmanager
def fresh_source_reader(ledger):
    """Pre-reuse source behavior: one ordinary read scope per SELECT."""
    class Reader:
        def execute(self, *args, **kwargs):
            scope = ledger.connect(read_only=True); db = scope.__enter__()
            try: cursor = db.execute(*args, **kwargs)
            except BaseException:
                scope.__exit__(*sys.exc_info()); raise
            class CursorLease:
                def __getattr__(self, key): return getattr(cursor, key)
                def close(self):
                    try: cursor.close()
                    finally: scope.__exit__(None, None, None)
            return CursorLease()
    yield Reader()


def reader_kernel_comparison(*, pairs=2, population=480, iterations=4):
    if not 1 <= pairs <= 2 or not 30 <= population <= 480 or not 1 <= iterations <= 4: raise ValueError("bounded population")
    native = ledger_module.Ledger._source_reader
    result = {"contract": "pa.kernel-source-reader-comparison/v1", "runs": [], "pairs": pairs,
        "population": population, "iterations": iterations, "sql_observation_columns": SQL_FIELDS}
    with tempfile.TemporaryDirectory(prefix="pa-kernel-source-reader-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        for pair in range(pairs):
            for mode in (("fresh", "reused") if pair % 2 == 0 else ("reused", "fresh")):
                with patch.object(ledger_module.Ledger, "_source_reader", fresh_source_reader if mode == "fresh" else native):
                    run = one(root/f"{pair}-{mode}", mode=mode, population=population, iterations=iterations)
                run["pair"] = pair; result["runs"].append(run)
                if run.get("failed"): break
            if any(run.get("failed") for run in result["runs"]): break
        result["fixture_storage"] = inbox.shared.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Alternating actual independent private full Kernels. Only source-read connection lifetime changes; fresh control uses original query_only/BEGIN/commit/close scope perSELECT. Default100commit bound, all writes/claims/checkpoints/auth/model/error semantics and full inbox oracle unchanged. Numeric SQLite observer active in both modes. No detailed prototype, only preserved-name poll timer/rusage. Small cohorts and dynamic source heads; no native/deployed/whole-card acceptance.")
    return result


def compact_runs(result):
    """Losslessly factor repeated numeric-grid labels before bounded reporting."""
    runs = result.get("runs", [])
    projected = {**result, "runs": [dict(run) for run in runs], "numeric_grid_headers": {}}
    for field in ("request_ms", "response_bytes", "operation_deltas", "status_counts"):
        mappings = [run.get(field, {}) for run in runs]
        names = sorted({key for mapping in mappings for key in mapping})
        cells = [value for mapping in mappings for value in mapping.values()]
        if not cells: continue
        columns = None
        if all(isinstance(value, dict) for value in cells):
            columns = sorted({key for value in cells for key in value})
            if any(set(value) != set(columns) for value in cells): continue
        elif any(isinstance(value, dict) for value in cells): continue
        projected["numeric_grid_headers"][field] = {"names": names, "columns": columns}
        for target, mapping in zip(projected["runs"], mappings):
            values = [[mapping[name][key] for key in columns] if columns is not None else mapping[name]
                for name in names if name in mapping]
            present = [index for index, name in enumerate(names) if name in mapping]
            rebuilt = {names[index]: dict(zip(columns, value)) if columns is not None else value
                for index, value in zip(present, values)}
            assert rebuilt == mapping
            if field in target: target[field] = {"present": present, "values": values}
    return projected


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(); parser.add_argument("--factory-overhead", action="store_true")
    parser.add_argument("--reader-kernel", action="store_true")
    args = parser.parse_args()
    print(json.dumps(compact_runs(factory_overhead() if args.factory_overhead else reader_kernel_comparison() if args.reader_kernel else benchmark()), separators=(",", ":")))
