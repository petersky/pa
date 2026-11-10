"""Bounded actual private DAG/ledger poll attribution; no delivery or server."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
import itertools
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

from pa.auth.users import UserDirectory
from pa.domain.models import CardEvent, EventType
from pa.domain.notifications import Notification
from pa.scheduling import events
from pa.scheduling.store import Ledger, Schedule
import pa.scheduling.store as ledger_module
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

spec = importlib.util.spec_from_file_location("scheduler_inventory_support",
    Path(__file__).with_name("benchmark_systemic_latency.py"))
support = importlib.util.module_from_spec(spec); spec.loader.exec_module(support)


class Observations:
    def __init__(self):
        self.active = ContextVar("scheduler_probe", default=None)
        self.lock = threading.Lock()
        self.phases = defaultdict(list)
        self.counts = Counter()

    def add(self, name, wall, cpu, **counts):
        phase = self.active.get()
        if phase is None: return
        with self.lock:
            self.phases[phase+":"+name+":wall_ms"].append((time.perf_counter()-wall)*1000)
            self.phases[phase+":"+name+":thread_cpu_ms"].append((time.thread_time()-cpu)*1000)
            for key, value in counts.items(): self.counts[phase+":"+name+":"+key] += value

    def wrap(self, name, fn):
        def observed(*args, **kwargs):
            wall, cpu = time.perf_counter(), time.thread_time()
            try:
                value = fn(*args, **kwargs)
                if isinstance(value, bytes):
                    phase = self.active.get()
                    if phase:
                        with self.lock: self.counts[phase+":"+name+":bytes"] += len(value)
                return value
            finally: self.add(name, wall, cpu, calls=1)
        return observed


def profile(root, *, name, batches, commits_per_batch, schedules, progress):
    root.mkdir()
    users = UserDirectory(root)
    user = users.ensure_default_user()
    denied = users.create_user("denied", "synthetic-private-password")
    owner = "user:"+user.id
    log = EventLog(ObjectStore(root/"objects"), root, "synthetic")
    ctx = SimpleNamespace(settings=SimpleNamespace(subscribed_realms=["default"], data_dir=root),
        services={"event_log": log})
    ledger = Ledger(root/"schedule.db")
    now = 0
    expected_matches = set()
    def notice(index):
        record = Notification(id=f"synthetic-{index}", title="Synthetic source event", body="x"*8192,
            distributable=False)
        return CardEvent(id=f"synthetic-event-{index}", type=EventType.NOTIFICATION_UPSERTED,
            realm_id="default", author_principal="user:local", author_instance="synthetic",
            timestamp=datetime(2026, 11, 10, tzinfo=UTC), payload=record.model_dump(mode="json"))
    log.append_event(notice("baseline"))
    events.poll_events(ctx, ledger)  # Installation boundary: no historical triggers.
    matching_id = None
    for index in range(schedules):
        selected_owner = "user:"+denied.id if index == 2 else owner
        created = ledger.create(selected_owner, f"create-{index}", Schedule(name=f"Synthetic{index}",
            trigger={"kind": "event", "event_type": (EventType.NOTIFICATION_UPSERTED.value
                if index != 1 else EventType.CARD_CREATED.value)},
            destination={"kind": "inbox", "target": selected_owner}, payload={"synthetic": True}), now)
        if index == 0: matching_id = created["id"]
    observations = Observations()
    original_connect = sqlite3.connect
    class Connection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            operation = sql.lstrip().split(None, 1)[0].lower()
            wall, cpu = time.perf_counter(), time.thread_time()
            before = self.total_changes
            try: return super().execute(sql, *args, **kwargs)
            finally: observations.add("sql_"+operation, wall, cpu, calls=1, changes=self.total_changes-before)
        def commit(self):
            wall, cpu = time.perf_counter(), time.thread_time()
            try: return super().commit()
            finally: observations.add("commit", wall, cpu, calls=1)
        def close(self):
            wall, cpu = time.perf_counter(), time.thread_time()
            try: return super().close()
            finally: observations.add("close", wall, cpu, calls=1)
    def connected(database, *args, **kwargs):
        if Path(database) != ledger.path: return original_connect(database, *args, **kwargs)
        wall, cpu = time.perf_counter(), time.thread_time()
        try: return original_connect(database, *args, **(kwargs|{"factory": Connection}))
        finally: observations.add("connect", wall, cpu, calls=1)
    poll_times, poll_cpu, foreground, setup = [], [], [], []
    progress.update(profile=name, poll_times=poll_times, foreground=foreground, counts=observations.counts)
    batch_counts = []
    def consume():
        token = observations.active.set("poll")
        wall, cpu = time.perf_counter(), time.thread_time()
        try: events.poll_events(ctx, ledger)
        finally:
            poll_times.append((time.perf_counter()-wall)*1000)
            poll_cpu.append((time.thread_time()-cpu)*1000)
            observations.active.reset(token)
    def foreground_read():
        token = observations.active.set("foreground")
        wall = time.perf_counter()
        try:
            result = ledger.list_page(owner, limit=50)
            assert len(result["schedules"]) == (2 if schedules else 0)
        finally:
            foreground.append((time.perf_counter()-wall)*1000)
            observations.active.reset(token)
    original_user_init = UserDirectory.__init__
    with (patch.object(sqlite3, "connect", connected),
          patch.object(log, "get_head", observations.wrap("get_head", log.get_head)),
          patch.object(log, "get_commit", observations.wrap("get_commit", log.get_commit)),
          patch.object(log, "get_event", observations.wrap("get_event", log.get_event)),
          patch.object(log.store, "get", observations.wrap("object_get", log.store.get)),
          patch.object(events, "_records", observations.wrap("owners", events._records)),
          patch.object(UserDirectory, "__init__", observations.wrap("users_init", original_user_init)),
          patch.object(ledger, "event", observations.wrap("ledger_event", ledger.event))):
        with ThreadPoolExecutor(max_workers=2) as executor:
            for batch in range(batches):
                started = time.perf_counter()
                for index in range(commits_per_batch):
                    event = notice(f"{batch}-{index}")
                    log.append_event(event)
                    if schedules: expected_matches.add(event.id)
                setup.append((time.perf_counter()-started)*1000)
                before = dict(observations.counts)
                worker = executor.submit(consume)
                # At most20 ordinary metadata reads per batch; no scheduler delivery loop.
                for _ in range(20):
                    foreground_read()
                    if worker.done(): break
                    time.sleep(.005)
                worker.result()
                batch_counts.append({key: value-before.get(key, 0) for key, value in observations.counts.items()
                    if key.startswith("poll:") and value-before.get(key, 0)})
                # The100 bound is unchanged; each declared batch contains exactly100 commits.
                assert batch_counts[-1]["poll:get_commit:calls"] == commits_per_batch
        # Reopen the real ledger to verify durable source cursors and occurrence deduplication.
        reopened = Ledger(ledger.path)
        events.poll_events(ctx, reopened)
        with reopened.connect() as db:
            occurrences = [dict(row) for row in db.execute("SELECT schedule_id,identity,envelope FROM occurrences")]
            pending_sources = db.execute("SELECT count(*) FROM source_commits WHERE processed=0").fetchone()[0]
            source_heads = db.execute("SELECT hash FROM source_heads WHERE realm='default'").fetchone()[0]
        assert not pending_sources and source_heads == log.get_head("default")
        assert {row["identity"].removeprefix("event:") for row in occurrences} == expected_matches
        assert len(occurrences) == len(expected_matches)
        assert all(row["schedule_id"] == matching_id for row in occurrences)
        assert all(json.loads(row["envelope"])["event_at"] is not None for row in occurrences)
        if not schedules: assert not ledger.pending(time.time(), limit=1)
        page = ledger.list_page(owner)
        public_state = {"page": page, "pending": ledger.pending(time.time()),
            "health": ledger.delivery_health(owner, time.time()),
            "get": [ledger.get(owner, row["id"]) for row in page["schedules"]],
            "history": [ledger.history(owner, row["id"]) for row in page["schedules"]],
            "status": [ledger.status(owner, row["id"]) for row in page["schedules"]]}
        encoded = json.dumps(public_state, sort_keys=True, separators=(",", ":")).encode()
    return {"name": name, "batches": batches, "commits_per_batch": commits_per_batch, "schedules": schedules,
        "poll_ms": support.distribution(poll_times), "poll_thread_cpu_ms": support.distribution(poll_cpu),
        "foreground_ms": support.distribution(foreground), "setup_ms": support.distribution(setup),
        "poll_ge500ms": sum(v >= 500 for v in poll_times), "foreground_ge500ms": sum(v >= 500 for v in foreground),
        "phase_totals": {key: {"calls": len(values), "total_ms": round(sum(values), 3),
            "max_ms": round(max(values), 3)} for key, values in observations.phases.items()},
        "counters": dict(observations.counts), "batch_counters": batch_counts,
        "occurrences": len(occurrences), "source_pending": pending_sources, "reopen_replay_duplicates": 0,
        "complete_read_state_bytes": len(encoded), "complete_read_state_sha256": hashlib.sha256(encoded).hexdigest()}


def benchmark(*, batches=3, commits_per_batch=100):
    if not 1 <= batches <= 3 or not 1 <= commits_per_batch <= 100: raise ValueError("bounded population")
    result = {"contract": "pa.scheduler-event-poll-inventory/v1", "profiles": []}
    progress = {}
    with tempfile.TemporaryDirectory(prefix="pa-scheduler-poll-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        try:
            for name, schedules in (("empty_owners", 0), ("matching_nonmatching_denied", 3)):
                result["profiles"].append(profile(root/name, name=name, batches=batches,
                    commits_per_batch=commits_per_batch, schedules=schedules, progress=progress))
        except Exception as exc:
            result.update(failed=True, **support.minimum_failure(exc), failed_profile=progress.get("profile"),
                partial_poll_ms=support.distribution(progress.get("poll_times", [])),
                partial_foreground_ms=support.distribution(progress.get("foreground", [])),
                partial_counts=dict(progress.get("counts", {})))
        result["fixture_storage"] = support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Actual private EventLog/ObjectStore/DagIndex/UserDirectory/Ledger; direct sole synchronous poll consumer, no Kernel/provider/network/delivery or live data. One admin matching schedule, one nonmatching, one editor without realm membership denied. Default100 commit bound unchanged. Initial head installation/reopened cursor/replay/dedup and event time verified. Concurrent ordinary ledger list reads. Observer phases overlap and add overhead; setup/CPU/wall distinct;3samples/profile not fleet percentiles. No durability pragma changes; SQLite commit timing is not direct fsync/physical IO attribution. No native/deployed/startup/whole-card acceptance.")
    assert len(json.dumps(result).encode()) <= 128*1024
    return result


def compare(*, pairs=3, commits=100):
    if not 1 <= pairs <= 3 or not 1 <= commits <= 100: raise ValueError("bounded population")
    result = {"contract": "pa.scheduler-read-transaction-comparison/v1", "runs": []}
    native = Ledger.connect; progress = {}
    pooled = defaultdict(lambda: {"poll": [], "foreground": []})
    @contextmanager
    def immediate(ledger, *, read_only=False):
        # Original behavior: every scope, including pure reads, reserves a writer.
        with native(ledger) as db: yield db
    with tempfile.TemporaryDirectory(prefix="pa-scheduler-read-compare-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3: raise RuntimeError("At least4GiB free required")
        try:
            for name, schedules in (("empty_owners", 0), ("matching_nonmatching_denied", 3)):
                for pair in range(pairs):
                    digests = []
                    for mode in (("before", "after") if pair % 2 == 0 else ("after", "before")):
                        identifiers = itertools.count()
                        with (patch.object(Ledger, "connect", immediate if mode == "before" else native),
                              patch.object(ledger_module, "uuid4", lambda: f"synthetic-{next(identifiers):04d}"),
                              patch.object(events.time, "time", lambda: 1794312000.0)):
                            run = profile(root/f"{name}-{pair}-{mode}", name=name, batches=1,
                                commits_per_batch=commits, schedules=schedules, progress=progress)
                        run.update(mode=mode, pair=pair); result["runs"].append(run)
                        pooled[name+":"+mode]["poll"].extend(progress["poll_times"])
                        pooled[name+":"+mode]["foreground"].extend(progress["foreground"])
                        digests.append(run["complete_read_state_sha256"])
                    assert digests[0] == digests[1]
        except Exception as exc:
            result.update(failed=True, **support.minimum_failure(exc), failed_profile=progress.get("profile"),
                partial_counts=dict(progress.get("counts", {})))
        result["fixture_storage"] = support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),
        pooled_ms={key: {phase: support.distribution(values) for phase, values in item.items()}
            for key, item in pooled.items()},
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Alternating independent real private DAG/ledger fixtures, same100-commit bound and original writer-reserving connect behavior vs native read-only scopes. No copied databases. Fixture-only deterministic schedule/occurrence IDs and wall clock permit COMPLETE final get/list/pending/history/status/health byte parity; no fields removed. Event timestamps fixed, full objects still validated. Observers and concurrent list reads retained; all write transactions unchanged. No delivery/Kernel/native/deployed/whole-card acceptance; no physical IO claim.")
    assert len(json.dumps(result).encode()) <= 128*1024
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(); parser.add_argument("--compare", action="store_true")
    print(json.dumps(compare() if parser.parse_args().compare else benchmark(), separators=(",", ":")))
