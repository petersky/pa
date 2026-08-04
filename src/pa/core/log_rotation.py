"""Lossless service-stream rotation with asynchronous archive maintenance."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from pa.config import Settings
from pa.core.io import atomic_write_text
from pa.core.logging import redact_log_text

STATUS_FILE = "status.json"
_ACTIVE_NAMES = ("server.log", "server.err.log")


@dataclass(frozen=True)
class LogRotationPolicy:
    max_bytes: int
    interval_seconds: float
    retention_count: int
    retention_max_age_seconds: float
    retention_max_total_bytes: int
    disk_pressure_free_bytes: int

    @classmethod
    def from_settings(cls, settings: Settings) -> LogRotationPolicy:
        return cls(
            max_bytes=settings.log_rotation_max_bytes,
            interval_seconds=settings.log_rotation_interval_seconds,
            retention_count=settings.log_retention_count,
            retention_max_age_seconds=settings.log_retention_max_age_seconds,
            retention_max_total_bytes=settings.log_retention_max_total_bytes,
            disk_pressure_free_bytes=settings.log_disk_pressure_free_bytes,
        )


def read_log_status(data_dir: Path) -> dict[str, object] | None:
    """Read the redaction-safe snapshot written by the service supervisor."""
    path = data_dir / "logs" / STATUS_FILE
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError, OSError, ValueError, json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


class ServiceLogSupervisor:
    """Own active service logs while archive work runs on a separate worker."""

    def __init__(
        self,
        log_dir: Path,
        policy: LogRotationPolicy,
        *,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        disk_usage: Callable[[Path], object] = shutil.disk_usage,
    ) -> None:
        self.log_dir = log_dir
        self.policy = policy
        self._clock = clock
        self._monotonic = monotonic
        self._disk_usage = disk_usage
        self._state_lock = threading.Lock()
        self._archive_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pa-log-maintenance"
        )
        self._futures: set[Future[None]] = set()
        self._closed = False
        self._sequence = 0
        self._last_status_write = 0.0
        self._last_status_submit = 0.0
        previous = read_log_status(self.log_dir.parent) or {}

        def previous_counter(name: str) -> int:
            try:
                return max(0, int(previous.get(name, 0)))
            except TypeError, ValueError:
                return 0

        self._counters = {
            name: previous_counter(name)
            for name in (
                "rotation_failures",
                "compression_failures",
                "prune_failures",
                "dropped_bytes",
            )
        }
        self._disk_state = "ok"
        self._free_bytes: int | None = None
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.stdout = _ActiveLog(self, "server.log")
        self.stderr = _ActiveLog(self, "server.err.log")
        self._submit_maintenance(None)
        self._write_status(force=True)

    def write(self, name: str, data: bytes) -> None:
        sink = self.stdout if name == "server.log" else self.stderr
        sink.write(data)

    def _under_pressure(self) -> bool:
        try:
            free = int(self._disk_usage(self.log_dir).free)
        except OSError, TypeError, ValueError:
            with self._state_lock:
                self._disk_state = "unknown"
                self._free_bytes = None
            return False
        pressure = free < self.policy.disk_pressure_free_bytes
        with self._state_lock:
            self._disk_state = "pressure" if pressure else "ok"
            self._free_bytes = free
        if pressure:
            self._submit_maintenance(None)
        return pressure

    def _dropped(self, amount: int) -> None:
        with self._state_lock:
            self._disk_state = "dropping"
            self._counters["dropped_bytes"] += amount
        self._submit_status()

    def _failed(self, counter: str) -> None:
        with self._state_lock:
            self._counters[counter] += 1
        self._submit_status(force=True)

    def _next_archive(self, active: Path) -> Path:
        with self._state_lock:
            self._sequence += 1
            sequence = self._sequence
        stamp = datetime.fromtimestamp(self._clock(), UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        return active.with_name(f"{active.name}.{stamp}.{os.getpid()}.{sequence}")

    def _submit_maintenance(self, archive: Path | None) -> None:
        with self._state_lock:
            if self._closed:
                return
            if archive is None and self._futures:
                return
            future = self._executor.submit(self._maintain, archive)
            self._futures.add(future)
        future.add_done_callback(self._maintenance_done)

    def _maintenance_done(self, future: Future[None]) -> None:
        with self._state_lock:
            self._futures.discard(future)

    def _archives(self) -> list[Path]:
        result: list[Path] = []
        for active_name in _ACTIVE_NAMES:
            for path in self.log_dir.glob(f"{active_name}.*"):
                if path.name.endswith(".tmp") or not path.is_file():
                    continue
                result.append(path)
        return result

    def _compress(self, archive: Path) -> None:
        if archive.suffix == ".gz" or not archive.exists():
            return
        compressed = archive.with_name(archive.name + ".gz")
        temporary = compressed.with_name(compressed.name + ".tmp")
        try:
            source_stat = archive.stat()
            with archive.open("rb") as source, gzip.open(temporary, "wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            os.utime(
                temporary,
                ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
            )
            os.replace(temporary, compressed)
            archive.unlink()
        except OSError:
            self._failed("compression_failures")
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def _prune(self) -> None:
        now = self._clock()

        def mtime(path: Path) -> float:
            try:
                return path.stat().st_mtime
            except OSError:
                return -1

        archives = sorted(self._archives(), key=mtime, reverse=True)
        keep: list[Path] = []
        total = 0
        for active_name in _ACTIVE_NAMES:
            try:
                total += (self.log_dir / active_name).stat().st_size
            except OSError:
                pass
        victims: list[Path] = []
        for path in archives:
            try:
                stat = path.stat()
            except OSError:
                self._failed("prune_failures")
                continue
            over_count = len(keep) >= self.policy.retention_count
            over_age = now - stat.st_mtime > self.policy.retention_max_age_seconds
            over_bytes = total + stat.st_size > self.policy.retention_max_total_bytes
            if over_count or over_age or over_bytes:
                victims.append(path)
            else:
                keep.append(path)
                total += stat.st_size

        try:
            free = int(self._disk_usage(self.log_dir).free)
        except OSError, TypeError, ValueError:
            free = self.policy.disk_pressure_free_bytes
        if free < self.policy.disk_pressure_free_bytes:
            for path in reversed(keep):
                victims.append(path)
                try:
                    free += path.stat().st_size
                except OSError:
                    pass
                if free >= self.policy.disk_pressure_free_bytes:
                    break

        for path in dict.fromkeys(victims):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                self._failed("prune_failures")

    def _maintain(self, archive: Path | None) -> None:
        with self._archive_lock:
            for active_name in _ACTIVE_NAMES:
                for temporary in self.log_dir.glob(f"{active_name}.*.gz.tmp"):
                    try:
                        temporary.unlink(missing_ok=True)
                    except OSError:
                        self._failed("prune_failures")
            if archive is not None:
                self._compress(archive)
            else:
                for path in self._archives():
                    if path.suffix != ".gz":
                        self._compress(path)
            self._prune()
            self._write_status(force=True)

    def _submit_status(self, *, force: bool = False) -> None:
        now = self._monotonic()
        with self._state_lock:
            if self._closed or (not force and now - self._last_status_submit < 1.0):
                return
            self._last_status_submit = now
            future = self._executor.submit(self._write_status, force=True)
            self._futures.add(future)
        future.add_done_callback(self._maintenance_done)

    def snapshot(self) -> dict[str, object]:
        now = self._clock()
        files = [
            path
            for path in self.log_dir.iterdir()
            if path.is_file()
            and (
                path.name in _ACTIVE_NAMES
                or any(path.name.startswith(f"{name}.") for name in _ACTIVE_NAMES)
            )
            and not path.name.endswith(".tmp")
        ]
        sizes: dict[str, int] = {}
        mtimes: list[float] = []
        total = 0
        for path in files:
            try:
                stat = path.stat()
            except OSError:
                continue
            total += stat.st_size
            mtimes.append(stat.st_mtime)
            if path.name in _ACTIVE_NAMES:
                sizes[path.name] = stat.st_size
        with self._state_lock:
            counters = dict(self._counters)
            state = self._disk_state
            free = self._free_bytes
        return {
            "schema_version": 1,
            "updated_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "current_bytes": sum(sizes.values()),
            "active_bytes": sizes,
            "total_bytes": total,
            "oldest_age_seconds": max(0.0, now - min(mtimes)) if mtimes else 0.0,
            **counters,
            "disk_pressure": {
                "state": state,
                "free_bytes": free,
                "minimum_free_bytes": self.policy.disk_pressure_free_bytes,
            },
        }

    def _write_status(self, *, force: bool = False) -> None:
        now = self._monotonic()
        with self._state_lock:
            if not force and now - self._last_status_write < 1.0:
                return
            self._last_status_write = now
        try:
            atomic_write_text(
                self.log_dir / STATUS_FILE,
                json.dumps(self.snapshot(), sort_keys=True) + "\n",
                mode=0o600,
            )
        except OSError:
            # Status I/O must never stop pipe draining under disk pressure.
            pass

    def wait_for_maintenance(self) -> None:
        while True:
            with self._state_lock:
                futures = list(self._futures)
            if not futures:
                return
            for future in futures:
                future.result()

    def close(self) -> None:
        self.stdout.close()
        self.stderr.close()
        self.wait_for_maintenance()
        with self._state_lock:
            self._closed = True
        self._executor.shutdown(wait=True)
        self._write_status(force=True)


class _ActiveLog:
    def __init__(self, supervisor: ServiceLogSupervisor, name: str) -> None:
        self.supervisor = supervisor
        self.path = supervisor.log_dir / name
        self._lock = threading.Lock()
        try:
            self._opened_at = min(supervisor._clock(), self.path.stat().st_mtime)
        except OSError:
            self._opened_at = supervisor._clock()
        self._handle: BinaryIO = self._open()

    def _open(self) -> BinaryIO:
        descriptor = os.open(
            self.path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
        except OSError:
            os.close(descriptor)
            raise
        return os.fdopen(descriptor, "ab", buffering=0)

    def write(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            if self.supervisor._under_pressure():
                self.supervisor._dropped(len(data))
                return
            try:
                self._handle.write(data)
            except OSError:
                self.supervisor._dropped(len(data))
                return
            try:
                size = self._handle.tell()
            except OSError:
                size = self.path.stat().st_size
            age = self.supervisor._clock() - self._opened_at
            if (
                size >= self.supervisor.policy.max_bytes
                or age >= self.supervisor.policy.interval_seconds
            ):
                self._rotate()
            self.supervisor._submit_status()

    def _rotate(self) -> None:
        archive = self.supervisor._next_archive(self.path)
        try:
            self._handle.close()
            os.replace(self.path, archive)
            self._handle = self._open()
            self._opened_at = self.supervisor._clock()
        except OSError:
            self.supervisor._failed("rotation_failures")
            if self._handle.closed:
                self._handle = self._open()
            return
        self.supervisor._submit_maintenance(archive)

    def close(self) -> None:
        with self._lock:
            if not self._handle.closed:
                self._handle.close()


def _pump(stream: BinaryIO, supervisor: ServiceLogSupervisor, name: str) -> None:
    try:
        while chunk := stream.read(64 * 1024):
            supervisor.write(name, chunk)
    except OSError, ValueError:
        # The supervisor may close a leaked pipe descriptor after the server
        # child exits; request handling has already ended at that point.
        return


def supervise_service_process(
    settings: Settings,
    command: Sequence[str] | None = None,
) -> int:
    """Run ``pa serve`` behind lossless stdout/stderr pipe consumers."""
    supervisor = ServiceLogSupervisor(
        settings.data_dir / "logs", LogRotationPolicy.from_settings(settings)
    )
    child_command = list(command or (sys.executable, "-m", "pa", "serve"))
    try:
        child = subprocess.Popen(
            child_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
    except OSError as exc:
        supervisor.write(
            "server.err.log",
            f"PA service child failed to start: {redact_log_text(exc)}\n".encode(),
        )
        supervisor.close()
        return 1

    assert child.stdout is not None and child.stderr is not None
    threads = [
        threading.Thread(
            target=_pump,
            args=(child.stdout, supervisor, "server.log"),
            name="pa-log-stdout",
        ),
        threading.Thread(
            target=_pump,
            args=(child.stderr, supervisor, "server.err.log"),
            name="pa-log-stderr",
        ),
    ]
    for thread in threads:
        thread.start()

    previous: dict[int, object] = {}

    def forward(signum: int, _frame: object) -> None:
        if child.poll() is None:
            try:
                child.send_signal(signum)
            except ProcessLookupError:
                pass

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, forward)
    try:
        return child.wait()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        for thread in threads:
            thread.join(timeout=5.0)
        for stream, thread in zip((child.stdout, child.stderr), threads, strict=True):
            if thread.is_alive():
                stream.close()
                thread.join(timeout=1.0)
        supervisor.close()
