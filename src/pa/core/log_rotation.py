"""Private, bounded service-stream rotation and diagnostics supervision."""

from __future__ import annotations

import fcntl
import gzip
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from pa.config import Settings, default_data_dir
from pa.core.io import atomic_write_text
from pa.core.logging import redact_log_text

STATUS_FILE = "status.json"
BOOTSTRAP_FILE = "supervisor-bootstrap.log"
OWNER_FILE = ".service-log-supervisor.lock"
_ACTIVE_NAMES = ("server.log", "server.err.log")
_BOOTSTRAP_MAX_BYTES = 1024 * 1024
_BOOTSTRAP_KEEP_BYTES = 256 * 1024
_GZIP_OVERHEAD_ALLOWANCE = 64 * 1024

_PROCESS_OWNERS_LOCK = threading.Lock()
_PROCESS_OWNERS: set[str] = set()


class LogSupervisorAlreadyOwnedError(RuntimeError):
    """A different supervisor already owns the service-log directory."""


class UnsafeLogPathError(RuntimeError):
    """A service-log path is a symlink or a non-regular file."""


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


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _is_regular_path(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except OSError:
        return False


def _ensure_private_log_dir(path: Path) -> None:
    try:
        current = path.lstat()
    except FileNotFoundError:
        path.mkdir(parents=True, mode=0o700)
        current = path.lstat()
    if stat.S_ISLNK(current.st_mode) or not stat.S_ISDIR(current.st_mode):
        raise UnsafeLogPathError(
            f"PA service-log directory must be a real directory: {path}"
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise UnsafeLogPathError(
                f"PA service-log directory must be a real directory: {path}"
            )
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def _open_private_regular(path: Path, flags: int, *, exclusive: bool = False) -> int:
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None and not stat.S_ISREG(existing.st_mode):
        raise UnsafeLogPathError(f"PA service-log path must be a regular file: {path}")
    safe_flags = (
        flags
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if exclusive:
        safe_flags |= os.O_EXCL
    try:
        descriptor = os.open(path, safe_flags, 0o600)
    except OSError as exc:
        try:
            unsafe = not stat.S_ISREG(path.lstat().st_mode)
        except OSError:
            unsafe = False
        if unsafe:
            raise UnsafeLogPathError(
                f"PA service-log path must not be a symlink: {path}"
            ) from exc
        raise
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise UnsafeLogPathError(
                f"PA service-log path must be a regular file: {path}"
            )
        current_flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        fcntl.fcntl(
            descriptor,
            fcntl.F_SETFL,
            current_flags & ~getattr(os, "O_NONBLOCK", 0),
        )
        os.fchmod(descriptor, 0o600)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _safe_unlink_regular(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(mode):
        raise UnsafeLogPathError(f"Refusing to unlink unsafe service-log path: {path}")
    path.unlink()
    return True


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("short write to service-log file")
        remaining = remaining[written:]


def read_log_status(data_dir: Path) -> dict[str, object] | None:
    """Read the redaction-safe snapshot written by the service supervisor."""
    path = data_dir / "logs" / STATUS_FILE
    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                return None
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                descriptor = -1
                value = json.load(handle)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    except FileNotFoundError, OSError, ValueError, json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def bootstrap_log_path(data_dir: Path) -> Path:
    return data_dir / "logs" / BOOTSTRAP_FILE


def _prepare_bootstrap_file(log_dir: Path) -> Path:
    _ensure_private_log_dir(log_dir)
    path = log_dir / BOOTSTRAP_FILE
    created = not _is_regular_path(path)
    descriptor = _open_private_regular(path, os.O_RDWR | os.O_CREAT | os.O_APPEND)
    changed = created
    try:
        size = os.fstat(descriptor).st_size
        if size > _BOOTSTRAP_MAX_BYTES:
            keep = os.pread(
                descriptor, _BOOTSTRAP_KEEP_BYTES, size - _BOOTSTRAP_KEEP_BYTES
            )
            os.ftruncate(descriptor, 0)
            os.lseek(descriptor, 0, os.SEEK_SET)
            _write_all(
                descriptor,
                b"[older supervisor bootstrap diagnostics truncated]\n" + keep,
            )
            changed = True
        if changed:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if changed:
        _fsync_directory(log_dir)
    return path


def prepare_bootstrap_log(data_dir: Path) -> Path:
    """Create and bound the host-manager-owned early-error stream."""
    return _prepare_bootstrap_file(data_dir / "logs")


def _bootstrap_data_dir() -> Path:
    raw = os.environ.get("PA_DATA_DIR", "").strip()
    return Path(raw).expanduser() if raw else default_data_dir()


def _record_bootstrap_failure(data_dir: Path, exc: BaseException) -> None:
    try:
        path = prepare_bootstrap_log(data_dir)
        descriptor = _open_private_regular(path, os.O_WRONLY | os.O_APPEND)
        try:
            detail = redact_log_text("".join(traceback.format_exception(exc)))
            header = (
                f"{datetime.now(UTC).isoformat()} "
                "PA service supervisor startup failed\n"
            ).encode()
            body = f"{detail.rstrip()}\n".encode(errors="replace")
            body_limit = max(0, _BOOTSTRAP_KEEP_BYTES - len(header))
            if len(body) > body_limit:
                marker = b"[startup traceback head truncated]\n"
                body = marker + body[-max(0, body_limit - len(marker)) :]
            _write_all(descriptor, header + body)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        prepare_bootstrap_log(data_dir)
    except BaseException:  # noqa: BLE001 - final fallback must not recurse
        # The host manager also owns stderr. Preserve a final diagnostic path
        # even when PA_DATA_DIR itself is unusable.
        print(
            f"PA service supervisor startup failed: {redact_log_text(exc)}",
            file=sys.stderr,
            flush=True,
        )


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
        periodic_seconds: float | None = None,
    ) -> None:
        self.log_dir = log_dir
        self.policy = policy
        self._clock = clock
        self._monotonic = monotonic
        self._disk_usage = disk_usage
        self._state_lock = threading.Lock()
        self._archive_lock = threading.Lock()
        self._closed = False
        self._sequence = 0
        self._last_status_write = 0.0
        self._last_status_submit = 0.0
        self._futures: set[Future[None]] = set()
        self._maintenance_future: Future[None] | None = None
        self._maintenance_again = False
        self._status_future: Future[None] | None = None
        self._status_again = False
        self._stop_periodic = threading.Event()
        self._owner_fd: int | None = None
        self._owner_key: str | None = None

        _ensure_private_log_dir(self.log_dir)
        self._claim_ownership()
        try:
            _prepare_bootstrap_file(self.log_dir)
            self._recover_handoffs()
            self._harden_existing_files()
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
                    "pump_failures",
                    "status_failures",
                    "bootstrap_failures",
                )
            }
            self._disk_state = "ok"
            self._free_bytes: int | None = None
            self._last_error: str | None = None
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="pa-log-maintenance"
            )
            self.stdout = _ActiveLog(self, "server.log")
            try:
                self.stderr = _ActiveLog(self, "server.err.log")
            except BaseException:
                self.stdout.close()
                raise

            # Upgrade and pressure recovery cannot depend on receiving another
            # child byte: rotate pre-existing oversized or stale active files now.
            self.stdout.rotate_if_due()
            self.stderr.rotate_if_due()
            self._submit_maintenance(None)
            self._refresh_disk_state(schedule=False)
            self._write_status(force=True)

            interval = periodic_seconds
            if interval is None:
                interval = max(
                    1.0,
                    min(
                        60.0,
                        self.policy.interval_seconds / 4,
                        self.policy.retention_max_age_seconds / 4,
                    ),
                )
            self._periodic_seconds = interval
            self._periodic_thread = threading.Thread(
                target=self._periodic_loop,
                name="pa-log-periodic-maintenance",
                daemon=True,
            )
            self._periodic_thread.start()
        except BaseException:
            with self._state_lock:
                self._closed = True
            executor = getattr(self, "_executor", None)
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            self._release_ownership()
            raise

    def _claim_ownership(self) -> None:
        key = str(self.log_dir.resolve())
        with _PROCESS_OWNERS_LOCK:
            if key in _PROCESS_OWNERS:
                raise LogSupervisorAlreadyOwnedError(
                    f"A service-log supervisor already owns {self.log_dir}"
                )
            path = self.log_dir / OWNER_FILE
            descriptor = _open_private_regular(path, os.O_RDWR | os.O_CREAT)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                os.close(descriptor)
                raise LogSupervisorAlreadyOwnedError(
                    f"A service-log supervisor already owns {self.log_dir}"
                ) from exc
            except OSError:
                os.close(descriptor)
                raise
            try:
                os.ftruncate(descriptor, 0)
                os.write(
                    descriptor,
                    json.dumps({"pid": os.getpid()}, sort_keys=True).encode(),
                )
                os.fsync(descriptor)
            except BaseException:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
                raise
            _PROCESS_OWNERS.add(key)
            self._owner_fd = descriptor
            self._owner_key = key

    def _release_ownership(self) -> None:
        descriptor, key = self._owner_fd, self._owner_key
        self._owner_fd = None
        self._owner_key = None
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        if key is not None:
            with _PROCESS_OWNERS_LOCK:
                _PROCESS_OWNERS.discard(key)

    def _recover_handoffs(self) -> None:
        changed = False
        for active_name in _ACTIVE_NAMES:
            active = self.log_dir / active_name
            temporaries = sorted(self.log_dir.glob(f".{active_name}.*.handoff.tmp"))
            for temporary in temporaries:
                if not _is_regular_path(temporary):
                    continue
                if not active.exists():
                    os.replace(temporary, active)
                else:
                    _safe_unlink_regular(temporary)
                changed = True
        if changed:
            _fsync_directory(self.log_dir)

    def _harden_existing_files(self) -> None:
        for path in self.log_dir.iterdir():
            if not _is_regular_path(path):
                continue
            if path.name in {
                *_ACTIVE_NAMES,
                STATUS_FILE,
                BOOTSTRAP_FILE,
                OWNER_FILE,
            } or any(path.name.startswith(f"{name}.") for name in _ACTIVE_NAMES):
                descriptor = _open_private_regular(path, os.O_RDONLY)
                os.close(descriptor)

    def _next_sequence(self) -> int:
        with self._state_lock:
            self._sequence += 1
            return self._sequence

    def _next_archive(self, active: Path) -> Path:
        stamp = datetime.fromtimestamp(self._clock(), UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        while True:
            sequence = self._next_sequence()
            candidate = active.with_name(
                f"{active.name}.{stamp}.{os.getpid()}.{sequence}"
            )
            try:
                candidate.lstat()
            except FileNotFoundError:
                return candidate

    def _next_handoff(self, active: Path) -> Path:
        sequence = self._next_sequence()
        return active.with_name(f".{active.name}.{os.getpid()}.{sequence}.handoff.tmp")

    def write(self, name: str, data: bytes) -> None:
        sink = self.stdout if name == "server.log" else self.stderr
        sink.write(data)

    def emergency_drop(self, amount: int, *, pump_failure: bool = False) -> None:
        with self._state_lock:
            self._disk_state = "dropping"
            self._counters["dropped_bytes"] += amount
            if pump_failure:
                self._counters["pump_failures"] += 1
        self._submit_status()

    def _refresh_disk_state(self, *, schedule: bool = True) -> bool:
        try:
            free = int(self._disk_usage(self.log_dir).free)
        except OSError, TypeError, ValueError:
            with self._state_lock:
                self._disk_state = "unknown"
                self._free_bytes = None
            return False
        pressure = free < self.policy.disk_pressure_free_bytes
        with self._state_lock:
            self._disk_state = (
                "dropping"
                if pressure and self._disk_state == "dropping"
                else "pressure"
                if pressure
                else "ok"
            )
            self._free_bytes = free
        if pressure and schedule:
            self._submit_maintenance(None)
        return pressure

    def _failed(self, counter: str, exc: BaseException | None = None) -> None:
        with self._state_lock:
            self._counters[counter] += 1
            if exc is not None:
                self._last_error = redact_log_text(f"{type(exc).__name__}: {exc}")
        self._submit_status(force=True)

    def _submit_maintenance(self, archive: Path | None) -> None:
        # Every pass scans all archives. Coalescing requests bounds executor
        # memory while ensuring a rotation that arrives mid-pass gets one rerun.
        del archive
        with self._state_lock:
            if self._closed:
                return
            if self._maintenance_future is not None:
                self._maintenance_again = True
                return
            future = self._executor.submit(self._maintain)
            self._maintenance_future = future
            self._futures.add(future)
        future.add_done_callback(self._maintenance_done)

    def _maintenance_done(self, future: Future[None]) -> None:
        failure: BaseException | None = None
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - worker failures are status data
            failure = exc
        next_future: Future[None] | None = None
        with self._state_lock:
            self._futures.discard(future)
            if future is self._maintenance_future:
                self._maintenance_future = None
                rerun = self._maintenance_again and not self._closed
                self._maintenance_again = False
                if rerun:
                    next_future = self._executor.submit(self._maintain)
                    self._maintenance_future = next_future
                    self._futures.add(next_future)
            if failure is not None:
                self._counters["prune_failures"] += 1
                self._last_error = redact_log_text(
                    f"{type(failure).__name__}: {failure}"
                )
        if next_future is not None:
            next_future.add_done_callback(self._maintenance_done)
        if failure is not None:
            self._submit_status(force=True)

    def _archives(self) -> list[Path]:
        result: list[Path] = []
        for active_name in _ACTIVE_NAMES:
            for path in self.log_dir.glob(f"{active_name}.*"):
                if path.name.endswith(".tmp") or not _is_regular_path(path):
                    continue
                result.append(path)
        return result

    def _compress(self, archive: Path) -> None:
        if archive.suffix == ".gz" or not _is_regular_path(archive):
            return
        compressed = archive.with_name(archive.name + ".gz")
        temporary = compressed.with_name(compressed.name + ".tmp")
        try:
            if temporary.exists():
                _safe_unlink_regular(temporary)
            source_fd = -1
            target_fd = -1
            try:
                source_fd = _open_private_regular(archive, os.O_RDONLY)
                target_fd = _open_private_regular(
                    temporary, os.O_WRONLY | os.O_CREAT, exclusive=True
                )
                source_stat = os.fstat(source_fd)
                with (
                    os.fdopen(source_fd, "rb") as source,
                    os.fdopen(target_fd, "wb", buffering=0) as raw_target,
                ):
                    source_fd = -1
                    target_fd = -1
                    with gzip.GzipFile(fileobj=raw_target, mode="wb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)
                    raw_target.flush()
                    os.fsync(raw_target.fileno())
                os.utime(
                    temporary,
                    ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
                    follow_symlinks=False,
                )
                os.replace(temporary, compressed)
                _fsync_directory(self.log_dir)
                _safe_unlink_regular(archive)
                _fsync_directory(self.log_dir)
            finally:
                if source_fd >= 0:
                    os.close(source_fd)
                if target_fd >= 0:
                    os.close(target_fd)
        except (OSError, UnsafeLogPathError) as exc:
            self._failed("compression_failures", exc)
            try:
                if _is_regular_path(temporary) and _safe_unlink_regular(temporary):
                    _fsync_directory(self.log_dir)
            except OSError, UnsafeLogPathError:
                pass

    def _has_compression_headroom(self, archive: Path) -> bool:
        reserve = self.policy.disk_pressure_free_bytes
        if reserve <= 0:
            return True
        try:
            archive_bytes = archive.lstat().st_size
            free = int(self._disk_usage(self.log_dir).free)
        except OSError, TypeError, ValueError:
            return False
        overhead = max(_GZIP_OVERHEAD_ALLOWANCE, archive_bytes // 1000)
        return free - archive_bytes - overhead >= reserve

    def _prune(self) -> None:
        now = self._clock()

        def details(path: Path) -> os.stat_result | None:
            try:
                value = path.lstat()
            except OSError as exc:
                self._failed("prune_failures", exc)
                return None
            if not stat.S_ISREG(value.st_mode):
                return None
            return value

        archives = sorted(
            ((path, details(path)) for path in self._archives()),
            key=lambda item: item[1].st_mtime if item[1] else -1,
            reverse=True,
        )
        total = 0
        for active_name in _ACTIVE_NAMES:
            value = details(self.log_dir / active_name)
            if value is not None:
                total += value.st_size

        keep: list[tuple[Path, os.stat_result]] = []
        victims: list[tuple[Path, os.stat_result]] = []
        for path, value in archives:
            if value is None:
                continue
            over_count = len(keep) >= self.policy.retention_count
            over_age = now - value.st_mtime > self.policy.retention_max_age_seconds
            over_bytes = total + value.st_size > self.policy.retention_max_total_bytes
            if over_count or over_age or over_bytes:
                victims.append((path, value))
            else:
                keep.append((path, value))
                total += value.st_size

        try:
            free = int(self._disk_usage(self.log_dir).free)
        except OSError, TypeError, ValueError:
            free = self.policy.disk_pressure_free_bytes
        estimated_free = free + sum(value.st_size for _, value in victims)
        if estimated_free < self.policy.disk_pressure_free_bytes:
            for path, value in reversed(keep):
                victims.append((path, value))
                estimated_free += value.st_size
                if estimated_free >= self.policy.disk_pressure_free_bytes:
                    break

        changed = False
        for path, _ in dict.fromkeys(victims):
            try:
                changed = _safe_unlink_regular(path) or changed
            except (OSError, UnsafeLogPathError) as exc:
                self._failed("prune_failures", exc)
        if changed:
            _fsync_directory(self.log_dir)
        self._refresh_disk_state(schedule=False)

    def _maintain(self) -> None:
        with self._archive_lock:
            changed = False
            for active_name in _ACTIVE_NAMES:
                for temporary in self.log_dir.glob(f"{active_name}.*.gz.tmp"):
                    try:
                        if _is_regular_path(temporary):
                            changed = _safe_unlink_regular(temporary) or changed
                    except (OSError, UnsafeLogPathError) as exc:
                        self._failed("prune_failures", exc)
            if changed:
                _fsync_directory(self.log_dir)

            # Reclaim according to retention and pressure policies before gzip
            # needs temporary disk space. Compression is deferred when its
            # worst-case output would consume the configured free-space reserve.
            self._prune()
            for path in self._archives():
                if path.suffix != ".gz" and self._has_compression_headroom(path):
                    self._compress(path)
            self._prune()
            try:
                _prepare_bootstrap_file(self.log_dir)
            except (OSError, UnsafeLogPathError) as exc:
                self._failed("bootstrap_failures", exc)
            self._write_status(force=True)

    def _submit_status(self, *, force: bool = False) -> None:
        now = self._monotonic()
        with self._state_lock:
            if self._closed or (not force and now - self._last_status_submit < 1.0):
                return
            self._last_status_submit = now
            if self._status_future is not None:
                self._status_again = True
                return
            future = self._executor.submit(self._write_status, force=True)
            self._status_future = future
            self._futures.add(future)
        future.add_done_callback(self._status_done)

    def _status_done(self, future: Future[None]) -> None:
        failure: BaseException | None = None
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - status must not stop draining
            failure = exc
        next_future: Future[None] | None = None
        with self._state_lock:
            self._futures.discard(future)
            if future is self._status_future:
                self._status_future = None
                rerun = self._status_again and not self._closed
                self._status_again = False
                if rerun:
                    next_future = self._executor.submit(self._write_status, force=True)
                    self._status_future = next_future
                    self._futures.add(next_future)
            if failure is not None:
                self._counters["status_failures"] += 1
                self._last_error = redact_log_text(
                    f"{type(failure).__name__}: {failure}"
                )
        if next_future is not None:
            next_future.add_done_callback(self._status_done)

    def snapshot(self) -> dict[str, object]:
        now = self._clock()
        files: list[Path] = []
        try:
            candidates = list(self.log_dir.iterdir())
        except OSError:
            candidates = []
        for path in candidates:
            if (
                _is_regular_path(path)
                and (
                    path.name in _ACTIVE_NAMES
                    or any(path.name.startswith(f"{name}.") for name in _ACTIVE_NAMES)
                )
                and not path.name.endswith(".tmp")
            ):
                files.append(path)
        sizes: dict[str, int] = {}
        mtimes: list[float] = []
        total = 0
        for path in files:
            try:
                value = path.lstat()
            except OSError:
                continue
            total += value.st_size
            mtimes.append(value.st_mtime)
            if path.name in _ACTIVE_NAMES:
                sizes[path.name] = value.st_size
        with self._state_lock:
            counters = dict(self._counters)
            state = self._disk_state
            free = self._free_bytes
            last_error = self._last_error
            closed = self._closed
        return {
            "schema_version": 2,
            "updated_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "supervisor": {
                "pid": os.getpid(),
                "state": "closed" if closed else "running",
                "ownership": "exclusive",
            },
            "current_bytes": sum(sizes.values()),
            "active_bytes": sizes,
            "total_bytes": total,
            "oldest_age_seconds": max(0.0, now - min(mtimes)) if mtimes else 0.0,
            **counters,
            "last_error": last_error,
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
        except OSError as exc:
            # Status I/O must never stop pipe draining under disk pressure.
            with self._state_lock:
                self._counters["status_failures"] += 1
                self._last_error = redact_log_text(f"{type(exc).__name__}: {exc}")

    def _periodic_loop(self) -> None:
        while not self._stop_periodic.wait(self._periodic_seconds):
            try:
                self.stdout.rotate_if_due()
                self.stderr.rotate_if_due()
                self._submit_maintenance(None)
                self._refresh_disk_state(schedule=True)
                self._submit_status(force=True)
            except Exception as exc:  # noqa: BLE001 - periodic worker must survive
                self._failed("prune_failures", exc)

    def wait_for_maintenance(self) -> None:
        while True:
            with self._state_lock:
                futures = list(self._futures)
            if not futures:
                return
            for future in futures:
                try:
                    future.result()
                except Exception:  # noqa: BLE001,S112 - callback already accounted it
                    # Completion callbacks account for failures without making
                    # service shutdown abandon descriptor cleanup.
                    continue

    def close(self) -> None:
        if getattr(self, "_closed", False):
            return
        self._stop_periodic.set()
        periodic = getattr(self, "_periodic_thread", None)
        if periodic is not None:
            periodic.join(timeout=max(1.0, min(5.0, self._periodic_seconds + 0.5)))
        try:
            self.stdout.close()
            self.stderr.close()
            self._submit_maintenance(None)
            self.wait_for_maintenance()
            # A pending status write can coalesce the first full-maintenance
            # request. Submit once more after the executor is known idle.
            self._submit_maintenance(None)
            self.wait_for_maintenance()
            with self._state_lock:
                self._closed = True
            self._executor.shutdown(wait=True)
            self._write_status(force=True)
        finally:
            self._release_ownership()


class _ActiveLog:
    def __init__(self, supervisor: ServiceLogSupervisor, name: str) -> None:
        self.supervisor = supervisor
        self.path = supervisor.log_dir / name
        self._lock = threading.Lock()
        self._detached_archive: Path | None = None
        self._rotation_blocked = False
        try:
            self._opened_at = min(supervisor._clock(), self.path.lstat().st_mtime)
        except OSError:
            self._opened_at = supervisor._clock()
        self._handle: BinaryIO = self._open_active()

    def _open_active(self) -> BinaryIO:
        descriptor = _open_private_regular(
            self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND
        )
        return os.fdopen(descriptor, "ab", buffering=0)

    def _create_replacement(self) -> tuple[Path, BinaryIO]:
        temporary = self.supervisor._next_handoff(self.path)
        descriptor = _open_private_regular(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_APPEND, exclusive=True
        )
        handle = os.fdopen(descriptor, "ab", buffering=0)
        try:
            os.fsync(handle.fileno())
            _fsync_directory(self.supervisor.log_dir)
        except BaseException:
            handle.close()
            try:
                if _safe_unlink_regular(temporary):
                    _fsync_directory(self.supervisor.log_dir)
            except OSError, UnsafeLogPathError:
                pass
            raise
        return temporary, handle

    def _size(self) -> int:
        try:
            return os.fstat(self._handle.fileno()).st_size
        except OSError, ValueError:
            return self.supervisor.policy.max_bytes

    def _due(self) -> bool:
        size = self._size()
        age = self.supervisor._clock() - self._opened_at
        return size >= self.supervisor.policy.max_bytes or (
            size > 0 and age >= self.supervisor.policy.interval_seconds
        )

    def write(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            if (
                self._detached_archive is not None
                or self._rotation_blocked
                or self._due()
            ) and not self._rotate():
                self.supervisor.emergency_drop(len(data))
                return
            # Rotate recoverable active data before pressure policy can drop the
            # only byte that would otherwise trigger rotation.
            if self.supervisor._refresh_disk_state(schedule=True):
                self.supervisor.emergency_drop(len(data))
                return
            try:
                self._handle.write(data)
            except (OSError, ValueError) as exc:
                self.supervisor._failed("rotation_failures", exc)
                self._rotation_blocked = True
                self.supervisor.emergency_drop(len(data))
                return
            if self._due():
                self._rotate()
            self.supervisor._submit_status()

    def rotate_if_due(self) -> bool:
        with self._lock:
            if (
                self._detached_archive is None
                and not self._rotation_blocked
                and not self._due()
            ):
                return True
            return self._rotate()

    def _finish_detached(self) -> bool:
        archive = self._detached_archive
        if archive is None:
            return True
        temporary: Path | None = None
        replacement: BinaryIO | None = None
        try:
            if self.path.exists():
                raise OSError(
                    f"active service-log path unexpectedly exists: {self.path}"
                )
            temporary, replacement = self._create_replacement()
            os.replace(temporary, self.path)
            _fsync_directory(self.supervisor.log_dir)
            old = self._handle
            self._handle = replacement
            replacement = None
            self._opened_at = self.supervisor._clock()
            self._detached_archive = None
            self._rotation_blocked = False
            old.close()
            self.supervisor._submit_maintenance(archive)
            return True
        except (OSError, UnsafeLogPathError) as exc:
            self.supervisor._failed("rotation_failures", exc)
            self._rotation_blocked = True
            return False
        finally:
            if replacement is not None:
                replacement.close()
            if temporary is not None and _is_regular_path(temporary):
                try:
                    if _safe_unlink_regular(temporary):
                        _fsync_directory(self.supervisor.log_dir)
                except OSError, UnsafeLogPathError:
                    pass

    def _rotate(self) -> bool:
        if self._detached_archive is not None:
            return self._finish_detached()
        archive: Path | None = None
        temporary: Path | None = None
        replacement: BinaryIO | None = None
        moved = False
        try:
            archive = self.supervisor._next_archive(self.path)
            temporary, replacement = self._create_replacement()
            os.fsync(self._handle.fileno())
            os.replace(self.path, archive)
            moved = True
            _fsync_directory(self.supervisor.log_dir)
            os.replace(temporary, self.path)
            _fsync_directory(self.supervisor.log_dir)
            old = self._handle
            self._handle = replacement
            replacement = None
            self._opened_at = self.supervisor._clock()
            self._rotation_blocked = False
            old.close()
            self.supervisor._submit_maintenance(archive)
            return True
        except (OSError, UnsafeLogPathError) as exc:
            if moved:
                assert archive is not None
                try:
                    if not self.path.exists():
                        os.replace(archive, self.path)
                        _fsync_directory(self.supervisor.log_dir)
                    else:
                        raise OSError("replacement path appeared during failed handoff")
                except OSError:
                    # Keep writing the still-open archived inode. A later attempt
                    # installs a fresh active path before this archive is compressed.
                    self._detached_archive = archive
            self._rotation_blocked = True
            self.supervisor._failed("rotation_failures", exc)
            return False
        finally:
            if replacement is not None:
                replacement.close()
            if temporary is not None and _is_regular_path(temporary):
                try:
                    if _safe_unlink_regular(temporary):
                        _fsync_directory(self.supervisor.log_dir)
                except OSError, UnsafeLogPathError:
                    pass

    def close(self) -> None:
        with self._lock:
            if not self._handle.closed:
                try:
                    os.fsync(self._handle.fileno())
                except OSError as exc:
                    self.supervisor._failed("rotation_failures", exc)
                try:
                    self._handle.close()
                except OSError as exc:
                    self.supervisor._failed("rotation_failures", exc)


def _pump(stream: BinaryIO, supervisor: ServiceLogSupervisor, name: str) -> None:
    while True:
        try:
            chunk = stream.read(64 * 1024)
        except Exception as exc:  # noqa: BLE001 - record an undrainable pipe
            try:
                supervisor._failed("pump_failures", exc)
            except Exception:  # noqa: BLE001,S110 - pipe cleanup must continue
                pass
            return
        if not chunk:
            return
        try:
            supervisor.write(name, chunk)
        except Exception:  # noqa: BLE001 - pipe draining is the safety boundary
            # A supervisor bug or filesystem edge must degrade diagnostics, not
            # stop draining and back-pressure the PA request process.
            try:
                supervisor.emergency_drop(len(chunk), pump_failure=True)
            except Exception:  # noqa: BLE001,S110 - draining must continue unaided
                pass


def supervise_service_process(
    settings: Settings,
    command: Sequence[str] | None = None,
    *,
    periodic_seconds: float | None = None,
) -> int:
    """Run ``pa serve`` behind bounded stdout/stderr pipe consumers."""
    supervisor = ServiceLogSupervisor(
        settings.data_dir / "logs",
        LogRotationPolicy.from_settings(settings),
        periodic_seconds=periodic_seconds,
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
    streams = (child.stdout, child.stderr)
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
        for stream, thread in zip(streams, threads, strict=True):
            if thread.is_alive():
                stream.close()
                thread.join(timeout=1.0)
            if not stream.closed:
                stream.close()
        supervisor.close()


def service_entrypoint() -> int:
    """Start the supervisor while preserving bounded pre-supervisor failures."""
    data_dir = _bootstrap_data_dir()
    try:
        prepare_bootstrap_log(data_dir)
        from pa.config import get_settings

        return supervise_service_process(get_settings())
    except Exception as exc:  # noqa: BLE001 - persist all startup diagnostics
        _record_bootstrap_failure(data_dir, exc)
        return 1
