"""Source-aware, redacted service log reader used by ``pa logs``."""

from __future__ import annotations

import gzip
import hashlib
import heapq
import io
import json
import os
import re
import stat
import subprocess
import sys
import time
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, TextIO

from pa.config import Settings
from pa.core.log_rotation import (
    BOOTSTRAP_FILE,
    UnsafeLogPathError,
    read_log_status,
)
from pa.core.logging import redact_log_text

_LEVELS = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "WARN": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}
_STAMP = re.compile(
    r"^(?P<stamp>\d{4}-\d\d-\d\d(?:[ T]\d\d:\d\d:\d\d(?:[,.]\d+)?(?:Z|[+-]\d\d:?\d\d)?))\s+"
)
_HUMAN = re.compile(
    r"^(?:\d{4}-\d\d-\d\d[^ ]*\s+)?(?P<level>DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\s+(?:\[(?P<logger>[^]]+)\]\s+)?(?P<message>.*)$"
)
_DEDUP_WINDOW_ENTRIES = 20_000
_FOLLOW_ANCHOR_BYTES = 128


@dataclass(frozen=True)
class LogRecord:
    timestamp: datetime
    source: str
    message: str
    level: str = "INFO"
    logger: str | None = None
    fields: dict[str, object] | None = None


def parse_since(value: str | None, *, now: datetime | None = None) -> datetime | None:
    if not value:
        return None
    now = now or datetime.now(UTC)
    relative = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", value.strip(), re.IGNORECASE)
    if relative:
        seconds = (
            float(relative.group(1))
            * {"s": 1, "m": 60, "h": 3600, "d": 86400}[relative.group(2).lower()]
        )
        return now - timedelta(seconds=seconds)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "--since must be ISO-8601 or a duration such as 30m, 2h, or 1d"
        ) from exc
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


def _timestamp(line: str, fallback: datetime) -> datetime:
    match = _STAMP.match(line)
    if not match:
        return fallback
    raw = match.group("stamp").replace(",", ".").replace("Z", "+00:00")
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        return fallback
    return value.replace(tzinfo=value.tzinfo or UTC).astimezone(UTC)


def parse_line(line: str, source: str, fallback: datetime) -> LogRecord:
    line = line.rstrip("\r\n")
    if source == "structured":
        try:
            payload = json.loads(line)
            stamp = datetime.fromisoformat(str(payload["timestamp"]))
            known = {"timestamp", "level", "logger", "message", "exception"}
            fields = {key: value for key, value in payload.items() if key not in known}
            message = str(payload.get("message", ""))
            if payload.get("exception"):
                message += "\n" + str(payload["exception"])
            return LogRecord(
                stamp.astimezone(UTC),
                source,
                message,
                str(payload.get("level", "INFO")).upper(),
                str(payload.get("logger") or "") or None,
                fields,
            )
        except ValueError, TypeError, KeyError, json.JSONDecodeError:
            pass
    stamp = _timestamp(line, fallback)
    body = _STAMP.sub("", line, count=1)
    match = _HUMAN.match(body)
    if match:
        return LogRecord(
            stamp,
            source,
            match.group("message"),
            match.group("level").replace("WARN", "WARNING"),
            match.group("logger"),
        )
    # Uvicorn's legacy output uses ``INFO: logger`` and access lines have no level.
    legacy = re.match(
        r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):\s*(?P<message>.*)$", body
    )
    if legacy:
        return LogRecord(
            stamp, source, legacy.group("message"), legacy.group("level"), "uvicorn"
        )
    return LogRecord(stamp, source, body)


def iter_file_records(path: Path, source: str) -> Iterator[LogRecord]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(path, flags)
    try:
        value = os.fstat(descriptor)
        if not stat.S_ISREG(value.st_mode):
            raise UnsafeLogPathError(f"Refusing to read unsafe log path: {path}")
        raw = os.fdopen(descriptor, "rb")
        descriptor = -1
        with raw:
            binary = (
                gzip.GzipFile(fileobj=raw, mode="rb") if path.suffix == ".gz" else raw
            )
            with (
                binary,
                io.TextIOWrapper(binary, encoding="utf-8", errors="replace") as handle,
            ):
                fallback = datetime.fromtimestamp(value.st_mtime, UTC)
                for line in handle:
                    if line.strip():
                        yield parse_line(line, source, fallback)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def file_records(path: Path, source: str) -> list[LogRecord]:
    """Compatibility helper; streaming callers should use ``iter_file_records``."""
    return list(iter_file_records(path, source))


def source_log_paths(active: Path) -> list[Path]:
    """Return completed archives oldest-first followed by the active file."""

    def archive_order(path: Path) -> tuple[int, object]:
        suffix = path.name.removeprefix(f"{active.name}.").removesuffix(".gz")
        # logging.handlers.RotatingFileHandler numbers newest as .1, while
        # PA's service supervisor uses naturally sortable UTC timestamps.
        return (0, -int(suffix)) if suffix.isdigit() else (1, suffix)

    archives = sorted(
        (
            path
            for path in active.parent.glob(f"{active.name}.*")
            if _regular_file(path)
            and not path.name.endswith(".tmp")
            and not (
                path.suffix != ".gz"
                and _regular_file(path.with_name(path.name + ".gz"))
            )
        ),
        key=archive_order,
    )
    return [*archives, active]


def _regular_file(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except OSError:
        return False


def journal_records(lines: int) -> list[LogRecord]:
    result = subprocess.run(
        [
            "journalctl",
            "--user",
            "-u",
            "pa-server.service",
            "--no-pager",
            "-o",
            "json",
            "-n",
            str(max(lines, 1)),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"Cannot read journald for pa-server.service: {result.stderr.strip() or 'journalctl failed'}"
        )
    records: list[LogRecord] = []
    for line in result.stdout.splitlines():
        try:
            item = json.loads(line)
            stamp = datetime.fromtimestamp(
                int(item["__REALTIME_TIMESTAMP"]) / 1_000_000, UTC
            )
            fields = {"unit": item.get("_SYSTEMD_USER_UNIT"), "pid": item.get("_PID")}
            records.append(
                LogRecord(
                    stamp,
                    "journal",
                    str(item.get("MESSAGE", "")),
                    str(item.get("PRIORITY", "6")),
                    item.get("SYSLOG_IDENTIFIER"),
                    fields,
                )
            )
        except ValueError, TypeError, KeyError, json.JSONDecodeError:
            continue
    return records


def _severity(value: str) -> int:
    if value.isdigit():
        # syslog: 0 is most severe, 7 least severe
        return {0: 50, 1: 50, 2: 50, 3: 40, 4: 30, 5: 20, 6: 20, 7: 10}.get(
            int(value), 20
        )
    return _LEVELS.get(value.upper(), 20)


def _iter_deduplicated(records: Iterable[LogRecord]) -> Iterator[LogRecord]:
    """Deduplicate an ordered stream with state bounded to the two-second window."""
    seen: dict[tuple[str, bytes], datetime] = {}
    expiry: deque[tuple[datetime, tuple[str, bytes]]] = deque()
    for record in records:
        while expiry and (record.timestamp - expiry[0][0]).total_seconds() > 2:
            stamp, expired = expiry.popleft()
            if seen.get(expired) == stamp:
                seen.pop(expired, None)
        # Structured/file and journal/file sinks can carry the same record with
        # different prefixes. A small time window avoids hiding true repeats.
        key = (record.level, _message_fingerprint(record.message))
        previous = seen.get(key)
        if (
            previous is not None
            and abs((record.timestamp - previous).total_seconds()) <= 2
        ):
            continue
        seen[key] = record.timestamp
        expiry.append((record.timestamp, key))
        while len(expiry) > _DEDUP_WINDOW_ENTRIES:
            stamp, expired = expiry.popleft()
            if seen.get(expired) == stamp:
                seen.pop(expired, None)
        yield record


def _deduplicate(records: Iterable[LogRecord]) -> list[LogRecord]:
    return list(_iter_deduplicated(sorted(records, key=lambda item: item.timestamp)))


def _matches(
    record: LogRecord,
    *,
    since: datetime | None,
    severity: str | None,
    component: str | None,
) -> bool:
    if since and record.timestamp < since:
        return False
    if severity and _severity(record.level) < _severity(severity):
        return False
    return not (
        component and component.casefold() not in (record.logger or "").casefold()
    )


def _json_record(record: LogRecord, settings: Settings) -> str:
    service = (
        "systemd"
        if sys.platform.startswith("linux")
        else "launchd"
        if sys.platform == "darwin"
        else "process"
    )
    payload: dict[str, object] = {
        "timestamp": record.timestamp.isoformat(),
        "source": record.source,
        "level": record.level,
        "logger": record.logger,
        "message": redact_log_text(record.message),
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "service": service,
    }
    payload.update(
        {
            key: redact_log_text(value) if isinstance(value, str) else value
            for key, value in (record.fields or {}).items()
        }
    )
    return json.dumps(payload, ensure_ascii=False)


def emit(
    record: LogRecord, settings: Settings, *, json_output: bool, output: TextIO
) -> None:
    if json_output:
        print(_json_record(record, settings), file=output, flush=True)
        return
    stamp = record.timestamp.astimezone().isoformat(timespec="milliseconds")
    logger = f" [{record.logger}]" if record.logger else ""
    print(
        f"{stamp} {record.level:<8} [{record.source}]{logger} {redact_log_text(record.message)}",
        file=output,
        flush=True,
    )


def _message_fingerprint(message: str) -> bytes:
    normalized = re.sub(r"\s+", " ", message).strip().encode(errors="replace")
    return hashlib.blake2b(normalized, digest_size=16).digest()


def _record_key(record: LogRecord) -> tuple[str, bytes, int]:
    return (
        record.level,
        _message_fingerprint(record.message),
        int(record.timestamp.timestamp()),
    )


class _RecentKeys:
    def __init__(self, maximum: int = 20_000) -> None:
        self.maximum = maximum
        self._queue: deque[tuple[str, bytes, int]] = deque()
        self._values: set[tuple[str, bytes, int]] = set()

    def __contains__(self, value: tuple[str, bytes, int]) -> bool:
        return value in self._values

    def add(self, value: tuple[str, bytes, int]) -> None:
        if value in self._values:
            return
        self._values.add(value)
        self._queue.append(value)
        while len(self._queue) > self.maximum:
            self._values.discard(self._queue.popleft())


class _FollowFile:
    """Follow one active file across rename and fast truncate/regrow cycles."""

    def __init__(self, path: Path, source: str) -> None:
        self.path = path
        self.source = source
        self.handle: BinaryIO | None = None
        self.inode: int | None = None
        self.offset = 0
        self.anchor = b""
        self._open(at_end=True)

    def _open(self, *, at_end: bool) -> bool:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = -1
        try:
            descriptor = os.open(self.path, flags)
            value = os.fstat(descriptor)
            if not stat.S_ISREG(value.st_mode):
                os.close(descriptor)
                descriptor = -1
                return False
            handle = os.fdopen(descriptor, "rb")
            descriptor = -1
        except OSError:
            if descriptor >= 0:
                os.close(descriptor)
            return False
        if at_end:
            handle.seek(0, os.SEEK_END)
        self.handle = handle
        self.inode = value.st_ino
        self._remember_position()
        return True

    def _read_anchor(self, offset: int) -> bytes:
        if self.handle is None or offset <= 0:
            return b""
        start = max(0, offset - _FOLLOW_ANCHOR_BYTES)
        try:
            return os.pread(self.handle.fileno(), offset - start, start)
        except OSError:
            return b""

    def _remember_position(self) -> None:
        if self.handle is None:
            self.offset = 0
            self.anchor = b""
            return
        self.offset = int(self.handle.tell())
        self.anchor = self._read_anchor(self.offset)

    def _anchor_matches(self) -> bool:
        return self.anchor == self._read_anchor(self.offset)

    def _drain(self) -> Iterator[LogRecord]:
        if self.handle is None:
            return
        fallback = datetime.now(UTC)
        while raw_line := self.handle.readline():
            line = raw_line.decode("utf-8", errors="replace")
            if line.strip():
                yield parse_line(line, self.source, fallback)
        self._remember_position()

    def poll(self) -> Iterator[LogRecord]:
        if self.handle is None:
            if not self._open(at_end=False):
                return
            yield from self._drain()
            return
        try:
            current = self.path.lstat()
        except FileNotFoundError:
            yield from self._drain()
            return
        if not stat.S_ISREG(current.st_mode):
            return

        if current.st_ino == self.inode:
            try:
                size = os.fstat(self.handle.fileno()).st_size
            except OSError:
                size = 0
            # Size-only tailers lose the prefix when copytruncate regrows past
            # the prior offset between polls. Verify an anchor from the consumed
            # prefix so that cycle also restarts at byte zero.
            if size < self.offset or not self._anchor_matches():
                self.handle.seek(0)
                self._remember_position()
            yield from self._drain()
            return

        # PA's rename/create handoff closes its old writer before publishing the
        # new path, so draining the old descriptor here is complete and lossless.
        yield from self._drain()
        old = self.handle
        if self._open(at_end=False):
            old.close()
            yield from self._drain()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None


def _source_records(
    active: Path, source: str, diagnostics: TextIO
) -> Iterator[LogRecord]:
    for source_path in source_log_paths(active):
        if not _regular_file(source_path):
            continue
        try:
            yield from iter_file_records(source_path, source)
        except FileNotFoundError:
            # Compression atomically replaces a plain archive with .gz.
            continue
        except (OSError, EOFError, gzip.BadGzipFile, UnsafeLogPathError) as exc:
            print(
                f"warning: cannot read {source} log archive {source_path}: {exc}",
                file=diagnostics,
                flush=True,
            )


def show_logs(
    *,
    settings: Settings,
    sources: list[str],
    lines: int,
    follow: bool = False,
    since: str | None = None,
    severity: str | None = None,
    component: str | None = None,
    json_output: bool = False,
    output: TextIO = sys.stdout,
    diagnostics: TextIO = sys.stderr,
) -> None:
    if lines < 0:
        raise ValueError("line count must be zero or greater")
    if severity and severity.upper() not in _LEVELS:
        raise ValueError("--severity must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
    threshold = parse_since(since)
    paths = {
        "stdout": settings.data_dir / "logs" / "server.log",
        "stderr": settings.data_dir / "logs" / "server.err.log",
        "structured": settings.data_dir / "logs" / "pa.jsonl",
        "supervisor": settings.data_dir / "logs" / BOOTSTRAP_FILE,
    }
    selected: list[tuple[str, Path]] = []
    iterators: list[Iterator[LogRecord]] = []
    journal: list[LogRecord] = []
    for source in dict.fromkeys(sources):
        if source == "journal":
            if not sys.platform.startswith("linux"):
                raise RuntimeError(
                    "journald is only available on systemd/Linux; use --source stdout,stderr on launchd"
                )
            journal = sorted(journal_records(lines), key=lambda item: item.timestamp)
            iterators.append(iter(journal))
            continue
        path = paths[source]
        available = [item for item in source_log_paths(path) if _regular_file(item)]
        if not available:
            print(
                f"warning: {source} log is missing: {path} (start/restart PA or choose another --source)",
                file=diagnostics,
            )
            continue
        if any(not os.access(item, os.R_OK) for item in available):
            raise RuntimeError(
                f"Cannot read every {source} log at {path.parent}; "
                "check file ownership and permissions"
            )
        selected.append((source, path))
        iterators.append(_source_records(path, source, diagnostics))
    source_text = ", ".join(f"{name}={path}" for name, path in selected)
    if "journal" in sources:
        source_text += (", " if source_text else "") + "journal=pa-server.service"
    service = (
        "systemd"
        if sys.platform.startswith("linux")
        else "launchd"
        if sys.platform == "darwin"
        else "process"
    )
    print(
        f"Log sources: {source_text or 'none'}; instance={settings.instance_name} "
        f"({settings.instance_id}); service={service}",
        file=diagnostics,
        flush=True,
    )
    storage = read_log_status(settings.data_dir)
    if storage:
        pressure = storage.get("disk_pressure")
        pressure_state = (
            pressure.get("state", "unknown")
            if isinstance(pressure, dict)
            else "unknown"
        )
        free_bytes = pressure.get("free_bytes") if isinstance(pressure, dict) else None
        minimum_free_bytes = (
            pressure.get("minimum_free_bytes") if isinstance(pressure, dict) else None
        )
        supervisor = storage.get("supervisor")
        supervisor_state = (
            supervisor.get("state", "unknown")
            if isinstance(supervisor, dict)
            else "unknown"
        )
        ownership = (
            supervisor.get("ownership", "unknown")
            if isinstance(supervisor, dict)
            else "unknown"
        )
        print(
            "Log storage: "
            f"current_bytes={storage.get('current_bytes', 0)} "
            f"total_bytes={storage.get('total_bytes', 0)} "
            f"oldest_age_seconds={storage.get('oldest_age_seconds', 0)} "
            f"rotation_failures={storage.get('rotation_failures', 0)} "
            f"compression_failures={storage.get('compression_failures', 0)} "
            f"prune_failures={storage.get('prune_failures', 0)} "
            f"pump_failures={storage.get('pump_failures', 0)} "
            f"status_failures={storage.get('status_failures', 0)} "
            f"bootstrap_failures={storage.get('bootstrap_failures', 0)} "
            f"dropped_bytes={storage.get('dropped_bytes', 0)} "
            f"disk_pressure={pressure_state} "
            f"free_bytes={free_bytes} "
            f"minimum_free_bytes={minimum_free_bytes} "
            f"supervisor={supervisor_state} "
            f"ownership={ownership} "
            f"last_error={storage.get('last_error')}",
            file=diagnostics,
            flush=True,
        )
    if not selected and "journal" not in sources:
        raise RuntimeError(
            "No readable log sources selected; run `pa status` and verify the PA data directory"
        )
    merged = heapq.merge(*iterators, key=lambda item: item.timestamp)
    tail: deque[LogRecord] | None = deque(maxlen=lines) if lines else None
    recent = _RecentKeys()
    journal_cursor = max(
        (record.timestamp for record in journal),
        default=datetime.fromtimestamp(0, UTC),
    )
    for record in _iter_deduplicated(merged):
        if not _matches(
            record, since=threshold, severity=severity, component=component
        ):
            continue
        if tail is None:
            emit(record, settings, json_output=json_output, output=output)
            recent.add(_record_key(record))
        else:
            tail.append(record)
    if tail is not None:
        for record in tail:
            emit(record, settings, json_output=json_output, output=output)
            recent.add(_record_key(record))
    if not follow:
        return
    followers = [_FollowFile(path, source) for source, path in selected]
    try:
        while True:
            for follower in followers:
                for record in follower.poll():
                    key = _record_key(record)
                    if key not in recent and _matches(
                        record,
                        since=threshold,
                        severity=severity,
                        component=component,
                    ):
                        emit(
                            record,
                            settings,
                            json_output=json_output,
                            output=output,
                        )
                        recent.add(key)
            if "journal" in sources:
                for record in journal_records(max(lines, 100)):
                    key = _record_key(record)
                    if (
                        record.timestamp > journal_cursor
                        and key not in recent
                        and _matches(
                            record,
                            since=threshold,
                            severity=severity,
                            component=component,
                        )
                    ):
                        emit(record, settings, json_output=json_output, output=output)
                        recent.add(key)
                    journal_cursor = max(journal_cursor, record.timestamp)
            time.sleep(0.2)
    except KeyboardInterrupt:
        return
    finally:
        for follower in followers:
            follower.close()
