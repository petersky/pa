"""Source-aware, redacted service log reader used by ``pa logs``."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TextIO

from pa.config import Settings
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


def file_records(path: Path, source: str) -> list[LogRecord]:
    fallback = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return [parse_line(line, source, fallback) for line in handle if line.strip()]


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


def _deduplicate(records: Iterable[LogRecord]) -> list[LogRecord]:
    result: list[LogRecord] = []
    seen: dict[tuple[str, str], datetime] = {}
    for record in sorted(records, key=lambda item: item.timestamp):
        # Structured/file and journal/file sinks can carry the same record with
        # different prefixes. A small time window avoids hiding true repeats.
        key = (record.level, re.sub(r"\s+", " ", record.message).strip())
        previous = seen.get(key)
        if (
            previous is not None
            and abs((record.timestamp - previous).total_seconds()) <= 2
        ):
            continue
        seen[key] = record.timestamp
        result.append(record)
    return result


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
    }
    selected: list[tuple[str, Path]] = []
    records: list[LogRecord] = []
    for source in sources:
        if source == "journal":
            if not sys.platform.startswith("linux"):
                raise RuntimeError(
                    "journald is only available on systemd/Linux; use --source stdout,stderr on launchd"
                )
            records.extend(journal_records(lines))
            continue
        path = paths[source]
        if not path.exists():
            print(
                f"warning: {source} log is missing: {path} (start/restart PA or choose another --source)",
                file=diagnostics,
            )
            continue
        if not os.access(path, os.R_OK):
            raise RuntimeError(
                f"Cannot read {source} log: {path}; check file ownership and permissions"
            )
        selected.append((source, path))
        records.extend(file_records(path, source))
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
    if not selected and "journal" not in sources:
        raise RuntimeError(
            "No readable log sources selected; run `pa status` and verify the PA data directory"
        )
    filtered = [
        record
        for record in _deduplicate(records)
        if _matches(record, since=threshold, severity=severity, component=component)
    ]
    for record in filtered[-lines:] if lines else filtered:
        emit(record, settings, json_output=json_output, output=output)
    if not follow:
        return
    offsets: dict[Path, tuple[int, int]] = {}
    for _, path in selected:
        stat = path.stat()
        offsets[path] = (stat.st_ino, stat.st_size)
    emitted = {
        (
            record.level,
            re.sub(r"\s+", " ", record.message).strip(),
            int(record.timestamp.timestamp()),
        )
        for record in filtered
    }
    journal_cursor = max(
        (record.timestamp for record in records if record.source == "journal"),
        default=datetime.fromtimestamp(0, UTC),
    )
    try:
        while True:
            for source, path in selected:
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                inode, offset = offsets.get(path, (stat.st_ino, 0))
                if stat.st_ino != inode or stat.st_size < offset:
                    offset = 0
                if stat.st_size > offset:
                    with path.open("r", encoding="utf-8", errors="replace") as handle:
                        handle.seek(offset)
                        for line in handle:
                            record = parse_line(line, source, datetime.now(UTC))
                            key = (
                                record.level,
                                re.sub(r"\s+", " ", record.message).strip(),
                                int(record.timestamp.timestamp()),
                            )
                            if key not in emitted and _matches(
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
                                emitted.add(key)
                        offset = handle.tell()
                offsets[path] = (stat.st_ino, offset)
            if "journal" in sources:
                for record in journal_records(max(lines, 100)):
                    key = (
                        record.level,
                        re.sub(r"\s+", " ", record.message).strip(),
                        int(record.timestamp.timestamp()),
                    )
                    if (
                        record.timestamp > journal_cursor
                        and key not in emitted
                        and _matches(
                            record,
                            since=threshold,
                            severity=severity,
                            component=component,
                        )
                    ):
                        emit(record, settings, json_output=json_output, output=output)
                        emitted.add(key)
                    journal_cursor = max(journal_cursor, record.timestamp)
            time.sleep(0.2)
    except KeyboardInterrupt:
        return
