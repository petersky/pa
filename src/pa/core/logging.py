import json
import logging
import re
import sys
from copy import deepcopy
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler

from pa.config import Settings

_STANDARD_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
}

_SECRET_PATTERNS = (
    re.compile(
        r"(?i)((?:authorization|proxy-authorization|cookie|set-cookie|x-api-key|password|client[_-]?secret)\s*[:=]\s*)[^\r\n,;]+"
    ),
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(
        r"(?i)((?:api[_-]?key|access[_-]?token|sync[_-]?token|secret)\s*[:=]\s*)[^\s,;]+"
    ),
    re.compile(r"\b(?:sk|gh[opusr])_[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"(https?://[^\s:/@]+:)[^\s/@]+(@)", re.IGNORECASE),
)


def redact_log_text(value: object) -> str:
    text = str(value)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(
            lambda match: (
                (match.group(1) if match.lastindex else "")
                + "[redacted]"
                + (match.group(2) if match.lastindex and match.lastindex > 1 else "")
            ),
            text,
        )
    return text


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        original_msg, original_args = record.msg, record.args
        record.msg, record.args = redact_log_text(record.getMessage()), ()
        try:
            return super().format(record)
        finally:
            record.msg, record.args = original_msg, original_args


class JsonFormatter(RedactingFormatter):
    """Stable one-record-per-line production log format."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_log_text(record.getMessage()),
        }
        if record.exc_info:
            payload["exception"] = redact_log_text(
                self.formatException(record.exc_info)
            )
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_FIELDS:
                try:
                    json.dumps(value)
                    payload[key] = (
                        redact_log_text(value) if isinstance(value, str) else value
                    )
                except TypeError, ValueError:
                    payload[key] = redact_log_text(value)
        return json.dumps(payload, ensure_ascii=False)


class ExpectedShutdownCancellationFilter(logging.Filter):
    """Turn PA-initiated request cancellation into an actionable summary."""

    def filter(self, record: logging.LogRecord) -> bool:
        import asyncio

        if not record.exc_info or not issubclass(
            record.exc_info[0], asyncio.CancelledError
        ):
            return True
        from pa.server.shutdown import is_shutting_down

        if is_shutting_down():
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
            record.msg = "request cancelled after PA graceful-shutdown policy deadline (expected shutdown cancellation)"
            record.args = ()
            record.exc_info = None
            record.exc_text = None
        return True


def uvicorn_log_config() -> dict[str, object]:
    """Return Uvicorn's standard config with timestamps and PA redaction."""
    from uvicorn.config import LOGGING_CONFIG

    config = deepcopy(LOGGING_CONFIG)
    config["formatters"]["default"]["fmt"] = (
        "%({asctime})s %(levelprefix)s %(message)s".format(asctime="asctime")
    )
    config["formatters"]["default"]["datefmt"] = "%Y-%m-%dT%H:%M:%S%z"
    config["formatters"]["access"]["fmt"] = (
        '%({asctime})s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'.format(
            asctime="asctime"
        )
    )
    config["formatters"]["access"]["datefmt"] = "%Y-%m-%dT%H:%M:%S%z"
    return config


def configure_logging(settings: Settings) -> None:
    level = (
        logging.DEBUG
        if settings.debug
        else getattr(logging, settings.log_level.upper(), logging.INFO)
    )
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        RedactingFormatter(
            "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    cancellation_filter = ExpectedShutdownCancellationFilter()
    stderr.addFilter(cancellation_filter)
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    structured = RotatingFileHandler(
        log_dir / "pa.jsonl", maxBytes=10 * 1024 * 1024, backupCount=5
    )
    structured.setFormatter(JsonFormatter())
    structured.addFilter(cancellation_filter)
    root = logging.getLogger()
    root.handlers = [stderr, structured]
    root.setLevel(level)
    # httpx logs every successful request at INFO. PA logs actionable transport
    # and HTTP failures at their call sites, so keep routine 2xx traffic quiet.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # Uvicorn configures non-propagating handlers before the app factory runs.
    # Apply the same shutdown classification directly to those handlers.
    for handler in logging.getLogger("uvicorn.error").handlers:
        handler.addFilter(cancellation_filter)
    if settings.debug:
        logging.getLogger("pa").setLevel(logging.DEBUG)
