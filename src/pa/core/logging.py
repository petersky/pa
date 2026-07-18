import json
import logging
import re
import sys
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler

from pa.config import Settings

_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(
        r"(?i)((?:api[_-]?key|access[_-]?token|sync[_-]?token|secret)\s*[:=]\s*)[^\s,;]+"
    ),
    re.compile(r"\b(?:sk|gh[opusr])_[A-Za-z0-9_-]{12,}\b"),
)


def redact_log_text(value: object) -> str:
    text = str(value)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(
            lambda match: (match.group(1) if match.lastindex else "") + "[redacted]",
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
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: Settings) -> None:
    level = (
        logging.DEBUG
        if settings.debug
        else getattr(logging, settings.log_level.upper(), logging.INFO)
    )
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        RedactingFormatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s")
    )
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    structured = RotatingFileHandler(
        log_dir / "pa.jsonl", maxBytes=10 * 1024 * 1024, backupCount=5
    )
    structured.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [stderr, structured]
    root.setLevel(level)
    if settings.debug:
        logging.getLogger("pa").setLevel(logging.DEBUG)
