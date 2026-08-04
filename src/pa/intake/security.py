from __future__ import annotations

import hmac
import ipaddress
import re
import threading
import time
from collections import defaultdict, deque
from urllib.parse import urlsplit

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from pa.execution.progress import sanitize_text

_INJECTION_PATTERNS = (
    re.compile(
        r"\bignore (?:all |the )?(?:previous|prior|system) instructions\b", re.I
    ),
    re.compile(r"\b(system|developer) prompt\b", re.I),
    re.compile(
        r"\b(exfiltrate|reveal|print)\b.{0,40}\b(secret|token|credential|prompt)\b",
        re.I,
    ),
    re.compile(r"<\s*/?\s*(?:system|assistant|tool)\b", re.I),
)
_EXECUTABLE_TYPES = {
    "application/x-dosexec",
    "application/x-executable",
    "application/x-msdownload",
    "application/x-sharedlib",
}
_EXECUTABLE_SUFFIXES = {
    ".apk",
    ".app",
    ".bat",
    ".cmd",
    ".com",
    ".dll",
    ".dmg",
    ".exe",
    ".jar",
    ".msi",
    ".ps1",
    ".scr",
}
_EICAR = b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE"


def redact_channel_text(value: str | None, *, limit: int = 32_000) -> str | None:
    if value is None:
        return None
    return sanitize_text(value, limit=limit)


def detect_prompt_injection(value: str | None) -> bool:
    text = value or ""
    return any(pattern.search(text) for pattern in _INJECTION_PATTERNS)


def verify_telegram_secret(expected: str, supplied: str | None) -> bool:
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def verify_discord_signature(
    public_key_hex: str,
    timestamp: str | None,
    body: bytes,
    signature_hex: str | None,
) -> bool:
    if not public_key_hex or not timestamp or not signature_hex:
        return False
    try:
        public_key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        public_key.verify(bytes.fromhex(signature_hex), timestamp.encode() + body)
    except (ValueError, InvalidSignature):
        return False
    return True


def inspect_artifact(
    content: bytes, *, filename: str | None, media_type: str | None
) -> tuple[bool, str | None]:
    normalized = (media_type or "").split(";", 1)[0].strip().lower()
    suffix = ""
    if filename and "." in filename:
        suffix = "." + filename.rsplit(".", 1)[-1].lower()
    if normalized in _EXECUTABLE_TYPES or suffix in _EXECUTABLE_SUFFIXES:
        return False, "executable content is quarantined"
    if _EICAR in content:
        return False, "malware test signature detected"
    if content.startswith(
        (b"MZ", b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf")
    ):
        return False, "executable file signature detected"
    if normalized.startswith("image/"):
        signatures = {
            "image/png": (b"\x89PNG\r\n\x1a\n",),
            "image/jpeg": (b"\xff\xd8\xff",),
            "image/gif": (b"GIF87a", b"GIF89a"),
            "image/webp": (b"RIFF",),
        }
        expected = signatures.get(normalized)
        if expected and not content.startswith(expected):
            return False, "declared image type does not match file signature"
    return True, None


def validate_discord_attachment_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or parsed.username or parsed.password:
            return False
        if host not in {"cdn.discordapp.com", "media.discordapp.net"}:
            return False
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            return True
        return not (address.is_private or address.is_loopback or address.is_link_local)
    except ValueError:
        return False


class SlidingWindowLimiter:
    """Small process-local abuse limiter; provider retries remain idempotent."""

    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(
        self,
        key: str,
        *,
        limit: int,
        window_seconds: float = 60.0,
        now: float | None = None,
    ) -> bool:
        current = time.monotonic() if now is None else now
        with self._lock:
            events = self._events[key]
            boundary = current - window_seconds
            while events and events[0] <= boundary:
                events.popleft()
            if len(events) >= limit:
                return False
            events.append(current)
            if not events:
                self._events.pop(key, None)
            return True
