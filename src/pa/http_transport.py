"""HTTP transport failure classification shared by MCP and fleet clients."""

from __future__ import annotations

import re


_HTTP2_CANCEL = re.compile(
    r"(?:http/?2.*(?:cancel|0x0*8)|stream.*(?:cancel|0x0*8)|error code cancel)",
    re.IGNORECASE,
)


def is_http2_cancel(exc: BaseException) -> bool:
    """Return whether an exception chain identifies HTTP/2 CANCEL (0x8)."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _HTTP2_CANCEL.search(str(current)):
            return True
        current = current.__cause__ or current.__context__
    return False


def stable_idempotency_key(headers: object) -> str | None:
    """Extract a non-empty idempotency key from a header mapping."""
    items = getattr(headers, "items", None)
    if not callable(items):
        return None
    for key, value in items():
        if str(key).lower() == "idempotency-key" and str(value).strip():
            return str(value).strip()
    return None
