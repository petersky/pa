"""Cookie security helpers."""

from __future__ import annotations

from starlette.requests import Request

from pa.config import Settings


def use_secure_cookies(request: Request | None = None, settings: Settings | None = None) -> bool:
    if settings and settings.secure_cookies:
        return True
    if request is None:
        return False
    if request.url.scheme == "https":
        return True
    forwarded = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    return forwarded == "https"
