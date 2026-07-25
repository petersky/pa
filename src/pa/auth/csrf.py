"""Time-bounded, signed double-submit CSRF tokens."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass
from typing import Literal

from starlette.requests import Request

COOKIE_NAME = "pa_csrf"
HEADER_NAME = "X-CSRF-Token"
TOKEN_VERSION = "v1"
TOKEN_TTL_SECONDS = 86400


@dataclass(frozen=True)
class CSRFValidation:
    ok: bool
    code: Literal["ok", "csrf_missing", "csrf_mismatch", "csrf_invalid", "csrf_expired"]


def generate_token(secret: str, *, now: float | None = None) -> str:
    issued = int(time.time() if now is None else now)
    nonce = secrets.token_urlsafe(24)
    payload = f"{TOKEN_VERSION}.{issued}.{nonce}"
    signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def inspect_token(
    token: str | None,
    secret: str,
    *,
    now: float | None = None,
    ttl_seconds: int = TOKEN_TTL_SECONDS,
) -> Literal["valid", "missing", "invalid", "expired"]:
    if not token:
        return "missing"
    parts = token.split(".")
    if len(parts) != 4 or parts[0] != TOKEN_VERSION:
        return "invalid"
    try:
        issued = int(parts[1])
    except ValueError:
        return "invalid"
    payload = ".".join(parts[:3])
    expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, parts[3]):
        return "invalid"
    current = time.time() if now is None else now
    if issued > current + 60 or current - issued > ttl_seconds:
        return "expired"
    return "valid"


def token_for_request(request: Request) -> str:
    """Return the token selected by middleware, including on a first request."""

    return str(
        getattr(request.state, "csrf_token", "") or request.cookies.get(COOKIE_NAME, "")
    )


def token_from_request(request: Request) -> str | None:
    header = request.headers.get(HEADER_NAME)
    if header:
        return header
    # Form field fallback for non-HTMX posts
    if request.method == "POST" and hasattr(request, "_form"):
        form = request._form  # type: ignore[attr-defined]
        if form and "_csrf" in form:
            return str(form["_csrf"])
    return None


def validate_request(request: Request, secret: str) -> CSRFValidation:
    cookie = request.cookies.get(COOKIE_NAME)
    submitted = token_from_request(request)
    if not cookie or not submitted:
        return CSRFValidation(False, "csrf_missing")
    if not secrets.compare_digest(cookie, submitted):
        return CSRFValidation(False, "csrf_mismatch")
    status = inspect_token(cookie, secret)
    if status == "expired":
        return CSRFValidation(False, "csrf_expired")
    if status != "valid":
        return CSRFValidation(False, "csrf_invalid")
    return CSRFValidation(True, "ok")
