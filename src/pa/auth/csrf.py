"""CSRF protection via double-submit cookie."""

from __future__ import annotations

import secrets

from starlette.requests import Request

COOKIE_NAME = "pa_csrf"
HEADER_NAME = "X-CSRF-Token"


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def token_from_request(request: Request) -> str | None:
    header = request.headers.get(HEADER_NAME)
    if header:
        return header
    if request.method == "POST":
        # Form field fallback for non-HTMX posts
        if hasattr(request, "_form"):
            form = request._form  # type: ignore[attr-defined]
            if form and "_csrf" in form:
                return str(form["_csrf"])
    return None


def validate_request(request: Request) -> bool:
    cookie = request.cookies.get(COOKIE_NAME)
    submitted = token_from_request(request)
    if not cookie or not submitted:
        return False
    return secrets.compare_digest(cookie, submitted)
