"""Authenticated PA HTTP automation without exposing CSRF transport details."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self
from urllib.parse import urljoin, urlsplit

import httpx

from pa.auth.csrf import COOKIE_NAME, HEADER_NAME

_UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}
_RETRYABLE_CSRF = {"csrf_expired", "csrf_invalid", "csrf_missing", "csrf_mismatch"}


class PAHTTPError(RuntimeError):
    """Sanitized PA response error; cookies and credentials are never included."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"PA request failed ({status} {code}): {message}")
        self.status = status
        self.code = code
        self.message = message


class PAClient:
    """Cookie/bearer-aware client with bounded CSRF rotation and peer routing."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str | None = None,
        peer_urls: Mapping[str, str] | None = None,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = self._normalize_origin(base_url)
        self.peer_urls = {
            key: self._normalize_origin(value)
            for key, value in (peer_urls or {}).items()
        }
        headers = {"Accept": "application/json"}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        self._bearer = bool(bearer_token)
        self._csrf_tokens: dict[str, str] = {}
        self._client = httpx.Client(
            headers=headers,
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )

    @staticmethod
    def _normalize_origin(url: str) -> str:
        parsed = urlsplit(url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("PA URLs must be absolute http(s) origins")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError(
                "PA instance URLs must not contain a path, query, or fragment"
            )
        return f"{parsed.scheme}://{parsed.netloc}"

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _origin(self, instance_id: str | None) -> str:
        if instance_id is None:
            return self.base_url
        try:
            return self.peer_urls[instance_id]
        except KeyError as exc:
            raise ValueError(
                f"No PA URL is registered for instance {instance_id!r}"
            ) from exc

    def _url(self, origin: str, path: str) -> str:
        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc:
            candidate = self._normalize_origin(f"{parsed.scheme}://{parsed.netloc}")
            if candidate != origin:
                raise ValueError(
                    "Cross-origin PA requests require explicit peer routing"
                )
            path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        if not path.startswith("/"):
            raise ValueError("PA request paths must begin with /")
        return urljoin(origin + "/", path.lstrip("/"))

    def _capture_csrf(self, origin: str, response: httpx.Response) -> None:
        token = response.cookies.get(COOKIE_NAME)
        if token:
            self._csrf_tokens[origin] = str(token)

    def _prime_csrf(self, origin: str) -> None:
        response = self._client.get(self._url(origin, "/api/health"))
        response.raise_for_status()
        self._capture_csrf(origin, response)
        if not self._csrf_tokens.get(origin):
            raise PAHTTPError(403, "csrf_missing", "PA did not issue a CSRF cookie")

    def login(
        self, username: str, password: str, *, instance_id: str | None = None
    ) -> dict[str, Any]:
        """Create a browser-style authenticated session without returning its secrets."""
        origin = self._origin(instance_id)
        self._prime_csrf(origin)
        response = self.request(
            "POST",
            "/api/auth/login",
            instance_id=instance_id,
            data={"username": username, "password": password},
            retry_csrf=True,
        )
        return dict(response.json())

    def request(
        self,
        method: str,
        path: str,
        *,
        instance_id: str | None = None,
        idempotency_key: str | None = None,
        retry_csrf: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a PA request, retrying one safe CSRF rotation when appropriate."""
        method = method.upper()
        origin = self._origin(instance_id)
        url = self._url(origin, path)
        headers = dict(kwargs.pop("headers", {}) or {})
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        unsafe = method in _UNSAFE
        if unsafe and not self._bearer:
            if not self._csrf_tokens.get(origin):
                self._prime_csrf(origin)
            headers[HEADER_NAME] = self._csrf_tokens[origin]
            headers.setdefault("Origin", origin)
        response = self._client.request(method, url, headers=headers, **kwargs)
        self._capture_csrf(origin, response)
        code, message = self._diagnostic(response)
        may_retry = (
            unsafe
            and not self._bearer
            and retry_csrf
            and code in _RETRYABLE_CSRF
            and (bool(idempotency_key) or path == "/api/auth/login")
        )
        if may_retry:
            if not self._csrf_tokens.get(origin):
                self._prime_csrf(origin)
            headers[HEADER_NAME] = self._csrf_tokens[origin]
            response = self._client.request(method, url, headers=headers, **kwargs)
            self._capture_csrf(origin, response)
            code, message = self._diagnostic(response)
        if response.status_code >= 400:
            raise PAHTTPError(response.status_code, code, message)
        return response

    @staticmethod
    def _diagnostic(response: httpx.Response) -> tuple[str, str]:
        if response.status_code < 400:
            return "ok", "ok"
        try:
            detail = response.json().get("detail")
        except ValueError, AttributeError:
            detail = None
        if isinstance(detail, dict):
            return (
                str(detail.get("code") or "http_error"),
                str(detail.get("message") or "PA rejected the request")[:1000],
            )
        return "http_error", str(detail or "PA rejected the request")[:1000]
