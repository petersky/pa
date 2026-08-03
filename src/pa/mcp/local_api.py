"""Client used by PA's stdio MCP process to reach the sole local writer."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any
from uuid import uuid4

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings


class LocalPAServerUnavailable(RuntimeError):
    pass


@dataclass
class _Circuit:
    failures: int = 0
    retry_at: float = 0.0
    last_failure: str | None = None
    last_success: float | None = None


_circuit = _Circuit()
_circuit_lock = threading.Lock()


class LocalPARequestError(LocalPAServerUnavailable):
    """An HTTP error returned by the owning PA API."""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        endpoint: str,
        status: int,
        correlation_id: str,
        validation: list[dict[str, Any]] | None = None,
        detail: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.endpoint = endpoint
        self.status = status
        self.correlation_id = correlation_id
        self.validation = validation
        # Keep the machine-readable API failure intact for MCP hosts and UIs.
        # The human-readable exception remains bounded, but callers must not
        # have to scrape it to recover retry guidance or entity provenance.
        self.detail = detail
        self.code = detail.get("code") if isinstance(detail, dict) else None
        self.recoverable = (
            detail.get("recoverable") if isinstance(detail, dict) else None
        )
        self.retry_after = (
            detail.get("retry_after") if isinstance(detail, dict) else None
        )


def _normalized_query_params(params: dict | None) -> dict | None:
    if not params:
        return None
    normalized = {
        key: value.value if isinstance(value, Enum) else value
        for key, value in params.items()
        if value is not None and (not isinstance(value, str) or value.strip())
    }
    return normalized or None


def local_pa_url(settings: Settings) -> str:
    explicit = os.environ.get("PA_LOCAL_API_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    host = settings.host if settings.host not in {"0.0.0.0", "::"} else "127.0.0.1"
    return f"http://{host}:{settings.port}"


def _validation_details(response: httpx.Response) -> list[dict[str, Any]] | None:
    """Return useful Pydantic fields without reflecting submitted values."""
    try:
        body = response.json()
    except ValueError:
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    if not isinstance(detail, list):
        return None
    sanitized = []
    for item in detail:
        if not isinstance(item, dict):
            continue
        field = {
            key: item[key]
            for key in ("type", "loc", "msg")
            if key in item and isinstance(item[key], (str, int, float, bool, list))
        }
        if field:
            sanitized.append(field)
    return sanitized or None


def _http_error(
    response: httpx.Response,
    *,
    method: str,
    path: str,
    correlation_id: str,
    expected_instance_id: str,
) -> LocalPARequestError:
    status = response.status_code
    actual_instance_id = response.headers.get("X-PA-Instance-ID", "").strip()
    response_correlation_id = (
        response.headers.get("X-Request-ID", "").strip() or correlation_id
    )
    context = (
        f"operation={method.upper()} endpoint={path} status={status} "
        f"correlation_id={response_correlation_id}"
    )
    structured_detail: dict[str, Any] | list[dict[str, Any]] | str | None = None
    try:
        response_body = response.json()
        if isinstance(response_body, dict):
            structured_detail = response_body.get("detail")
    except ValueError:
        structured_detail = response.text[:1000] or None
    if (
        expected_instance_id
        and actual_instance_id
        and expected_instance_id != actual_instance_id
    ):
        message = (
            f"PA MCP instance mismatch ({context}): bridge instance "
            f"{expected_instance_id!r} reached server instance {actual_instance_id!r}."
        )
        validation = None
    else:
        validation = _validation_details(response)
        code = None
        detail_message = None
        try:
            detail = structured_detail
            if isinstance(detail, dict):
                code = str(detail.get("code") or "")[:100]
                detail_message = str(detail.get("message") or "")[:1000]
        except ValueError, AttributeError:
            pass
        suffix = f" validation={validation!r}" if validation else ""
        if code:
            suffix += f" code={code}"
        if detail_message:
            suffix += f" detail={detail_message}"
        message = f"The PA API rejected the MCP request ({context}).{suffix}"
    return LocalPARequestError(
        message,
        operation=method.upper(),
        endpoint=path,
        status=status,
        correlation_id=response_correlation_id,
        validation=validation,
        detail=structured_detail,
    )


def request_local_pa(
    settings: Settings,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
    files: dict | None = None,
    headers: dict[str, str] | None = None,
    allow_not_found: bool = False,
    timeout_seconds: float = 2.0,
):
    token = os.environ.get("PA_LOCAL_API_TOKEN", "").strip()
    if not token:
        token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    expected_instance_id = os.environ.get("PA_INSTANCE_ID", "").strip()
    correlation_id = str(uuid4())
    request_headers = {
        "Authorization": f"Bearer {token}",
        "X-Request-ID": correlation_id,
    }
    reserved_headers = {"authorization", "x-request-id", "x-pa-mcp-instance-id"}
    request_headers.update(
        {
            key: value
            for key, value in (headers or {}).items()
            if key.lower() not in reserved_headers
        }
    )
    if expected_instance_id:
        request_headers["X-PA-MCP-Instance-ID"] = expected_instance_id
    now = time.monotonic()
    with _circuit_lock:
        retry_at = _circuit.retry_at
        last_failure = _circuit.last_failure
    if retry_at > now:
        retry_in = max(0.0, retry_at - now)
        endpoint_type = os.environ.get("PA_LOCAL_API_ENDPOINT_TYPE", "configured")
        raise LocalPAServerUnavailable(
            "The PA MCP owner channel is disconnected "
            f"(classification={last_failure or 'unreachable'} "
            f"endpoint={endpoint_type} retry_in={retry_in:.1f}s). "
            "PA itself may still be healthy; do not write PA_DATA_DIR."
        )
    timeout_seconds = max(0.1, min(float(timeout_seconds), 120.0))
    deadline = now + timeout_seconds
    while True:
        try:
            socket_path = os.environ.get("PA_LOCAL_API_SOCKET", "").strip()
            request_args = {
                "params": _normalized_query_params(params),
                "json": json,
                "files": files,
                "headers": request_headers,
                "timeout": max(0.1, deadline - time.monotonic()),
            }
            if socket_path:
                with httpx.Client(
                    transport=httpx.HTTPTransport(uds=socket_path)
                ) as client:
                    response = client.request(
                        method, f"{local_pa_url(settings)}{path}", **request_args
                    )
            else:
                response = httpx.request(
                    method, f"{local_pa_url(settings)}{path}", **request_args
                )
            if allow_not_found and response.status_code == 404:
                return None
            response.raise_for_status()
            actual_instance_id = response.headers.get("X-PA-Instance-ID", "").strip()
            if expected_instance_id and actual_instance_id != expected_instance_id:
                raise LocalPARequestError(
                    "PA MCP instance mismatch "
                    f"(operation={method.upper()} endpoint={path} "
                    f"correlation_id={correlation_id}): bridge instance "
                    f"{expected_instance_id!r} reached server instance "
                    f"{actual_instance_id or '<missing>'!r}.",
                    operation=method.upper(),
                    endpoint=path,
                    status=response.status_code,
                    correlation_id=correlation_id,
                )
            with _circuit_lock:
                _circuit.failures = 0
                _circuit.retry_at = 0.0
                _circuit.last_failure = None
                _circuit.last_success = time.time()
            if response.status_code == 204:
                return None
            return response.json()
        except httpx.ConnectError as exc:
            if time.monotonic() >= deadline:
                with _circuit_lock:
                    _circuit.failures += 1
                    _circuit.last_failure = "unreachable"
                    _circuit.retry_at = time.monotonic() + min(
                        0.25 * (2 ** (_circuit.failures - 1)), 5.0
                    )
                endpoint_type = os.environ.get(
                    "PA_LOCAL_API_ENDPOINT_TYPE", "configured"
                )
                raise LocalPAServerUnavailable(
                    "The PA MCP owner channel is unreachable "
                    f"(endpoint={endpoint_type}); PA itself may still be healthy. "
                    "Recovery probes will retry automatically. "
                    "Do not write PA_DATA_DIR from the MCP process."
                ) from exc
            time.sleep(0.1)
        except httpx.HTTPError as exc:
            # Request/transport exceptions (notably ReadTimeout) do not expose
            # ``response``.  Do not let diagnostics mask the real failure.
            response = getattr(exc, "response", None)
            if response is not None:
                raise _http_error(
                    response,
                    method=method,
                    path=path,
                    correlation_id=correlation_id,
                    expected_instance_id=expected_instance_id,
                ) from exc
            raise LocalPAServerUnavailable(
                f"The PA API request failed (operation={method.upper()} "
                f"endpoint={path} correlation_id={correlation_id}). "
                "The request outcome is unknown; retry mutations only with the "
                "same idempotency key."
            ) from exc
