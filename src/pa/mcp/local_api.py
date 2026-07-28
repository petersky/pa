"""Client used by PA's stdio MCP process to reach the sole local writer."""

from __future__ import annotations

import os
import threading
import time
from enum import Enum
from typing import Any
from uuid import uuid4

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings


class LocalPAServerUnavailable(RuntimeError):
    pass


_connectivity_lock = threading.Lock()
_next_connect_probe = 0.0
_connect_failures = 0


def _connection_succeeded() -> None:
    global _connect_failures, _next_connect_probe
    with _connectivity_lock:
        _connect_failures = 0
        _next_connect_probe = 0.0


def _connection_failed() -> float:
    global _connect_failures, _next_connect_probe
    with _connectivity_lock:
        _connect_failures += 1
        delay = min(5.0, 0.25 * (2 ** min(_connect_failures - 1, 5)))
        _next_connect_probe = time.monotonic() + delay
        return delay


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
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.endpoint = endpoint
        self.status = status
        self.correlation_id = correlation_id
        self.validation = validation


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
    from pa.acp.owner_channel import owner_endpoint

    return owner_endpoint(settings).url


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
            detail = response.json().get("detail")
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
    )


def request_local_pa(
    settings: Settings,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
    files: dict | None = None,
    allow_not_found: bool = False,
):
    token = os.environ.get("PA_LOCAL_API_TOKEN", "").strip()
    if not token:
        token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    expected_instance_id = os.environ.get("PA_INSTANCE_ID", "").strip()
    correlation_id = str(uuid4())
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Request-ID": correlation_id,
    }
    if expected_instance_id:
        headers["X-PA-MCP-Instance-ID"] = expected_instance_id
    endpoint_type = os.environ.get("PA_LOCAL_API_ENDPOINT_TYPE", "derived")
    with _connectivity_lock:
        retry_at = _next_connect_probe
    if retry_at > time.monotonic():
        raise LocalPAServerUnavailable(
            "PA MCP owner channel is disconnected "
            f"(endpoint_type={endpoint_type}, "
            f"retry_in={retry_at - time.monotonic():.2f}s). "
            "The PA service may still be healthy; recovery probes continue "
            "automatically. Do not write PA_DATA_DIR from the MCP process."
        )
    deadline = time.monotonic() + 2.0
    while True:
        try:
            response = httpx.request(
                method,
                f"{local_pa_url(settings)}{path}",
                params=_normalized_query_params(params),
                json=json,
                files=files,
                headers=headers,
                timeout=min(2.0, max(0.1, deadline - time.monotonic())),
            )
            if allow_not_found and response.status_code == 404:
                return None
            response.raise_for_status()
            actual_instance_id = response.headers.get("X-PA-Instance-ID", "").strip()
            if expected_instance_id and actual_instance_id != expected_instance_id:
                _connection_failed()
                raise LocalPAServerUnavailable(
                    "PA MCP owner channel instance mismatch "
                    f"(endpoint_type={endpoint_type}): expected "
                    f"{expected_instance_id!r}, reached "
                    f"{actual_instance_id or 'a server without identity'!r}."
                )
            _connection_succeeded()
            if response.status_code == 204:
                return None
            return response.json()
        except httpx.ConnectError as exc:
            if time.monotonic() >= deadline:
                delay = _connection_failed()
                raise LocalPAServerUnavailable(
                    "PA MCP owner channel is unreachable "
                    f"(endpoint_type={endpoint_type}, retry_in={delay:.2f}s). "
                    "The PA service may still be healthy on another interface. "
                    "Verify the configured bind and process network namespace. "
                    "Do not write PA_DATA_DIR from the MCP process."
                ) from exc
            time.sleep(0.1)
        except httpx.HTTPError as exc:
            if exc.response is not None:
                raise _http_error(
                    exc.response,
                    method=method,
                    path=path,
                    correlation_id=correlation_id,
                    expected_instance_id=expected_instance_id,
                ) from exc
            raise LocalPAServerUnavailable(
                f"The PA API request failed (operation={method.upper()} "
                f"endpoint={path} correlation_id={correlation_id})."
            ) from exc
