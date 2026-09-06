"""Client used by PA's stdio MCP process to reach the sole local writer."""

from __future__ import annotations

import json as jsonlib
import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any
from uuid import uuid4

import httpx

from pa.acp.environment import (
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
    assigned_service_session_capability,
)
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.http_transport import is_http2_cancel

HTTP2_CANCEL_RETRIES = 2


class LocalPAServerUnavailable(RuntimeError):
    pass


class LocalPAUnknownOutcome(LocalPAServerUnavailable):
    """A mutation may have committed; recovery is an authoritative lookup."""

    def __init__(
        self,
        message: str,
        *,
        operation_id: str,
        correlation_id: str,
        endpoint: str,
        status: int | None = None,
        detail: dict[str, Any] | list[dict[str, Any]] | str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = "mutation_outcome_unknown"
        self.operation_id = operation_id
        self.idempotency_key = operation_id
        self.correlation_id = correlation_id
        self.endpoint = endpoint
        self.status = status
        self.detail = detail
        self.recoverable = True
        self.recovery_state = "lookup_required"
        self.recovery_action = "get_operation_outcome"


def _response_proves_non_commit(
    response: httpx.Response,
    detail: dict[str, Any] | list[dict[str, Any]] | str | None,
) -> bool:
    """Accept only explicit server evidence that a mutation did not commit."""
    committed_header = response.headers.get(
        "X-PA-Mutation-Committed", ""
    ).strip().lower()
    if committed_header == "false":
        return True
    if not isinstance(detail, dict):
        return False
    return detail.get("committed") is False or detail.get("outcome") in {
        "not_committed",
        "rejected_before_commit",
    }


_ASSIGNED_MCP_ENDPOINTS = frozenset(
    {
        ("GET", "/api/goal-assigned-session/goal"),
        ("GET", "/api/goal-assigned-session/dispatch"),
        ("POST", "/api/goal-assigned-session/proposals"),
        ("POST", "/api/goal-assigned-session/evidence"),
        ("POST", "/api/goal-assigned-session/audit"),
        ("POST", "/api/goal-assigned-session/progress"),
        ("POST", "/api/goal-assigned-session/restart-handoff"),
        ("POST", "/api/goal-assigned-session/restart-handoff/edit"),
    }
)


def _rejection_codes(rejected_candidates: Any) -> list[str] | None:
    if not isinstance(rejected_candidates, list):
        return None
    codes: list[str] = []
    seen: set[str] = set()
    for item in rejected_candidates:
        if not isinstance(item, dict):
            continue
        for code in item.get("rejection_codes") or []:
            text = str(code).strip()
            if text and text not in seen:
                seen.add(text)
                codes.append(text)
    return codes or None


def _rejected_candidate_suffix(detail: Any) -> str:
    if not isinstance(detail, dict):
        return ""
    rejected = detail.get("rejected_candidates")
    if not isinstance(rejected, list) or not rejected:
        return ""
    summaries: list[str] = []
    for item in rejected[:12]:
        if not isinstance(item, dict):
            continue
        instance_id = str(item.get("instance_id") or item.get("name") or "?").strip()
        codes = [
            str(code).strip()
            for code in (item.get("rejection_codes") or [])
            if str(code).strip()
        ]
        summaries.append(
            f"{instance_id}:{','.join(codes)}" if codes else instance_id
        )
    extra = len(rejected) - 12
    suffix = f" rejected_candidates=[{'; '.join(summaries)}]"
    if extra > 0:
        suffix += f" (+{extra} more)"
    codes = _rejection_codes(rejected)
    if codes:
        suffix += f" rejection_codes={','.join(codes)}"
    return suffix


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
        self.rejected_candidates = (
            detail.get("rejected_candidates") if isinstance(detail, dict) else None
        )
        self.rejection_codes = _rejection_codes(self.rejected_candidates)


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
        suffix += _rejected_candidate_suffix(structured_detail)
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
    timeout_seconds: float = 10.0,
):
    method = method.upper()
    assigned_mode = os.environ.get(ASSIGNED_SERVICE_MODE_ENV, "") == "1"
    operation_id: str | None = None
    if method in {"POST", "PUT", "PATCH", "DELETE"}:
        operation_id = next(
            (
                value.strip()
                for key, value in (headers or {}).items()
                if key.lower() == "idempotency-key" and value.strip()
            ),
            None,
        )
        operation_id = operation_id or str(
            (json or {}).get("idempotency_key") or ""
        ).strip()
        operation_id = operation_id or str(uuid4())
    assigned_dispatch_id = os.environ.get(
        ASSIGNED_SERVICE_DISPATCH_ENV, ""
    ).strip()
    assigned_session_id = os.environ.get(ASSIGNED_SERVICE_SESSION_ENV, "").strip()
    if assigned_mode and (not assigned_dispatch_id or not assigned_session_id):
        raise LocalPAServerUnavailable(
            "The assigned Goal MCP session binding is incomplete; reload the session."
        )
    if assigned_mode and (method, path) not in _ASSIGNED_MCP_ENDPOINTS:
        raise LocalPAServerUnavailable(
            "Assigned Goal sessions cannot invoke this ordinary PA tool."
        )
    token = ""
    if not assigned_mode:
        token = os.environ.get("PA_LOCAL_API_TOKEN", "").strip()
        if not token:
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    expected_instance_id = os.environ.get("PA_INSTANCE_ID", "").strip()
    assigned_capability = (
        assigned_service_session_capability(
            secret=settings.session_secret,
            dispatch_id=assigned_dispatch_id,
            session_id=assigned_session_id,
            target_instance_id=expected_instance_id,
        )
        if assigned_mode
        else ""
    )
    correlation_id = str(uuid4())
    request_headers = {
        "Authorization": (
            f"GoalSession {assigned_capability}"
            if assigned_mode
            else f"Bearer {token}"
        ),
        "X-Request-ID": correlation_id,
    }
    if assigned_mode:
        request_headers["X-PA-Assigned-Dispatch-ID"] = assigned_dispatch_id
        request_headers["X-PA-Assigned-Session-ID"] = assigned_session_id
    reserved_headers = {
        "authorization",
        "x-request-id",
        "x-pa-mcp-instance-id",
        "x-pa-assigned-dispatch-id",
        "x-pa-assigned-session-id",
        "idempotency-key",
    }
    request_headers.update(
        {
            key: value
            for key, value in (headers or {}).items()
            if key.lower() not in reserved_headers
        }
    )
    if expected_instance_id:
        request_headers["X-PA-MCP-Instance-ID"] = expected_instance_id
    if operation_id:
        request_headers["Idempotency-Key"] = operation_id
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
    cancel_attempts = 0
    while True:
        try:
            socket_path = os.environ.get("PA_LOCAL_API_SOCKET", "").strip()
            request_base_url = local_pa_url(settings)
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
                        method, f"{request_base_url}{path}", **request_args
                    )
            else:
                response = httpx.request(
                    method, f"{request_base_url}{path}", **request_args
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
            try:
                return response.json()
            except (jsonlib.JSONDecodeError, UnicodeDecodeError) as exc:
                response_correlation_id = (
                    response.headers.get("X-Request-ID", "").strip()
                    or correlation_id
                )
                if operation_id:
                    raise LocalPAUnknownOutcome(
                        "The PA API returned an unreadable success response "
                        f"(operation={method.upper()} endpoint={path} "
                        f"status={response.status_code} "
                        f"correlation_id={response_correlation_id} "
                        f"operation_id={operation_id}). The mutation outcome is "
                        "unknown. Call get_operation_outcome with the same "
                        "idempotency key; if it reports "
                        "safe_to_retry_with_same_key, retry with that exact key.",
                        operation_id=operation_id,
                        correlation_id=response_correlation_id,
                        endpoint=path,
                        status=response.status_code,
                        detail={
                            "code": "invalid_success_response",
                            "message": (
                                "The PA API success response was not valid JSON."
                            ),
                        },
                    ) from exc
                raise LocalPAServerUnavailable(
                    f"The PA API returned an unreadable response "
                    f"(operation={method.upper()} endpoint={path} "
                    f"status={response.status_code} "
                    f"correlation_id={response_correlation_id})."
                ) from exc
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
                error = _http_error(
                    response,
                    method=method,
                    path=path,
                    correlation_id=correlation_id,
                    expected_instance_id=expected_instance_id,
                )
                if (
                    operation_id
                    and response.status_code >= 500
                    and not _response_proves_non_commit(response, error.detail)
                ):
                    raise LocalPAUnknownOutcome(
                        "The PA API returned an ambiguous server failure "
                        f"(operation={method.upper()} endpoint={path} "
                        f"status={response.status_code} "
                        f"correlation_id={error.correlation_id} "
                        f"operation_id={operation_id}). The mutation outcome is "
                        "unknown. Call get_operation_outcome with the same "
                        "idempotency key; if it reports "
                        "safe_to_retry_with_same_key, retry with that exact key.",
                        operation_id=operation_id,
                        correlation_id=error.correlation_id,
                        endpoint=path,
                        status=response.status_code,
                        detail=error.detail,
                    ) from exc
                raise error from exc
            if is_http2_cancel(exc) and (
                operation_id or method in {"GET", "HEAD", "OPTIONS"}
            ):
                cancel_attempts += 1
                if (
                    cancel_attempts <= HTTP2_CANCEL_RETRIES
                    and time.monotonic() < deadline
                ):
                    time.sleep(min(0.05 * cancel_attempts, 0.1))
                    continue
            if operation_id:
                raise LocalPAUnknownOutcome(
                    f"The PA API request failed"
                    f"{' because HTTP/2 stream CANCEL (0x8)' if is_http2_cancel(exc) else ''} "
                    f"(operation={method.upper()} "
                    f"endpoint={path} correlation_id={correlation_id} "
                    f"operation_id={operation_id}). The mutation outcome is "
                    "unknown. Call get_operation_outcome with the same "
                    "idempotency key; if it reports safe_to_retry_with_same_key, "
                    "retry with that exact key.",
                    operation_id=operation_id,
                    correlation_id=correlation_id,
                    endpoint=path,
                ) from exc
            raise LocalPAServerUnavailable(
                f"The PA API request failed"
                f"{' because HTTP/2 stream CANCEL (0x8)' if is_http2_cancel(exc) else ''} "
                f"(operation={method.upper()} endpoint={path} "
                f"correlation_id={correlation_id} "
                f"safe_to_retry={method in {'GET', 'HEAD', 'OPTIONS'}})."
            ) from exc
