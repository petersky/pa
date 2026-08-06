"""Typed, operator-readable ACP / provider failure classification."""

from __future__ import annotations

import re
from typing import Any

from pa.acp.sandbox_health import sanitize_provider_error

try:
    from acp.exceptions import RequestError
except Exception:  # pragma: no cover - optional at import time for unit tests
    RequestError = ()  # type: ignore[misc, assignment]


_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "auth_missing",
        re.compile(
            r"(api[_ -]?key|auth(entication|orization)?|credential|unauthorized|401|"
            r"missing.*(key|token)|not configured)",
            re.IGNORECASE,
        ),
        "Provider authentication failed or a required API credential is missing.",
    ),
    (
        "invalid_model",
        re.compile(r"(unknown|invalid|unsupported).*(model|model_id)", re.IGNORECASE),
        "The requested model is not valid for this provider.",
    ),
    (
        "invalid_model_provider",
        re.compile(
            r"(unknown|invalid|unsupported).*(model[_ ]?provider|provider id)|"
            r"model_provider",
            re.IGNORECASE,
        ),
        "The requested model provider is not valid for this agent runtime.",
    ),
    (
        "probe_failed",
        re.compile(r"(initialize probe failed|acp initialize)", re.IGNORECASE),
        "ACP initialize/probe failed before a session could start.",
    ),
    (
        "connection_closed",
        re.compile(
            r"(connection closed|connection reset|broken pipe|transport.*(closed|dead))",
            re.IGNORECASE,
        ),
        (
            "The ACP provider process closed during startup or a prompt. Check "
            "model_provider configuration, credentials, and provider stderr."
        ),
    ),
    (
        "configuration_failed",
        re.compile(
            r"(configuration compatibility|session configuration is not confirmed)",
            re.IGNORECASE,
        ),
        "ACP session configuration could not be applied or verified.",
    ),
)


def format_acp_error(exc: BaseException) -> str:
    """Render an ACP exception, including RequestError payload when present."""
    parts = [str(exc).strip() or type(exc).__name__]
    data = getattr(exc, "data", None)
    if data is not None:
        rendered = sanitize_provider_error(data, limit=1200)
        if rendered and rendered not in parts[0]:
            parts.append(rendered)
    message = getattr(exc, "message", None)
    if message and str(message) not in parts[0]:
        parts.append(sanitize_provider_error(message, limit=400))
    return " — ".join(part for part in parts if part)


class ProviderStartError(RuntimeError):
    """Typed OpenInterpreter / provider admission failure."""

    def __init__(self, payload: dict[str, Any]):
        self.payload = dict(payload or {})
        super().__init__(self.payload.get("message") or "Provider start failed")


def classify_acp_failure(
    exc: BaseException,
    *,
    provider_id: str | None = None,
    stage: str | None = None,
    stderr: str | None = None,
) -> dict[str, Any]:
    """Map an exception to a structured, UI-safe failure payload."""
    if isinstance(exc, ProviderStartError):
        detail = dict(exc.payload)
        detail.setdefault("recoverable", True)
        detail.setdefault("stage", stage)
        detail.setdefault("provider", provider_id)
        detail.setdefault("error_type", type(exc).__name__)
        if "message" in detail:
            detail["message"] = sanitize_provider_error(detail["message"], limit=2000)
        return detail
    raw = format_acp_error(exc)
    if stderr:
        raw = f"{raw}\n{stderr}"
    text = sanitize_provider_error(raw, limit=2500)
    code = "acp_internal_error"
    message = text
    if RequestError and isinstance(exc, RequestError):
        code_num = getattr(exc, "code", None)
        if code_num == -32603 or "internal error" in str(exc).lower():
            code = "acp_internal_error"
            message = (
                "ACP provider returned Internal error"
                + (f" during {stage}" if stage else "")
                + ". "
                + (
                    "For OpenInterpreter this often means an invalid model_provider "
                    "override, missing credential, or provider process crash. "
                    if (provider_id or "") == "openinterpreter"
                    else ""
                )
                + f"Detail: {text}"
            )
        elif code_num == -32602:
            code = "acp_invalid_params"
            message = f"ACP request had invalid parameters: {text}"
        elif code_num == -32601:
            code = "acp_method_not_found"
            message = f"ACP method not found: {text}"
    elif isinstance(exc, ConnectionError) or "connection closed" in text.lower():
        code = "connection_closed"
        message = (
            "The ACP provider process closed unexpectedly"
            + (f" during {stage}" if stage else "")
            + ". "
            + (
                "Verify OpenInterpreter model_provider configuration and credentials, "
                "then retry. "
                if (provider_id or "") == "openinterpreter"
                else ""
            )
            + f"Detail: {text}"
        )
    else:
        for pattern_code, pattern, fallback in _PATTERNS:
            if pattern.search(text):
                code = pattern_code
                message = f"{fallback} Detail: {text}"
                break
        else:
            message = text

    detail: dict[str, Any] = {
        "code": code,
        "message": sanitize_provider_error(message, limit=2000),
        "recoverable": code
        in {
            "auth_missing",
            "invalid_model",
            "invalid_model_provider",
            "configuration_failed",
            "connection_closed",
            "probe_failed",
            "acp_internal_error",
            "provider_not_installed",
            "model_provider_missing",
        },
        "stage": stage,
        "provider": provider_id,
        "error_type": type(exc).__name__,
    }
    return detail
