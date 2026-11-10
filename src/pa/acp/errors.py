"""Typed, operator-readable ACP / provider failure classification."""

from __future__ import annotations

import errno
import re
from datetime import datetime, timedelta
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
            # Anchored: a bare "auth" substring also matches "authority",
            # "author" and "authoritative", which are never credential failures.
            r"(api[_ -]?key|\bauth\b|\bauthenticat(ion|e|ed)\b|\bauthori[sz]ation\b|"
            r"\bnot\s+authori[sz]ed\b|credential|\bunauthori[sz]ed\b|\b401\b|"
            r"missing.*(key|token)|"
            r"\b(credentials?|api[_ -]?key|token|login|provider)\b[^.\n]{0,40}not configured)",
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


# Provider errors that waiting cures. ``rate_limited`` and ``quota_exhausted``
# are retried indefinitely (the provider says when it will serve again);
# ``provider_overloaded`` is a transient server-side condition with a bounded
# number of attempts. Checked before every generic pattern because their text
# routinely contains words such as "api key", "token" or "internal error".
RETRYABLE_PROVIDER_CODES = frozenset(
    {"rate_limited", "quota_exhausted", "provider_overloaded"}
)
UNBOUNDED_RETRY_CODES = frozenset({"rate_limited", "quota_exhausted"})

_QUOTA_RE = re.compile(
    r"(exceeded\s+(your\s+)?(current\s+)?quota|insufficient[_ ]quota|quota\s+(exceeded|exhausted|reached)|"
    r"out\s+of\s+(credits?|usage)|usage\s+limit|(hit|reached)\s+(your|the)\s+(\w+\s+)?limit|"
    r"credit\s+balance|billing\s+(hard\s+)?limit|plan\s+limit|limit\s+reached|"
    r"resource[_ ]exhausted)",
    re.IGNORECASE,
)
_RATE_RE = re.compile(
    r"(rate[\s_-]?limit|too\s+many\s+requests|(status|http|error|code|response)\W{0,12}429\b|\b429\s+too|(^|\n)\W*429\b|\b429\s*[:-]|throttl(ed|ing)|requests?\s+per\s+(minute|second|day))",
    re.IGNORECASE,
)
_OVERLOADED_RE = re.compile(
    r"(overloaded|over\s+capacity|at\s+capacity|capacity\s+(exceeded|constraints?)|(status|http|error|code|response)\W{0,12}(529|50[234])\b|"
    r"\b(529|50[234])\s+(bad|service|gateway|overloaded)|service\s+unavailable|bad\s+gateway|gateway\s+time-?out|"
    r"temporarily\s+unavailable|server\s+(is\s+)?(busy|overloaded)|upstream\s+(connect|request)\s+(error|timeout)|"
    r"internal\s+server\s+error|\b500\b[^.\n]{0,40}(error|server))",
    re.IGNORECASE,
)

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DURATION_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)\b",
    re.IGNORECASE,
)
_RETRY_CONTEXT_RE = re.compile(
    r"(retry[-_ ]after|retry\s+(in|after)|try\s+again\s+(in|after)|resets?\s+in|"
    r"available\s+again\s+in|wait\s+(for\s+)?|please\s+wait|in\s+about)",
    re.IGNORECASE,
)
_CLOCK_RE = re.compile(
    r"(try\s+again|resets?|available\s+again|retry)\s+(at|after|on)\s+"
    r"(?P<when>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?|"
    r"\d{1,2}:\d{2}\s*(?:[ap]m)?)",
    re.IGNORECASE,
)


def parse_retry_after(text: str, *, now: datetime | None = None) -> float | None:
    """Seconds the provider asked us to wait, when its message says so."""
    if not text:
        return None
    header = re.search(r"retry[-_ ]after\W+(\d+(?:\.\d+)?)\s*(?:s\b|sec|$|\W)", text, re.IGNORECASE)
    if header:
        return max(0.0, float(header.group(1)))
    context = _RETRY_CONTEXT_RE.search(text)
    if context:
        window = text[context.end() : context.end() + 80]
        total = 0.0
        matched = False
        for amount, unit in _DURATION_RE.findall(window):
            matched = True
            total += float(amount) * _UNITS[unit[0].lower()]
        if matched:
            return total
    clock = _CLOCK_RE.search(text)
    if clock:
        when = clock.group("when").strip()
        current = now or datetime.now().astimezone()
        try:
            if "-" in when:
                parsed = datetime.fromisoformat(when.replace("Z", "+00:00").replace(" ", "T"))
                if parsed.tzinfo is None:
                    parsed = parsed.astimezone()
                return max(0.0, (parsed - current).total_seconds())
            match = re.match(r"(\d{1,2}):(\d{2})\s*([ap]m)?", when, re.IGNORECASE)
            hour, minute, meridiem = int(match.group(1)), int(match.group(2)), match.group(3)
            if meridiem:
                hour = hour % 12 + (12 if meridiem.lower() == "pm" else 0)
            target = current.replace(hour=hour % 24, minute=minute, second=0, microsecond=0)
            if target <= current:
                target += timedelta(days=1)
            return (target - current).total_seconds()
        except (ValueError, AttributeError):
            return None
    return None


def classify_provider_availability(text: str) -> dict[str, Any] | None:
    """Return ``{code, message, retry_after_seconds}`` for a waitable provider error."""
    if not text:
        return None
    if _QUOTA_RE.search(text):
        code = "quota_exhausted"
        message = "The provider reports its usage quota or limit is exhausted. PA will keep waiting and retry."
    elif _RATE_RE.search(text):
        code = "rate_limited"
        message = "The provider is rate limiting requests. PA will wait and retry."
    elif _OVERLOADED_RE.search(text):
        code = "provider_overloaded"
        message = "The provider is overloaded or temporarily unavailable. PA will retry."
    else:
        return None
    return {
        "code": code,
        "message": message,
        "retry_after_seconds": parse_retry_after(text),
    }


# Failures that retrying cannot fix. Recovery must block on the first attempt
# with a precise code instead of backing off as if the provider were flaky.
# Each entry: (code, lowercase marker substrings, operator remedy).
_DETERMINISTIC_BLOCKS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        "workspace_lease_conflict",
        ("already has a live worktree lease",),
        "Another live session holds this card's worktree lease. Resume or close "
        "that session, or explicitly allow a concurrent dispatch, then retry.",
    ),
    (
        "dispatch_binding_mismatch",
        ("does not match its durable assigned dispatch binding",),
        "The session no longer matches its durable dispatch (the dispatch is "
        "terminal or was reassigned). Close the session or start a new dispatch "
        "for the card.",
    ),
    (
        "empty_index_checkout",
        ("empty-index checkout retained",),
        "The retained checkout has an empty index. Use targeted materialization "
        "preservation or repair before admission; PA will not reset it.",
    ),
    (
        "interrupted_checkout_retained",
        ("interrupted checkout has retained changes",),
        "An interrupted checkout holds uncommitted changes. Repair it explicitly "
        "before the session is admitted again.",
    ),
)


def deterministic_recovery_block(value: BaseException | str) -> dict[str, str] | None:
    """Return ``{code, remedy}`` when a failure can never succeed on retry."""
    text = (value if isinstance(value, str) else format_acp_error(value)).casefold()
    for code, markers, remedy in _DETERMINISTIC_BLOCKS:
        if any(marker in text for marker in markers):
            return {"code": code, "remedy": remedy}
    return None


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


class ProviderTurnError(RuntimeError):
    """Typed provider failure after ACP accepted a prompt."""

    def __init__(self, payload: dict[str, Any]):
        self.payload = dict(payload or {})
        super().__init__(self.payload.get("message") or "Provider turn failed")


class ACPConnectTimeout(ProviderStartError):
    """An ACP connect step (initialize, session/new, session/load) never answered."""

    def __init__(self, stage: str, timeout_s: float, provider: str | None = None):
        self.stage = stage
        self.timeout_s = timeout_s
        super().__init__(
            {
                "code": "connect_timeout",
                "message": (
                    f"The ACP provider did not answer {stage} within {timeout_s:g}s. "
                    "The provider process was stopped; PA will retry."
                ),
                "recoverable": True,
                "retryable": True,
                "stage": stage,
                "provider": provider,
            }
        )


class TurnInactivityTimeout(ConnectionError):
    """A turn produced no real ACP output for the inactivity deadline.

    A ``ConnectionError`` on purpose: the provider was cancelled, did not end the
    turn within the grace period and was killed, so the session goes through the
    ordinary connection-loss recovery path, which requeues the interrupted prompt.
    """

    reason = "inactivity_timeout"

    def __init__(self, idle_seconds: float, grace_seconds: float):
        self.idle_seconds = float(idle_seconds)
        self.grace_seconds = float(grace_seconds)
        super().__init__(
            f"Provider made no progress for {int(idle_seconds)}s and did not end the "
            f"cancelled turn within {grace_seconds:g}s; the provider process was killed"
        )


def classify_acp_failure(
    exc: BaseException,
    *,
    provider_id: str | None = None,
    stage: str | None = None,
    stderr: str | None = None,
) -> dict[str, Any]:
    """Map an exception to a structured, UI-safe failure payload."""
    if isinstance(exc, (ProviderStartError, ProviderTurnError)):
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
    missing_executable = isinstance(exc, FileNotFoundError) or (
        isinstance(exc, OSError) and getattr(exc, "errno", None) == errno.ENOENT
    )
    deterministic = deterministic_recovery_block(text)
    availability = (
        None
        if deterministic is not None or missing_executable
        else classify_provider_availability(text)
    )
    if deterministic is not None:
        code = deterministic["code"]
        message = f"{deterministic['remedy']} Detail: {text}"
    elif availability is not None:
        code = availability["code"]
        message = f"{availability['message']} Detail: {text}"
    elif missing_executable:
        missing = getattr(exc, "filename", None) or text
        code = "provider_not_installed"
        message = (
            "ACP provider executable was not found"
            + (f" during {stage}" if stage else "")
            + f" ({missing}). Install the provider CLI or set PA_AGENT_COMMAND."
        )
    elif RequestError and isinstance(exc, RequestError):
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
        }
        | RETRYABLE_PROVIDER_CODES,
        "stage": stage,
        "provider": provider_id,
        "error_type": type(exc).__name__,
    }
    if availability is not None:
        detail["retryable"] = True
        detail["retry_unbounded"] = code in UNBOUNDED_RETRY_CODES
        if availability["retry_after_seconds"] is not None:
            detail["retry_after_seconds"] = availability["retry_after_seconds"]
    if deterministic is not None:
        detail["recoverable"] = True  # an operator can fix it; a retry cannot
        detail["deterministic"] = True
        detail["action"] = deterministic["remedy"]
    return detail
