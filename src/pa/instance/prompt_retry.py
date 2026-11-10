"""Backoff policy for provider errors that waiting cures (rate limit, quota, overload)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RetryPlan:
    """What to do after a failed prompt attempt."""

    attempt: int
    delay_seconds: float
    unbounded: bool
    code: str
    #: Past the attempt cap of a bounded error: stop retrying and tell the owner.
    give_up: bool = False
    #: The wait is already long (provider-announced or accumulated): tell the owner once.
    long_wait: bool = False


def _setting(settings: Any, name: str, default: float) -> float:
    value = getattr(settings, name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return float(default)
    return float(value)


def plan_retry(
    classified: dict[str, Any],
    *,
    attempt: int,
    waited_seconds: float,
    settings: Any,
) -> RetryPlan | None:
    """Plan the next attempt for a classified failure, or ``None`` when terminal.

    ``attempt`` is the 1-based number of the attempt that just failed and
    ``waited_seconds`` the time since this prompt first failed. Rate limits and
    quota exhaustion have no attempt cap (the card must be able to wait out a
    limit and finish); other retryable errors stop after
    ``agent_retry_max_attempts``.
    """
    if not classified.get("retryable"):
        return None
    base = max(0.001, _setting(settings, "agent_retry_base_seconds", 30.0))
    cap = max(base, _setting(settings, "agent_retry_cap_seconds", 900.0))
    max_attempts = int(_setting(settings, "agent_retry_max_attempts", 5))
    notify_after = _setting(settings, "agent_rate_limit_notify_after_seconds", 1800.0)
    unbounded = bool(classified.get("retry_unbounded"))
    code = str(classified.get("code") or "provider_error")
    if not unbounded and attempt >= max_attempts:
        return RetryPlan(attempt, 0.0, unbounded, code, give_up=True)
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    announced = classified.get("retry_after_seconds")
    if isinstance(announced, (int, float)) and announced > 0:
        # The provider said when it will serve us again: never retry earlier
        # than that, even past the cap (a usage limit can reset hours away).
        delay = max(delay, float(announced))
    long_wait = bool(
        unbounded
        and notify_after > 0
        and (waited_seconds + delay >= notify_after)
    )
    return RetryPlan(attempt, delay, unbounded, code, long_wait=long_wait)
