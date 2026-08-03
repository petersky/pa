"""Typed fleet execution capacity and mixed-version compatibility helpers."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

# Four preserves the pre-v0.2.26 admission ceiling while making it visible and
# configurable. It is deliberately conservative for an instance whose host and
# provider limits are not known to PA.
DEFAULT_DISPATCH_CAPACITY = 4
MAX_DISPATCH_CAPACITY = 256
DispatchCapacity = Annotated[int, Field(ge=1, le=MAX_DISPATCH_CAPACITY, strict=True)]

# Waiting dispatches are cheap durable records, but still need a finite bound so a
# broken or abusive submitter cannot grow the authority ledger forever.
DEFAULT_DISPATCH_QUEUE_CAPACITY = 100
MAX_DISPATCH_QUEUE_CAPACITY = 10_000
DispatchQueueCapacity = Annotated[
    int, Field(ge=0, le=MAX_DISPATCH_QUEUE_CAPACITY, strict=True)
]


class EffectiveCapacity(BaseModel):
    """Effective global/provider limit and where the value came from."""

    limit: int = Field(ge=1, le=MAX_DISPATCH_CAPACITY)
    global_limit: int = Field(ge=1, le=MAX_DISPATCH_CAPACITY)
    provider_limit: int | None = Field(default=None, ge=1, le=MAX_DISPATCH_CAPACITY)
    source: str
    provider: str | None = None
    legacy_capability: str | None = None
    rationale: str | None = None


class EffectiveQueueCapacity(BaseModel):
    """Effective waiting-queue limit and its configuration provenance."""

    limit: int = Field(ge=0, le=MAX_DISPATCH_QUEUE_CAPACITY)
    global_limit: int = Field(ge=0, le=MAX_DISPATCH_QUEUE_CAPACITY)
    provider_limit: int | None = Field(
        default=None, ge=0, le=MAX_DISPATCH_QUEUE_CAPACITY
    )
    source: str
    provider: str | None = None


def _valid_limit(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except TypeError, ValueError:
        return None
    return parsed if 1 <= parsed <= MAX_DISPATCH_CAPACITY else None


def legacy_capacity(capabilities: list[str]) -> tuple[int | None, str | None]:
    """Return the first valid legacy ``capacity:N`` advertisement."""

    for capability in capabilities:
        if not isinstance(capability, str) or not capability.startswith("capacity:"):
            continue
        parsed = _valid_limit(capability.partition(":")[2])
        if parsed is not None:
            return parsed, capability
    return None, None


def effective_capacity(
    *,
    configured: Any = None,
    provider_capacities: dict[str, Any] | None = None,
    capabilities: list[str] | None = None,
    provider: str | None = None,
) -> EffectiveCapacity:
    """Resolve configured > legacy capability > documented default precedence."""

    explicit = _valid_limit(configured)
    legacy, legacy_tag = legacy_capacity(capabilities or [])
    if explicit is not None:
        global_limit = explicit
        source = "configured"
        rationale = None
    elif legacy is not None:
        global_limit = legacy
        source = "legacy_capability"
        rationale = (
            "Compatibility value from capacity:N; configure dispatch_capacity "
            "and remove the deprecated capability tag."
        )
    else:
        global_limit = DEFAULT_DISPATCH_CAPACITY
        source = "documented_default"
        rationale = (
            "Conservative compatibility default used because host/provider "
            "resource limits are not known to PA."
        )

    normalized_provider = provider.strip().lower() if provider else None
    provider_limit = None
    if normalized_provider and provider_capacities:
        provider_limit = _valid_limit(
            provider_capacities.get(normalized_provider)
            or provider_capacities.get(provider or "")
        )
    limit = min(global_limit, provider_limit or global_limit)
    return EffectiveCapacity(
        limit=limit,
        global_limit=global_limit,
        provider_limit=provider_limit,
        source=(
            "configured_provider"
            if provider_limit is not None and provider_limit <= global_limit
            else source
        ),
        provider=normalized_provider,
        legacy_capability=legacy_tag,
        rationale=rationale,
    )


def effective_queue_capacity(
    *,
    configured: Any = None,
    provider_capacities: dict[str, Any] | None = None,
    provider: str | None = None,
) -> EffectiveQueueCapacity:
    """Resolve a provider ceiling together with the instance queue limit."""

    global_limit = (
        configured
        if isinstance(configured, int)
        and not isinstance(configured, bool)
        and 0 <= configured <= MAX_DISPATCH_QUEUE_CAPACITY
        else DEFAULT_DISPATCH_QUEUE_CAPACITY
    )
    normalized_provider = provider.strip().lower() if provider else None
    provider_limit = None
    if normalized_provider and provider_capacities:
        candidate = provider_capacities.get(normalized_provider)
        if (
            isinstance(candidate, int)
            and not isinstance(candidate, bool)
            and 0 <= candidate <= MAX_DISPATCH_QUEUE_CAPACITY
        ):
            provider_limit = candidate
    return EffectiveQueueCapacity(
        limit=min(global_limit, provider_limit if provider_limit is not None else global_limit),
        global_limit=global_limit,
        provider_limit=provider_limit,
        source=(
            "configured_provider"
            if provider_limit is not None and provider_limit <= global_limit
            else (
                "configured"
                if configured is not None
                else "documented_default"
            )
        ),
        provider=normalized_provider,
    )


def workload_counts(
    activity: dict[str, Any], *, provider: str | None = None
) -> dict[str, Any]:
    """Normalize new workload semantics with conservative old-peer fallbacks."""

    provider_key = provider.strip().lower() if provider else None
    provider_counts = (
        (activity.get("provider_concurrency") or {}).get(provider_key, {})
        if provider_key
        else {}
    )
    if provider_counts:
        active = max(0, int(provider_counts.get("active_capacity_consumers") or 0))
        queued = max(0, int(provider_counts.get("queued_prompts") or 0))
        reservations = max(0, int(provider_counts.get("dispatch_reservations") or 0))
        semantic_source = "provider_concurrency"
    else:
        explicit_active = activity.get("active_capacity_consumers")
        if explicit_active is not None:
            active = max(0, int(explicit_active or 0))
            semantic_source = "capacity_consumers"
        elif activity.get("prompting_turns") is not None:
            active = max(0, int(activity.get("prompting_turns") or 0))
            semantic_source = "prompting_turns"
        else:
            sessions = activity.get("sessions") or []
            working = sum(
                1
                for item in sessions
                if isinstance(item, dict)
                and str(item.get("status") or "").lower() in {"working", "prompting"}
            )
            if working or sessions:
                active = working
                semantic_source = "legacy_session_states"
            else:
                active = max(0, int(activity.get("active_sessions") or 0))
                semantic_source = "legacy_connected_conservative"
        queued = max(0, int(activity.get("queued_prompts") or 0))
        reservations = max(0, int(activity.get("dispatch_reservations") or 0))

    return {
        "active": active,
        "queued": queued,
        "reservations": reservations,
        "consumed": active + queued + reservations,
        "semantic_source": semantic_source,
    }
