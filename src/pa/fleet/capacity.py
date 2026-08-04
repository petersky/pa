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

# Mixed-version peers can only backfill a durable reservation identity when its
# projected dispatch state canonically holds a pre-session execution slot.
LEGACY_RESERVATION_CONSUMER_STATES = frozenset(
    {
        "queued",
        "checking_sync",
        "materializing",
        "provisioning",
        "starting_session",
        "delivering_prompt",
    }
)


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
    """Normalize concurrency and backlog with conservative old-peer fallbacks.

    A prompting session occupies one execution slot. Prompts serialized behind
    that turn remain useful load/fairness telemetry, but cannot execute until
    the same session's current turn completes and therefore consume no
    additional concurrency. Pre-session dispatch reservations remain distinct
    consumers because they may materialize into independent sessions.
    """

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
        "consumed": active + reservations,
        "semantic_source": semantic_source,
    }


def deduplicate_consumer_links(links: Any) -> list[dict[str, Any]]:
    """Return one one-slot identity for every session or pre-session dispatch."""

    if not isinstance(links, list):
        return []
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in links:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        kind = str(item.get("kind") or "work").strip().lower()
        if kind == "session" and item.get("session_id"):
            identity = f"session:{item['session_id']}"
        elif kind == "dispatch" and item.get("dispatch_id"):
            identity = f"dispatch:{item['dispatch_id']}"
        else:
            identity = str(item.get("consumer_id") or item.get("href") or "")
        if not identity or identity in seen:
            continue
        seen.add(identity)
        item["consumer_id"] = identity
        # A session remains one concurrency consumer regardless of its prompt
        # backlog. Durable pre-session dispatches likewise reserve one slot.
        item["slots"] = 1
        result.append(item)
    return result


def normalize_capacity_consumer_links(
    activity: dict[str, Any],
    *,
    reservation_links: Any = None,
    limit: int = MAX_DISPATCH_CAPACITY,
) -> list[dict[str, Any]]:
    """Project bounded one-slot session and reservation identities."""

    counts = workload_counts(activity)
    existing = deduplicate_consumer_links(
        activity.get("capacity_consumer_links") or []
    )
    sessions = activity.get("sessions")
    current_session_links = [
        {
            "kind": "session",
            "session_id": item.get("session_id") or item.get("id"),
            "href": item.get("href")
            or f"/agent?session={item.get('session_id') or item.get('id')}",
            "state": item.get("status") or item.get("state"),
            "slots": 1,
        }
        for item in (sessions or [])
        if isinstance(item, dict)
        and (item.get("session_id") or item.get("id"))
        and (
            item.get("capacity_consuming") is True
            or str(item.get("status") or item.get("state") or "").lower()
            in {"working", "prompting"}
        )
    ]
    allow_stateless_legacy = not isinstance(sessions, list) and counts["active"] > 0
    legacy_session_links = [
        item
        for item in existing
        if item.get("kind") == "session"
        and item.get("capacity_consuming") is not False
        and str(item.get("status") or item.get("state") or "").lower()
        not in {"idle", "deferred", "non_consuming", "non-consuming"}
        and (
            item.get("capacity_consuming") is True
            or str(item.get("status") or item.get("state") or "").lower()
            in {"working", "prompting"}
            or (
                allow_stateless_legacy and not (item.get("status") or item.get("state"))
            )
        )
    ]
    normalized_sessions = deduplicate_consumer_links(
        current_session_links + legacy_session_links
    )[: min(counts["active"], limit)]
    authoritative_reservations = deduplicate_consumer_links(reservation_links or [])
    existing_reservations = [
        item
        for item in existing
        if item.get("kind") == "dispatch"
        and str(item.get("state") or item.get("status") or "").strip().lower()
        in LEGACY_RESERVATION_CONSUMER_STATES
    ]
    remaining = max(0, limit - len(normalized_sessions))
    normalized_reservations = deduplicate_consumer_links(
        authoritative_reservations + existing_reservations
    )[: min(counts["reservations"], remaining)]
    return normalized_sessions + normalized_reservations


def normalize_activity_capacity(
    activity: dict[str, Any],
    *,
    authority_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Overlay authoritative dispatch counts and reconcile consumer identities."""

    value = dict(activity or {})
    authority = authority_snapshot or {}
    if authority_snapshot is not None:
        value["dispatch_reservations"] = max(
            int(value.get("dispatch_reservations") or 0),
            int(authority.get("dispatch_reservations") or 0),
        )
        value["dispatch_waiting"] = max(
            int(value.get("dispatch_waiting") or 0),
            int(authority.get("dispatch_waiting") or 0),
        )
        if value.get("queue_capacity"):
            queue_capacity = dict(value["queue_capacity"])
            queue_capacity["consumed"] = max(
                int(queue_capacity.get("consumed") or 0),
                int(authority.get("dispatch_waiting") or 0),
            )
            value["queue_capacity"] = queue_capacity
        provider_concurrency = {
            key: dict(counts)
            for key, counts in (value.get("provider_concurrency") or {}).items()
        }
        for provider, counts in (authority.get("provider_concurrency") or {}).items():
            current = provider_concurrency.setdefault(provider, {})
            for key, count in counts.items():
                current[key] = max(int(current.get(key) or 0), int(count or 0))
        value["provider_concurrency"] = provider_concurrency

    counts = workload_counts(value)
    links = normalize_capacity_consumer_links(
        value,
        reservation_links=authority.get("reservation_links") or [],
    )
    value["capacity_consumer_links"] = links
    value["capacity_consumer_link_count"] = len(links)
    value["capacity_consumer_links_omitted"] = max(
        0, counts["consumed"] - len(links)
    )
    if value.get("capacity"):
        capacity = dict(value["capacity"])
        capacity["consumed"] = counts["consumed"]
        value["capacity"] = capacity
    return value
