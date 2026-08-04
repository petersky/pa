"""Explainable, durable fleet placement policy resolution."""

from __future__ import annotations

import bisect
import json
import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import Any

from pydantic import BaseModel, Field, model_validator

from pa.core.io import atomic_write_json
from pa.fleet.capacity import (
    DispatchCapacity,
    DispatchQueueCapacity,
    EffectiveCapacity,
    EffectiveQueueCapacity,
    effective_capacity,
    effective_queue_capacity,
    normalize_capacity_consumer_links,
    workload_counts,
)
from pa.fleet.policy import (
    DispatchIntent,
    InstanceParticipationPolicy,
    ParticipationMode,
    compatibility_policy,
)
from pa.workloads import (
    LEGACY_CODE_PROFILE_REASON,
    WorkloadProfile,
    normalize_workload_profile,
)


class PlacementPolicy(StrEnum):
    BEST_MATCH = "best_match"
    LEAST_BUSY = "least_busy"
    ROUND_ROBIN = "round_robin"
    RANDOM_ELIGIBLE = "random_eligible"


class PlacementCandidate(BaseModel):
    instance_id: str
    name: str
    zone: str = "default"
    lifecycle_state: str = "active"
    local: bool = False
    capabilities: list[str] = Field(default_factory=list)
    dispatch_capacity: int | None = None
    dispatch_provider_capacities: dict[str, DispatchCapacity] = Field(
        default_factory=dict
    )
    dispatch_queue_capacity: int | None = None
    dispatch_provider_queue_capacities: dict[str, DispatchQueueCapacity] = Field(
        default_factory=dict
    )
    reachability: dict[str, Any] = Field(default_factory=dict)
    activity: dict[str, Any] = Field(default_factory=dict)
    providers: dict[str, Any] = Field(default_factory=dict)
    mcp_bootstrap: dict[str, Any] = Field(default_factory=dict)
    repositories: dict[str, Any] = Field(default_factory=dict)
    authorized: bool = True
    authorization_reason: str | None = None
    participation_policy: InstanceParticipationPolicy | None = None
    participation_policy_explicit: bool = False
    participation_policy_supported: bool = True
    group_membership: str = "included"
    group_id: str | None = None
    self_protection: dict[str, Any] = Field(default_factory=dict)


class PlacementRequest(BaseModel):
    realm_id: str
    fleet_id: str
    policy: PlacementPolicy | None = None
    instance_id: str | None = None
    card_id: str | None = None
    provider: str | None = None
    model_id: str | None = None
    required_capabilities: list[str] = Field(default_factory=list)
    preferred_capabilities: list[str] = Field(default_factory=list)
    repository_ids: list[str] = Field(default_factory=list)
    workload_profile: WorkloadProfile = WorkloadProfile.RESEARCH
    profile_normalization_reason: str | None = None
    project_id: str | None = None
    dispatch_intent: DispatchIntent = DispatchIntent.AUTOMATIC
    requested_group_id: str | None = None
    resolved_group_id: str | None = None
    resolved_group_name: str | None = None
    group_version: int | None = None
    default_source: str | None = None
    permitted_placement_policies: list[str] = Field(default_factory=list)
    principal_id: str | None = None
    participation_override_reason: str | None = None
    policy_enforcement_active: bool = False
    workspace_eligible: bool = True
    workspace_reason: str | None = None
    allow_concurrent: bool = False
    resume_session_id: str | None = None
    capacity_override: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_profile(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        payload = dict(value)
        resolution = normalize_workload_profile(
            payload.get("workload_profile", WorkloadProfile.RESEARCH),
            allow_automatic=False,
        )
        payload["workload_profile"] = resolution.profile
        supplied_reason = payload.get("profile_normalization_reason")
        payload["profile_normalization_reason"] = resolution.migration_reason or (
            LEGACY_CODE_PROFILE_REASON
            if supplied_reason == LEGACY_CODE_PROFILE_REASON
            else None
        )
        return payload


class PlacementDecision(BaseModel):
    policy: str
    chosen_instance_id: str
    chosen_instance_name: str
    resolved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    eligible_candidates: list[dict[str, Any]] = Field(default_factory=list)
    rejected_candidates: list[dict[str, Any]] = Field(default_factory=list)
    scores: dict[str, dict[str, float]] = Field(default_factory=dict)
    tie_breaking_reason: str
    freshness: dict[str, str | None] = Field(default_factory=dict)
    requested_group_id: str | None = None
    resolved_group_id: str | None = None
    resolved_group_name: str | None = None
    group_version: int | None = None
    default_source: str | None = None
    workload_profile: WorkloadProfile = WorkloadProfile.RESEARCH
    profile_normalization_reason: str | None = None
    dispatch_intent: str = DispatchIntent.AUTOMATIC.value
    policy_versions: dict[str, int | None] = Field(default_factory=dict)
    principal_id: str | None = None
    participation_override_reason: str | None = None


class PlacementError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        rejected_candidates: list[dict[str, Any]] | None = None,
        recoverable: bool = True,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.rejected_candidates = rejected_candidates or []
        self.recoverable = recoverable
        self.detail = detail or {}


class RoundRobinCursorStore:
    """Durable next-member cursor keyed by fleet and realm."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "placement_cursors.json"
        self._lock = RLock()

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            payload = json.loads(self.path.read_text())
        except OSError, json.JSONDecodeError:
            return {}
        cursors = payload.get("cursors")
        return cursors if isinstance(cursors, dict) else {}

    def choose(
        self,
        fleet_id: str,
        realm_id: str,
        eligible_ids: list[str],
        *,
        scope: str = "",
    ) -> str:
        ordered = sorted(dict.fromkeys(eligible_ids))
        if not ordered:
            raise ValueError("round-robin requires at least one eligible instance")
        key = f"{fleet_id}:{realm_id}:{scope}"
        with self._lock:
            cursors = self._load()
            previous = str((cursors.get(key) or {}).get("last_instance_id") or "")
            index = bisect.bisect(ordered, previous) if previous else 0
            if index >= len(ordered):
                index = 0
            chosen = ordered[index]
            sequence = int((cursors.get(key) or {}).get("sequence") or 0) + 1
            cursors[key] = {
                "last_instance_id": chosen,
                "sequence": sequence,
                "updated_at": datetime.now(UTC).isoformat(),
            }
            atomic_write_json(self.path, {"version": 1, "cursors": cursors}, mode=0o600)
            return chosen


def _envelope(candidate: PlacementCandidate, name: str) -> dict[str, Any]:
    value = getattr(candidate, name)
    return value if isinstance(value, dict) else {}


def _provider_statuses(candidate: PlacementCandidate) -> list[dict[str, Any]]:
    value = _envelope(candidate, "providers").get("value")
    return (
        [item for item in value if isinstance(item, dict)]
        if isinstance(value, list)
        else []
    )


def _provider_ready(
    candidate: PlacementCandidate, provider: str | None, model_id: str | None
) -> tuple[bool, str, float]:
    statuses = _provider_statuses(candidate)
    requested = provider.strip().lower() if provider else None
    matches = [
        item
        for item in statuses
        if not requested or str(item.get("id") or "").lower() == requested
    ]
    if not matches:
        return (
            False,
            (
                f"provider {provider!r} is not available"
                if provider
                else "no provider readiness data is available"
            ),
            0.0,
        )
    for item in matches:
        if not bool(item.get("available")):
            continue
        if str(item.get("auth_state") or "unknown") != "authenticated":
            continue
        models = item.get("models") or (item.get("meta") or {}).get("models")
        if model_id and isinstance(models, list) and model_id not in models:
            continue
        active = max(0, int(item.get("active_session_count") or 0))
        return (
            True,
            "provider is available and authenticated",
            min(1.0, 0.8 + active * 0.05),
        )
    return (
        False,
        (
            f"provider {provider!r} is unavailable, unauthenticated, or lacks model {model_id!r}"
            if provider
            else "no available provider has authenticated readiness"
        ),
        0.0,
    )


def _selected_provider_id(
    candidate: PlacementCandidate, provider: str | None, model_id: str | None
) -> str | None:
    requested = provider.strip().lower() if provider else None
    for item in _provider_statuses(candidate):
        provider_id = str(item.get("id") or "").strip().lower()
        if not provider_id or (requested and provider_id != requested):
            continue
        models = item.get("models") or (item.get("meta") or {}).get("models")
        if (
            bool(item.get("available"))
            and str(item.get("auth_state") or "unknown") == "authenticated"
            and not (model_id and isinstance(models, list) and model_id not in models)
        ):
            return provider_id
    return None


def _capacity(
    candidate: PlacementCandidate, provider: str | None = None
) -> EffectiveCapacity:
    activity = _envelope(candidate, "activity").get("value") or {}
    advertised = activity.get("capacity") or {}
    configured = candidate.dispatch_capacity
    if configured is None:
        configured = advertised.get("configured")
    provider_capacities = candidate.dispatch_provider_capacities or advertised.get(
        "provider_limits"
    )
    return effective_capacity(
        configured=configured,
        provider_capacities=provider_capacities,
        capabilities=candidate.capabilities,
        provider=provider,
    )


def _workload(
    candidate: PlacementCandidate, provider: str | None = None
) -> tuple[dict[str, Any], EffectiveCapacity, float]:
    activity = _envelope(candidate, "activity").get("value") or {}
    capacity = _capacity(candidate, provider)
    counts = workload_counts(
        activity,
        provider=provider if capacity.source == "configured_provider" else None,
    )
    return counts, capacity, counts["consumed"] / capacity.limit


def _queue_capacity(
    candidate: PlacementCandidate, provider: str | None = None
) -> EffectiveQueueCapacity:
    activity = _envelope(candidate, "activity").get("value") or {}
    advertised = activity.get("queue_capacity") or {}
    return effective_queue_capacity(
        configured=(
            candidate.dispatch_queue_capacity
            if candidate.dispatch_queue_capacity is not None
            else advertised.get("configured")
        ),
        provider_capacities=(
            candidate.dispatch_provider_queue_capacities
            or advertised.get("provider_limits")
        ),
        provider=provider,
    )


def _repository_locality(
    candidate: PlacementCandidate, repository_ids: list[str]
) -> tuple[float, list[str]]:
    if not repository_ids:
        return 1.0, []
    payload = _envelope(candidate, "repositories").get("value") or {}
    available: set[str] = set()
    for observation in payload.get("observations") or []:
        if not isinstance(observation, dict):
            continue
        snapshot = observation.get("snapshot") or {}
        if observation.get("state") == "fresh" and snapshot.get("repository_id"):
            available.add(str(snapshot["repository_id"]))
    for workspace in payload.get("workspaces") or []:
        if (
            isinstance(workspace, dict)
            and workspace.get("repository_id")
            and workspace.get("state") not in {"failed", "cleaned"}
        ):
            available.add(str(workspace["repository_id"]))
    matches = [
        repository_id for repository_id in repository_ids if repository_id in available
    ]
    return len(matches) / len(repository_ids), matches


def _freshness(candidate: PlacementCandidate) -> dict[str, str | None]:
    return {
        name: _envelope(candidate, name).get("observed_at")
        for name in ("reachability", "activity", "providers", "repositories")
    }


def _evaluate(
    request: PlacementRequest, candidate: PlacementCandidate
) -> tuple[list[str], dict[str, float], dict[str, Any]]:
    reasons: list[str] = []
    rejection_codes: list[str] = []
    bootstrap_classification: str | None = None

    def reject(code: str, message: str) -> None:
        if code not in rejection_codes:
            rejection_codes.append(code)
        if message not in reasons:
            reasons.append(message)

    if candidate.lifecycle_state != "active":
        reject(
            "instance_not_active",
            f"canonical fleet lifecycle is {candidate.lifecycle_state}",
        )
    if candidate.group_membership == "explicitly_excluded_from_group":
        reject(
            "explicitly_excluded_from_group",
            "instance is explicitly excluded from the resolved worker group",
        )
    elif candidate.group_membership != "included":
        reject(
            "not_in_requested_group",
            "instance is not a member of the resolved worker group",
        )

    if not candidate.authorized:
        reject(
            "insufficient_authorization",
            candidate.authorization_reason or "principal is not authorized",
        )

    policy = candidate.participation_policy or compatibility_policy(
        candidate.instance_id, request.realm_id
    )
    privileged_override = (
        request.dispatch_intent == DispatchIntent.PRIVILEGED_OVERRIDE
    )
    if (
        request.dispatch_intent == DispatchIntent.AUTOMATIC
        and not candidate.local
        and (
            not candidate.participation_policy_supported
            or (
                request.policy_enforcement_active
                and not candidate.participation_policy_explicit
            )
        )
    ):
        reject(
            "policy_unknown_on_mixed_version_peer",
            "automatic placement cannot prove a synchronized participation policy "
            "for this mixed-version peer",
        )
    if (
        policy.participation_mode == ParticipationMode.DISABLED
        and not privileged_override
    ):
        reject(
            "participation_disabled",
            policy.reason or "instance policy disables all dispatched work",
        )
    elif request.dispatch_intent == DispatchIntent.AUTOMATIC and not bool(
        policy.automatic_dispatch
    ):
        reject(
            "automatic_participation_disabled",
            policy.reason or "instance policy permits named/manual dispatch only",
        )
    elif request.dispatch_intent == DispatchIntent.MANUAL and not bool(
        policy.manual_dispatch
    ):
        reject(
            "manual_participation_disabled",
            policy.reason or "instance policy does not permit named/manual dispatch",
        )

    workload_profile = request.workload_profile
    hard_denied = set(policy.hard_denied_profiles) | set(
        candidate.self_protection.get("denied_profiles") or []
    )
    if workload_profile in hard_denied:
        reject(
            "self_protective_workload_denied",
            f"instance self-protection denies {workload_profile} work",
        )
    if workload_profile in policy.denied_profiles and not privileged_override:
        reject(
            "workload_profile_denied",
            policy.reason or f"instance policy denies {workload_profile} work",
        )
    elif (
        not privileged_override
        and policy.allowed_profiles
        and workload_profile not in policy.allowed_profiles
    ):
        reject(
            "workload_profile_not_allowed",
            f"instance policy does not allow {workload_profile} work",
        )

    if request.project_id and not privileged_override:
        if request.project_id in policy.denied_project_ids:
            reject(
                "project_not_allowed",
                "instance policy explicitly denies the requested project",
            )
        elif (
            policy.allowed_project_ids
            and request.project_id not in policy.allowed_project_ids
        ):
            reject(
                "project_not_allowed",
                "requested project is outside the instance policy allow-list",
            )
    for repository_id in request.repository_ids if not privileged_override else []:
        if repository_id in policy.denied_repository_ids:
            reject(
                "repository_not_allowed",
                f"instance policy explicitly denies repository {repository_id}",
            )
        elif (
            policy.allowed_repository_ids
            and repository_id not in policy.allowed_repository_ids
        ):
            reject(
                "repository_not_allowed",
                f"repository {repository_id} is outside the instance policy allow-list",
            )
    normalized_provider = (request.provider or "").strip().lower()
    if normalized_provider:
        if normalized_provider in {
            value.casefold() for value in policy.denied_provider_ids
        }:
            reject(
                "provider_denied",
                f"instance policy denies provider {request.provider!r}",
            )
        elif policy.allowed_provider_ids and normalized_provider not in {
            value.casefold() for value in policy.allowed_provider_ids
        }:
            reject(
                "provider_not_allowed",
                f"provider {request.provider!r} is outside the instance policy allow-list",
            )
    normalized_model = (request.model_id or "").strip().casefold()
    if normalized_model:
        if any(
            normalized_model.startswith(value.casefold())
            for value in policy.denied_model_families
        ):
            reject(
                "model_family_denied",
                f"instance policy denies model family {request.model_id!r}",
            )
        elif policy.allowed_model_families and not any(
            normalized_model.startswith(value.casefold())
            for value in policy.allowed_model_families
        ):
            reject(
                "model_family_not_allowed",
                f"model {request.model_id!r} is outside the instance policy allow-list",
            )
    if policy.maintenance:
        reject("maintenance", "instance participation policy is in maintenance")
    if policy.quiescing:
        reject("quiescing", "instance participation policy is quiescing")
    if not request.workspace_eligible:
        reject(
            "workspace_unavailable",
            request.workspace_reason or "requested workspace cannot be materialized",
        )

    dimensions = ["reachability", "activity", "providers"]
    if workload_profile == "repository" or request.repository_ids:
        dimensions.append("repositories")
    for dimension in dimensions:
        state = str(_envelope(candidate, dimension).get("state") or "unavailable")
        if state != "fresh":
            reject(
                f"{dimension}_stale",
                f"{dimension} data is {state}; fresh data is required",
            )

    reachability = _envelope(candidate, "reachability").get("value") or {}
    if reachability.get("health") != "up":
        reject("instance_unreachable", "instance is not online and healthy")

    available_capabilities = set(candidate.capabilities)
    missing = sorted(
        set(request.required_capabilities) - available_capabilities
    )
    if missing:
        reject(
            "capability_unavailable",
            f"missing required capabilities: {', '.join(missing)}",
        )
    preferred = set(request.preferred_capabilities)
    matched_preferred = sorted(preferred & available_capabilities)
    missing_preferred = sorted(preferred - available_capabilities)
    capability_match = (
        len(matched_preferred) / len(preferred) if preferred else 1.0
    )

    activity = _envelope(candidate, "activity").get("value") or {}
    lifecycle = str(activity.get("state") or "unknown")
    if lifecycle in {
        "quiescing",
        "updating",
        "starting",
        "shutting_down",
        "unavailable",
    }:
        reject("instance_unavailable", f"instance lifecycle is {lifecycle}")
    if bool(activity.get("quiescing")):
        reject("quiescing", "instance is quiescing")

    bootstrap_envelope = _envelope(candidate, "mcp_bootstrap")
    if bootstrap_envelope:
        bootstrap = bootstrap_envelope.get("value") or {}
        if (
            bootstrap_envelope.get("state") != "fresh"
            or bootstrap.get("state") != "connected"
        ):
            raw_classification = (
                bootstrap.get("classification")
                or (bootstrap_envelope.get("failure") or {}).get("code")
                or bootstrap_envelope.get("last_attempt_state")
                or bootstrap_envelope.get("state")
                or "unhealthy"
            )
            aliases = {
                "deadline_exceeded": "timeout",
                "error": "transient_probe_failure",
                "probe_failed": "transient_probe_failure",
                "unavailable": "unhealthy",
                "unreachable": "owner_unreachable",
            }
            bootstrap_classification = aliases.get(
                str(raw_classification), str(raw_classification)
            )
            recovery = {
                "timeout": (
                    "retry the bounded refresh; if it repeats, run pa doctor --verbose"
                ),
                "dependency_incompatible": (
                    "repair the PA/MCP dependency versions and restart PA"
                ),
                "owner_unreachable": (
                    "restore the private owner endpoint or restart its listener"
                ),
                "transient_probe_failure": (
                    "retry the refresh after checking target transport"
                ),
                "unhealthy": (
                    "run pa doctor --verbose and repair the reported health failure"
                ),
            }.get(bootstrap_classification, "run pa doctor --verbose on the target")
            reject(
                "mcp_bootstrap_unavailable",
                "PA stdio MCP bootstrap is unavailable "
                f"({bootstrap_classification}); {recovery}",
            )

    provider_ready, provider_reason, provider_score = _provider_ready(
        candidate, request.provider, request.model_id
    )
    if not provider_ready:
        reject("provider_unavailable", provider_reason)

    counts, capacity, normalized = _workload(candidate, request.provider)
    queue_capacity = _queue_capacity(candidate, request.provider)
    activity_value = _envelope(candidate, "activity").get("value") or {}
    global_counts = workload_counts(activity_value)
    provider_counts = (
        workload_counts(activity_value, provider=request.provider)
        if request.provider
        else global_counts
    )
    queue_advertised = candidate.dispatch_queue_capacity is not None or bool(
        activity_value.get("queue_capacity")
    )
    provider_key = (request.provider or "").strip().lower()
    provider_activity = (activity_value.get("provider_concurrency") or {}).get(
        provider_key, {}
    )
    global_waiting_count = max(0, int(activity_value.get("dispatch_waiting") or 0))
    provider_waiting_count = max(0, int(provider_activity.get("dispatch_waiting") or 0))
    global_queue_full = global_waiting_count >= queue_capacity.global_limit
    provider_queue_full = (
        queue_capacity.provider_limit is not None
        and provider_waiting_count >= queue_capacity.provider_limit
    )
    if provider_queue_full:
        waiting_count = provider_waiting_count
        waiting_limit = queue_capacity.provider_limit
        queue_constraint_source = "provider"
    else:
        waiting_count = global_waiting_count
        waiting_limit = queue_capacity.global_limit
        queue_constraint_source = "global"
    profile_active_limit = policy.max_concurrent_by_profile.get(workload_profile)
    if profile_active_limit is not None and counts["active"] >= profile_active_limit:
        reject(
            "profile_capacity_exhausted",
            f"{workload_profile} active workload limit "
            f"({profile_active_limit}) is exhausted",
        )
    profile_queue_limit = policy.max_queued_by_profile.get(workload_profile)
    if profile_queue_limit is not None and counts["queued"] >= profile_queue_limit:
        reject(
            "profile_queue_exhausted",
            f"{workload_profile} queued workload limit "
            f"({profile_queue_limit}) is exhausted",
        )
    hard_limit = policy.hard_max_concurrent_by_profile.get(workload_profile)
    advertised_hard_limit = (
        candidate.self_protection.get("max_concurrent_by_profile") or {}
    ).get(workload_profile)
    hard_limits = [
        int(value) for value in (hard_limit, advertised_hard_limit) if value is not None
    ]
    if hard_limits and counts["active"] >= min(hard_limits):
        reject(
            "self_protective_capacity_exhausted",
            f"instance self-protective {workload_profile} limit "
            f"({min(hard_limits)}) is exhausted",
        )
    execution_available = request.capacity_override or (
        global_counts["consumed"] < capacity.global_limit
        and (
            capacity.provider_limit is None
            or provider_counts["consumed"] < capacity.provider_limit
        )
    )
    queue_available = not global_queue_full and not provider_queue_full
    if (
        not execution_available
        and not queue_advertised
        and not request.capacity_override
    ):
        reject(
            "capacity_exhausted",
            "capacity is exhausted and this mixed-version peer does not advertise durable queue admission",
        )
    elif (
        not execution_available
        and not queue_available
        and not request.capacity_override
    ):
        reject(
            "dispatch_queue_full",
            "execution slots are occupied and the durable dispatch queue is full "
            f"({waiting_count} of {waiting_limit})",
        )

    locality, cached_repositories = _repository_locality(
        candidate, request.repository_ids
    )
    payload = _envelope(candidate, "repositories").get("value") or {}
    for workspace in payload.get("workspaces") or []:
        if not isinstance(workspace, dict):
            continue
        if (
            request.card_id
            and not request.allow_concurrent
            and workspace.get("card_id") == request.card_id
            and workspace.get("state") in {"provisioning", "ready"}
            and (
                not request.resume_session_id
                or workspace.get("session_id") != request.resume_session_id
            )
        ):
            reject(
                "workspace_unavailable",
                "card already has a live worktree lease on this instance; resume "
                "it or allow a concurrent dispatch explicitly",
            )
            break

    scores = {
        "provider_readiness": provider_score,
        "capability_match": capability_match,
        "repository_locality": locality,
        "authority_locality": 1.0 if candidate.local else 0.0,
        "available_capacity": max(0.0, 1.0 - normalized),
        "normalized_workload": normalized,
    }
    consumer_links = normalize_capacity_consumer_links(activity_value)
    detail = {
        "instance_id": candidate.instance_id,
        "name": candidate.name,
        "provider_id": _selected_provider_id(
            candidate, request.provider, request.model_id
        ),
        "mcp_bootstrap_classification": bootstrap_classification,
        "preferred_capabilities": sorted(preferred),
        "matched_preferred_capabilities": matched_preferred,
        "missing_preferred_capabilities": missing_preferred,
        "active": counts["active"],
        "queued": counts["queued"],
        "reserved": counts["reservations"],
        "consumed": counts["consumed"],
        "workload_semantics": counts["semantic_source"],
        "capacity": capacity.limit,
        "capacity_detail": capacity.model_dump(mode="json"),
        "execution_slot_available": execution_available,
        "admission_disposition": ("launchable" if execution_available else "queued"),
        "queue_count": waiting_count,
        "queue_capacity": waiting_limit,
        "queue_constraint_source": queue_constraint_source,
        "queue_capacity_detail": queue_capacity.model_dump(mode="json"),
        "global_workload": global_counts,
        "provider_workload": provider_counts,
        "global_queue_count": global_waiting_count,
        "provider_queue_count": provider_waiting_count,
        "consumer_links": consumer_links,
        "consumer_link_count": len(consumer_links),
        "consumer_links_omitted": max(
            0, global_counts["consumed"] - len(consumer_links)
        ),
        "cached_repository_ids": cached_repositories,
        "freshness": _freshness(candidate),
        "group_id": candidate.group_id,
        "group_membership": candidate.group_membership,
        "workload_profile": workload_profile,
        "dispatch_intent": request.dispatch_intent.value,
        "policy_version": (
            policy.version if candidate.participation_policy_explicit else None
        ),
        "policy_source": policy.source,
        "policy_summary": policy.summary(),
        "policy_reason": policy.reason,
        "rejection_codes": rejection_codes,
    }
    return reasons, scores, detail


class PlacementService:
    def __init__(
        self,
        cursor_store: RoundRobinCursorStore,
        *,
        randbelow: Callable[[int], int] = secrets.randbelow,
    ) -> None:
        self.cursor_store = cursor_store
        self.randbelow = randbelow

    def resolve(
        self, request: PlacementRequest, candidates: list[PlacementCandidate]
    ) -> PlacementDecision:
        if bool(request.instance_id) == bool(request.policy):
            raise PlacementError(
                "invalid_placement_target",
                "Specify exactly one concrete instance_id or placement policy.",
                recoverable=False,
            )
        if (
            request.policy
            and request.permitted_placement_policies
            and request.policy.value not in request.permitted_placement_policies
        ):
            raise PlacementError(
                "placement_policy_not_allowed_for_group",
                f"Placement policy {request.policy.value!r} is not permitted by "
                f"group {request.resolved_group_id!r}.",
                recoverable=False,
            )

        rejected: list[dict[str, Any]] = []
        eligible: list[tuple[PlacementCandidate, dict[str, float], dict[str, Any]]] = []
        for candidate in sorted(candidates, key=lambda item: item.instance_id):
            reasons, scores, detail = _evaluate(request, candidate)
            if request.instance_id and candidate.instance_id != request.instance_id:
                continue
            if reasons:
                rejected.append({**detail, "eligible": False, "reasons": reasons})
            else:
                detail["eligible"] = True
                eligible.append((candidate, scores, detail))

        if request.instance_id and not any(
            item.instance_id == request.instance_id for item in candidates
        ):
            raise PlacementError(
                "instance_not_found",
                f"Fleet instance {request.instance_id!r} was not found.",
                recoverable=True,
            )
        if not eligible:
            rejection_codes = {
                code for item in rejected for code in item.get("rejection_codes") or []
            }
            if rejection_codes and rejection_codes <= {"provider_unavailable"}:
                code = "provider_unavailable"
                message = (
                    f"Provider {request.provider!r} is unavailable on every candidate."
                )
            elif rejection_codes and rejection_codes <= {"mcp_bootstrap_unavailable"}:
                code = "mcp_bootstrap_unavailable"
                message = "PA stdio MCP bootstrap is unavailable on every candidate."
            else:
                code = "no_eligible_instance"
                message = (
                    f"No eligible fleet instance{' ' + repr(request.instance_id) if request.instance_id else ''} "
                    "passed group, participation, workload, readiness, authorization, provider, repository, and capacity checks. "
                    "PA will not fall back to the authority/local instance."
                )
            raise PlacementError(
                code,
                message,
                rejected_candidates=rejected,
                recoverable=True,
            )

        policy = request.policy.value if request.policy else "named_instance"
        ordered = sorted(eligible, key=lambda item: item[0].instance_id)
        tie_reason = "The requested named instance passed all eligibility checks."
        if request.instance_id:
            chosen = ordered[0]
        elif request.policy == PlacementPolicy.LEAST_BUSY:
            chosen = min(
                ordered,
                key=lambda item: (
                    item[1]["normalized_workload"],
                    item[0].instance_id,
                ),
            )
            tie_reason = (
                "Lowest normalized execution-slot consumption (active capacity "
                "consumers plus durable dispatch reservations); queued prompts are "
                "backlog telemetry. Instance ID breaks exact ties."
            )
        elif request.policy == PlacementPolicy.ROUND_ROBIN:
            chosen_id = self.cursor_store.choose(
                request.fleet_id,
                request.realm_id,
                [item[0].instance_id for item in ordered],
                scope=(
                    f"{request.resolved_group_id or 'ungrouped'}:"
                    f"{request.workload_profile}"
                ),
            )
            chosen = next(item for item in ordered if item[0].instance_id == chosen_id)
            tie_reason = "Advanced the durable fleet/realm cursor over the current sorted eligible membership."
        elif request.policy == PlacementPolicy.RANDOM_ELIGIBLE:
            chosen = ordered[self.randbelow(len(ordered))]
            tie_reason = "Uniform random index over the sorted eligible set; the resolved choice is persisted."
        else:

            def best_score(item):
                scores = item[1]
                total = (
                    scores["provider_readiness"] * 30
                    + scores["capability_match"] * 20
                    + scores["repository_locality"] * 20
                    + scores["authority_locality"] * 10
                    + scores["available_capacity"] * 20
                )
                return (-total, item[0].instance_id)

            chosen = min(ordered, key=best_score)
            tie_reason = "Highest deterministic readiness, capability, repository/locality, and capacity score; instance ID breaks exact ties."

        score_map: dict[str, dict[str, float]] = {}
        eligible_public: list[dict[str, Any]] = []
        for candidate, scores, detail in ordered:
            total = (
                scores["provider_readiness"] * 30
                + scores["capability_match"] * 20
                + scores["repository_locality"] * 20
                + scores["authority_locality"] * 10
                + scores["available_capacity"] * 20
            )
            score_map[candidate.instance_id] = {
                **{key: round(value, 6) for key, value in scores.items()},
                "best_match_total": round(total, 6),
            }
            eligible_public.append(detail)

        selected, _, selected_detail = chosen
        return PlacementDecision(
            policy=policy,
            chosen_instance_id=selected.instance_id,
            chosen_instance_name=selected.name,
            eligible_candidates=eligible_public,
            rejected_candidates=rejected,
            scores=score_map,
            tie_breaking_reason=tie_reason,
            freshness=selected_detail["freshness"],
            requested_group_id=request.requested_group_id,
            resolved_group_id=request.resolved_group_id,
            resolved_group_name=request.resolved_group_name,
            group_version=request.group_version,
            default_source=request.default_source,
            workload_profile=request.workload_profile,
            profile_normalization_reason=request.profile_normalization_reason,
            dispatch_intent=request.dispatch_intent.value,
            policy_versions={
                candidate.instance_id: (
                    candidate.participation_policy.version
                    if candidate.participation_policy
                    and candidate.participation_policy_explicit
                    else None
                )
                for candidate, _scores, _detail in ordered
            }
            | {
                str(item["instance_id"]): item.get("policy_version")
                for item in rejected
            },
            principal_id=request.principal_id,
            participation_override_reason=request.participation_override_reason,
        )
