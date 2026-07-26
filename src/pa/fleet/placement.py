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

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json
from pa.fleet.capacity import (
    DispatchCapacity,
    EffectiveCapacity,
    effective_capacity,
    workload_counts,
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
    local: bool = False
    capabilities: list[str] = Field(default_factory=list)
    dispatch_capacity: int | None = None
    dispatch_provider_capacities: dict[str, DispatchCapacity] = Field(
        default_factory=dict
    )
    reachability: dict[str, Any] = Field(default_factory=dict)
    activity: dict[str, Any] = Field(default_factory=dict)
    providers: dict[str, Any] = Field(default_factory=dict)
    repositories: dict[str, Any] = Field(default_factory=dict)
    authorized: bool = True
    authorization_reason: str | None = None


class PlacementRequest(BaseModel):
    realm_id: str
    fleet_id: str
    policy: PlacementPolicy | None = None
    instance_id: str | None = None
    card_id: str | None = None
    provider: str | None = None
    model_id: str | None = None
    required_capabilities: list[str] = Field(default_factory=list)
    repository_ids: list[str] = Field(default_factory=list)
    allow_concurrent: bool = False
    capacity_override: bool = False


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


class PlacementError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        rejected_candidates: list[dict[str, Any]] | None = None,
        recoverable: bool = True,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.rejected_candidates = rejected_candidates or []
        self.recoverable = recoverable


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

    def choose(self, fleet_id: str, realm_id: str, eligible_ids: list[str]) -> str:
        ordered = sorted(dict.fromkeys(eligible_ids))
        if not ordered:
            raise ValueError("round-robin requires at least one eligible instance")
        key = f"{fleet_id}:{realm_id}"
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
    counts = workload_counts(activity, provider=provider)
    capacity = _capacity(candidate, provider)
    return counts, capacity, counts["consumed"] / capacity.limit


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
    if not candidate.authorized:
        reasons.append(candidate.authorization_reason or "principal is not authorized")

    for dimension in ("reachability", "activity", "providers", "repositories"):
        state = str(_envelope(candidate, dimension).get("state") or "unavailable")
        if state != "fresh":
            reasons.append(f"{dimension} data is {state}; fresh data is required")

    reachability = _envelope(candidate, "reachability").get("value") or {}
    if reachability.get("health") != "up":
        reasons.append("instance is not online and healthy")

    missing = sorted(set(request.required_capabilities) - set(candidate.capabilities))
    if missing:
        reasons.append(f"missing required capabilities: {', '.join(missing)}")

    activity = _envelope(candidate, "activity").get("value") or {}
    lifecycle = str(activity.get("state") or "unknown")
    if lifecycle in {
        "quiescing",
        "updating",
        "starting",
        "shutting_down",
        "unavailable",
    }:
        reasons.append(f"instance lifecycle is {lifecycle}")
    if bool(activity.get("quiescing")):
        reasons.append("instance is quiescing")

    provider_ready, provider_reason, provider_score = _provider_ready(
        candidate, request.provider, request.model_id
    )
    if not provider_ready:
        reasons.append(provider_reason)

    counts, capacity, normalized = _workload(candidate, request.provider)
    if counts["consumed"] >= capacity.limit and not request.capacity_override:
        reasons.append(
            "capacity is exhausted "
            f"({counts['active']} working + {counts['queued']} queued + "
            f"{counts['reservations']} reserved of {capacity.limit} "
            f"{capacity.source} slots)"
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
        ):
            reasons.append(
                "card already has a live worktree lease on this instance; resume it or allow a concurrent dispatch explicitly"
            )
            break

    scores = {
        "provider_readiness": provider_score,
        "capability_match": 1.0 if not missing else 0.0,
        "repository_locality": locality,
        "authority_locality": 1.0 if candidate.local else 0.0,
        "available_capacity": max(0.0, 1.0 - normalized),
        "normalized_workload": normalized,
    }
    detail = {
        "instance_id": candidate.instance_id,
        "name": candidate.name,
        "active": counts["active"],
        "queued": counts["queued"],
        "reserved": counts["reservations"],
        "consumed": counts["consumed"],
        "workload_semantics": counts["semantic_source"],
        "capacity": capacity.limit,
        "capacity_detail": capacity.model_dump(mode="json"),
        "consumer_links": (_envelope(candidate, "activity").get("value") or {}).get(
            "capacity_consumer_links", []
        ),
        "cached_repository_ids": cached_repositories,
        "freshness": _freshness(candidate),
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

        rejected: list[dict[str, Any]] = []
        eligible: list[tuple[PlacementCandidate, dict[str, float], dict[str, Any]]] = []
        for candidate in sorted(candidates, key=lambda item: item.instance_id):
            reasons, scores, detail = _evaluate(request, candidate)
            if request.instance_id and candidate.instance_id != request.instance_id:
                continue
            if reasons:
                rejected.append({**detail, "reasons": reasons})
            else:
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
            named = f" {request.instance_id!r}" if request.instance_id else ""
            raise PlacementError(
                "no_eligible_instance",
                f"No eligible fleet instance{named} passed fresh readiness, authorization, provider, repository, and capacity checks.",
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
            tie_reason = "Lowest normalized active-plus-queued workload; instance ID breaks exact ties."
        elif request.policy == PlacementPolicy.ROUND_ROBIN:
            chosen_id = self.cursor_store.choose(
                request.fleet_id,
                request.realm_id,
                [item[0].instance_id for item in ordered],
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
        )
