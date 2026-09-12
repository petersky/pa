"""Completion predicates shared by canonical writes, replay and lease consumers."""

from __future__ import annotations

from typing import Any
from datetime import datetime


COMPLETION_CAPABILITY = "completion-requirements:v1"


def completion_runtime_capabilities(configured) -> list[str]:
    # Advertise implemented support without persisting it into user configuration
    # (which could survive a downgrade to an incompatible binary).
    return sorted(set(configured) | {COMPLETION_CAPABILITY})


def completion_capabilities(requirement: Any) -> set[str]:
    """Version-specific ownership eligibility, only for explicitly protected cards."""
    if not requirement:
        return set()
    version = _dict(requirement).get("schema_version", 1)
    return {f"completion-requirements:v{version}"}


class CompletionConflict(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def completion_state(requirement: Any, evidence: Any = ()) -> dict:
    requirement = _dict(requirement)
    if not requirement:
        return {"reason_code": "legacy_completion", "missing": [], "satisfied": [], "accepted": True}
    if requirement.get("schema_version") != 1:
        return {"reason_code": "unsupported_completion_requirement", "missing": ["compatible_owner"], "satisfied": [], "accepted": False}
    revision = requirement.get("revision")
    receipts = [_dict(item) for item in evidence or ()]
    # Editing acceptance criteria invalidates acceptance, not the immutable
    # fact that an exact source subject was integrated.
    current = [item for item in receipts if (item.get("requirement_revision") == revision or item.get("outcome") == "integrated") and item.get("actor") and item.get("recorded_at")]
    integrated = [item for item in current if item.get("outcome") == "integrated"]
    # A receipt accepting build A never certifies a later integrated build B.
    subjects = integrated or [item for item in current if item.get("outcome") == "accepted"]
    if subjects:
        subject = subjects[-1].get("subject_revision")
        current = [item for item in current if item.get("subject_revision") == subject or item.get("outcome") == "human_override"]
    human = any(item.get("outcome") == "human_override" for item in current)
    accepted = any(item.get("outcome") == "accepted" for item in current)
    # Acceptance cannot manufacture the supervisor's integration outcome, even
    # when an older receipt explicitly claimed that milestone.
    satisfied = sorted({stage for item in current for stage in item.get("milestones", [])
                        if stage != "integrated" or item.get("outcome") == "integrated"})
    required = list(requirement.get("milestones", []))
    if requirement.get("mode") == "integration_only" and "integrated" not in required:
        required.insert(0, "integrated")
    missing = [stage for stage in required if stage not in satisfied]
    if requirement.get("mode") == "explicit_acceptance" and not accepted:
        missing.append("acceptance")
    if human:
        missing = []
    owner_missing = bool(missing and not requirement.get("acceptance_principals") and (requirement.get("mode") == "explicit_acceptance" or set(missing) - {"integrated"}))
    return {"reason_code": "completion_owner_unconfigured" if owner_missing else "requirements_satisfied" if not missing else "acceptance_pending", "missing": missing, "satisfied": satisfied, "accepted": not missing}


def _dict(value: Any) -> dict:
    return value.model_dump(mode="json") if hasattr(value, "model_dump") else dict(value or {})


def _same_version(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    try:
        return datetime.fromisoformat(left) == datetime.fromisoformat(right)
    except (TypeError, ValueError):
        return False


def protected_event_payload(current: dict, payload: dict, *, expected_version: str | None = None, field_intent: list[str] | None = None) -> dict:
    """Project incompatible completion claims safely without changing history.

    Old full snapshots cannot erase a known declaration. New completion events
    carry the exact declaration and evidence; lane-only claims remain pending.
    """
    result = dict(payload)
    requirement = current.get("completion_requirement")
    if not requirement:
        return result
    if "lane" not in result and "status" in result:
        from pa.domain.models import lane_from_legacy_status
        result["lane"] = lane_from_legacy_status(result["status"]).value
    if not result.get("completion_requirement"):
        result["completion_requirement"] = requirement
    elif result["completion_requirement"] != requirement and (
        not _same_version(expected_version, current.get("updated_at"))
        or "completion_requirement" not in (field_intent or [])
    ):
        result["completion_requirement"] = requirement
    evidence = list(current.get("completion_evidence") or [])
    for receipt in result.get("completion_evidence") or []:
        if receipt not in evidence:
            evidence.append(receipt)
    result["completion_evidence"] = evidence
    effective = result["completion_requirement"]
    if result.get("lane") == "done" and current.get("lane") != "done":
        if not completion_state(effective, evidence)["accepted"]:
            result["lane"] = "waiting"
    return result


def completion_history_effect(state: dict, event: Any) -> tuple[dict, str]:
    """Annotate an incompatible claim while preserving the immutable event."""
    from pa.domain.models import EventType
    if event.type == EventType.CARD_DELETED:
        return {}, "applied"
    projected = protected_event_payload(state, event.payload, expected_version=event.causal_card_version, field_intent=event.field_intent)
    effect = "applied"
    if state.get("completion_requirement") and any(
        field in event.payload and projected.get(field) != event.payload[field]
        for field in ("lane", "completion_requirement")
    ):
        effect = "completion_claim_preserved_pending"
    if event.type in {EventType.CARD_CREATED, EventType.CARD_UPSERTED}:
        return projected, effect
    return {**state, **projected}, effect
