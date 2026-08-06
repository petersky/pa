"""Bounded, identity-free projections for assigned Goal services."""

from __future__ import annotations

from typing import Any

from pa.goals.governance import GoalAssignedServiceAuthorization


def assigned_goal_projection(
    authorization: GoalAssignedServiceAuthorization,
    *,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """Expose only the assigned package and its criterion-scoped evidence."""

    if not 0 <= offset <= 100_000 or not 1 <= limit <= 100:
        raise ValueError("invalid assigned Goal page")

    def text(value: Any, maximum: int = 8_000) -> str:
        return str(value or "")[:maximum]

    def text_list(values: list[str], maximum: int = 50) -> list[str]:
        return [text(value, 2_000) for value in values[:maximum]]

    goal = authorization.goal
    package = authorization.work_package
    criterion_ids = set(package.criterion_ids)
    all_criteria = [item for item in goal.criteria if item.id in criterion_ids]
    all_evidence = [
        item
        for item in goal.evidence
        if set(item.criterion_ids) <= criterion_ids
    ]
    visible_evidence_ids = {item.id for item in all_evidence}
    criteria = [
        {
            **item.model_dump(
                mode="json",
                exclude={"evidence_ids", "required_evidence_kinds"},
            ),
            "description": text(item.description),
            "verification_method": text(item.verification_method),
            "evidence_requirement": text(item.evidence_requirement),
            "explanation": text(item.explanation),
            "evidence_ids": [
                evidence_id
                for evidence_id in item.evidence_ids
                if evidence_id in visible_evidence_ids
            ][:100],
            "evidence_id_total": sum(
                evidence_id in visible_evidence_ids
                for evidence_id in item.evidence_ids
            ),
            "required_evidence_kinds": [
                kind.value for kind in item.required_evidence_kinds[:50]
            ],
            "required_evidence_kind_total": len(item.required_evidence_kinds),
        }
        for item in all_criteria[offset : offset + limit]
    ]
    evidence = [
        {
            **item.model_dump(
                mode="json",
                exclude={
                    "criterion_ids",
                    "provenance",
                    "recorded_by_principal",
                    "recorded_by_instance_id",
                    "producer_role",
                    "producer_service_id",
                },
            ),
            "criterion_ids": item.criterion_ids[:100],
            "criterion_id_total": len(item.criterion_ids),
            "uri": text(item.uri, 2_000),
            "summary": text(item.summary),
            "content_hash": text(item.content_hash, 256),
            "sensitivity": text(item.sensitivity, 100),
        }
        for item in all_evidence[offset : offset + limit]
    ]
    page_end = offset + limit
    next_offset = (
        page_end
        if page_end < max(len(all_criteria), len(all_evidence))
        else None
    )
    return {
        "objective": text(goal.objective, 16_000),
        "motivation": text(goal.motivation),
        "constraints": text_list(goal.constraints),
        "non_goals": text_list(goal.non_goals),
        "assumptions": text_list(goal.assumptions),
        "risks": text_list(goal.risks),
        "context_totals": {
            "constraints": len(goal.constraints),
            "non_goals": len(goal.non_goals),
            "assumptions": len(goal.assumptions),
            "risks": len(goal.risks),
        },
        "state": goal.state.value,
        "version": goal.version,
        "policy_revision": goal.policy.revision,
        "progress_summary": text(goal.progress_summary),
        "work_package": {
            "title": text(package.title, 2_000),
            "objective": text(package.objective, 16_000),
            "state": package.state.value,
            "criterion_count": len(package.criterion_ids),
            "attempts": package.attempts,
            "max_attempts": package.max_attempts,
            "result_summary": text(package.result_summary),
        },
        "criteria": criteria,
        "evidence": evidence,
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": next_offset,
            "criteria_total": len(all_criteria),
            "evidence_total": len(all_evidence),
        },
    }


def assigned_goal_mutation_response(
    authorization: GoalAssignedServiceAuthorization,
    *,
    operation: str,
) -> dict[str, Any]:
    """Acknowledge one mutation without exposing the persisted Goal envelope."""

    return {
        "accepted": True,
        "operation": operation,
        "goal": assigned_goal_projection(authorization),
    }
