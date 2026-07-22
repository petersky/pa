"""Versioned card-disposition contracts and server-side completion guards."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pa.domain.models import CardLane
from pa.pr_supervisor.models import PRWatch, PRWatchStatus, utcnow


CARD_DISPOSITION_V1 = "pa.card-disposition/v1"


class CardDispositionEvidenceV1(BaseModel):
    """Evidence supplied with an explicit card disposition."""

    model_config = ConfigDict(extra="forbid")

    integration_required: bool | None = None
    pr_watch_id: str | None = None
    watched_head_sha: str | None = None
    merge_commit_sha: str | None = None
    references: list[str] = Field(default_factory=list)


class CardDispositionV1(BaseModel):
    """Business disposition emitted separately from transport completion."""

    model_config = ConfigDict(extra="forbid")

    contract: Literal["pa.card-disposition/v1"]
    lane: Literal[CardLane.ACTIVE, CardLane.WAITING, CardLane.DONE]
    outcome: str = Field(min_length=1, max_length=4000)
    evidence: CardDispositionEvidenceV1


class CardDispositionDecision(BaseModel):
    """Auditable server decision for one requested disposition."""

    status: Literal[
        "absent",
        "malformed",
        "applied",
        "downgraded",
        "preserved_done",
    ]
    requested_lane: CardLane | None = None
    applied_lane: CardLane
    reason: str
    disposition: CardDispositionV1 | None = None
    watch_id: str | None = None


def parse_card_disposition(
    value: Any,
) -> tuple[CardDispositionV1 | None, str | None]:
    """Parse v1 without allowing malformed business data to fail transport ACK."""
    if value is None:
        return None, None
    try:
        return CardDispositionV1.model_validate(value), None
    except (ValidationError, TypeError, ValueError) as exc:
        return None, str(exc)


def decide_card_disposition(
    value: Any,
    *,
    current_lane: CardLane,
    watches: list[PRWatch] | None = None,
    now: datetime | None = None,
) -> CardDispositionDecision:
    """Resolve a requested lane while defaulting to preservation and guarding Done."""
    disposition, error = parse_card_disposition(value)
    if value is None:
        return CardDispositionDecision(
            status="absent",
            applied_lane=current_lane,
            reason="No card disposition was supplied; the current lane was preserved.",
        )
    if not disposition:
        return CardDispositionDecision(
            status="malformed",
            applied_lane=current_lane,
            reason=(
                "The card disposition was malformed and was ignored: "
                f"{(error or 'validation failed')[:1000]}"
            ),
        )
    if disposition.lane != CardLane.DONE:
        return CardDispositionDecision(
            status="applied",
            requested_lane=disposition.lane,
            applied_lane=disposition.lane,
            reason=f"Explicit {disposition.contract} disposition applied.",
            disposition=disposition,
        )

    linked = list(watches or [])
    allowed, reason, watch_id = _done_evidence_is_safe(
        disposition, linked, now=now or utcnow()
    )
    if allowed:
        return CardDispositionDecision(
            status="applied",
            requested_lane=CardLane.DONE,
            applied_lane=CardLane.DONE,
            reason=reason,
            disposition=disposition,
            watch_id=watch_id,
        )
    if current_lane == CardLane.DONE:
        return CardDispositionDecision(
            status="preserved_done",
            requested_lane=CardLane.DONE,
            applied_lane=CardLane.DONE,
            reason=(
                f"Done evidence was not sufficient ({reason}); an existing Done lane "
                "was preserved for legacy reconciliation safety."
            ),
            disposition=disposition,
            watch_id=watch_id,
        )
    return CardDispositionDecision(
        status="downgraded",
        requested_lane=CardLane.DONE,
        applied_lane=CardLane.WAITING,
        reason=f"Done was downgraded to Waiting: {reason}",
        disposition=disposition,
        watch_id=watch_id,
    )


def disposition_for_merged_watch(watch: PRWatch) -> dict[str, Any]:
    """Build the same public v1 contract for supervisor-owned merge completion."""
    state = watch.state or {}
    return CardDispositionV1(
        contract=CARD_DISPOSITION_V1,
        lane=CardLane.DONE,
        outcome=f"Pull request {watch.pr_url} was merged.",
        evidence=CardDispositionEvidenceV1(
            integration_required=True,
            pr_watch_id=watch.id,
            watched_head_sha=str(state.get("head_sha") or watch.head_sha or "") or None,
            merge_commit_sha=str(state.get("merge_commit_sha") or "") or None,
            references=[watch.pr_url],
        ),
    ).model_dump(mode="json")


def _done_evidence_is_safe(
    disposition: CardDispositionV1,
    watches: list[PRWatch],
    *,
    now: datetime,
) -> tuple[bool, str, str | None]:
    evidence = disposition.evidence
    if not watches:
        if evidence.integration_required is False:
            return True, "Done accepted with an explicit no-integration outcome.", None
        return (
            False,
            "integration requirements were not explicitly ruled out and no linked PR watch exists",
            evidence.pr_watch_id,
        )

    blockers = [watch for watch in watches if watch.status != PRWatchStatus.MERGED]
    if blockers:
        blocker = blockers[0]
        state = str((blocker.state or {}).get("state") or blocker.status.value)
        return (
            False,
            f"linked PR watch {blocker.id} is {state}, not merged",
            blocker.id,
        )

    if not evidence.pr_watch_id:
        return False, "linked integration requires a pr_watch_id", None
    watch = next((item for item in watches if item.id == evidence.pr_watch_id), None)
    if not watch:
        return (
            False,
            "the disposition references an unknown linked PR watch",
            evidence.pr_watch_id,
        )

    state = watch.state or {}
    if str(state.get("state") or "").lower() != "merged":
        return False, "the linked PR's merged state is unknown", watch.id
    expected_head = evidence.watched_head_sha
    observed_head = str(state.get("head_sha") or "") or None
    confirmed_head = str(state.get("confirmed_head_sha") or "") or None
    if not expected_head:
        return False, "watched_head_sha evidence is required", watch.id
    if not watch.head_sha or expected_head != watch.head_sha:
        return (
            False,
            "the disposition head does not match the exact watched head",
            watch.id,
        )
    if observed_head != expected_head or confirmed_head != expected_head:
        return (
            False,
            "the merged observation does not confirm the exact watched head",
            watch.id,
        )

    expected_merge = evidence.merge_commit_sha
    observed_merge = str(state.get("merge_commit_sha") or "") or None
    if not expected_merge or not observed_merge:
        return False, "merge commit evidence is required", watch.id
    if expected_merge != observed_merge:
        return (
            False,
            "the merge commit does not match the watched observation",
            watch.id,
        )

    green = state.get("stable_green_evidence") or {}
    if green.get("green") is not True or green.get("head_sha") != expected_head:
        return (
            False,
            "stable-green evidence for the exact watched head is missing",
            watch.id,
        )
    if watch.stable_head_observations < watch.policy.stable_observations:
        return False, "the watched head lacks enough stable observations", watch.id
    if not watch.stable_head_since:
        return False, "the watched head has no stability timestamp", watch.id
    if (
        now - watch.stable_head_since
    ).total_seconds() < watch.policy.stable_head_seconds:
        return (
            False,
            "the watched head has not satisfied the stability window",
            watch.id,
        )
    if watch.policy.agent_merge_on_green and watch.status != PRWatchStatus.MERGED:
        return (
            False,
            "merge-on-green policy requires the linked PR to be merged",
            watch.id,
        )
    return (
        True,
        "Done accepted for an exact stable-green watched head with matching merge commit evidence.",
        watch.id,
    )
