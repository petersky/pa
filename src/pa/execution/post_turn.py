"""Versioned neutral turn-end evidence and deterministic follow-up contracts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

TURN_END_SNAPSHOT_V1 = "pa.turn-end-snapshot/v1"
POST_TURN_CONTEXT_V1 = "pa.post-turn-context/v1"
POST_TURN_EVALUATION_V1 = "pa.post-turn-evaluation/v1"
FOLLOWUP_ACTION_V1 = "pa.followup-action/v1"
ACTION_CATALOG_V1 = "pa.followup-action-catalog/v1"

MAX_SNAPSHOT_TEXT = 8_000
MAX_EVIDENCE_REFERENCES = 100
MAX_AUTOMATIC_FOLLOWUP_TURNS = 10
MAX_EVALUATOR_ATTEMPTS = 5
DEFAULT_EVALUATION_TIMEOUT_SECONDS = 60

EVALUATOR_READ_ONLY_INSTRUCTIONS = """
You are PA's bounded post-turn evaluator. Decide whether the requested card
outcome was achieved from the supplied versioned evidence. You are read-only.
Never write or move cards, prompt sessions, dispatch or retry work, create or
merge pull requests, reply to reviews, change configuration, operate services,
delete data, or mutate any external system. Return only
pa.post-turn-evaluation/v1 JSON. Recommend only actions from the supplied
catalog; never return commands, code, tool calls, or unenumerated actions.
""".strip()


class PostTurnDecision(StrEnum):
    OUTCOME_ACHIEVED = "outcome_achieved"
    FURTHER_AGENT_WORK_NEEDED = "further_agent_work_needed"
    WAITING_ON_EXTERNAL_CONDITION = "waiting_on_external_condition"
    OPERATOR_INPUT_REQUIRED = "operator_input_required"
    RETRYABLE_RUNTIME_FAILURE = "retryable_runtime_failure"
    NONRETRYABLE_FAILURE = "nonretryable_failure"
    FOLLOWUP_RECORD_REQUIRED = "followup_record_required"
    UNABLE_TO_DETERMINE = "unable_to_determine"


class FollowupActionName(StrEnum):
    NO_ACTION = "no_action"
    RECORD_TURN_OUTCOME = "record_turn_outcome"
    MOVE_CARD = "move_card"
    PROMPT_SAME_SESSION = "prompt_same_session"
    RETRY_DISPATCH = "retry_dispatch"
    REDISPATCH_CARD = "redispatch_card"
    WAIT_FOR_PR_SUPERVISOR = "wait_for_pr_supervisor"
    REQUEST_OPERATOR_INPUT = "request_operator_input"
    CREATE_FOLLOWUP_CARD = "create_followup_card"
    RECORD_BUG_OR_FAILURE = "record_bug_or_failure"
    REFRESH_READ_ONLY_EVIDENCE = "refresh_read_only_evidence"
    ESCALATE_FOR_MANUAL_REVIEW = "escalate_for_manual_review"


class FollowupActionStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    DEFERRED = "deferred"
    FAILED = "failed"
    SUPERSEDED = "superseded"


class SafetyClassification(StrEnum):
    RECORD_ONLY = "record_only"
    REVERSIBLE = "reversible"
    EXTERNAL_WRITE = "external_write"
    HIGH_IMPACT = "high_impact"


class _ActionParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _ReasonParameters(_ActionParameters):
    reason: str = Field(min_length=1, max_length=2_000)


class _MoveCardParameters(_ReasonParameters):
    lane: Literal["inbox", "active", "waiting", "done"]
    expected_card_version: str


class _PromptParameters(_ActionParameters):
    purpose: str = Field(min_length=1, max_length=1_000)
    prompt: str = Field(min_length=1, max_length=20_000)
    session_id: str


class _RetryParameters(_ReasonParameters):
    dispatch_id: str


class _RedispatchParameters(_ReasonParameters):
    card_id: str
    instance_id: str
    provider: str
    mode: str | None = None


class _WaitParameters(_ActionParameters):
    watch_id: str
    condition: str = Field(min_length=1, max_length=1_000)


class _OperatorInputChoice(_ActionParameters):
    id: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=300)
    description: str | None = Field(default=None, max_length=1_000)
    value: Any = None


class _OperatorInputParameters(_ActionParameters):
    question: str = Field(min_length=1, max_length=2_000)
    keep_lane: Literal["inbox", "active", "waiting"]
    request_id: str | None = Field(default=None, max_length=300)
    response_schema: dict[str, Any] | None = None
    choices: list[_OperatorInputChoice] = Field(default_factory=list, max_length=100)
    allow_freeform: bool = True
    allow_cancel: bool = True
    sensitive: bool = False
    deadline: datetime | None = None


class _CreateCardParameters(_ActionParameters):
    project_id: str
    title: str = Field(min_length=1, max_length=500)
    body: str = Field(min_length=1, max_length=20_000)
    evidence: list[str] = Field(default_factory=list, max_length=40)
    parent_card_id: str
    deduplication_key: str


class _RefreshParameters(_ActionParameters):
    evidence_kinds: list[str] = Field(min_length=1, max_length=20)
    not_before: datetime | None = None


_ACTION_PARAMETERS: dict[FollowupActionName, type[_ActionParameters]] = {
    FollowupActionName.NO_ACTION: _ReasonParameters,
    FollowupActionName.RECORD_TURN_OUTCOME: _ReasonParameters,
    FollowupActionName.MOVE_CARD: _MoveCardParameters,
    FollowupActionName.PROMPT_SAME_SESSION: _PromptParameters,
    FollowupActionName.RETRY_DISPATCH: _RetryParameters,
    FollowupActionName.REDISPATCH_CARD: _RedispatchParameters,
    FollowupActionName.WAIT_FOR_PR_SUPERVISOR: _WaitParameters,
    FollowupActionName.REQUEST_OPERATOR_INPUT: _OperatorInputParameters,
    FollowupActionName.CREATE_FOLLOWUP_CARD: _CreateCardParameters,
    FollowupActionName.RECORD_BUG_OR_FAILURE: _CreateCardParameters,
    FollowupActionName.REFRESH_READ_ONLY_EVIDENCE: _RefreshParameters,
    FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW: _ReasonParameters,
}

_ADMISSIBLE_ACTIONS: dict[PostTurnDecision, set[FollowupActionName]] = {
    PostTurnDecision.OUTCOME_ACHIEVED: {
        FollowupActionName.NO_ACTION,
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.MOVE_CARD,
    },
    PostTurnDecision.FURTHER_AGENT_WORK_NEEDED: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.PROMPT_SAME_SESSION,
        FollowupActionName.REDISPATCH_CARD,
        FollowupActionName.CREATE_FOLLOWUP_CARD,
        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
    },
    PostTurnDecision.WAITING_ON_EXTERNAL_CONDITION: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.WAIT_FOR_PR_SUPERVISOR,
        FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
    },
    PostTurnDecision.OPERATOR_INPUT_REQUIRED: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.REQUEST_OPERATOR_INPUT,
        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
    },
    PostTurnDecision.RETRYABLE_RUNTIME_FAILURE: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.RETRY_DISPATCH,
        FollowupActionName.REDISPATCH_CARD,
        FollowupActionName.RECORD_BUG_OR_FAILURE,
    },
    PostTurnDecision.NONRETRYABLE_FAILURE: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.RECORD_BUG_OR_FAILURE,
        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
    },
    PostTurnDecision.FOLLOWUP_RECORD_REQUIRED: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.CREATE_FOLLOWUP_CARD,
    },
    PostTurnDecision.UNABLE_TO_DETERMINE: {
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
    },
}

_APPROVAL_REQUIRED = {
    FollowupActionName.MOVE_CARD,
    FollowupActionName.PROMPT_SAME_SESSION,
    FollowupActionName.RETRY_DISPATCH,
    FollowupActionName.REDISPATCH_CARD,
    FollowupActionName.CREATE_FOLLOWUP_CARD,
    FollowupActionName.RECORD_BUG_OR_FAILURE,
}


def context_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class EvidenceReferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1, max_length=80)
    reference: str = Field(min_length=1, max_length=2_000)
    observed_at: datetime | None = None
    provenance: str = Field(min_length=1, max_length=500)
    fresh: bool = True


class TurnEndSnapshotV1(BaseModel):
    """Immutable neutral evidence captured after one card-linked ACP turn."""

    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, serialize_by_alias=True
    )

    contract: Literal["pa.turn-end-snapshot/v1"] = Field(
        default=TURN_END_SNAPSHOT_V1, alias="schema"
    )
    snapshot_id: str = Field(default_factory=lambda: str(uuid4()))
    turn_id: str
    turn_sequence: int = Field(ge=1)
    dispatch_id: str
    session_id: str | None = None
    card_id: str
    project_id: str | None = None
    authority_instance_id: str
    authority_version: str | None = None
    originating_instance_id: str
    captured_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    stop_reason: str | None = Field(default=None, max_length=200)
    provider_status: str | None = Field(default=None, max_length=200)
    session_status: str | None = Field(default=None, max_length=200)
    card_lane_before: str | None = None
    card_lane_after: str | None = None
    dispatch_state: str
    completion_delivery: dict[str, Any] = Field(default_factory=dict)
    disposition: dict[str, Any] | None = None
    disposition_status: str | None = None
    disposition_parse_error: str | None = Field(default=None, max_length=2_000)
    final_outcome_text: str = Field(default="", max_length=MAX_SNAPSHOT_TEXT)
    deliverables: dict[str, Any] = Field(default_factory=dict)
    validations: list[dict[str, Any]] = Field(default_factory=list, max_length=40)
    blockers: list[str] = Field(default_factory=list, max_length=40)
    failures: list[dict[str, Any]] = Field(default_factory=list, max_length=40)
    operator_input_requests: list[str] = Field(default_factory=list, max_length=20)
    queued_prompts: list[dict[str, Any]] = Field(default_factory=list, max_length=40)
    followup_state: dict[str, Any] = Field(default_factory=dict)
    evidence: list[EvidenceReferenceV1] = Field(
        default_factory=list, max_length=MAX_EVIDENCE_REFERENCES
    )
    provenance: dict[str, Any] = Field(default_factory=dict)


class PostTurnContextV1(BaseModel):
    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, serialize_by_alias=True
    )

    contract: Literal["pa.post-turn-context/v1"] = Field(
        default=POST_TURN_CONTEXT_V1, alias="schema"
    )
    context_version: int = 1
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_version: str | None = None
    snapshot: TurnEndSnapshotV1
    card: dict[str, Any]
    project: dict[str, Any] | None = None
    execution_contract: dict[str, Any] | None = None
    dispatch_history: list[dict[str, Any]] = Field(default_factory=list, max_length=100)
    prior_evaluations: list[dict[str, Any]] = Field(
        default_factory=list, max_length=20
    )
    watches: list[dict[str, Any]] = Field(default_factory=list, max_length=40)
    fleet_capabilities: list[str] = Field(default_factory=list, max_length=100)
    action_catalog_version: Literal[
        "pa.followup-action-catalog/v1"
    ] = ACTION_CATALOG_V1
    evaluator_instructions: str = EVALUATOR_READ_ONLY_INSTRUCTIONS


class FollowupActionV1(BaseModel):
    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, serialize_by_alias=True
    )

    contract: Literal["pa.followup-action/v1"] = Field(
        default=FOLLOWUP_ACTION_V1, alias="schema"
    )
    action_id: str = Field(default_factory=lambda: str(uuid4()))
    name: FollowupActionName
    parameters: dict[str, Any]
    preconditions: dict[str, Any] = Field(default_factory=dict)
    idempotency_key_inputs: list[str] = Field(min_length=1, max_length=20)
    target_scope: dict[str, Any] = Field(default_factory=dict)
    safety: SafetyClassification
    human_approval_required: bool
    status: FollowupActionStatus = FollowupActionStatus.PENDING
    status_reason: str | None = Field(default=None, max_length=2_000)
    executed_at: datetime | None = None
    audit: list[dict[str, Any]] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def validate_parameters_and_policy(self) -> FollowupActionV1:
        model = _ACTION_PARAMETERS[self.name]
        self.parameters = model.model_validate(self.parameters).model_dump(
            mode="json", exclude_none=True
        )
        forbidden = {"command", "commands", "shell", "script", "tool_call"}
        if forbidden.intersection(self.parameters):
            raise ValueError("free-form executable commands are prohibited")
        expected_approval = self.name in _APPROVAL_REQUIRED
        if expected_approval and not self.human_approval_required:
            raise ValueError(f"{self.name} requires explicit operator approval")
        return self


class PostTurnEvaluationV1(BaseModel):
    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, serialize_by_alias=True
    )

    contract: Literal["pa.post-turn-evaluation/v1"] = Field(
        default=POST_TURN_EVALUATION_V1, alias="schema"
    )
    evaluation_id: str = Field(default_factory=lambda: str(uuid4()))
    snapshot_id: str
    context_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_authority_version: str | None = None
    decision: PostTurnDecision
    rationale: str = Field(min_length=1, max_length=4_000)
    evidence: list[EvidenceReferenceV1] = Field(
        default_factory=list, max_length=MAX_EVIDENCE_REFERENCES
    )
    confidence: float = Field(ge=0, le=1)
    missing_or_ambiguous_evidence: list[str] = Field(
        default_factory=list, max_length=40
    )
    lane_appropriate: bool | None = None
    recommended_actions: list[FollowupActionV1] = Field(
        min_length=1, max_length=12
    )
    operator_status_text: str = Field(min_length=1, max_length=1_000)
    evaluator_attempt: int = Field(default=1, ge=1, le=MAX_EVALUATOR_ATTEMPTS)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_action_sources(self) -> PostTurnEvaluationV1:
        allowed = _ADMISSIBLE_ACTIONS[self.decision]
        names = [action.name for action in self.recommended_actions]
        unknown = set(names) - allowed
        if unknown:
            raise ValueError(
                f"actions are inadmissible for {self.decision}: "
                f"{sorted(str(item) for item in unknown)}"
            )
        if len(names) != len(set(names)):
            raise ValueError("duplicate follow-up action names are not allowed")
        return self


def action_catalog() -> dict[str, Any]:
    entries = []
    for name, model in _ACTION_PARAMETERS.items():
        entries.append(
            {
                "name": name.value,
                "parameters_schema": model.model_json_schema(),
                "human_approval_required": name in _APPROVAL_REQUIRED,
                "safety": (
                    SafetyClassification.RECORD_ONLY.value
                    if name
                    in {
                        FollowupActionName.NO_ACTION,
                        FollowupActionName.RECORD_TURN_OUTCOME,
                        FollowupActionName.WAIT_FOR_PR_SUPERVISOR,
                        FollowupActionName.REQUEST_OPERATOR_INPUT,
                        FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
                        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
                    }
                    else SafetyClassification.EXTERNAL_WRITE.value
                ),
                "admissible_decisions": [
                    decision.value
                    for decision, allowed in _ADMISSIBLE_ACTIONS.items()
                    if name in allowed
                ],
            }
        )
    return {
        "schema": ACTION_CATALOG_V1,
        "actions": entries,
        "policy": {
            "evaluator_is_read_only": True,
            "pa_is_sole_executor": True,
            "unknown_actions_rejected": True,
            "free_form_commands_rejected": True,
        },
    }


def _action(
    name: FollowupActionName,
    parameters: dict[str, Any],
    snapshot: TurnEndSnapshotV1,
    *,
    safety: SafetyClassification = SafetyClassification.RECORD_ONLY,
    approval: bool | None = None,
) -> FollowupActionV1:
    return FollowupActionV1(
        name=name,
        parameters=parameters,
        preconditions={
            "authority_version": snapshot.authority_version,
            "dispatch_state": snapshot.dispatch_state,
            "snapshot_id": snapshot.snapshot_id,
        },
        idempotency_key_inputs=[
            snapshot.dispatch_id,
            snapshot.turn_id,
            name.value,
            snapshot.authority_version or "unversioned",
        ],
        target_scope={
            "card_id": snapshot.card_id,
            "dispatch_id": snapshot.dispatch_id,
            "session_id": snapshot.session_id,
        },
        safety=safety,
        human_approval_required=(
            name in _APPROVAL_REQUIRED if approval is None else approval
        ),
    )


class PostTurnEvaluator:
    """Evidence-only bounded evaluator; it never receives a mutation capability."""

    def build_context(
        self,
        snapshot: TurnEndSnapshotV1,
        *,
        card: dict[str, Any],
        project: dict[str, Any] | None,
        execution_contract: dict[str, Any] | None,
        dispatch_history: list[dict[str, Any]],
        prior_evaluations: list[dict[str, Any]],
        watches: list[dict[str, Any]],
        fleet_capabilities: list[str],
    ) -> PostTurnContextV1:
        payload = {
            "schema": POST_TURN_CONTEXT_V1,
            "authority_version": snapshot.authority_version,
            "snapshot": snapshot.model_dump(mode="json"),
            "card": card,
            "project": project,
            "execution_contract": execution_contract,
            "dispatch_history": dispatch_history[-100:],
            "prior_evaluations": prior_evaluations[-20:],
            "watches": watches[:40],
            "fleet_capabilities": fleet_capabilities[:100],
            "action_catalog_version": ACTION_CATALOG_V1,
        }
        return PostTurnContextV1(
            digest=context_digest(payload),
            authority_version=snapshot.authority_version,
            snapshot=snapshot,
            card=card,
            project=project,
            execution_contract=execution_contract,
            dispatch_history=dispatch_history[-100:],
            prior_evaluations=prior_evaluations[-20:],
            watches=watches[:40],
            fleet_capabilities=fleet_capabilities[:100],
        )

    def evaluate(self, context: PostTurnContextV1) -> PostTurnEvaluationV1:
        snapshot = context.snapshot
        report = snapshot.deliverables
        disposition = snapshot.disposition or {}
        lane = str(context.card.get("lane") or snapshot.card_lane_after or "")
        evidence = list(snapshot.evidence)
        blockers = [item.casefold() for item in snapshot.blockers]
        failures = json.dumps(snapshot.failures, default=str).casefold()
        validations = list(snapshot.validations)
        failed_validations = [
            item
            for item in validations
            if str(item.get("status") or "").casefold()
            in {"failed", "error", "cancelled"}
        ]
        watches = context.watches
        def watch_is_merged(watch: dict[str, Any]) -> bool:
            state = watch.get("state") or {}
            return bool(
                str(watch.get("status") or "").casefold() == "merged"
                or state.get("merge_commit_sha")
                or state.get("state") == "merged"
            )

        merged_watch = next(
            (
                watch
                for watch in watches
                if watch_is_merged(watch)
            ),
            None,
        )
        active_watch = next(
            (
                watch
                for watch in watches
                if str(watch.get("status") or "").casefold()
                in {"active", "blocked"}
                and not watch_is_merged(watch)
            ),
            None,
        )
        runtime_markers = (
            "sandbox",
            "permission denied",
            "bubblewrap",
            "provider",
            "connection",
            "timeout",
            "unavailable",
            "rate limit",
        )
        runtime_failure = any(
            marker in " ".join(blockers) or marker in failures
            for marker in runtime_markers
        )
        nonretryable = any(
            marker in " ".join(blockers) or marker in failures
            for marker in ("invalid request", "unsupported", "security policy")
        )
        operator_request = snapshot.operator_input_requests or any(
            "operator" in item or "input" in item for item in blockers
        )
        has_deliverable = any(
            report.get(key)
            for key in (
                "commit_sha",
                "pr_url",
                "pr_number",
                "merge_commit_sha",
                "changed_files",
            )
        )
        disposition_lane = str(disposition.get("lane") or "")
        integration_required = (
            (disposition.get("evidence") or {}).get("integration_required")
            if isinstance(disposition.get("evidence"), dict)
            else None
        )

        actions: list[FollowupActionV1]
        missing: list[str] = []
        if (
            merged_watch
            and report.get("merge_commit_sha")
            and not failed_validations
        ) or (
            disposition_lane == "done"
            and integration_required is False
            and snapshot.final_outcome_text
            and not failed_validations
        ):
            decision = PostTurnDecision.OUTCOME_ACHIEVED
            rationale = (
                "Exact terminal outcome evidence is present and validations do "
                "not report a failure."
            )
            actions = [
                _action(
                    FollowupActionName.NO_ACTION,
                    {"reason": "The requested outcome is supported by durable evidence."},
                    snapshot,
                )
            ]
            status = "Attempt succeeded; the requested outcome is evidenced."
            lane_ok = lane == "done"
            confidence = 0.97 if merged_watch else 0.9
        elif operator_request:
            decision = PostTurnDecision.OPERATOR_INPUT_REQUIRED
            question = (
                snapshot.operator_input_requests[0]
                if snapshot.operator_input_requests
                else "Operator input is required before the card can proceed."
            )
            rationale = "The turn explicitly requested bounded operator input."
            actions = [
                _action(
                    FollowupActionName.RECORD_TURN_OUTCOME,
                    {"reason": rationale},
                    snapshot,
                ),
                _action(
                    FollowupActionName.REQUEST_OPERATOR_INPUT,
                    {"question": question, "keep_lane": "waiting"},
                    snapshot,
                ),
            ]
            status = "Attempt blocked; operator input is required."
            lane_ok = lane == "waiting"
            confidence = 0.95
        elif runtime_failure:
            decision = (
                PostTurnDecision.NONRETRYABLE_FAILURE
                if nonretryable
                else PostTurnDecision.RETRYABLE_RUNTIME_FAILURE
            )
            rationale = (
                "Provider, sandbox, permission, or runtime evidence prevented "
                "the requested implementation outcome."
            )
            actions = [
                _action(
                    FollowupActionName.RECORD_TURN_OUTCOME,
                    {"reason": rationale},
                    snapshot,
                )
            ]
            if decision == PostTurnDecision.RETRYABLE_RUNTIME_FAILURE:
                actions.append(
                    _action(
                        FollowupActionName.REDISPATCH_CARD,
                        {
                            "card_id": snapshot.card_id,
                            "instance_id": snapshot.originating_instance_id,
                            "provider": str(
                                snapshot.provenance.get("provider") or "default"
                            ),
                            "mode": snapshot.provenance.get("mode"),
                            "reason": "Retry on an explicitly eligible runtime.",
                        },
                        snapshot,
                        safety=SafetyClassification.EXTERNAL_WRITE,
                    )
                )
            else:
                actions.append(
                    _action(
                        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
                        {"reason": "The recorded failure is not safe to retry."},
                        snapshot,
                    )
                )
            status = (
                "Attempt failed due to a retryable runtime condition."
                if decision == PostTurnDecision.RETRYABLE_RUNTIME_FAILURE
                else "Attempt failed and is not automatically retryable."
            )
            lane_ok = lane in {"active", "waiting"}
            confidence = 0.9
        elif failed_validations:
            decision = PostTurnDecision.FURTHER_AGENT_WORK_NEEDED
            rationale = "One or more recorded validations failed."
            actions = [
                _action(
                    FollowupActionName.RECORD_TURN_OUTCOME,
                    {"reason": rationale},
                    snapshot,
                ),
                _action(
                    FollowupActionName.PROMPT_SAME_SESSION,
                    {
                        "purpose": "Address recorded validation failures.",
                        "prompt": (
                            "Continue from the recorded turn and address the "
                            "failed validations. Preserve terminal dispatch history."
                        ),
                        "session_id": snapshot.session_id or "unavailable",
                    },
                    snapshot,
                    safety=SafetyClassification.EXTERNAL_WRITE,
                ),
            ]
            status = "Attempt blocked; validation failures need follow-up."
            lane_ok = lane in {"active", "waiting"}
            confidence = 0.95
        elif active_watch or (
            report.get("pr_url") and not report.get("merge_commit_sha")
        ):
            decision = PostTurnDecision.WAITING_ON_EXTERNAL_CONDITION
            rationale = "Integration evidence exists but the watched terminal condition is not met."
            if active_watch:
                actions = [
                    _action(
                        FollowupActionName.RECORD_TURN_OUTCOME,
                        {"reason": rationale},
                        snapshot,
                    ),
                    _action(
                        FollowupActionName.WAIT_FOR_PR_SUPERVISOR,
                        {
                            "watch_id": str(active_watch.get("id")),
                            "condition": "stable green exact head and merge evidence",
                        },
                        snapshot,
                    ),
                ]
            else:
                missing.append("linked PR watch with explicit integration provenance")
                actions = [
                    _action(
                        FollowupActionName.RECORD_TURN_OUTCOME,
                        {"reason": rationale},
                        snapshot,
                    ),
                    _action(
                        FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
                        {
                            "evidence_kinds": [
                                "pull_request",
                                "ci",
                                "review",
                                "merge",
                            ]
                        },
                        snapshot,
                    ),
                ]
            status = "Attempt ended; waiting on an external integration condition."
            lane_ok = lane == "waiting"
            confidence = 0.86
        elif snapshot.blockers or disposition_lane in {"active", "ready", "waiting"}:
            decision = PostTurnDecision.FURTHER_AGENT_WORK_NEEDED
            rationale = "The recorded disposition or blockers show that requested work remains."
            actions = [
                _action(
                    FollowupActionName.RECORD_TURN_OUTCOME,
                    {"reason": rationale},
                    snapshot,
                )
            ]
            if snapshot.session_id:
                actions.append(
                    _action(
                        FollowupActionName.PROMPT_SAME_SESSION,
                        {
                            "purpose": "Continue the incomplete card outcome.",
                            "prompt": (
                                "Continue from the neutral turn-end snapshot and "
                                "complete the remaining requested outcome."
                            ),
                            "session_id": snapshot.session_id,
                        },
                        snapshot,
                        safety=SafetyClassification.EXTERNAL_WRITE,
                    )
                )
            else:
                actions.append(
                    _action(
                        FollowupActionName.ESCALATE_FOR_MANUAL_REVIEW,
                        {"reason": "No linked session is available for bounded follow-up."},
                        snapshot,
                    )
                )
            status = "Attempt blocked or incomplete; further work is needed."
            lane_ok = lane in {"active", "waiting"}
            confidence = 0.82
        else:
            decision = PostTurnDecision.UNABLE_TO_DETERMINE
            rationale = (
                "The turn ended without sufficient disposition, deliverable, "
                "validation, blocker, or integration evidence."
            )
            missing.extend(
                [
                    "structured card disposition",
                    "deliverable or exact integration evidence",
                    "validation results",
                ]
            )
            actions = [
                _action(
                    FollowupActionName.RECORD_TURN_OUTCOME,
                    {"reason": rationale},
                    snapshot,
                ),
                _action(
                    FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
                    {
                        "evidence_kinds": [
                            "card",
                            "repository",
                            "session",
                            "pull_request",
                        ]
                    },
                    snapshot,
                ),
            ]
            status = "Turn ended; the outcome still needs evaluation evidence."
            lane_ok = lane != "done"
            confidence = 0.4 if not has_deliverable else 0.55

        return PostTurnEvaluationV1(
            snapshot_id=snapshot.snapshot_id,
            context_digest=context.digest,
            observed_authority_version=context.authority_version,
            decision=decision,
            rationale=rationale,
            evidence=evidence,
            confidence=confidence,
            missing_or_ambiguous_evidence=missing,
            lane_appropriate=lane_ok,
            recommended_actions=actions,
            operator_status_text=status,
        )

    @staticmethod
    def validate_result(
        result: dict[str, Any] | PostTurnEvaluationV1,
        *,
        expected_context_digest: str,
        expected_authority_version: str | None,
    ) -> PostTurnEvaluationV1:
        parsed = (
            result
            if isinstance(result, PostTurnEvaluationV1)
            else PostTurnEvaluationV1.model_validate(result)
        )
        if parsed.context_digest != expected_context_digest:
            raise ValueError("stale or mismatched post-turn context digest")
        if parsed.observed_authority_version != expected_authority_version:
            raise ValueError("stale authority version")
        return parsed


def mark_record_only_actions(evaluation: PostTurnEvaluationV1) -> None:
    """Deterministically execute audit-only actions; all mutations remain pending."""
    now = datetime.now(UTC)
    for action in evaluation.recommended_actions:
        if action.status == FollowupActionStatus.EXECUTED:
            continue
        if action.name in {
            FollowupActionName.NO_ACTION,
            FollowupActionName.RECORD_TURN_OUTCOME,
        }:
            action.status = FollowupActionStatus.EXECUTED
            action.executed_at = now
            action.status_reason = "Recorded idempotently by PA."
            action.audit.append(
                {
                    "event": "executed",
                    "at": now.isoformat(),
                    "executor": "pa.post-turn",
                    "idempotency_key_inputs": action.idempotency_key_inputs,
                }
            )
        elif not action.human_approval_required:
            action.status = FollowupActionStatus.DEFERRED
            action.status_reason = (
                "Recorded as a deterministic recommendation; its condition "
                "executor will act only when configured and eligible."
            )
