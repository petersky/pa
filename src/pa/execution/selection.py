"""Versioned, deterministic execution selection. No I/O or provider-name heuristics.

Capabilities are evidence, preferences are intent, and neither grants authority.
Receipts describe requested settings; ACP confirmation remains a separate contract.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictStr,
    model_validator,
)

VERSION = "pa.execution-selection/v1"
FIELDS = ("harness", "connection", "model_provider", "model", "reasoning")
RESERVED_OPTIONS = {
    "mode",
    "modeid",
    "sessionmode",
    "agentmode",
    "initialagentmode",
    "collaborationmode",
    "sandbox",
    "approvalpolicy",
    "permissions",
    "model",
    "modelid",
    "modelprovider",
    "provider",
    "reasoning",
    "effort",
    "reasoningeffort",
    "reasoninglevel",
    "thinkinglevel",
    "thoughtlevel",
    "sandboxmode",
    "permissionmode",
    "permissionpolicy",
    "approvalmode",
    "approval",
    "executionmode",
    "bypasspermissions",
    "networkaccess",
    "allownetwork",
}


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    ).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Preference(StrictModel):
    """Automatic terminates inheritance; prefer permits a recorded alternative."""

    intent: Literal["inherit", "automatic", "required", "preferred"] = "inherit"
    value: StrictStr | StrictBool | None = None

    @model_validator(mode="after")
    def valid_value(self):
        specified = self.intent in {"required", "preferred"}
        if specified != (self.value is not None):
            raise ValueError(
                "required/preferred need a value; inherit/automatic must omit it"
            )
        if isinstance(self.value, str) and (
            not self.value.strip() or len(self.value) > 300
        ):
            raise ValueError("selection values must contain 1–300 characters")
        return self


class ExecutionPreferences(StrictModel):
    version: Literal[1] = 1
    harness: Preference = Field(default_factory=Preference)
    connection: Preference = Field(default_factory=Preference)
    model_provider: Preference = Field(default_factory=Preference)
    model: Preference = Field(default_factory=Preference)
    reasoning: Preference = Field(default_factory=Preference)
    options: dict[str, Preference] = Field(default_factory=dict, max_length=30)
    hard_constraints: SelectionConstraints = Field(
        default_factory=lambda: SelectionConstraints()
    )
    task: TaskAssessment | None = None

    @model_validator(mode="after")
    def safe_options(self):
        for name in self.options:
            normalized = re.sub(r"[^a-z0-9]", "", name.lower())
            if (
                normalized in RESERVED_OPTIONS
                or len(name) > 100
                or re.search(
                    r"permission|sandbox|approval|collaboration|networkaccess",
                    normalized,
                )
                or re.search(
                    r"secret|token|password|credential|api.?key|authorization",
                    name,
                    re.IGNORECASE,
                )
            ):
                raise ValueError(f"{name!r} is not an execution-selection option")
        for name in FIELDS:
            value = getattr(self, name).value
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a native string identifier")
        return self


class SelectionConstraints(StrictModel):
    """Conjunctive authority constraints. Never populated by task-text inference."""

    instance_ids: list[str] | None = None
    harnesses: list[str] | None = None
    connection_ids: list[str] | None = None
    model_providers: list[str] | None = None
    models: list[str] | None = None
    required_tools: list[str] = Field(default_factory=list)
    modalities: list[str] = Field(default_factory=list)
    min_context_tokens: int = Field(default=0, ge=0)
    max_cost_usd: float | None = Field(default=None, ge=0)
    permission_eligible: bool = True
    ownership_eligible: bool = True


class TaskAssessment(StrictModel):
    version: Literal[1] = 1
    role: str = "executor"
    complexity: Literal["routine", "moderate", "complex"] = "moderate"
    ambiguity: Literal["low", "high", "unknown"] = "unknown"
    risk: Literal["low", "high", "unknown"] = "unknown"
    scope: Literal["focused", "broad", "unknown"] = "unknown"
    objective: Literal["balanced", "quality", "cost", "latency"] = "balanced"
    tools: list[str] = Field(default_factory=list)
    modalities: list[str] = Field(default_factory=list)
    context_tokens: int | None = Field(default=None, ge=0)
    provenance: dict[str, str] = Field(default_factory=dict)


def assess_task(
    title: str = "",
    body: str = "",
    *,
    role: str = "executor",
    explicit: TaskAssessment | None = None,
) -> TaskAssessment:
    if explicit is not None:
        result = explicit.model_copy(deep=True)
        result.provenance = {
            key: "explicit_task_assessment"
            for key in result.model_dump(exclude={"version", "provenance"})
        }
        return result
    text = (title + "\n" + body)[:16_000].lower()
    broad = len(text) > 4000 or bool(
        re.search(r"\b(migration|architecture|cross.platform|end.to.end)\b", text)
    )
    risky = bool(
        re.search(
            r"\b(authentication|authorization|encryption|payments|production|delet(?:e|ion))\b",
            text,
        )
    )
    ambiguous = bool(
        re.search(r"\b(explore|investigate|unclear|alternatives|design)\b", text)
    )
    return TaskAssessment(
        role=role,
        complexity="complex" if broad else "routine" if len(text) < 300 else "moderate",
        scope="broad" if broad else "focused",
        risk="high" if risky else "unknown",
        ambiguity="high" if ambiguous else "unknown",
        provenance={
            "role": "invocation",
            "complexity": "bounded_text_heuristic/v1",
            "scope": "bounded_text_heuristic/v1",
            "risk": "bounded_text_heuristic/v1",
            "ambiguity": "bounded_text_heuristic/v1",
            "objective": "cold_start_default",
        },
    )


class NativeOption(StrictModel):
    support: Literal["supported", "unsupported", "unknown"] = "unknown"
    values: list[str | bool] = Field(default_factory=list)
    default: str | bool | None = None
    mutable_between_turns: bool | None = None


class ExecutionCandidate(StrictModel):
    """One actually discoverable tuple; IDs have meaning only inside its scope."""

    instance_id: str
    harness: str
    connection: str
    connection_revision: str | None = None
    native_model_provider: str | None = None
    model_provider: str | None = None
    account_label: str | None = None
    endpoint_label: str | None = None
    model: str | None = None
    model_state: Literal["known", "provider_default", "unknown"] = "unknown"
    model_version: str | None = None
    reasoning: NativeOption = Field(default_factory=NativeOption)
    options: dict[str, NativeOption] = Field(default_factory=dict)
    tools: list[str] | None = None
    modalities: list[str] | None = None
    context_tokens: int | None = Field(default=None, ge=0)
    estimated_cost_usd: float | None = Field(default=None, ge=0)
    estimated_latency_ms: float | None = Field(default=None, ge=0)
    readiness: Literal["ready", "unavailable", "unknown"] = "unknown"
    can_attempt_unverified: bool = False
    health_evidence: list[str] = Field(default_factory=list)
    capacity_available: bool | None = None
    default: bool = False
    catalog_source: str
    catalog_version: str
    observed_at: datetime
    freshness: Literal["fresh", "stale", "unknown"] = "unknown"
    schema_version: int = 1

    @property
    def key(self) -> str:
        return digest(
            [
                self.instance_id,
                self.harness,
                self.connection,
                self.connection_revision,
                self.model_provider,
                self.model,
                self.model_version,
            ]
        )


class SelectionRule(StrictModel):
    id: str = Field(min_length=1, max_length=100)
    description: str = Field(max_length=500)
    match: dict[str, list[str]] = Field(default_factory=dict)
    prefer: ExecutionPreferences = Field(default_factory=ExecutionPreferences)
    weight: int = Field(default=10, ge=0, le=100)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_match(self):
        if set(self.match) - {
            "role",
            "complexity",
            "ambiguity",
            "risk",
            "scope",
            "objective",
        }:
            raise ValueError(
                "policy rules may match only structured task assessment fields"
            )
        # Rules are advisory and cannot create pins, permissions, or routing authority.
        if any(
            p.intent == "required"
            for p in [
                *(getattr(self.prefer, f) for f in FIELDS),
                *self.prefer.options.values(),
            ]
        ):
            raise ValueError(
                "automatic rules may prefer, but may not impose required settings"
            )
        return self


class SelectionPolicy(StrictModel):
    version: Literal[1] = 1
    revision: int = Field(default=1, ge=1)
    rules: list[SelectionRule] = Field(default_factory=list, max_length=100)
    defaults: ExecutionPreferences = Field(default_factory=ExecutionPreferences)
    constraints: SelectionConstraints = Field(default_factory=SelectionConstraints)
    allow_stale_catalog: bool = False
    allow_unverified_start: bool = True
    use_feedback: bool = False
    feedback_min_samples: int = Field(default=5, ge=3, le=1000)
    feedback_max_age_days: int = Field(default=30, ge=1, le=365)
    feedback_weight: float = Field(default=5, ge=0, le=20)
    fallback_max_attempts: int = Field(default=0, ge=0, le=3)
    # Fallback is opt-in, finite and restricted to these exact connection IDs.
    fallback_connections: list[str] = Field(default_factory=list, max_length=30)

    @model_validator(mode="after")
    def unique_rules(self):
        if len({r.id for r in self.rules}) != len(self.rules):
            raise ValueError(
                "Policy rule IDs must be unique for deterministic provenance"
            )
        return self


class OutcomeEvidence(StrictModel):
    id: str
    candidate_key: str
    decision_id: str
    observed_at: datetime
    kind: Literal["task_outcome", "provider_protocol"] = "task_outcome"
    validated: bool = False
    completed: bool | None = None
    tests_passed: bool | None = None
    review_passed: bool | None = None
    latency_ms: float | None = Field(default=None, ge=0)
    cost_usd: float | None = Field(default=None, ge=0)
    references: list[str] = Field(default_factory=list, max_length=30)


class SelectionError(ValueError):
    def __init__(self, code: str, message: str, receipt: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.receipt = receipt or {}


def merge_preferences(
    layers: list[tuple[str, ExecutionPreferences]],
) -> tuple[dict, dict]:
    """Layers are most-specific first. No truthiness merging of explicit auto/false."""
    fields = list(FIELDS) + sorted(
        {"options." + k for _, prefs in layers for k in prefs.options}
    )
    values, provenance = {}, {}
    for name in fields:
        for source, prefs in layers:
            pref = (
                prefs.options.get(name[8:], Preference())
                if name.startswith("options.")
                else getattr(prefs, name)
            )
            if pref.intent != "inherit":
                values[name] = pref.model_dump()
                provenance[name] = source
                break
        else:
            values[name] = Preference(intent="automatic").model_dump()
            provenance[name] = "automatic_policy"
    return values, provenance


def legacy_preferences(
    *, provider=None, model_id=None, effort=None, model_provider=None, config=None
) -> ExecutionPreferences:
    from pa.acp.configuration import SessionConfigurationRequest

    request = SessionConfigurationRequest.from_values(
        model_id=model_id,
        reasoning=effort,
        model_provider=model_provider,
        config=config,
    )
    fields = {}
    for name, value in (
        ("harness", provider),
        ("model_provider", request.model_provider),
        ("model", request.model_id),
        ("reasoning", request.reasoning),
    ):
        if value is not None and value != "":
            fields[name] = Preference(intent="required", value=value)
    options = {
        key: Preference(intent="required", value=value)
        for key, value in request.config.items()
        if re.sub(r"[^a-z0-9]", "", key.lower()) not in RESERVED_OPTIONS
    }
    return ExecutionPreferences(**fields, options=options)


def feedback_summary(
    candidate: ExecutionCandidate,
    evidence: list[OutcomeEvidence],
    policy: SelectionPolicy,
    now: datetime,
) -> dict:
    records = {
        e.id: e
        for e in evidence
        if e.candidate_key == candidate.key
        and e.validated
        and e.references
        and 0
        <= (now - e.observed_at).total_seconds()
        <= policy.feedback_max_age_days * 86400
    }
    outcomes = [e for e in records.values() if e.completed is not None]
    success = sum(
        e.completed is True
        and e.tests_passed is not False
        and e.review_passed is not False
        for e in outcomes
    )
    n = len(outcomes)
    lower = None
    if n:
        p, z = success / n, 1.96
        lower = (
            p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
        ) / (1 + z * z / n)
    return {
        "samples": n,
        "successes": success,
        "confidence_lower_bound": lower,
        "task_samples": sum(e.kind == "task_outcome" for e in outcomes),
        "protocol_samples": sum(e.kind == "provider_protocol" for e in outcomes),
        "interpretation": "Observed reliability, not a learned task-competence rating",
        "oldest_at": min(
            (e.observed_at.isoformat() for e in records.values()), default=None
        ),
        "contradictory": any(
            e.completed and (e.tests_passed is False or e.review_passed is False)
            for e in outcomes
        ),
        "use": "reliability_only"
        if policy.use_feedback and n >= policy.feedback_min_samples
        else "insufficient_or_disabled",
        "cost_usd_mean": sum(
            e.cost_usd for e in records.values() if e.cost_usd is not None
        )
        / max(1, sum(e.cost_usd is not None for e in records.values()))
        if any(e.cost_usd is not None for e in records.values())
        else None,
        "latency_ms_mean": sum(
            e.latency_ms for e in records.values() if e.latency_ms is not None
        )
        / max(1, sum(e.latency_ms is not None for e in records.values()))
        if any(e.latency_ms is not None for e in records.values())
        else None,
    }


def constraint_rejections(candidate: ExecutionCandidate, constraints) -> list[str]:
    reasons = []
    for constraint in constraints:
        for key, actual in (
            ("instance_ids", candidate.instance_id),
            ("harnesses", candidate.harness),
            ("connection_ids", candidate.connection),
            ("model_providers", candidate.model_provider),
            ("models", candidate.model),
        ):
            allowed = getattr(constraint, key)
            if allowed is not None and actual not in allowed:
                reasons.append("constraint_" + key)
        if not constraint.permission_eligible:
            reasons.append("permission_constraint")
        if not constraint.ownership_eligible:
            reasons.append("ownership_constraint")
        if not set(constraint.required_tools).issubset(candidate.tools or []):
            reasons.append("tools_unsupported_or_unknown")
        if not set(constraint.modalities).issubset(candidate.modalities or []):
            reasons.append("modalities_unsupported_or_unknown")
        if constraint.min_context_tokens > (candidate.context_tokens or 0):
            reasons.append("context_unsupported_or_unknown")
        if constraint.max_cost_usd is not None and (
            candidate.estimated_cost_usd is None
            or candidate.estimated_cost_usd > constraint.max_cost_usd
        ):
            reasons.append("cost_exceeds_budget_or_unknown")
    return reasons


def revalidate_attempt_constraints(receipt: dict, constraints) -> None:
    """Check current authority without rediscovering/reselecting a native thread.

    This is not a health probe. Runtime restore and provider confirmation remain
    separate gates; capability evidence comes from the original admission.
    """
    validate_reuse(receipt)
    chosen = next(
        (
            a
            for a in receipt["alternatives"]
            if a["candidate_key"] == receipt["candidate_key"]
        ),
        {},
    )
    selected, capabilities = receipt["selected"], chosen.get("capabilities") or {}
    candidate = ExecutionCandidate(
        instance_id=selected["instance_id"],
        harness=selected["harness"],
        connection=selected["connection"],
        model_provider=selected.get("model_provider"),
        model=selected.get("model"),
        tools=capabilities.get("tools"),
        modalities=capabilities.get("modalities"),
        context_tokens=capabilities.get("context_tokens"),
        estimated_cost_usd=(chosen.get("estimates") or {}).get("cost_usd"),
        catalog_source="persisted_attempt",
        catalog_version=receipt["decision_id"],
        observed_at=datetime.now(UTC),
    )
    reasons = constraint_rejections(candidate, constraints)
    if reasons:
        raise SelectionError(
            "attempt_policy_restricted",
            "The persisted attempt no longer satisfies current authority constraints: "
            + ", ".join(sorted(set(reasons)))
            + ". Operator action is required; settings were not substituted.",
        )


def resolve_selection(
    *,
    layers: list[tuple[str, ExecutionPreferences]],
    candidates: list[ExecutionCandidate],
    policy: SelectionPolicy,
    assessment: TaskAssessment,
    constraints: list[SelectionConstraints] = (),
    evidence: list[OutcomeEvidence] = (),
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(UTC)
    merged, sources = merge_preferences([*layers, ("installation", policy.defaults)])
    authority_constraints = [
        policy.constraints,
        policy.defaults.hard_constraints,
        *constraints,
        *(p.hard_constraints for _, p in layers),
    ]
    rules = sorted(
        [
            r
            for r in policy.rules
            if r.enabled
            and all(getattr(assessment, k) in v for k, v in r.match.items())
        ],
        key=lambda r: (r.weight, r.id),
    )
    receipt = {
        "contract": VERSION,
        "policy_revision": policy.revision,
        "policy_digest": digest(policy.model_dump(mode="json")),
        "policy_snapshot": policy.model_dump(mode="json"),
        "inputs": [
            {"source": s, "preferences": p.model_dump(mode="json")} for s, p in layers
        ],
        "resolved_inputs": merged,
        "provenance": sources,
        "assessment": assessment.model_dump(mode="json"),
        "constraints": [c.model_dump(mode="json") for c in authority_constraints],
        "matched_rules": [r.id for r in rules],
        "alternatives": [],
        "provider_confirmation": {"state": "pending", "effective": None},
        "attempts": [],
        "fallback": {
            "max_attempts": policy.fallback_max_attempts,
            "connections": policy.fallback_connections,
        },
    }
    eligible = []
    for candidate in sorted(candidates, key=lambda c: c.key):
        reasons, tradeoffs, score = [], [], 0.0
        selected = {
            f: getattr(candidate, f) for f in ("harness", "connection", "model")
        }
        selected.update(
            instance_id=candidate.instance_id,
            model_provider=candidate.model_provider,
            connection_revision=candidate.connection_revision,
            native_model_provider=candidate.native_model_provider,
            reasoning=None,
            options={},
            model_state=candidate.model_state,
            model_version=candidate.model_version,
        )
        if candidate.schema_version != 1:
            reasons.append("unsupported_catalog_version")
        if candidate.readiness != "ready":
            if (
                candidate.readiness == "unknown"
                and candidate.can_attempt_unverified
                and policy.allow_unverified_start
                and assessment.risk != "high"
            ):
                tradeoffs.append("unverified_provider_start")
            else:
                reasons.append("readiness_" + candidate.readiness)
        if candidate.freshness != "fresh":
            (tradeoffs if policy.allow_stale_catalog else reasons).append(
                "catalog_" + candidate.freshness
            )
        if candidate.capacity_available is False:
            tradeoffs.append("waiting_for_capacity")
        if candidate.estimated_cost_usd is None:
            tradeoffs.append("cost_unknown")
        reasons.extend(constraint_rejections(candidate, authority_constraints))

        def apply(
            name: str,
            pref: dict,
            weight: float,
            *,
            policy_rule=False,
            candidate=candidate,
            selected=selected,
            reasons=reasons,
            tradeoffs=tradeoffs,
        ):
            nonlocal score
            if pref["intent"] in {"automatic", "inherit"}:
                return
            value = pref["value"]
            if name == "reasoning" or name.startswith("options."):
                option = (
                    candidate.reasoning
                    if name == "reasoning"
                    else candidate.options.get(name[8:], NativeOption())
                )
                supported = option.support == "supported" and value in option.values
                if supported and (
                    not policy_rule
                    or merged.get(name, {}).get("intent", "automatic") == "automatic"
                ):
                    if name == "reasoning":
                        selected["reasoning"] = value
                    else:
                        selected["options"][name[8:]] = value
            else:
                supported = selected.get(name) == value
            if supported:
                score += weight
            elif pref["intent"] == "required":
                reasons.append(f"required_{name}_incompatible_or_unknown")
            elif not policy_rule:
                tradeoffs.append(f"preferred_{name}_unavailable")

        for name, pref in merged.items():
            apply(name, pref, 1000)
        for rule in rules:
            for field in FIELDS:
                if merged[field]["intent"] == "automatic":
                    apply(
                        field,
                        getattr(rule.prefer, field).model_dump(),
                        rule.weight,
                        policy_rule=True,
                    )
            for field, pref in rule.prefer.options.items():
                apply(
                    "options." + field, pref.model_dump(), rule.weight, policy_rule=True
                )
        # Cold start favors the configured default; model names/size never score.
        if candidate.default:
            score += 1
        if candidate.readiness == "ready":
            score += 0.5
        if candidate.capacity_available is True:
            score += 0.25
        if assessment.objective == "cost" and candidate.estimated_cost_usd is not None:
            score += 1 / (1 + candidate.estimated_cost_usd)
        if (
            assessment.objective == "latency"
            and candidate.estimated_latency_ms is not None
        ):
            score += 1 / (1 + candidate.estimated_latency_ms / 1000)
        feedback = feedback_summary(candidate, list(evidence), policy, now)
        if feedback["use"] == "reliability_only":
            score += policy.feedback_weight * feedback["confidence_lower_bound"]
        alternative = {
            "candidate_key": candidate.key,
            "selected": selected,
            "eligible": not reasons,
            "rejections": sorted(set(reasons)),
            "tradeoffs": sorted(set(tradeoffs)),
            "score": score,
            "catalog": {
                "source": candidate.catalog_source,
                "version": candidate.catalog_version,
                "observed_at": candidate.observed_at.isoformat(),
                "freshness": candidate.freshness,
            },
            "health": {
                "state": candidate.readiness,
                "evidence": candidate.health_evidence,
                "capacity_available": candidate.capacity_available,
            },
            "estimates": {
                "cost_usd": candidate.estimated_cost_usd,
                "latency_ms": candidate.estimated_latency_ms,
            },
            "capabilities": {
                "reasoning": candidate.reasoning.model_dump(mode="json"),
                "options": {
                    k: v.model_dump(mode="json") for k, v in candidate.options.items()
                },
                "tools": candidate.tools,
                "modalities": candidate.modalities,
                "context_tokens": candidate.context_tokens,
            },
            "feedback": feedback,
        }
        receipt["alternatives"].append(alternative)
        if not reasons:
            eligible.append(alternative)
    if not eligible:
        receipt["state"] = "rejected"
        raise SelectionError(
            "no_compatible_execution",
            "No eligible execution matches the requested settings and hard constraints. Inspect rejections; refresh discovery or change explicit preferences.",
            receipt,
        )
    winner = min(eligible, key=lambda c: (-c["score"], c["candidate_key"]))
    receipt.update(
        state="resolved",
        selected=winner["selected"],
        candidate_key=winner["candidate_key"],
        explanation="Selected the highest deterministic policy/preference score among compatible, authorized candidates; stable tuple identity breaks ties.",
        tradeoffs=winner["tradeoffs"],
    )
    receipt["decision_id"] = digest(receipt)
    return receipt


def validate_reuse(
    receipt: dict, *, requested: ExecutionPreferences | None = None
) -> dict:
    """A persisted attempt never silently reselects after defaults or catalogs change."""
    if receipt.get("contract") != VERSION or receipt.get("state") != "resolved":
        raise SelectionError(
            "selection_version_unavailable",
            "This persisted selection needs its original compatible resolver; do not reselect it.",
        )
    if receipt.get("decision_id") != digest(
        {k: v for k, v in receipt.items() if k != "decision_id"}
    ):
        raise SelectionError(
            "selection_receipt_integrity",
            "The persisted selection receipt changed or is incomplete. Recover its original authority receipt; do not reselect it.",
        )
    if requested:
        extra_constraints = [requested.hard_constraints]
        if requested.task:
            extra_constraints.append(
                SelectionConstraints(
                    required_tools=requested.task.tools,
                    modalities=requested.task.modalities,
                    min_context_tokens=requested.task.context_tokens or 0,
                )
            )
        revalidate_attempt_constraints(receipt, extra_constraints)

        def same_preference(name, pref):
            return pref.intent == "preferred" and receipt.get(
                "resolved_inputs", {}
            ).get(name) == pref.model_dump(mode="json")

        for name in FIELDS:
            pref = getattr(requested, name)
            if (
                pref.intent in {"required", "preferred"}
                and receipt["selected"].get(name) != pref.value
                and not same_preference(name, pref)
            ):
                raise SelectionError(
                    "context_boundary_required",
                    "Existing attempts preserve selection. Use a supported between-turn setting change or a linked new attempt.",
                )
        for name, pref in requested.options.items():
            if (
                pref.intent in {"required", "preferred"}
                and receipt["selected"].get("options", {}).get(name) != pref.value
                and not same_preference("options." + name, pref)
            ):
                raise SelectionError(
                    "context_boundary_required",
                    "Native options of an existing attempt require an explicit between-turn change.",
                )
    return receipt


def receipt_in_lineage(config: dict, decision_id: str) -> bool:
    return any(
        r and r.get("decision_id") == decision_id
        for r in [
            config.get("execution_selection"),
            config.get("execution_selection_origin"),
            *config.get("execution_selection_history", []),
        ]
    )


def validate_attempt_request(config: dict, requested: ExecutionPreferences) -> None:
    """Replayed admission inputs cannot undo an audited between-turn change."""
    current = config["execution_selection"]
    for receipt in [
        current,
        config.get("execution_selection_origin"),
        *config.get("execution_selection_history", []),
    ]:
        if not receipt:
            continue
        normalized = requested.model_copy(deep=True)
        selected = receipt["selected"]
        if (
            selected.get("native_model_provider")
            and normalized.model_provider.value == selected["native_model_provider"]
        ):
            normalized.model_provider.value = selected.get("model_provider")
        try:
            validate_reuse(receipt, requested=normalized)
            return
        except SelectionError:
            pass
    validate_reuse(current, requested=requested)


def authorize_fallback(
    receipt: dict,
    alternative: dict,
    *,
    attempt: int,
    prompt_digest: str,
    original_prompt_digest: str,
) -> None:
    fallback = receipt.get("fallback") or {}
    selected = alternative.get("selected") or {}
    original = receipt.get("selected") or {}
    if (
        prompt_digest != original_prompt_digest
        or attempt < 1
        or attempt > fallback.get("max_attempts", 0)
        or selected.get("connection") not in fallback.get("connections", [])
        or selected.get("connection") != original.get("connection")
        or selected.get("harness") != original.get("harness")
        or any(
            selected.get(k) != original.get(k)
            for k in ("instance_id", "model_provider", "connection_revision")
        )
        or not alternative.get("eligible")
        or alternative not in receipt.get("alternatives", [])
    ):
        raise SelectionError(
            "fallback_not_authorized",
            "Selection cannot be recovered inside its authorized connection, harness and attempt budget. Operator action is required.",
        )


ExecutionPreferences.model_rebuild()


def selection_presentation(
    config_json: dict, *, model_id=None, mode_id=None
) -> dict | None:
    """Receipt projection using #399 confirmation, not a competing truth source."""
    from pa.acp.configuration import confirmed_session_configuration

    receipt = config_json.get("execution_selection")
    if not receipt:
        return None
    confirmed = confirmed_session_configuration(
        config_json, model_id=model_id, mode_id=mode_id
    )
    configuration = config_json.get("configuration") or {}
    selected = receipt.get("selected") or {}
    binding = config_json.get("execution_native_binding") or {}
    if binding.get("decision_id") != receipt["decision_id"]:
        binding = {}
    expected = {
        "model": selected.get("model") or binding.get("model_id"),
        "reasoning": selected.get("reasoning") or binding.get("reasoning"),
    }
    mismatches = [
        field
        for field, effective in (
            ("model", confirmed.get("model_id")),
            ("reasoning", confirmed.get("reasoning")),
        )
        if expected.get(field) is not None and expected[field] != effective
    ]
    state = configuration.get("state", "unknown")
    if state == "ready" and mismatches:
        state = "mismatch"
    from pa.execution.selection_audit import safe_evidence

    return safe_evidence(
        {
            "decision": receipt,
            "requested": selected,
            "native_default_binding": binding or None,
            "provider_confirmation": {
                "state": state,
                "effective": confirmed,
                "mismatches": mismatches,
                "confirmed_at": configuration.get("confirmed_at"),
                "model_provider_confirmation": "unknown",
                "connection_confirmation": "configured_routing",
            },
            "pending_changes": config_json.get("execution_pending_settings"),
            "blocked": config_json.get("execution_selection_block"),
            "context_boundary_from": {
                k: v
                for k, v in (
                    config_json.get("execution_context_boundary_from") or {}
                ).items()
                if k not in {"selection", "saved_excerpt"}
            },
            "context_boundary_to": {
                k: v
                for k, v in (
                    config_json.get("execution_context_boundary") or {}
                ).items()
                if k not in {"selection", "saved_excerpt"}
            },
            "attempts": [
                *configuration.get("history", []),
                {k: v for k, v in configuration.items() if k != "history"},
            ],
        }
    )
