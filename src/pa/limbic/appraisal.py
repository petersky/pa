from __future__ import annotations

import queue
import re
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from pa.domain.models import CardEvent, EventType
from pa.limbic.models import (
    URGENCY_RANK,
    Appraisal,
    AppraisalDiagnostic,
    AppraisalResult,
    ControlAuthority,
    ControlEvent,
    Novelty,
    ProcessingPath,
    ReplayCase,
    ReplayCaseResult,
    ReplayReport,
    RouteDecision,
    Sensitivity,
    SignalEnvelope,
    Urgency,
    Valence,
    VerifiedControlProvenance,
)
from pa.limbic.projection import find_signal_by_dedupe

AppraisalProvider = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class _EmergencyRule:
    bypass: str
    risk_classes: tuple[str, ...]
    authorities: frozenset[ControlAuthority]
    wake: tuple[str, ...] = ("goal_supervisor", "notification_service")
    allowed_actions: tuple[str, ...] = (
        "apply_pre_authorized_emergency_policy",
        "notify",
    )


_EMERGENCY_RULES = {
    ControlEvent.SECURITY_REVOCATION: _EmergencyRule(
        "security_revocation",
        ("security", "authorization"),
        frozenset({ControlAuthority.INTEGRATION, ControlAuthority.AUTHORITY}),
    ),
    ControlEvent.OPERATOR_STOP: _EmergencyRule(
        "operator_stop",
        ("operator_control",),
        frozenset({ControlAuthority.OPERATOR, ControlAuthority.AUTHORITY}),
    ),
    ControlEvent.DATA_INTEGRITY_ALARM: _EmergencyRule(
        "data_integrity_alarm",
        ("data_integrity",),
        frozenset({ControlAuthority.INTEGRATION, ControlAuthority.AUTHORITY}),
    ),
    ControlEvent.LEASE_FENCING: _EmergencyRule(
        "lease_fencing",
        ("lease", "concurrency"),
        frozenset({ControlAuthority.AUTHORITY}),
    ),
    ControlEvent.HARD_RESOURCE_LIMIT: _EmergencyRule(
        "hard_resource_limit",
        ("resource_limit",),
        frozenset({ControlAuthority.AUTHORITY}),
    ),
}

_PRIVILEGED_EVENT_ALIASES = {
    "security_revocation",
    "security_revoked",
    "operator_stop",
    "explicit_operator_stop",
    "data_integrity_alarm",
    "lease_fenced",
    "lease_fencing",
    "hard_resource_limit",
    "resource_limit",
}
_MODEL_FIELDS = {
    "salience",
    "urgency",
    "valence",
    "novelty",
    "confidence",
    "intent",
    "risk_classes",
    "recommended_path",
    "reason",
    "evaluator_version",
}
_MODEL_PRIVILEGED_FIELDS = {
    "allowed_actions",
    "deterministic_bypass",
    "wake",
}


def _diagnostic(code: str, category: str) -> AppraisalDiagnostic:
    return AppraisalDiagnostic(code=code, category=category, redacted=True)


def _safe_label(value: Any, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value)[:256].strip().lower()).strip("_")
    return normalized[:80] or fallback


class LimbicService:
    """Deterministic-first appraisal with a bounded optional model supplement."""

    evaluator_version = "limbic-rules-v2"

    def __init__(
        self,
        store,
        instance_id: str,
        *,
        provider: AppraisalProvider | None = None,
        provider_timeout_seconds: float = 0.25,
        circuit_failure_threshold: int = 1,
        circuit_open_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.provider = provider
        self.provider_timeout_seconds = max(0.001, provider_timeout_seconds)
        self.circuit_failure_threshold = max(1, circuit_failure_threshold)
        self.circuit_open_seconds = max(0.001, circuit_open_seconds)
        self._monotonic = monotonic
        self._provider_lock = threading.Lock()
        self._provider_failures = 0
        self._provider_open_until = 0.0
        self._provider_probe_active = False

    def appraise(
        self,
        signal: SignalEnvelope,
        *,
        shadow_mode: bool = False,
        persist: bool = True,
        control_provenance: VerifiedControlProvenance | None = None,
    ) -> AppraisalResult:
        caller_claimed_trust = bool(
            signal.trusted_control or signal.control_provenance != "untrusted"
        )
        provenance = control_provenance or VerifiedControlProvenance()
        emergency_rule = self._emergency_rule(provenance)

        # The canonical event class for a verified control is selected from the
        # server-created provenance object, never from the signal body.
        if emergency_rule and provenance.control_event:
            signal = signal.model_copy(
                deep=True,
                update={"event_class": provenance.control_event.value},
            )
        signal = signal.canonicalized_for(provenance)

        if persist:
            duplicate = find_signal_by_dedupe(
                self.store, signal.realm_id, signal.dedupe_key
            )
            if duplicate and duplicate.get("route"):
                return AppraisalResult(
                    signal=duplicate["signal"],
                    appraisal=duplicate["appraisal"],
                    route=duplicate["route"],
                    deduplicated=True,
                )

        diagnostics: list[AppraisalDiagnostic] = []
        if caller_claimed_trust or (
            signal.event_class in _PRIVILEGED_EVENT_ALIASES and not emergency_rule
        ):
            diagnostics.append(_diagnostic("control_provenance_spoof", "security"))
        if provenance.control_event and not emergency_rule:
            diagnostics.append(_diagnostic("control_provenance_rejected", "security"))

        baseline = self._deterministic_appraisal(signal, emergency_rule, diagnostics)
        model_appraisal, provider_diagnostic = self._model_appraisal(signal, baseline)
        if provider_diagnostic:
            baseline = baseline.model_copy(
                update={"diagnostics": [*baseline.diagnostics, provider_diagnostic]}
            )
        if shadow_mode and model_appraisal:
            baseline.shadow = model_appraisal
            appraisal = baseline
        elif model_appraisal:
            appraisal = self._conservative_merge(baseline, model_appraisal)
        else:
            appraisal = baseline
        route = self._route(signal, appraisal, emergency_rule)
        result = AppraisalResult(signal=signal, appraisal=appraisal, route=route)
        if persist:
            event_payload = result.model_dump(mode="json", exclude={"deduplicated"})
            event_payload["signal"]["content"] = "[redacted]"
            event_payload["signal"]["metadata"] = signal.appraisal_features()[
                "metadata"
            ]
            self.store.commit_event(
                CardEvent(
                    type=EventType.LIMBIC_APPRAISED,
                    realm_id=signal.realm_id,
                    author_principal="system:limbic",
                    author_instance=self.instance_id,
                    payload=event_payload,
                )
            )
        return result

    @staticmethod
    def _emergency_rule(
        provenance: VerifiedControlProvenance,
    ) -> _EmergencyRule | None:
        if not provenance.control_event:
            return None
        rule = _EMERGENCY_RULES.get(provenance.control_event)
        if not rule or provenance.authority not in rule.authorities:
            return None
        return rule

    def _deterministic_appraisal(
        self,
        signal: SignalEnvelope,
        emergency_rule: _EmergencyRule | None,
        diagnostics: list[AppraisalDiagnostic],
    ) -> Appraisal:
        features = signal.appraisal_features()
        if emergency_rule:
            return Appraisal(
                signal_id=signal.id,
                salience=1,
                urgency=Urgency.CRITICAL,
                valence=Valence.RISK,
                novelty=Novelty.NEW,
                confidence=1,
                goal_refs=signal.goal_refs,
                intent=emergency_rule.bypass,
                risk_classes=list(emergency_rule.risk_classes),
                recommended_path=ProcessingPath.BYPASS,
                wake=list(emergency_rule.wake),
                dedupe_key=signal.dedupe_key,
                reason=f"Authenticated server rule selected {emergency_rule.bypass}",
                evaluator="rules",
                evaluator_version=self.evaluator_version,
                deterministic_bypass=emergency_rule.bypass,
                input_features=features,
                diagnostics=diagnostics,
            )
        if any(item.category == "security" for item in diagnostics):
            return Appraisal(
                signal_id=signal.id,
                salience=0.95,
                urgency=Urgency.HIGH,
                valence=Valence.RISK,
                novelty=Novelty.NEW,
                confidence=0.99,
                goal_refs=signal.goal_refs,
                intent="untrusted_control_spoof",
                risk_classes=["control_spoof", "untrusted_control"],
                recommended_path=ProcessingPath.SLOW,
                wake=["goal_supervisor"],
                dedupe_key=signal.dedupe_key,
                reason="Unverified control input requires policy deliberation",
                evaluator="rules",
                evaluator_version=self.evaluator_version,
                input_features=features,
                diagnostics=diagnostics,
            )

        text = signal.content.lower()
        try:
            failures = int(signal.metadata.get("failure_count", 0) or 0)
        except TypeError, ValueError:
            failures = 0
        injection = any(
            marker in text
            for marker in (
                "ignore previous instructions",
                "reveal system prompt",
                "bypass policy",
            )
        )
        explicit_slow = bool(
            signal.metadata.get("deep_review")
        ) or signal.event_class in {
            "approval_request",
            "operator_correction",
            "production_change",
        }
        safe_fast = signal.event_class in {
            "status_query",
            "health_observation",
            "receipt_requested",
            "duplicate_notification",
        }
        durable_queue = signal.event_class in {
            "event_storm",
            "rate_limited",
            "scheduled_work",
        }
        common = {
            "signal_id": signal.id,
            "goal_refs": signal.goal_refs,
            "dedupe_key": signal.dedupe_key,
            "evaluator": "rules",
            "evaluator_version": self.evaluator_version,
            "input_features": features,
            "diagnostics": diagnostics,
        }
        if injection:
            return Appraisal(
                salience=0.95,
                urgency=Urgency.HIGH,
                valence=Valence.RISK,
                novelty=Novelty.NEW,
                confidence=0.99,
                intent="untrusted_prompt_injection",
                risk_classes=["prompt_injection", "untrusted_content"],
                recommended_path=ProcessingPath.SLOW,
                wake=["goal_supervisor"],
                reason="Untrusted content contains a prompt-injection marker",
                **common,
            )
        if failures >= 3 or explicit_slow:
            return Appraisal(
                salience=0.85,
                urgency=Urgency.HIGH,
                valence=Valence.RISK,
                novelty=Novelty.NEW,
                confidence=0.9,
                intent=signal.event_class,
                risk_classes=["repeated_failure"]
                if failures >= 3
                else ["consequential"],
                recommended_path=ProcessingPath.SLOW,
                wake=["goal_supervisor"],
                reason="Consequential or repeatedly failing work requires deliberation",
                **common,
            )
        if durable_queue:
            return Appraisal(
                salience=0.5,
                urgency=Urgency.NORMAL,
                valence=Valence.ROUTINE,
                novelty=Novelty.EXPECTED,
                confidence=0.95,
                intent=signal.event_class,
                risk_classes=["backpressure"],
                recommended_path=ProcessingPath.QUEUE,
                wake=[],
                reason="Backpressure policy requires durable coalescing and queueing",
                **common,
            )
        if safe_fast:
            return Appraisal(
                salience=0.35,
                urgency=Urgency.NORMAL,
                valence=Valence.ROUTINE,
                novelty=Novelty.EXPECTED,
                confidence=0.95,
                intent=signal.event_class,
                risk_classes=[],
                recommended_path=ProcessingPath.FAST,
                wake=[],
                reason="Known low-consequence event has a bounded deterministic handler",
                **common,
            )
        return Appraisal(
            salience=0.6,
            urgency=Urgency.NORMAL,
            valence=Valence.RISK,
            novelty=Novelty.UNKNOWN,
            confidence=0.55,
            intent=signal.event_class,
            risk_classes=["unclassified"],
            recommended_path=ProcessingPath.SLOW,
            wake=["goal_supervisor"],
            reason="Unknown or uncertain input is conservatively escalated",
            **common,
        )

    def _model_appraisal(
        self, signal: SignalEnvelope, baseline: Appraisal
    ) -> tuple[Appraisal | None, AppraisalDiagnostic | None]:
        if (
            not self.provider
            or baseline.deterministic_bypass
            or signal.sensitivity in {Sensitivity.CONFIDENTIAL, Sensitivity.RESTRICTED}
        ):
            return None, None

        raw, failure_code = self._call_provider(signal.provider_features())
        if failure_code:
            return None, _diagnostic(failure_code, "provider")

        try:
            if type(raw) is not dict:
                raise TypeError("provider result must be an object")
            keys = set(raw)
            if keys & _MODEL_PRIVILEGED_FIELDS or keys - _MODEL_FIELDS:
                self._record_provider_failure()
                return None, _diagnostic("provider_output_rejected", "provider")
            if raw.get("recommended_path") == ProcessingPath.BYPASS:
                self._record_provider_failure()
                return None, _diagnostic("provider_output_rejected", "provider")
            risk_classes = raw.get("risk_classes") or []
            if not isinstance(risk_classes, list):
                raise TypeError("risk classes must be a list")
            return (
                Appraisal(
                    signal_id=signal.id,
                    goal_refs=signal.goal_refs,
                    dedupe_key=signal.dedupe_key,
                    evaluator="model",
                    evaluator_version=_safe_label(
                        raw.get("evaluator_version", "model_v1"), "model_v1"
                    ),
                    model_used=True,
                    input_features=signal.appraisal_features(),
                    salience=raw["salience"],
                    urgency=raw["urgency"],
                    valence=raw["valence"],
                    novelty=raw["novelty"],
                    confidence=raw["confidence"],
                    intent=_safe_label(raw.get("intent"), "model_assessment"),
                    risk_classes=sorted(
                        {_safe_label(item, "model_risk") for item in risk_classes[:20]}
                    ),
                    recommended_path=raw["recommended_path"],
                    wake=[],
                    reason="Model supplement accepted after policy validation",
                ),
                None,
            )
        # Provider-owned objects must never escape the deterministic fallback.
        except Exception:  # noqa: BLE001
            self._record_provider_failure()
            return None, _diagnostic("provider_output_malformed", "provider")

    def _call_provider(
        self, features: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str | None]:
        with self._provider_lock:
            now = self._monotonic()
            if now < self._provider_open_until or self._provider_probe_active:
                return None, "provider_circuit_open"
            self._provider_probe_active = True

        outcomes: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            # Network/provider implementations use heterogeneous exception types.
            try:
                outcomes.put((True, self.provider(features)), block=False)  # type: ignore[misc]
            except Exception:  # noqa: BLE001
                outcomes.put((False, None), block=False)

        threading.Thread(
            target=invoke,
            name="pa-limbic-provider",
            daemon=True,
        ).start()
        try:
            ok, value = outcomes.get(timeout=self.provider_timeout_seconds)
        except queue.Empty:
            self._record_provider_failure()
            return None, "provider_timeout"
        if not ok:
            self._record_provider_failure()
            return None, "provider_error"
        with self._provider_lock:
            self._provider_failures = 0
            self._provider_open_until = 0.0
            self._provider_probe_active = False
        return value, None

    def _record_provider_failure(self) -> None:
        with self._provider_lock:
            self._provider_failures += 1
            if self._provider_failures >= self.circuit_failure_threshold:
                self._provider_open_until = (
                    self._monotonic() + self.circuit_open_seconds
                )
            self._provider_probe_active = False

    @staticmethod
    def _conservative_merge(baseline: Appraisal, model: Appraisal) -> Appraisal:
        # A model may escalate but never downgrade deterministic risk or urgency,
        # choose bypass, wake targets, or executable actions.
        path = (
            ProcessingPath.SLOW
            if ProcessingPath.SLOW
            in {baseline.recommended_path, model.recommended_path}
            else baseline.recommended_path
        )
        urgency = max(
            (baseline.urgency, model.urgency), key=lambda item: URGENCY_RANK[item]
        )
        return model.model_copy(
            update={
                "signal_id": baseline.signal_id,
                "goal_refs": baseline.goal_refs,
                "dedupe_key": baseline.dedupe_key,
                "recommended_path": path,
                "urgency": urgency,
                "risk_classes": sorted(
                    set(baseline.risk_classes) | set(model.risk_classes)
                ),
                "wake": baseline.wake,
                "deterministic_bypass": baseline.deterministic_bypass,
                "confidence": min(baseline.confidence, model.confidence),
                "reason": f"{baseline.reason}; model supplement passed policy validation",
                "diagnostics": baseline.diagnostics,
            }
        )

    @staticmethod
    def _route(
        signal: SignalEnvelope,
        appraisal: Appraisal,
        emergency_rule: _EmergencyRule | None,
    ) -> RouteDecision:
        if emergency_rule:
            return RouteDecision(
                signal_id=signal.id,
                appraisal_id=appraisal.id,
                path=ProcessingPath.BYPASS,
                preliminary=False,
                allowed_actions=list(emergency_rule.allowed_actions),
                wake=list(emergency_rule.wake),
                reason=appraisal.reason,
            )
        if appraisal.recommended_path == ProcessingPath.FAST:
            return RouteDecision(
                signal_id=signal.id,
                appraisal_id=appraisal.id,
                path=ProcessingPath.FAST,
                preliminary=True,
                allowed_actions=[
                    "read_authoritative_state",
                    "acknowledge",
                    "notify",
                ],
                wake=[],
                reason="Fast handling is bounded, reversible, and explicitly preliminary",
            )
        if appraisal.recommended_path == ProcessingPath.QUEUE:
            return RouteDecision(
                signal_id=signal.id,
                appraisal_id=appraisal.id,
                path=ProcessingPath.QUEUE,
                preliminary=False,
                allowed_actions=["coalesce", "enqueue", "notify"],
                wake=[],
                reason="Backpressure policy selected a durable queue",
            )
        return RouteDecision(
            signal_id=signal.id,
            appraisal_id=appraisal.id,
            path=ProcessingPath.SLOW,
            preliminary=False,
            allowed_actions=[
                "reconstruct_context",
                "deliberate",
                "request_authorization",
            ],
            wake=[target for target in appraisal.wake if target == "goal_supervisor"],
            reason="Uncertainty or consequence requires policy-authorized deliberation",
        )

    def evaluate(self, cases: Iterable[ReplayCase | dict[str, Any]]) -> ReplayReport:
        results: list[ReplayCaseResult] = []
        true_positive = true_negative = false_positive = false_negative = 0
        invalid = 0
        for position, candidate in enumerate(cases):
            try:
                case = (
                    candidate
                    if isinstance(candidate, ReplayCase)
                    else ReplayCase.model_validate(candidate)
                )
            except TypeError, ValueError, ValidationError:
                invalid += 1
                results.append(
                    ReplayCaseResult(
                        name=f"invalid_case_{position}",
                        matched=False,
                        reasons=["invalid_case"],
                    )
                )
                continue

            actual_result = self.appraise(case.signal, persist=False)
            actual = actual_result.appraisal
            actual_path = actual_result.route.path
            reasons: list[str] = []
            if actual_path != case.expected_path:
                reasons.append("path")
            if actual.deterministic_bypass != case.expected_bypass:
                reasons.append("bypass")
            if case.expected_urgency and actual.urgency != case.expected_urgency:
                reasons.append("urgency")

            expected_escalation = case.expected_path in {
                ProcessingPath.SLOW,
                ProcessingPath.BYPASS,
            }
            actual_escalation = actual_path in {
                ProcessingPath.SLOW,
                ProcessingPath.BYPASS,
            }
            if expected_escalation and actual_escalation:
                outcome = "true_positive"
                true_positive += 1
            elif not expected_escalation and not actual_escalation:
                outcome = "true_negative"
                true_negative += 1
            elif expected_escalation:
                outcome = "false_negative"
                false_negative += 1
            else:
                outcome = "false_positive"
                false_positive += 1
            results.append(
                ReplayCaseResult(
                    name=case.name,
                    expected_path=case.expected_path,
                    actual_path=actual_path,
                    expected_bypass=case.expected_bypass,
                    actual_bypass=actual.deterministic_bypass,
                    expected_urgency=case.expected_urgency,
                    actual_urgency=actual.urgency,
                    matched=not reasons,
                    escalation_outcome=outcome,
                    reasons=reasons,
                )
            )

        valid_total = len(results) - invalid
        matched_count = sum(item.matched for item in results)
        status = "invalid" if invalid else ("no_data" if not results else "valid")
        return ReplayReport(
            evaluator_version=self.evaluator_version,
            status=status,
            total=len(results),
            matched=matched_count,
            accuracy=(
                matched_count / valid_total if valid_total and not invalid else None
            ),
            true_positives=true_positive,
            true_negatives=true_negative,
            false_positives=false_positive,
            false_negatives=false_negative,
            missed_escalations=false_negative,
            false_escalations=false_positive,
            invalid_cases=invalid,
            cases=results,
        )
