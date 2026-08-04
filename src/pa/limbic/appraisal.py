from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from pa.domain.models import CardEvent, EventType
from pa.limbic.models import (
    URGENCY_RANK,
    Appraisal,
    AppraisalResult,
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
)
from pa.limbic.projection import find_signal_by_dedupe

AppraisalProvider = Callable[[dict[str, Any]], dict[str, Any]]

_BYPASSES = {
    "security_revocation": ("security_revocation", ["security", "authorization"]),
    "security_revoked": ("security_revocation", ["security", "authorization"]),
    "operator_stop": ("operator_stop", ["operator_control"]),
    "explicit_operator_stop": ("operator_stop", ["operator_control"]),
    "data_integrity_alarm": ("data_integrity_alarm", ["data_integrity"]),
    "lease_fenced": ("lease_fencing", ["lease", "concurrency"]),
    "lease_fencing": ("lease_fencing", ["lease", "concurrency"]),
    "hard_resource_limit": ("hard_resource_limit", ["resource_limit"]),
    "resource_limit": ("hard_resource_limit", ["resource_limit"]),
}


class LimbicService:
    """Deterministic-first appraisal with a bounded optional model supplement."""

    evaluator_version = "limbic-rules-v1"

    def __init__(
        self,
        store,
        instance_id: str,
        *,
        provider: AppraisalProvider | None = None,
    ) -> None:
        self.store = store
        self.instance_id = instance_id
        self.provider = provider

    def appraise(
        self,
        signal: SignalEnvelope,
        *,
        shadow_mode: bool = False,
        persist: bool = True,
    ) -> AppraisalResult:
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

        baseline = self._deterministic_appraisal(signal)
        model_appraisal = self._model_appraisal(signal, baseline)
        if shadow_mode and model_appraisal:
            baseline.shadow = model_appraisal
            appraisal = baseline
        elif model_appraisal:
            appraisal = self._conservative_merge(baseline, model_appraisal)
        else:
            appraisal = baseline
        route = self._route(signal, appraisal)
        result = AppraisalResult(signal=signal, appraisal=appraisal, route=route)
        if persist:
            self.store.commit_event(
                CardEvent(
                    type=EventType.LIMBIC_APPRAISED,
                    realm_id=signal.realm_id,
                    author_principal="system:limbic",
                    author_instance=self.instance_id,
                    payload=result.model_dump(mode="json", exclude={"deduplicated"}),
                )
            )
        return result

    def _deterministic_appraisal(self, signal: SignalEnvelope) -> Appraisal:
        features = signal.appraisal_features()
        if signal.event_class in _BYPASSES:
            bypass, risks = _BYPASSES[signal.event_class]
            return Appraisal(
                signal_id=signal.id,
                salience=1,
                urgency=Urgency.CRITICAL,
                valence=Valence.RISK,
                novelty=Novelty.NEW,
                confidence=1,
                goal_refs=signal.goal_refs,
                intent=signal.event_class,
                risk_classes=risks,
                recommended_path=ProcessingPath.BYPASS,
                wake=["goal_supervisor", "notification_service"],
                dedupe_key=signal.dedupe_key,
                reason=f"Mandatory deterministic handling for {bypass}",
                evaluator="rules",
                evaluator_version=self.evaluator_version,
                deterministic_bypass=bypass,
                input_features=features,
            )

        text = signal.content.lower()
        failures = int(signal.metadata.get("failure_count", 0) or 0)
        injection = any(
            marker in text
            for marker in (
                "ignore previous instructions",
                "reveal system prompt",
                "bypass policy",
            )
        )
        explicit_slow = bool(signal.metadata.get("deep_review")) or signal.event_class in {
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
        if injection:
            return Appraisal(
                signal_id=signal.id, salience=0.95, urgency=Urgency.HIGH,
                valence=Valence.RISK, novelty=Novelty.NEW, confidence=0.99,
                goal_refs=signal.goal_refs, intent="untrusted_prompt_injection",
                risk_classes=["prompt_injection", "untrusted_content"],
                recommended_path=ProcessingPath.SLOW,
                wake=["goal_supervisor"], dedupe_key=signal.dedupe_key,
                reason="Untrusted content contains a prompt-injection marker",
                evaluator="rules", evaluator_version=self.evaluator_version,
                input_features=features,
            )
        if failures >= 3 or explicit_slow:
            return Appraisal(
                signal_id=signal.id, salience=0.85, urgency=Urgency.HIGH,
                valence=Valence.RISK, novelty=Novelty.NEW, confidence=0.9,
                goal_refs=signal.goal_refs, intent=signal.event_class,
                risk_classes=["repeated_failure"] if failures >= 3 else ["consequential"],
                recommended_path=ProcessingPath.SLOW,
                wake=["goal_supervisor"], dedupe_key=signal.dedupe_key,
                reason="Consequential or repeatedly failing work requires deliberation",
                evaluator="rules", evaluator_version=self.evaluator_version,
                input_features=features,
            )
        if durable_queue:
            return Appraisal(
                signal_id=signal.id,
                salience=0.5,
                urgency=Urgency.NORMAL,
                valence=Valence.ROUTINE,
                novelty=Novelty.EXPECTED,
                confidence=0.95,
                goal_refs=signal.goal_refs,
                intent=signal.event_class,
                risk_classes=["backpressure"],
                recommended_path=ProcessingPath.QUEUE,
                wake=[],
                dedupe_key=signal.dedupe_key,
                reason="Backpressure policy requires durable coalescing and queueing",
                evaluator="rules",
                evaluator_version=self.evaluator_version,
                input_features=features,
            )
        if safe_fast:
            return Appraisal(
                signal_id=signal.id, salience=0.35, urgency=Urgency.NORMAL,
                valence=Valence.ROUTINE, novelty=Novelty.EXPECTED, confidence=0.95,
                goal_refs=signal.goal_refs, intent=signal.event_class,
                risk_classes=[], recommended_path=ProcessingPath.FAST,
                wake=[], dedupe_key=signal.dedupe_key,
                reason="Known low-consequence event has a bounded deterministic handler",
                evaluator="rules", evaluator_version=self.evaluator_version,
                input_features=features,
            )
        return Appraisal(
            signal_id=signal.id, salience=0.6, urgency=Urgency.NORMAL,
            valence=Valence.RISK, novelty=Novelty.UNKNOWN, confidence=0.55,
            goal_refs=signal.goal_refs, intent=signal.event_class,
            risk_classes=["unclassified"], recommended_path=ProcessingPath.SLOW,
            wake=["goal_supervisor"], dedupe_key=signal.dedupe_key,
            reason="Unknown or uncertain input is conservatively escalated",
            evaluator="rules", evaluator_version=self.evaluator_version,
            input_features=features,
        )

    def _model_appraisal(
        self, signal: SignalEnvelope, baseline: Appraisal
    ) -> Appraisal | None:
        if (
            not self.provider
            or baseline.deterministic_bypass
            or signal.sensitivity in {Sensitivity.CONFIDENTIAL, Sensitivity.RESTRICTED}
        ):
            return None
        try:
            raw = self.provider(signal.appraisal_features())
            return Appraisal(
                signal_id=signal.id,
                goal_refs=signal.goal_refs,
                dedupe_key=signal.dedupe_key,
                evaluator="model",
                evaluator_version=str(raw.pop("evaluator_version", "model-v1")),
                model_used=True,
                input_features=signal.appraisal_features(),
                **raw,
            )
        except (TypeError, ValueError, KeyError):
            return None

    @staticmethod
    def _conservative_merge(baseline: Appraisal, model: Appraisal) -> Appraisal:
        # A model may escalate but never downgrade deterministic risk or urgency.
        path = (
            ProcessingPath.SLOW
            if ProcessingPath.SLOW in {baseline.recommended_path, model.recommended_path}
            else baseline.recommended_path
        )
        urgency = max(
            (baseline.urgency, model.urgency), key=lambda item: URGENCY_RANK[item]
        )
        return model.model_copy(
            update={
                "recommended_path": path,
                "urgency": urgency,
                "risk_classes": sorted(
                    set(baseline.risk_classes) | set(model.risk_classes)
                ),
                "wake": sorted(set(baseline.wake) | set(model.wake)),
                "confidence": min(baseline.confidence, model.confidence),
                "reason": f"{baseline.reason}; model supplement: {model.reason}",
            }
        )

    @staticmethod
    def _route(signal: SignalEnvelope, appraisal: Appraisal) -> RouteDecision:
        if appraisal.deterministic_bypass:
            return RouteDecision(
                signal_id=signal.id, appraisal_id=appraisal.id,
                path=ProcessingPath.BYPASS, preliminary=False,
                allowed_actions=["apply_pre_authorized_emergency_policy", "notify"],
                wake=appraisal.wake, reason=appraisal.reason,
            )
        if appraisal.recommended_path == ProcessingPath.FAST:
            return RouteDecision(
                signal_id=signal.id, appraisal_id=appraisal.id,
                path=ProcessingPath.FAST, preliminary=True,
                allowed_actions=["read_authoritative_state", "acknowledge", "notify", "wake"],
                wake=appraisal.wake,
                reason="Fast handling is bounded, reversible, and explicitly preliminary",
            )
        if appraisal.recommended_path == ProcessingPath.QUEUE:
            return RouteDecision(
                signal_id=signal.id,
                appraisal_id=appraisal.id,
                path=ProcessingPath.QUEUE,
                preliminary=False,
                allowed_actions=["coalesce", "enqueue", "notify"],
                wake=appraisal.wake,
                reason="Backpressure policy selected a durable queue",
            )
        return RouteDecision(
            signal_id=signal.id, appraisal_id=appraisal.id,
            path=ProcessingPath.SLOW, preliminary=False,
            allowed_actions=["reconstruct_context", "deliberate", "request_authorization"],
            wake=appraisal.wake,
            reason="Uncertainty or consequence requires policy-authorized deliberation",
        )

    def evaluate(self, cases: Iterable[ReplayCase]) -> ReplayReport:
        results: list[ReplayCaseResult] = []
        missed = false = 0
        for case in cases:
            actual = self.appraise(case.signal, persist=False).appraisal
            reasons: list[str] = []
            if actual.recommended_path != case.expected_path:
                reasons.append("path")
            if actual.deterministic_bypass != case.expected_bypass:
                reasons.append("bypass")
            if case.expected_urgency and actual.urgency != case.expected_urgency:
                reasons.append("urgency")
            matched = not reasons
            if case.expected_path in {ProcessingPath.SLOW, ProcessingPath.BYPASS} and actual.recommended_path == ProcessingPath.FAST:
                missed += 1
            if case.expected_path == ProcessingPath.FAST and actual.recommended_path != ProcessingPath.FAST:
                false += 1
            results.append(
                ReplayCaseResult(
                    name=case.name, expected_path=case.expected_path,
                    actual_path=actual.recommended_path, matched=matched, reasons=reasons,
                )
            )
        matched_count = sum(item.matched for item in results)
        return ReplayReport(
            evaluator_version=self.evaluator_version, total=len(results),
            matched=matched_count,
            accuracy=matched_count / len(results) if results else 1,
            missed_escalations=missed, false_escalations=false, cases=results,
        )
