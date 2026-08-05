from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.projection import CardProjection
from pa.goals.models import (
    CriterionVerdict,
    EvidenceKind,
    GoalAuditCreate,
    GoalCreate,
    GoalCriterion,
    GoalEvidence,
    GoalEvidenceCreate,
    GoalMutationContext,
    GoalPolicy,
    GoalRevision,
    GoalState,
    GoalSupervisionCheckpoint,
    GoalTransition,
    GoalWakeup,
)
from pa.goals.service import GoalConflict, GoalService
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class DurableGoalTests(unittest.TestCase):
    def _pair(self, tmp: str, *, clock=None):
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        authority = CardProjection(root / "authority.db", log)
        replica = CardProjection(root / "replica.db", log)
        return GoalService(authority, "instance-a", clock=clock), replica

    @staticmethod
    def _ctx(
        version: int,
        key: str,
        *,
        instance: str = "instance-a",
        fence: int | None = None,
        actor: str = "agent:supervisor",
    ) -> GoalMutationContext:
        return GoalMutationContext(
            actor_principal=actor,
            authority_instance_id=instance,
            idempotency_key=key,
            expected_version=version,
            policy_revision=1,
            fencing_token=fence,
        )

    def test_goal_replays_to_replacement_projection_with_attributable_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, replica = self._pair(tmp)
            criterion = GoalCriterion(
                description="restart-safe",
                verification_method="fleet replay",
                evidence_requirement="replica reconstruction",
            )
            goal = service.create(
                GoalCreate(
                    objective="Survive replacement",
                    owner_principal="user:operator",
                    criteria=[criterion],
                ),
                self._ctx(0, "create"),
            )
            evidence = GoalEvidence(
                criterion_ids=[criterion.id],
                kind=EvidenceKind.TEST,
                summary="Replica replayed the exact goal",
                provenance={"instance": "instance-b"},
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(
                    evidence=evidence,
                    criterion_verdicts={criterion.id: CriterionVerdict.SATISFIED},
                ),
                self._ctx(1, "evidence"),
            )
            service.schedule_wakeup(
                goal.id,
                GoalWakeup(
                    wake_at=datetime.now(UTC) + timedelta(hours=1),
                    reason="resume fleet work",
                    eligible_instance_ids=["instance-b"],
                ),
                self._ctx(2, "wake"),
            )

            replica.rebuild_from_log("default")
            replacement = GoalService(replica, "instance-b")
            restored = replacement.get(goal.id)
            assert restored is not None
            self.assertEqual(restored.objective, "Survive replacement")
            self.assertEqual(restored.evidence[0].id, evidence.id)
            self.assertEqual(restored.wakeup.eligible_instance_ids, ["instance-b"])
            events = replacement.events(goal.id)
            self.assertEqual(
                [event["event_type"] for event in events],
                ["goal.created", "goal.evidence_recorded", "goal.wakeup_scheduled"],
            )
            self.assertTrue(all(event["policy_revision"] == 1 for event in events))
            self.assertTrue(
                all(event["authority_instance_id"] == "instance-a" for event in events)
            )

    def test_owner_and_policy_author_are_derived_from_the_mutation_actor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            criterion = GoalCriterion(
                description="identity is attributable",
                verification_method="durable projection",
                evidence_requirement="server-derived principals",
            )
            create = GoalCreate(
                objective="Ignore caller identity assertions",
                owner_principal="user:forged-owner",
                criteria=[criterion],
                policy=GoalPolicy(authored_by="user:forged-author"),
            )
            goal = service.create(create, self._ctx(0, "derived-create-identity"))
            self.assertEqual(goal.owner_principal, "agent:supervisor")
            self.assertEqual(goal.policy.authored_by, "agent:supervisor")
            self.assertEqual(create.owner_principal, "user:forged-owner")
            self.assertEqual(create.policy.authored_by, "user:forged-author")

            policy = goal.policy.model_copy(
                update={
                    "revision": 2,
                    "authored_by": "user:forged-revision-author",
                }
            )
            revised = service.revise(
                goal.id,
                GoalRevision(policy=policy, reason="Advance policy safely"),
                self._ctx(goal.version, "derived-revision-identity"),
            )
            self.assertEqual(revised.policy.authored_by, "agent:supervisor")
            self.assertEqual(policy.authored_by, "user:forged-revision-author")

    def test_controller_lease_fences_stale_instances_and_idempotent_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            criterion = GoalCriterion(
                description="only one controller",
                verification_method="fencing",
                evidence_requirement="denied stale write",
            )
            goal = service.create(
                GoalCreate(objective="Fence controllers", criteria=[criterion]),
                self._ctx(0, "create"),
            )
            leased = service.acquire_lease(
                goal.id, self._ctx(1, "lease"), ttl_seconds=120
            )
            self.assertEqual(leased.lease.fencing_token, 1)
            retried = service.acquire_lease(
                goal.id, self._ctx(1, "lease"), ttl_seconds=120
            )
            self.assertEqual(retried.version, leased.version)
            with self.assertRaisesRegex(GoalConflict, "fencing token"):
                service.transition(
                    goal.id,
                    GoalTransition(state=GoalState.SHAPING, reason="stale controller"),
                    self._ctx(2, "stale", instance="instance-b", fence=1),
                )
            shaped = service.transition(
                goal.id,
                GoalTransition(state=GoalState.SHAPING, reason="valid controller"),
                self._ctx(2, "valid", fence=1),
            )
            self.assertEqual(shaped.state, GoalState.SHAPING)

    def test_achievement_requires_independent_complete_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            criterion = GoalCriterion(
                description="audited outcome",
                verification_method="independent review",
                evidence_requirement="test output",
            )
            goal = service.create(
                GoalCreate(
                    objective="Finish with proof",
                    owner_principal="user:operator",
                    criteria=[criterion],
                ),
                self._ctx(0, "create"),
            )
            for version, state in enumerate(
                (GoalState.READY, GoalState.ACTIVE, GoalState.VERIFYING), start=1
            ):
                goal = service.transition(
                    goal.id,
                    GoalTransition(state=state, reason="advance"),
                    self._ctx(version, f"state-{state.value}"),
                )
            with self.assertRaisesRegex(GoalConflict, "audit"):
                service.transition(
                    goal.id,
                    GoalTransition(state=GoalState.ACHIEVED, reason="claim"),
                    self._ctx(4, "premature"),
                )
            evidence = GoalEvidence(
                criterion_ids=[criterion.id],
                kind=EvidenceKind.TEST,
                summary="Focused regression passed",
            )
            goal = service.add_evidence(
                goal.id, GoalEvidenceCreate(evidence=evidence), self._ctx(4, "evidence")
            )
            audit = GoalAuditCreate(
                auditor_principal="agent:verifier",
                criterion_verdicts={criterion.id: CriterionVerdict.SATISFIED},
                evidence_ids=[evidence.id],
                explanation="Evidence is current and directly verifies the criterion",
            )
            goal = service.audit(
                goal.id, audit, self._ctx(5, "audit", actor="agent:verifier")
            )
            achieved = service.transition(
                goal.id,
                GoalTransition(
                    state=GoalState.ACHIEVED, reason="independent audit satisfied"
                ),
                self._ctx(6, "achieve"),
            )
            self.assertEqual(achieved.state, GoalState.ACHIEVED)
            self.assertEqual(achieved.audit.verdict, CriterionVerdict.SATISFIED)

    def test_audit_rejects_stale_expired_contradictory_and_wrong_kind_evidence(
        self,
    ) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp, clock=lambda: now)
            scenarios = [
                (
                    "stale",
                    GoalCriterion(
                        description="fresh test",
                        verification_method="recent regression",
                        evidence_requirement="fresh result",
                        freshness_seconds=60,
                    ),
                    GoalEvidence(
                        criterion_ids=["placeholder"],
                        kind=EvidenceKind.TEST,
                        summary="Old result",
                        observed_at=now - timedelta(minutes=2),
                    ),
                    "stale evidence",
                ),
                (
                    "expired",
                    GoalCriterion(
                        description="unexpired test",
                        verification_method="valid artifact",
                        evidence_requirement="unexpired result",
                    ),
                    GoalEvidence(
                        criterion_ids=["placeholder"],
                        kind=EvidenceKind.TEST,
                        summary="Expired result",
                        observed_at=now - timedelta(minutes=2),
                        expires_at=now - timedelta(seconds=1),
                    ),
                    "expired evidence",
                ),
                (
                    "contradictory",
                    GoalCriterion(
                        description="consistent test",
                        verification_method="uncontradicted result",
                        evidence_requirement="consistent evidence",
                    ),
                    GoalEvidence(
                        criterion_ids=["placeholder"],
                        kind=EvidenceKind.TEST,
                        summary="Contradictory result",
                        contradictory=True,
                        observed_at=now,
                    ),
                    "contradictory evidence",
                ),
                (
                    "kind",
                    GoalCriterion(
                        description="test-kind result",
                        verification_method="test runner",
                        evidence_requirement="test evidence",
                        required_evidence_kinds=[EvidenceKind.TEST],
                    ),
                    GoalEvidence(
                        criterion_ids=["placeholder"],
                        kind=EvidenceKind.ARTIFACT,
                        summary="Artifact without a test",
                        observed_at=now,
                    ),
                    "lacks required evidence kinds",
                ),
            ]
            for index, (name, criterion, evidence, expected) in enumerate(scenarios):
                goal = service.create(
                    GoalCreate(
                        objective=f"Reject {name} evidence", criteria=[criterion]
                    ),
                    self._ctx(0, f"create-{name}"),
                )
                evidence.criterion_ids = [criterion.id]
                goal = service.add_evidence(
                    goal.id,
                    GoalEvidenceCreate(evidence=evidence),
                    self._ctx(goal.version, f"evidence-{name}"),
                )
                with (
                    self.subTest(case=name),
                    self.assertRaisesRegex(GoalConflict, expected),
                ):
                    service.audit(
                        goal.id,
                        GoalAuditCreate(
                            criterion_verdicts={
                                criterion.id: CriterionVerdict.SATISFIED
                            },
                            evidence_ids=[evidence.id],
                            explanation="Adversarial evidence policy check",
                        ),
                        self._ctx(
                            goal.version,
                            f"audit-{index}",
                            actor="agent:independent-auditor",
                        ),
                    )

    def test_achievement_rechecks_evidence_freshness_after_a_valid_audit(self) -> None:
        clock = [datetime(2026, 8, 3, tzinfo=UTC)]
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp, clock=lambda: clock[0])
            criterion = GoalCriterion(
                description="fresh through completion",
                verification_method="recent test",
                evidence_requirement="test younger than one minute",
                freshness_seconds=60,
                required_evidence_kinds=[EvidenceKind.TEST],
            )
            goal = service.create(
                GoalCreate(
                    objective="Do not achieve on stale proof", criteria=[criterion]
                ),
                self._ctx(0, "create-freshness-goal"),
            )
            evidence = GoalEvidence(
                criterion_ids=[criterion.id],
                kind=EvidenceKind.TEST,
                summary="Fresh focused suite",
                observed_at=clock[0],
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=evidence),
                self._ctx(goal.version, "fresh-evidence"),
            )
            goal = service.audit(
                goal.id,
                GoalAuditCreate(
                    criterion_verdicts={criterion.id: CriterionVerdict.SATISFIED},
                    evidence_ids=[evidence.id],
                    explanation="The evidence is fresh at audit time",
                ),
                self._ctx(
                    goal.version,
                    "fresh-audit",
                    actor="agent:independent-auditor",
                ),
            )
            for state in (GoalState.READY, GoalState.ACTIVE, GoalState.VERIFYING):
                goal = service.transition(
                    goal.id,
                    GoalTransition(state=state, reason="advance toward completion"),
                    self._ctx(goal.version, f"advance-{state.value}"),
                )
            clock[0] += timedelta(seconds=61)
            with self.assertRaisesRegex(GoalConflict, "no longer valid.*stale"):
                service.transition(
                    goal.id,
                    GoalTransition(
                        state=GoalState.ACHIEVED,
                        reason="The old audit must not be enough",
                    ),
                    self._ctx(goal.version, "reject-stale-achievement"),
                )
            with self.assertRaisesRegex(
                GoalConflict, "supervisor completion requirements failed.*stale"
            ):
                service.checkpoint_supervision(
                    goal.id,
                    GoalSupervisionCheckpoint(
                        criteria=goal.criteria,
                        evidence=goal.evidence,
                        proposals=goal.proposals,
                        work_packages=goal.work_packages,
                        operator_interactions=goal.operator_interactions,
                        supervision=goal.supervision,
                        linked_card_ids=goal.linked_card_ids,
                        linked_dispatch_ids=goal.linked_dispatch_ids,
                        assumptions=goal.assumptions,
                        risks=goal.risks,
                        strategy_revision=goal.strategy_revision,
                        state=GoalState.ACHIEVED,
                        progress_summary=goal.progress_summary,
                        reason="The checkpoint must revalidate stale evidence",
                    ),
                    self._ctx(goal.version, "reject-stale-checkpoint"),
                )

    def test_invalid_revision_is_rejected_before_it_reaches_the_event_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            first = GoalCriterion(
                description="first criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            second = GoalCriterion(
                description="second criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            goal = service.create(
                GoalCreate(
                    objective="Preserve complete invariants", criteria=[first, second]
                ),
                self._ctx(0, "create"),
            )
            evidence = GoalEvidence(
                criterion_ids=[second.id],
                kind=EvidenceKind.TEST,
                summary="Second criterion passed",
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=evidence),
                self._ctx(1, "evidence"),
            )

            with self.assertRaisesRegex(GoalConflict, "invalid goal mutation"):
                service.revise(
                    goal.id,
                    GoalRevision(
                        criteria=[first],
                        reason="Incorrectly remove a referenced criterion",
                    ),
                    self._ctx(2, "invalid-reference-revision"),
                )
            with self.assertRaisesRegex(GoalConflict, "invalid goal mutation"):
                service.revise(
                    goal.id,
                    GoalRevision(
                        objective="", reason="Incorrectly erase the objective"
                    ),
                    self._ctx(2, "invalid-objective-revision"),
                )

            unchanged = service.get(goal.id)
            assert unchanged is not None
            self.assertEqual(unchanged.version, 2)
            self.assertEqual(len(unchanged.criteria), 2)
            self.assertEqual(
                [item["event_type"] for item in service.events(goal.id)],
                ["goal.created", "goal.evidence_recorded"],
            )

    def test_evidence_ids_and_verdict_mappings_must_be_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            first = GoalCriterion(
                description="first criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            second = GoalCriterion(
                description="second criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            goal = service.create(
                GoalCreate(objective="Map evidence exactly", criteria=[first, second]),
                self._ctx(0, "create"),
            )
            evidence = GoalEvidence(
                id="evidence-one",
                criterion_ids=[first.id],
                kind=EvidenceKind.TEST,
                summary="First criterion passed",
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=evidence),
                self._ctx(1, "first-evidence"),
            )

            with self.assertRaisesRegex(GoalConflict, "evidence id already exists"):
                service.add_evidence(
                    goal.id,
                    GoalEvidenceCreate(evidence=evidence),
                    self._ctx(2, "duplicate-evidence"),
                )
            with self.assertRaisesRegex(GoalConflict, "unknown criteria"):
                service.add_evidence(
                    goal.id,
                    GoalEvidenceCreate(
                        evidence=GoalEvidence(
                            criterion_ids=[first.id],
                            kind=EvidenceKind.TEST,
                            summary="Known evidence with an unknown verdict target",
                        ),
                        criterion_verdicts={
                            "missing-criterion": CriterionVerdict.SATISFIED
                        },
                    ),
                    self._ctx(2, "unknown-verdict-criterion"),
                )
            with self.assertRaisesRegex(GoalConflict, "mapped to each criterion"):
                service.add_evidence(
                    goal.id,
                    GoalEvidenceCreate(
                        evidence=GoalEvidence(
                            criterion_ids=[first.id],
                            kind=EvidenceKind.TEST,
                            summary="Evidence does not cover the second criterion",
                        ),
                        criterion_verdicts={second.id: CriterionVerdict.SATISFIED},
                    ),
                    self._ctx(2, "unmapped-verdict-criterion"),
                )

            unchanged = service.get(goal.id)
            assert unchanged is not None
            self.assertEqual(unchanged.version, 2)
            self.assertEqual(len(unchanged.evidence), 1)

    def test_revision_cannot_break_bidirectional_evidence_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            criterion = GoalCriterion(
                description="mapped criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            goal = service.create(
                GoalCreate(objective="Keep evidence linked", criteria=[criterion]),
                self._ctx(0, "create"),
            )
            evidence = GoalEvidence(
                criterion_ids=[criterion.id],
                kind=EvidenceKind.TEST,
                summary="Mapped evidence",
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=evidence),
                self._ctx(1, "evidence"),
            )
            revised_criterion = goal.criteria[0].model_copy(deep=True)
            revised_criterion.evidence_ids = []

            with self.assertRaisesRegex(GoalConflict, "invalid goal mutation"):
                service.revise(
                    goal.id,
                    GoalRevision(
                        criteria=[revised_criterion],
                        reason="Incorrectly detach the evidence ledger",
                    ),
                    self._ctx(2, "detach-evidence"),
                )

            unchanged = service.get(goal.id)
            assert unchanged is not None
            self.assertEqual(unchanged.criteria[0].evidence_ids, [evidence.id])

    def test_audit_requires_authenticated_identity_and_per_criterion_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            first = GoalCriterion(
                description="first criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            second = GoalCriterion(
                description="second criterion",
                verification_method="test",
                evidence_requirement="test output",
            )
            goal = service.create(
                GoalCreate(
                    objective="Audit every criterion",
                    owner_principal="user:operator",
                    criteria=[first, second],
                ),
                self._ctx(0, "create"),
            )
            first_evidence = GoalEvidence(
                criterion_ids=[first.id],
                kind=EvidenceKind.TEST,
                summary="First criterion passed",
            )
            goal = service.add_evidence(
                goal.id,
                GoalEvidenceCreate(evidence=first_evidence),
                self._ctx(1, "first-evidence"),
            )
            verdicts = {
                first.id: CriterionVerdict.SATISFIED,
                second.id: CriterionVerdict.SATISFIED,
            }

            with self.assertRaisesRegex(GoalConflict, "mapped to every criterion"):
                service.audit(
                    goal.id,
                    GoalAuditCreate(
                        criterion_verdicts=verdicts,
                        evidence_ids=[first_evidence.id],
                        explanation="Second criterion has no evidence",
                    ),
                    self._ctx(2, "incomplete-audit", actor="agent:verifier"),
                )
            with self.assertRaisesRegex(GoalConflict, "authenticated mutation actor"):
                service.audit(
                    goal.id,
                    GoalAuditCreate(
                        auditor_principal="agent:spoofed",
                        criterion_verdicts=verdicts,
                        evidence_ids=[first_evidence.id],
                        explanation="Spoof a different auditor",
                    ),
                    self._ctx(2, "spoofed-audit", actor="agent:verifier"),
                )

            unchanged = service.get(goal.id)
            assert unchanged is not None
            self.assertEqual(unchanged.version, 2)
            self.assertIsNone(unchanged.audit)


if __name__ == "__main__":
    unittest.main()
