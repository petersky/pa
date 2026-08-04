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
    GoalRevision,
    GoalState,
    GoalTransition,
    GoalWakeup,
)
from pa.goals.service import GoalConflict, GoalService
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class DurableGoalTests(unittest.TestCase):
    def _pair(self, tmp: str):
        root = Path(tmp)
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root, "instance-a")
        authority = CardProjection(root / "authority.db", log)
        replica = CardProjection(root / "replica.db", log)
        return GoalService(authority, "instance-a"), replica

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
