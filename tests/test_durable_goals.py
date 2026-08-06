from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

from pa.domain.models import CardEvent, EventType
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

    def test_legacy_blank_graph_ids_migrate_deterministically_across_rebuilds(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, replica = self._pair(tmp)
            goal_id = "legacy-blank-reference-goal"
            stamp = "2026-01-02T03:04:05Z"
            legacy_goal = {
                "id": goal_id,
                "parent_goal_id": " ",
                "objective": "Preserve a legacy goal graph without blank ids",
                "policy": {"effective_at": stamp},
                "lease": {
                    "holder_instance_id": " ",
                    "claim_id": "\t",
                    "eligible_instance_ids": ["", "instance-a"],
                },
                "linked_card_ids": ["", "legacy-card"],
                "linked_dispatch_ids": [" ", "legacy-dispatch"],
                "supervision": {
                    "controller_session_id": " ",
                    "replacement_session_ids": ["", "legacy-session"],
                },
                "created_at": stamp,
                "updated_at": stamp,
                "criteria": [
                    {
                        "id": "",
                        "description": "first criterion",
                        "verification_method": "legacy replay",
                        "evidence_requirement": "first evidence",
                        "evidence_ids": [""],
                    },
                    {
                        "id": "   ",
                        "description": "second criterion",
                        "verification_method": "replica rebuild",
                        "evidence_requirement": "second evidence",
                        "evidence_ids": ["\t"],
                    },
                ],
                "evidence": [
                    {
                        "id": "",
                        "criterion_ids": [""],
                        "kind": "test",
                        "summary": "first legacy observation",
                        "observed_at": stamp,
                        "recorded_by_principal": " ",
                        "recorded_by_instance_id": "\t",
                        "producer_service_id": " ",
                    },
                    {
                        "id": "\n",
                        "criterion_ids": [" "],
                        "kind": "test",
                        "summary": "second legacy observation",
                        "observed_at": stamp,
                    },
                ],
                "proposals": [
                    {
                        "id": "",
                        "proposer_principal": "agent:legacy",
                        "proposer_role": "coordinator",
                        "action": {
                            "kind": "create_work_package",
                            "title": "first package",
                            "objective": "verify first criterion",
                            "criterion_ids": [""],
                        },
                        "rationale": "legacy package",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                    {
                        "id": " ",
                        "proposer_principal": "agent:legacy",
                        "proposer_role": "coordinator",
                        "action": {
                            "kind": "dispatch_work_package",
                            "work_package_id": "",
                        },
                        "rationale": "legacy dispatch",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                    {
                        "id": "\t",
                        "proposer_principal": "agent:legacy",
                        "proposer_role": "verifier",
                        "action": {
                            "kind": "record_evidence",
                            "evidence": {
                                "id": "",
                                "criterion_ids": [""],
                                "kind": "test",
                                "summary": "first proposed evidence",
                                "observed_at": stamp,
                            },
                        },
                        "rationale": "first legacy evidence proposal",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                    {
                        "id": "\n",
                        "proposer_principal": "agent:legacy",
                        "proposer_role": "verifier",
                        "action": {
                            "kind": "record_evidence",
                            "evidence": {
                                "id": " ",
                                "criterion_ids": [" "],
                                "kind": "test",
                                "summary": "second proposed evidence",
                                "observed_at": stamp,
                            },
                        },
                        "rationale": "second legacy evidence proposal",
                        "expected_goal_version": 1,
                        "policy_revision": 1,
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                ],
                "work_packages": [
                    {
                        "id": "",
                        "proposal_id": "",
                        "title": "first package",
                        "objective": "verify first criterion",
                        "criterion_ids": [""],
                        "card_id": " ",
                        "preferred_instance_id": "\t",
                        "dispatch_ids": ["", "legacy-dispatch"],
                        "session_id": " ",
                        "replacement_session_ids": ["\n", "legacy-session"],
                        "executor_service_id": " ",
                        "action_reservation_id": "\t",
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                    {
                        "id": "\t",
                        "proposal_id": " ",
                        "title": "second package",
                        "objective": "verify second criterion",
                        "criterion_ids": [" "],
                        "depends_on": [""],
                        "created_at": stamp,
                        "updated_at": stamp,
                    },
                ],
                "operator_interactions": [
                    {
                        "id": "",
                        "proposal_id": "",
                        "notification_id": "notice-one",
                        "created_at": stamp,
                    },
                    {
                        "id": " ",
                        "proposal_id": " ",
                        "notification_id": "notice-two",
                        "created_at": stamp,
                    },
                ],
                "audit": {
                    "id": "",
                    "auditor_principal": "agent:legacy-verifier",
                    "auditor_instance_id": " ",
                    "verifier_service_id": "\t",
                    "verdict": "satisfied",
                    "criterion_verdicts": {
                        "": "satisfied",
                        " ": "unsatisfied",
                    },
                    "evidence_ids": ["", " "],
                    "explanation": "legacy evidence satisfied both criteria",
                    "created_at": stamp,
                },
            }
            service.store.commit_event(
                CardEvent(
                    type=EventType.GOAL_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    payload={
                        "goal": legacy_goal,
                        "goal_event": {
                            "goal_id": goal_id,
                            "event_type": "goal.legacy_imported",
                            "actor_principal": "agent:legacy",
                            "authority_instance_id": "instance-a",
                            "policy_revision": 1,
                            "idempotency_key": "legacy-blank-reference-import",
                            "version": 1,
                        },
                    },
                )
            )

            restored = service.get(goal_id)
            assert restored is not None
            replica.rebuild_from_log("default")
            rebuilt = GoalService(replica, "instance-b").get(goal_id)
            assert rebuilt is not None

            self.assertEqual(
                restored.model_dump(mode="json"), rebuilt.model_dump(mode="json")
            )
            self.assertEqual(
                restored.model_dump(mode="json"),
                service.get(goal_id).model_dump(mode="json"),
            )
            criterion_ids = [item.id for item in restored.criteria]
            evidence_ids = [item.id for item in restored.evidence]
            proposal_ids = [item.id for item in restored.proposals]
            package_ids = [item.id for item in restored.work_packages]
            interaction_ids = [item.id for item in restored.operator_interactions]
            for identifiers in (
                criterion_ids,
                evidence_ids,
                proposal_ids,
                package_ids,
                interaction_ids,
            ):
                self.assertEqual(len(identifiers), len(set(identifiers)))
                self.assertTrue(all(identifier.strip() for identifier in identifiers))
            self.assertTrue(restored.audit.id.strip())
            self.assertIsNone(restored.parent_goal_id)
            self.assertEqual(restored.linked_card_ids, ["legacy-card"])
            self.assertEqual(restored.linked_dispatch_ids, ["legacy-dispatch"])
            self.assertIsNone(restored.lease.holder_instance_id)
            self.assertIsNone(restored.lease.claim_id)
            self.assertEqual(restored.lease.eligible_instance_ids, ["instance-a"])
            self.assertIsNone(restored.supervision.controller_session_id)
            self.assertEqual(
                restored.supervision.replacement_session_ids, ["legacy-session"]
            )
            self.assertIsNone(restored.evidence[0].recorded_by_principal)
            self.assertIsNone(restored.evidence[0].recorded_by_instance_id)
            self.assertIsNone(restored.evidence[0].producer_service_id)
            self.assertEqual(restored.criteria[0].evidence_ids, [evidence_ids[0]])
            self.assertEqual(restored.criteria[1].evidence_ids, [evidence_ids[1]])
            self.assertEqual(restored.evidence[0].criterion_ids, [criterion_ids[0]])
            self.assertEqual(restored.evidence[1].criterion_ids, [criterion_ids[1]])
            self.assertEqual(restored.work_packages[0].proposal_id, proposal_ids[0])
            self.assertEqual(restored.work_packages[1].proposal_id, proposal_ids[1])
            self.assertEqual(restored.work_packages[1].depends_on, [package_ids[0]])
            self.assertEqual(
                restored.proposals[0].action.criterion_ids, [criterion_ids[0]]
            )
            self.assertEqual(
                restored.proposals[1].action.work_package_id, package_ids[0]
            )
            self.assertIsNone(restored.work_packages[0].card_id)
            self.assertIsNone(restored.work_packages[0].preferred_instance_id)
            self.assertEqual(
                restored.work_packages[0].dispatch_ids, ["legacy-dispatch"]
            )
            self.assertIsNone(restored.work_packages[0].session_id)
            self.assertEqual(
                restored.work_packages[0].replacement_session_ids,
                ["legacy-session"],
            )
            self.assertIsNone(restored.work_packages[0].executor_service_id)
            self.assertIsNone(restored.work_packages[0].action_reservation_id)
            proposed_evidence_ids = [
                restored.proposals[index].action.evidence.id for index in (2, 3)
            ]
            self.assertEqual(len(set(proposed_evidence_ids)), 2)
            self.assertTrue(set(proposed_evidence_ids).isdisjoint(evidence_ids))
            self.assertEqual(
                [item.proposal_id for item in restored.operator_interactions],
                proposal_ids[:2],
            )
            self.assertEqual(
                set(restored.audit.criterion_verdicts.values()),
                {CriterionVerdict.SATISFIED, CriterionVerdict.UNSATISFIED},
            )
            self.assertIsNone(restored.audit.auditor_instance_id)
            self.assertIsNone(restored.audit.verifier_service_id)
            self.assertEqual(restored.audit.evidence_ids, evidence_ids)

    def test_legacy_blank_top_level_goal_id_is_canonical_before_projection(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, replica = self._pair(tmp)
            canonical_id = "legacy-event-goal-id"
            stamp = "2026-01-02T03:04:05Z"
            service.store.commit_event(
                CardEvent(
                    type=EventType.GOAL_UPSERTED,
                    realm_id="default",
                    author_principal="agent:legacy",
                    author_instance="instance-a",
                    payload={
                        "goal": {
                            "id": " ",
                            "objective": "Canonicalize before indexing",
                            "criteria": [
                                {
                                    "id": "criterion-one",
                                    "description": "addressable after migration",
                                    "verification_method": "replica lookup",
                                    "evidence_requirement": "stable goal id",
                                }
                            ],
                            "policy": {"effective_at": stamp},
                            "created_at": stamp,
                            "updated_at": stamp,
                        },
                        "goal_event": {
                            "goal_id": canonical_id,
                            "event_type": "goal.legacy_imported",
                            "actor_principal": "agent:legacy",
                            "authority_instance_id": "instance-a",
                            "policy_revision": 1,
                            "idempotency_key": "legacy-blank-goal-id",
                            "version": 1,
                        },
                    },
                )
            )

            restored = service.get(canonical_id)
            assert restored is not None
            self.assertEqual(restored.id, canonical_id)
            self.assertEqual(service.get(restored.id).id, canonical_id)
            self.assertEqual([item.id for item in service.list()], [canonical_id])
            replica.rebuild_from_log("default")
            rebuilt = GoalService(replica, "instance-b")
            self.assertEqual(rebuilt.get(canonical_id).id, canonical_id)

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

    def test_idempotency_replay_binds_create_lease_and_checkpoint_operations(
        self,
    ) -> None:
        clock = datetime(2026, 8, 5, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp, clock=lambda: clock)
            criterion = GoalCriterion(
                description="operation identity is exact",
                verification_method="idempotent replay",
                evidence_requirement="one canonical mutation",
            )
            create = GoalCreate(
                objective="Bind every duplicate replay",
                criteria=[criterion],
            )
            create_context = self._ctx(0, "fingerprinted-create")
            goal = service.create(create, create_context)
            exact_create = service.create(create, create_context)
            self.assertEqual(exact_create.id, goal.id)
            with self.assertRaisesRegex(GoalConflict, "different goal operation"):
                service.create(
                    create.model_copy(update={"objective": "Changed create body"}),
                    create_context,
                )
            with self.assertRaisesRegex(GoalConflict, "different goal operation"):
                service.create(
                    create,
                    create_context.model_copy(
                        update={"actor_principal": "agent:other-creator"}
                    ),
                )

            lease_context = self._ctx(
                goal.version,
                "fingerprinted-lease",
                actor="agent:controller-a",
            )
            leased = service.acquire_lease(
                goal.id,
                lease_context,
                ttl_seconds=10,
            )
            exact_lease = service.acquire_lease(
                goal.id,
                lease_context,
                ttl_seconds=10,
            )
            self.assertEqual(exact_lease.version, leased.version)
            with self.assertRaisesRegex(GoalConflict, "different goal operation"):
                service.acquire_lease(
                    goal.id,
                    lease_context,
                    ttl_seconds=999,
                )
            with self.assertRaisesRegex(GoalConflict, "different goal operation"):
                service.acquire_lease(
                    goal.id,
                    lease_context.model_copy(
                        update={
                            "actor_principal": "agent:controller-b",
                            "authority_instance_id": "instance-b",
                        }
                    ),
                    ttl_seconds=10,
                )

            checkpoint = GoalSupervisionCheckpoint(
                criteria=leased.criteria,
                evidence=leased.evidence,
                proposals=leased.proposals,
                work_packages=leased.work_packages,
                operator_interactions=leased.operator_interactions,
                supervision=leased.supervision,
                linked_card_ids=leased.linked_card_ids,
                linked_dispatch_ids=leased.linked_dispatch_ids,
                assumptions=leased.assumptions,
                risks=leased.risks,
                strategy_revision=leased.strategy_revision,
                state=leased.state,
                progress_summary="Canonical checkpoint",
                reason="Persist one exact checkpoint",
            )
            checkpoint_context = self._ctx(
                leased.version,
                "fingerprinted-checkpoint",
                actor="agent:controller-a",
                fence=leased.lease.fencing_token,
            )
            checkpointed = service.checkpoint_supervision(
                goal.id,
                checkpoint,
                checkpoint_context,
            )
            exact_checkpoint = service.checkpoint_supervision(
                goal.id,
                checkpoint,
                checkpoint_context,
            )
            self.assertEqual(exact_checkpoint.version, checkpointed.version)
            with self.assertRaisesRegex(GoalConflict, "different goal operation"):
                service.checkpoint_supervision(
                    goal.id,
                    checkpoint.model_copy(
                        update={
                            "progress_summary": "Changed checkpoint",
                            "reason": "Changed body",
                        }
                    ),
                    checkpoint_context.model_copy(
                        update={
                            "actor_principal": "agent:controller-b",
                            "authority_instance_id": "instance-b",
                        }
                    ),
                )

            events = service.events(goal.id)
            self.assertTrue(
                all(len(event["operation_fingerprint"]) == 64 for event in events)
            )

    def test_concurrent_goal_creates_atomically_claim_the_idempotency_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            criterion = GoalCriterion(
                description="one concurrent create wins",
                verification_method="serialized mutation boundary",
                evidence_requirement="one durable goal and one event",
            )
            canonical = GoalCreate(
                objective="Concurrent exact create",
                criteria=[criterion],
            )
            exact_context = self._ctx(0, "concurrent-exact-create")
            start = Barrier(2)

            def exact_create():
                start.wait(timeout=5)
                return service.create(canonical, exact_context)

            with ThreadPoolExecutor(max_workers=2) as executor:
                exact = [executor.submit(exact_create) for _ in range(2)]
                exact_results = [future.result(timeout=10) for future in exact]

            self.assertEqual(exact_results[0].id, exact_results[1].id)
            self.assertEqual(len(service.events(exact_results[0].id)), 1)

            changed_context = self._ctx(0, "concurrent-changed-create")
            changed_start = Barrier(2)
            changed_bodies = [
                canonical.model_copy(update={"objective": "Concurrent body A"}),
                canonical.model_copy(update={"objective": "Concurrent body B"}),
            ]

            def changed_create(body: GoalCreate):
                changed_start.wait(timeout=5)
                return service.create(body, changed_context)

            results = []
            errors = []
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(changed_create, body) for body in changed_bodies
                ]
                for future in futures:
                    try:
                        results.append(future.result(timeout=10))
                    except GoalConflict as exc:
                        errors.append(exc)

            self.assertEqual(len(results), 1)
            self.assertEqual(len(errors), 1)
            self.assertIn("different goal operation", str(errors[0]))
            self.assertEqual(len(service.list()), 2)
            self.assertEqual(len(service.events(results[0].id)), 1)

    def test_legacy_idempotency_event_fails_closed_without_a_fingerprint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = self._pair(tmp)
            create = GoalCreate(
                objective="Fail closed on ambiguous legacy replay",
                criteria=[
                    GoalCriterion(
                        description="legacy replay is explicit",
                        verification_method="operation fingerprint",
                        evidence_requirement="conservative conflict",
                    )
                ],
            )
            context = self._ctx(0, "legacy-fingerprint-gap")
            service.create(create, context)
            with service.store._conn() as conn:
                conn.execute(
                    "UPDATE durable_goal_events SET operation_fingerprint='' "
                    "WHERE realm_id=? AND idempotency_key=?",
                    (create.realm_id, context.idempotency_key),
                )
            with self.assertRaisesRegex(
                GoalConflict, "legacy idempotency event.*exact operation fingerprint"
            ):
                service.create(create, context)

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
