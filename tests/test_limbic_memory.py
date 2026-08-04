from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.projection import CardProjection
from pa.limbic.appraisal import LimbicService
from pa.limbic.memory import MemoryService
from pa.limbic.models import (
    Appraisal,
    MemoryMutationContext,
    MemoryProvenance,
    MemoryQuery,
    MemoryRecord,
    MemoryTier,
    Novelty,
    ProcessingPath,
    ReplayCase,
    Sensitivity,
    SignalEnvelope,
    SignalSource,
    Urgency,
    Valence,
)
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class LimbicMemoryTests(unittest.TestCase):
    def _services(self, tmp: str):
        root = Path(tmp)
        log = EventLog(ObjectStore(root / "objects"), root, "instance-a")
        authority = CardProjection(root / "authority.db", log)
        replica = CardProjection(root / "replica.db", log)
        return (
            LimbicService(authority, "instance-a"),
            MemoryService(authority, "instance-a"),
            replica,
        )

    @staticmethod
    def _signal(event_class: str, **values) -> SignalEnvelope:
        return SignalEnvelope(
            source=SignalSource.SYSTEM,
            event_class=event_class,
            subject_type="dispatch",
            subject_id="dispatch-1",
            **values,
        )

    @staticmethod
    def _context(key: str) -> MemoryMutationContext:
        return MemoryMutationContext(
            actor_principal="agent:curator",
            authority_instance_id="instance-a",
            idempotency_key=key,
        )

    @staticmethod
    def _memory(tier: MemoryTier, value, **values) -> MemoryRecord:
        return MemoryRecord(
            tier=tier,
            subject=values.pop("subject", "repository:pa"),
            predicate=values.pop("predicate", "default_branch"),
            value=value,
            summary=values.pop("summary", f"Default branch is {value}"),
            provenance=values.pop(
                "provenance",
                MemoryProvenance(
                    source_type="repository",
                    source_id="repo-1",
                    actor_principal="agent:observer",
                    verified=True,
                ),
            ),
            **values,
        )

    def test_deterministic_bypass_deduplicates_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            limbic, _, replica = self._services(tmp)
            signal = self._signal("Explicit Operator Stop", goal_refs=["goal-1"])

            first = limbic.appraise(signal)
            duplicate = limbic.appraise(signal)

            self.assertEqual(first.route.path, ProcessingPath.BYPASS)
            self.assertEqual(first.appraisal.urgency, Urgency.CRITICAL)
            self.assertEqual(first.appraisal.deterministic_bypass, "operator_stop")
            self.assertTrue(duplicate.deduplicated)
            replica.rebuild_from_log("default")
            replayed = LimbicService(replica, "instance-b").appraise(signal)
            self.assertTrue(replayed.deduplicated)
            self.assertEqual(replayed.route.id, first.route.id)

    def test_model_cannot_downgrade_and_sensitive_content_is_never_sent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = EventLog(ObjectStore(root / "objects"), root, "instance-a")
            projection = CardProjection(root / "pa.db", log)
            calls: list[dict] = []

            def provider(features: dict) -> dict:
                calls.append(features)
                return Appraisal(
                    signal_id="placeholder",
                    salience=0.1,
                    urgency=Urgency.LOW,
                    valence=Valence.ROUTINE,
                    novelty=Novelty.EXPECTED,
                    confidence=0.99,
                    intent="dismiss",
                    recommended_path=ProcessingPath.FAST,
                    dedupe_key="placeholder",
                    reason="model considered it routine",
                    evaluator="model",
                    evaluator_version="tiny-v1",
                ).model_dump(
                    mode="python",
                    exclude={
                        "id", "signal_id", "goal_refs", "dedupe_key", "evaluator",
                        "evaluator_version", "model_used", "input_features", "created_at",
                    },
                ) | {"evaluator_version": "tiny-v1"}

            service = LimbicService(projection, "instance-a", provider=provider)
            result = service.appraise(
                self._signal("production_change", metadata={"deep_review": True}),
                persist=False,
            )
            self.assertEqual(result.route.path, ProcessingPath.SLOW)
            self.assertEqual(result.appraisal.urgency, Urgency.HIGH)
            self.assertEqual(len(calls), 1)

            secret = self._signal(
                "unknown_event",
                sensitivity=Sensitivity.RESTRICTED,
                content="password=do-not-send",
                metadata={"authorization_token": "do-not-send"},
            )
            result = service.appraise(secret, persist=False)
            self.assertEqual(result.route.path, ProcessingPath.SLOW)
            self.assertEqual(len(calls), 1)
            self.assertEqual(secret.appraisal_features()["content"], "[redacted]")
            self.assertNotIn("authorization_token", secret.appraisal_features()["metadata"])
            queued = service.appraise(self._signal("event_storm"), persist=False)
            self.assertEqual(queued.route.path, ProcessingPath.QUEUE)

    def test_replay_evaluation_scores_escalation_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            limbic, _, _ = self._services(tmp)
            report = limbic.evaluate(
                [
                    ReplayCase(
                        name="status",
                        signal=self._signal("status_query"),
                        expected_path=ProcessingPath.FAST,
                    ),
                    ReplayCase(
                        name="integrity",
                        signal=self._signal("data_integrity_alarm"),
                        expected_path=ProcessingPath.BYPASS,
                        expected_bypass="data_integrity_alarm",
                        expected_urgency=Urgency.CRITICAL,
                    ),
                ]
            )
            self.assertEqual(report.accuracy, 1)
            self.assertEqual(report.missed_escalations, 0)
            self.assertEqual(report.false_escalations, 0)

    def test_memory_scope_retention_contradiction_supersession_and_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, memory, replica = self._services(tmp)
            first = memory.remember(
                self._memory(
                    MemoryTier.SEMANTIC,
                    "main",
                    goal_id="goal-1",
                    allowed_principals=["agent:supervisor"],
                    sensitivity=Sensitivity.CONFIDENTIAL,
                ),
                self._context("fact-main"),
            )
            sensory = memory.remember(
                self._memory(
                    MemoryTier.SENSORY,
                    "transient",
                    predicate="observation",
                    expires_at=datetime.now(UTC) - timedelta(seconds=1),
                ),
                self._context("sensory"),
            )
            second = memory.remember(
                self._memory(
                    MemoryTier.SEMANTIC,
                    "trunk",
                    goal_id="goal-1",
                    allowed_principals=["agent:supervisor"],
                    sensitivity=Sensitivity.CONFIDENTIAL,
                ),
                self._context("fact-trunk"),
            )
            self.assertTrue(memory.get(first.id).contradiction)
            self.assertTrue(second.contradiction)

            ordinary = MemoryQuery(
                requester_principal="agent:supervisor",
                goal_ids=["goal-1"],
                max_sensitivity=Sensitivity.CONFIDENTIAL,
            )
            self.assertEqual(memory.retrieve(ordinary), [])
            contradictory = memory.retrieve(
                ordinary.model_copy(update={"include_contradictions": True})
            )
            self.assertEqual({item.record.id for item in contradictory}, {first.id, second.id})

            replacement = memory.remember(
                self._memory(
                    MemoryTier.SEMANTIC,
                    "main",
                    goal_id="goal-1",
                    allowed_principals=["agent:supervisor"],
                    sensitivity=Sensitivity.CONFIDENTIAL,
                    supersedes=second.id,
                ),
                self._context("resolve"),
            )
            self.assertEqual(memory.get(second.id).superseded_by, replacement.id)
            retrieved = memory.retrieve(ordinary)
            self.assertEqual([item.record.id for item in retrieved], [replacement.id])
            self.assertFalse(retrieved[0].instruction_trusted)
            self.assertNotIn(sensory.id, {item.record.id for item in retrieved})

            unauthorized = ordinary.model_copy(
                update={"requester_principal": "agent:other"}
            )
            self.assertEqual(memory.retrieve(unauthorized), [])
            too_low = ordinary.model_copy(
                update={"max_sensitivity": Sensitivity.INTERNAL}
            )
            self.assertEqual(memory.retrieve(too_low), [])

            replica.rebuild_from_log("default")
            restored = MemoryService(replica, "instance-b")
            restored_records = restored.retrieve(ordinary)
            self.assertEqual([item.record.id for item in restored_records], [replacement.id])


if __name__ == "__main__":
    unittest.main()
