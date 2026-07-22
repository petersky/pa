from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from pa.domain.models import CardLane
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.disposition import decide_card_disposition
from pa.pr_supervisor.models import PRPolicy, PRWatch, PRWatchStatus, utcnow


HEAD = "a" * 40
MERGE = "b" * 40


def done_disposition(
    *,
    watch_id: str = "watch-1",
    head: str = HEAD,
    merge: str = MERGE,
) -> dict:
    return {
        "contract": "pa.card-disposition/v1",
        "lane": "done",
        "outcome": "The exact watched change was integrated.",
        "evidence": {
            "integration_required": True,
            "pr_watch_id": watch_id,
            "watched_head_sha": head,
            "merge_commit_sha": merge,
            "references": ["https://github.com/owner/repo/pull/17"],
        },
    }


def linked_watch(
    *,
    status: PRWatchStatus = PRWatchStatus.ACTIVE,
    state: dict | None = None,
) -> PRWatch:
    return PRWatch(
        id="watch-1",
        realm_id="default",
        card_id="card-1",
        repository="owner/repo",
        pr_number=17,
        pr_url="https://github.com/owner/repo/pull/17",
        head_sha=HEAD,
        status=status,
        policy=PRPolicy(stable_head_seconds=0, stable_observations=1),
        stable_head_since=utcnow() - timedelta(seconds=1),
        stable_head_observations=1,
        state=state or {},
    )


class CardDispositionContractTests(unittest.TestCase):
    def test_missing_or_malformed_disposition_preserves_lane(self) -> None:
        missing = decide_card_disposition(None, current_lane=CardLane.ACTIVE)
        malformed = decide_card_disposition(
            {
                "contract": "pa.card-disposition/v2",
                "lane": "done",
                "outcome": "unsupported",
                "evidence": {},
            },
            current_lane=CardLane.ACTIVE,
        )

        self.assertEqual(missing.status, "absent")
        self.assertEqual(missing.applied_lane, CardLane.ACTIVE)
        self.assertEqual(malformed.status, "malformed")
        self.assertEqual(malformed.applied_lane, CardLane.ACTIVE)

    def test_end_turn_done_is_downgraded_for_each_nonterminal_pr_condition(
        self,
    ) -> None:
        unsafe_states = {
            "open_pr": {
                "state": "open",
                "mergeable_state": "clean",
                "gate": {"green": True},
            },
            "pending_ci": {
                "state": "open",
                "checks": [{"name": "test", "status": "in_progress"}],
                "gate": {"green": False, "pending": True},
            },
            "failing_ci": {
                "state": "open",
                "checks": [{"name": "test", "conclusion": "failure"}],
                "gate": {"green": False, "actionable": True},
            },
            "unresolved_review": {
                "state": "open",
                "review_threads": [{"id": "thread-1", "resolved": False}],
                "gate": {"green": False, "actionable": True},
            },
            "behind": {
                "state": "open",
                "mergeable_state": "behind",
                "gate": {"green": False, "pending": True},
            },
            "unknown_merge_state": {
                "state": "open",
                "mergeable_state": "unknown",
                "gate": {"green": False, "pending": True},
            },
        }

        for condition, state in unsafe_states.items():
            with self.subTest(condition=condition):
                decision = decide_card_disposition(
                    done_disposition(),
                    current_lane=CardLane.ACTIVE,
                    watches=[linked_watch(state=state)],
                )
                self.assertEqual(decision.status, "downgraded")
                self.assertEqual(decision.applied_lane, CardLane.WAITING)
                self.assertIn("not merged", decision.reason)

    def test_merged_watch_without_stable_green_or_merge_commit_is_waiting(self) -> None:
        state = {
            "state": "merged",
            "head_sha": HEAD,
            "confirmed_head_sha": HEAD,
            "merge_commit_sha": None,
        }
        decision = decide_card_disposition(
            done_disposition(),
            current_lane=CardLane.ACTIVE,
            watches=[linked_watch(status=PRWatchStatus.MERGED, state=state)],
        )
        self.assertEqual(decision.applied_lane, CardLane.WAITING)
        self.assertIn("merge commit evidence", decision.reason)

    def test_genuinely_merged_exact_stable_green_head_is_done(self) -> None:
        state = {
            "state": "merged",
            "head_sha": HEAD,
            "confirmed_head_sha": HEAD,
            "merge_commit_sha": MERGE,
            "stable_green_evidence": {"green": True, "head_sha": HEAD},
        }
        decision = decide_card_disposition(
            done_disposition(),
            current_lane=CardLane.WAITING,
            watches=[linked_watch(status=PRWatchStatus.MERGED, state=state)],
        )
        self.assertEqual(decision.status, "applied")
        self.assertEqual(decision.applied_lane, CardLane.DONE)

    def test_exact_head_and_merge_commit_must_match(self) -> None:
        state = {
            "state": "merged",
            "head_sha": HEAD,
            "confirmed_head_sha": HEAD,
            "merge_commit_sha": MERGE,
            "stable_green_evidence": {"green": True, "head_sha": HEAD},
        }
        decision = decide_card_disposition(
            done_disposition(merge="c" * 40),
            current_lane=CardLane.ACTIVE,
            watches=[linked_watch(status=PRWatchStatus.MERGED, state=state)],
        )
        self.assertEqual(decision.applied_lane, CardLane.WAITING)
        self.assertIn("merge commit does not match", decision.reason)

    def test_explicit_no_integration_done_is_allowed(self) -> None:
        decision = decide_card_disposition(
            {
                "contract": "pa.card-disposition/v1",
                "lane": "done",
                "outcome": "No repository integration was needed.",
                "evidence": {"integration_required": False},
            },
            current_lane=CardLane.ACTIVE,
        )
        self.assertEqual(decision.applied_lane, CardLane.DONE)

    def test_legacy_completed_dispatch_is_annotated_without_lane_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DispatchStore(root)
            store.put(
                DispatchRecord(
                    dispatch_id="legacy",
                    mutation_id="mutation",
                    card_id="card-1",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    state="completed",
                )
            )

            migrated = DispatchStore(root).get("legacy")

            self.assertEqual(migrated.state, "completed")
            self.assertEqual(migrated.card_disposition_status, "legacy_unrecorded")
            self.assertIsNone(migrated.card_lane_before)
            self.assertIsNone(migrated.card_lane_after)

    def test_dispatch_diagnostics_keep_turn_transport_and_card_states_separate(
        self,
    ) -> None:
        record = DispatchRecord(
            dispatch_id="dispatch-1",
            mutation_id="mutation-1",
            card_id="card-1",
            authority_instance_id="authority",
            authority_url="http://authority",
            target_instance_id="target",
            state="completed",
            completion_payload={"stop_reason": "end_turn"},
            card_disposition_status="downgraded",
            card_disposition_reason="CI is pending.",
            card_lane_before="active",
            card_lane_after="waiting",
        )

        diagnostic = record.public_dict()

        self.assertEqual(
            diagnostic["agent_turn"],
            {"completed": True, "stop_reason": "end_turn"},
        )
        self.assertTrue(diagnostic["dispatch_completion"]["completed"])
        self.assertEqual(diagnostic["card_completion"]["lane_after"], "waiting")
        self.assertEqual(diagnostic["card_completion"]["status"], "downgraded")


if __name__ == "__main__":
    unittest.main()
