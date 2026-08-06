from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.event_log import (
    EventHistoryCursorError,
    EventHistoryLimitError,
    EventHistoryObjectError,
    EventLog,
)
from pa.sync.object_store import ObjectStore


def _update_event(event_id: str) -> CardEvent:
    return CardEvent(
        id=event_id,
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="history-card",
        author_principal="user:test",
        author_instance="instance",
        payload={"title": event_id},
    )


class BoundedHistoryTests(unittest.TestCase):
    def test_direct_history_pages_more_than_2000_events_with_stable_cursor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = EventLog(
                ObjectStore(Path(tmp) / "objects"), Path(tmp), "instance"
            )
            commits: dict[str, SyncCommit] = {}
            events: dict[str, CardEvent] = {}
            parent: str | None = None
            for index in range(2_200):
                commit_hash = f"commit-{index}"
                event_hash = f"event-hash-{index}"
                commits[commit_hash] = SyncCommit(
                    hash=commit_hash,
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=[parent] if parent else [],
                    event_hashes=[event_hash],
                    author_principal="user:test",
                )
                events[event_hash] = _update_event(f"event-{index}")
                parent = commit_hash

            received: list[str] = []
            cursor: str | None = None
            immutable_head = parent
            with (
                patch.object(log, "get_head", return_value=parent),
                patch.object(log, "get_commit", side_effect=commits.get),
                patch.object(log, "get_event", side_effect=events.get),
            ):
                while True:
                    page = log.entity_history_page(
                        "default",
                        "card",
                        "history-card",
                        limit=500,
                        cursor=cursor,
                    )
                    self.assertEqual(page["head"], immutable_head)
                    received.extend(
                        item["event"]["id"] for item in page["events"]
                    )
                    cursor = page["next_cursor"]
                    if not page["has_more"]:
                        break

            self.assertEqual(
                received, [f"event-{index}" for index in range(2_200)]
            )
            self.assertIsNone(cursor)

    def test_wide_deep_merge_dag_over_5000_commits_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = EventLog(
                ObjectStore(Path(tmp) / "objects"), Path(tmp), "instance"
            )
            commits: dict[str, SyncCommit] = {}
            events: dict[str, CardEvent] = {}
            expected: list[str] = []
            parent: str | None = None

            def add(commit_hash: str, parents: list[str]) -> None:
                event_hash = f"event-{commit_hash}"
                commits[commit_hash] = SyncCommit(
                    hash=commit_hash,
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=parents,
                    event_hashes=[event_hash],
                    author_principal="user:test",
                )
                events[event_hash] = _update_event(commit_hash)

            for index in range(3_000):
                commit_hash = f"base-{index}"
                add(commit_hash, [parent] if parent else [])
                parent = commit_hash
                expected.append(commit_hash)
            base_head = parent
            branch_heads: list[str] = []
            for branch in range(100):
                branch_parent = base_head
                for depth in range(20):
                    commit_hash = f"branch-{branch}-{depth}"
                    add(commit_hash, [branch_parent])
                    branch_parent = commit_hash
                    expected.append(commit_hash)
                branch_heads.append(branch_parent)
            add("merge-head", branch_heads)
            expected.append("merge-head")

            with (
                patch.object(log, "get_head", return_value="merge-head"),
                patch.object(log, "get_commit", side_effect=commits.get),
                patch.object(log, "get_event", side_effect=events.get),
            ):
                history = log.entity_history(
                    "default", "card", "history-card"
                )

            self.assertEqual(len(history), 5_001)
            self.assertEqual(
                [item["event"]["id"] for item in history], expected
            )
            self.assertEqual(history[-1]["parent_hashes"], branch_heads)

    def test_missing_parent_is_a_typed_bounded_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = EventLog(
                ObjectStore(Path(tmp) / "objects"), Path(tmp), "instance"
            )
            head = SyncCommit(
                hash="head",
                realm_id="default",
                instance_id="instance",
                parent_hashes=["missing"],
                event_hashes=[],
                author_principal="user:test",
            )
            with (
                patch.object(log, "get_head", return_value="head"),
                patch.object(
                    log,
                    "get_commit",
                    side_effect=lambda value: head if value == "head" else None,
                ),
                self.assertRaises(EventHistoryObjectError) as raised,
            ):
                log.entity_history("default", "card", "history-card")
            self.assertEqual(raised.exception.code, "missing_parent")
            self.assertEqual(
                raised.exception.as_detail()["object_hash"], "missing"
            )

    def test_corrupt_and_unsupported_objects_fail_safely(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            objects = ObjectStore(Path(tmp) / "objects")
            log = EventLog(objects, Path(tmp), "instance")
            corrupt_hash = objects.put(b"not-json")
            with self.assertRaises(EventHistoryObjectError) as corrupt:
                log.get_commit(corrupt_hash)
            self.assertEqual(corrupt.exception.code, "corrupt_object")

            future_hash = objects.put_json(
                {
                    "schema_version": 2,
                    "hash": "",
                    "realm_id": "default",
                    "instance_id": "future",
                    "parent_hashes": [],
                    "event_hashes": [],
                    "author_principal": "user:test",
                }
            )
            with self.assertRaises(EventHistoryObjectError) as future:
                log.get_commit(future_hash)
            self.assertEqual(
                future.exception.code, "unsupported_object_version"
            )
            self.assertEqual(
                future.exception.as_detail()["supported_schema_version"], 1
            )

    def test_history_limit_and_cursor_errors_are_structured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = EventLog(
                ObjectStore(Path(tmp) / "objects"), Path(tmp), "instance"
            )
            commits = {
                f"commit-{index}": SyncCommit(
                    hash=f"commit-{index}",
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=[f"commit-{index - 1}"] if index else [],
                    event_hashes=[],
                    author_principal="user:test",
                )
                for index in range(20)
            }
            with (
                patch.object(log, "get_head", return_value="commit-19"),
                patch.object(log, "get_commit", side_effect=commits.get),
                self.assertRaises(EventHistoryLimitError) as limited,
            ):
                log.entity_history_page(
                    "default",
                    "card",
                    "history-card",
                    max_commits=10,
                )
            detail = limited.exception.as_detail()
            self.assertEqual(detail["code"], "history_limit_exceeded")
            self.assertEqual(detail["limit"], 10)
            self.assertIn("next_cursor", detail)

            with self.assertRaises(EventHistoryCursorError) as invalid:
                log.entity_history_page(
                    "default",
                    "card",
                    "history-card",
                    cursor="not-a-valid-cursor",
                )
            self.assertEqual(invalid.exception.code, "invalid_history_cursor")


if __name__ == "__main__":
    unittest.main()
