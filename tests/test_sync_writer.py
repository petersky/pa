from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
from pa.domain.models import CardCreate, CardEvent, EventType
from pa.domain.projection import CardProjection
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.object_store import ObjectStore


def _event(title: str) -> CardEvent:
    return CardEvent(
        type=EventType.CARD_CREATED,
        realm_id="default",
        card_id=title,
        author_principal="user:test",
        author_instance="test",
        payload={"id": title, "title": title},
    )


class EventLogWriterSafetyTests(unittest.TestCase):
    def test_multiple_event_log_objects_refresh_and_preserve_parent_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            first = EventLog(objects, data_dir, "same-instance")
            second = EventLog(objects, data_dir, "same-instance")

            _, first_commit = first.append_event(_event("one"))
            _, second_commit = second.append_event(_event("two"))

            self.assertEqual(second_commit.parent_hashes, [first_commit.hash])
            self.assertEqual(first.get_head("default"), second_commit.hash)

    def test_compare_and_swap_rejects_a_stale_ref_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            first = EventLog(objects, data_dir, "same-instance")
            stale = EventLog(objects, data_dir, "same-instance")
            _, commit = first.append_event(_event("one"))

            with self.assertRaises(StaleSyncHeadError):
                stale.advance_ref("default", commit.hash, expected_head=None)

    def test_projection_checkpoint_detects_and_repairs_unapplied_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")
            projection = CardProjection(data_dir / "pa.db", log)
            projection.create_card(CardCreate(title="before"))
            projected = projection.get_projection_head("default")

            log.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=projection.list_cards()[0].id,
                    author_principal="user:test",
                    author_instance="instance",
                    payload={"title": "after"},
                )
            )
            self.assertNotEqual(projected, log.get_head("default"))
            self.assertEqual(projection.get_projection_head("default"), projected)

            projection.rebuild_from_log("default")
            self.assertEqual(projection.list_cards()[0].title, "after")
            self.assertEqual(
                projection.get_projection_head("default"), log.get_head("default")
            )

    def test_manual_resolution_preserves_both_heads_and_wins_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            left = EventLog(objects, data_dir, "left")
            right = EventLog(objects, data_dir, "right")
            created, base = left.append_event(_event("card-1"))
            right.advance_ref("default", base.hash, expected_head=None)

            _, left_head = left.append_event(
                created.model_copy(
                    update={
                        "id": "left-update",
                        "type": EventType.CARD_UPDATED,
                        "payload": {"title": "left"},
                    }
                )
            )
            _, right_head = right.append_event(
                created.model_copy(
                    update={
                        "id": "right-update",
                        "type": EventType.CARD_UPDATED,
                        "payload": {"title": "right"},
                    }
                )
            )
            resolution = created.model_copy(
                update={
                    "id": "resolution",
                    "type": EventType.CARD_UPDATED,
                    "payload": {"title": "resolved"},
                }
            )
            merge = left.resolve_heads(
                "default",
                left_head.hash,
                right_head.hash,
                [resolution],
                "user:operator",
            )

            self.assertEqual(
                set(merge.parent_hashes), {left_head.hash, right_head.hash}
            )
            projection = CardProjection(data_dir / "resolved.db", left)
            projection.rebuild_from_log("default")
            self.assertEqual(projection.get_card("card-1").title, "resolved")


class DataDirWriterLockTests(unittest.TestCase):
    def test_only_one_server_writer_can_own_a_data_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first = DataDirWriterLock(Path(tmp))
            second = DataDirWriterLock(Path(tmp))
            first.acquire()
            try:
                with self.assertRaises(DataDirAlreadyOwnedError):
                    second.acquire()
            finally:
                first.release()

            second.acquire()
            second.release()


if __name__ == "__main__":
    unittest.main()
