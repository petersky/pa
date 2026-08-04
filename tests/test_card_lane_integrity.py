from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import (
    Card,
    CardCreate,
    CardEvent,
    CardLane,
    CardUpdate,
    EventType,
    SyncCommit,
)
from pa.domain.projection import CardProjection, CardVersionConflict
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.sync.event_log import DuplicateCardCreateError, EventLog
from pa.sync.object_store import ObjectStore

CARD_ID = "6989b229-666b-4854-8db8-d2e8799a9f93"


def append_unchecked(log: EventLog, event: CardEvent) -> SyncCommit:
    """Encode a pre-fix event without passing through the new append guard."""
    event_hash = log.store.put_json(event.model_dump(mode="json"))
    parent = log.get_head(event.realm_id)
    commit = SyncCommit(
        hash="",
        realm_id=event.realm_id,
        instance_id=log.instance_id,
        parent_hashes=[parent] if parent else [],
        event_hashes=[event_hash],
        author_principal=event.author_principal,
        timestamp=event.timestamp,
    )
    commit.hash = log.store.put_json(commit.model_dump(mode="json"))
    log.advance_ref(event.realm_id, commit.hash, expected_head=parent)
    return commit


class CardLaneIntegrityTests(unittest.TestCase):
    def pair(self, root: Path) -> tuple[CardProjection, EventLog]:
        objects = ObjectStore(root / "objects")
        log = EventLog(objects, root / "refs", "authority")
        projection = CardProjection(root / "projection.db", log)
        return projection, log

    def test_linear_stale_full_create_cannot_overwrite_newer_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            projection, log = self.pair(Path(tmp))
            card = projection.create_card(CardCreate(title="Get agents working"))
            done = projection.update_card(card.id, CardUpdate(lane=CardLane.DONE))
            assert done is not None
            stale = done.model_copy(update={"lane": CardLane.INBOX})

            with self.assertRaises(DuplicateCardCreateError):
                log.append_event(
                    CardEvent(
                        type=EventType.CARD_CREATED,
                        realm_id="default",
                        card_id=card.id,
                        author_principal="fleet:dispatch",
                        author_instance="stale-peer",
                        payload=stale.model_dump(mode="json"),
                    )
                )

            historical = CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id=card.id,
                author_principal="fleet:dispatch",
                author_instance="stale-peer",
                payload=stale.model_dump(mode="json"),
                timestamp=datetime.now(UTC),
            )
            append_unchecked(log, historical)
            projection.rebuild_from_log("default")

            restored = projection.get_card(card.id)
            assert restored is not None
            self.assertEqual(restored.lane, CardLane.DONE)
            history = log.entity_history("default", "card", card.id)
            self.assertEqual(
                history[-1]["projection_effect"], "ignored_duplicate_create"
            )
            snapshot = log.entity_snapshot(log.get_head("default"), "card", card.id)
            assert snapshot is not None
            self.assertEqual(snapshot["lane"], "done")

    def test_restart_does_not_reimport_open_legacy_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection, log = self.pair(root)
            stamp = datetime.now(UTC).isoformat()
            with sqlite3.connect(projection.db_path) as conn:
                conn.execute(
                    "INSERT INTO items (id, kind, title, body, status, parent_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        CARD_ID,
                        "goal",
                        "Get agents working",
                        "",
                        "open",
                        None,
                        "[]",
                        stamp,
                        stamp,
                    ),
                )
                # Simulate a database created before the monotonic migration marker.
                conn.execute("DELETE FROM projection_migrations")
            log.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:local",
                    author_instance="authority",
                    payload={"lane": "done", "updated_at": stamp},
                )
            )

            restarted = CardProjection(projection.db_path, log)
            self.assertIsNone(restarted.get_card(CARD_ID))
            with sqlite3.connect(projection.db_path) as conn:
                marker_count = conn.execute(
                    "SELECT COUNT(*) FROM projection_migrations WHERE name='legacy_items_to_cards_v2_monotonic'"
                ).fetchone()[0]
            self.assertEqual(marker_count, 1)

    def test_legacy_history_repair_is_idempotent_and_keeps_latest_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projection, log = self.pair(root)
            stamp = datetime.now(UTC).isoformat()
            with sqlite3.connect(projection.db_path) as conn:
                conn.execute(
                    "INSERT INTO items (id, kind, title, body, status, parent_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        CARD_ID,
                        "goal",
                        "Get agents working",
                        "",
                        "open",
                        None,
                        "[]",
                        stamp,
                        stamp,
                    ),
                )
            log.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:local",
                    author_instance="authority",
                    payload={"lane": "done", "updated_at": stamp},
                )
            )

            first = projection.repair_legacy_card_history([CARD_ID])
            first_head = log.get_head("default")
            second = projection.repair_legacy_card_history([CARD_ID])

            self.assertEqual(first[0]["status"], "repaired")
            self.assertEqual(first[0]["lane"], "done")
            self.assertEqual(second[0]["status"], "already_repaired")
            self.assertEqual(log.get_head("default"), first_head)
            repaired = projection.get_card(CARD_ID)
            assert repaired is not None
            self.assertEqual(repaired.lane, CardLane.DONE)

            restarted = CardProjection(projection.db_path, log)
            after_restart = restarted.get_card(CARD_ID)
            assert after_restart is not None
            self.assertEqual(after_restart.lane, CardLane.DONE)

    def test_stale_full_payload_is_rejected_but_field_intent_preserves_lane(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            projection, _ = self.pair(Path(tmp))
            card = projection.create_card(CardCreate(title="Ship", lane=CardLane.DONE))
            stale_version = card.updated_at
            current = projection.update_card(card.id, CardUpdate(body="new details"))
            assert current is not None

            with self.assertRaises(CardVersionConflict):
                projection.update_card(
                    card.id,
                    CardUpdate.model_validate(
                        {
                            **card.model_dump(mode="json"),
                            "title": "stale full edit",
                            "lane": "inbox",
                            "updated_at": stale_version.isoformat(),
                        }
                    ),
                )

            updated = projection.update_card(
                card.id,
                CardUpdate.model_validate(
                    {
                        **current.model_dump(mode="json"),
                        "title": "intentional title only",
                        "lane": "inbox",
                        "field_intent": ["title"],
                    }
                ),
            )
            assert updated is not None
            self.assertEqual(updated.title, "intentional title only")
            self.assertEqual(updated.lane, CardLane.DONE)

    def test_unrelated_edits_and_repeated_replay_preserve_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            projection, log = self.pair(Path(tmp))
            card = projection.create_card(CardCreate(title="Ship", lane=CardLane.DONE))
            updated = projection.update_card(card.id, CardUpdate(body="unrelated"))
            assert updated is not None
            for _ in range(3):
                projection.rebuild_from_log("default")
            replayed = projection.get_card(card.id)
            assert replayed is not None
            self.assertEqual(replayed.lane, CardLane.DONE)
            lane_events = [
                item
                for item in log.entity_history("default", "card", card.id)
                if "lane" in item["event"]["payload"]
            ]
            self.assertEqual(len(lane_events), 1)

    def test_offline_descendant_reconnect_cannot_reintroduce_inbox(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            authority_log = EventLog(objects, root / "authority", "authority")
            offline_log = EventLog(objects, root / "offline", "offline")
            projection = CardProjection(root / "projection.db", authority_log)
            card = projection.create_card(CardCreate(title="Reconnect"))
            done = projection.update_card(card.id, CardUpdate(lane=CardLane.DONE))
            assert done is not None
            done_head = authority_log.get_head("default")
            assert done_head is not None
            offline_log.advance_ref("default", done_head)
            stale = done.model_copy(update={"lane": CardLane.INBOX})
            remote = append_unchecked(
                offline_log,
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id=card.id,
                    author_principal="fleet:dispatch",
                    author_instance="offline",
                    payload=stale.model_dump(mode="json"),
                ),
            )

            authority_log.advance_ref("default", remote.hash, expected_head=done_head)
            projection.rebuild_from_log("default")
            reconnected = projection.get_card(card.id)
            assert reconnected is not None
            self.assertEqual(reconnected.lane, CardLane.DONE)

    def test_compatible_merge_parent_order_cannot_apply_duplicate_create(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            base_log = EventLog(objects, root / "base", "base")
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            merger = EventLog(objects, root / "merger", "merger")
            projection = CardProjection(root / "projection.db", merger)
            _, created = base_log.append_event(
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:local",
                    author_instance="base",
                    payload=Card(
                        id=CARD_ID, title="Merge", lane=CardLane.DONE
                    ).model_dump(mode="json"),
                )
            )
            left.advance_ref("default", created.hash)
            right.advance_ref("default", created.hash)
            _, left_head = left.append_event(
                CardEvent(
                    type=EventType.LEASE_GRANTED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:left",
                    author_instance="left",
                    payload={
                        "holder_instance": "left",
                        "holder_principal": "user:left",
                        "expires_at": datetime.now(UTC).isoformat(),
                    },
                )
            )
            stale = Card(id=CARD_ID, title="Merge", lane=CardLane.INBOX)
            right_head = append_unchecked(
                right,
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="fleet:dispatch",
                    author_instance="right",
                    payload=stale.model_dump(mode="json"),
                ),
            )
            compatible, health = merger.compatible_histories(
                left_head.hash, right_head.hash
            )
            self.assertTrue(compatible, health)
            merge = merger.merge_heads(
                "default", left_head.hash, right_head.hash, "sync:auto"
            )
            projection.rebuild_from_log("default")
            merged = projection.get_card(CARD_ID)
            assert merged is not None
            self.assertEqual(merged.lane, CardLane.DONE)
            self.assertEqual(
                merger.get_commit(merge.hash).parent_hashes,
                sorted([left_head.hash, right_head.hash]),
            )

    def test_legitimate_lane_change_has_complete_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            projection, log = self.pair(Path(tmp))
            card = projection.create_card(CardCreate(title="Intentional"))
            moved = projection.update_card(card.id, CardUpdate(lane=CardLane.DONE))
            assert moved is not None
            lane_event = log.entity_history("default", "card", card.id)[-1]

            self.assertEqual(lane_event["event"]["payload"]["lane"], "done")
            self.assertEqual(lane_event["event"]["source_operation"], "card.update")
            self.assertEqual(lane_event["event"]["field_intent"], ["lane"])
            self.assertEqual(
                lane_event["event"]["causal_card_version"], card.updated_at.isoformat()
            )
            self.assertEqual(
                lane_event["event"]["causal_parent"], lane_event["parent_hashes"][0]
            )
            self.assertEqual(lane_event["commit_instance"], "authority")

    def test_divergent_lane_values_remain_an_actionable_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            _, base = left.append_event(
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:local",
                    author_instance="left",
                    payload=Card(id=CARD_ID, title="base").model_dump(mode="json"),
                )
            )
            right.advance_ref("default", base.hash)
            _, left_head = left.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:left",
                    author_instance="left",
                    payload={"lane": "done"},
                )
            )
            _, right_head = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=CARD_ID,
                    author_principal="user:right",
                    author_instance="right",
                    payload={"lane": "inbox"},
                )
            )

            compatible, health = left.compatible_histories(
                left_head.hash, right_head.hash
            )
            self.assertFalse(compatible)
            self.assertEqual(health["conflicts"][0]["field"], "lane")
            self.assertEqual(health["conflicts"][0]["local"]["principal"], "user:left")
            self.assertEqual(
                health["conflicts"][0]["remote"]["principal"], "user:right"
            )

            resolution = CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id="default",
                card_id=CARD_ID,
                author_principal="user:operator",
                author_instance="left",
                payload={"lane": "done"},
                source_operation="sync.resolve_conflict",
                causal_parent=left_head.hash,
                field_intent=["lane"],
            )
            merge = left.resolve_heads(
                "default",
                left_head.hash,
                right_head.hash,
                [resolution],
                "user:operator",
            )
            projection = CardProjection(root / "resolved.db", left)
            projection.rebuild_from_log("default")
            resolved = projection.get_card(CARD_ID)
            assert resolved is not None
            self.assertEqual(resolved.lane, CardLane.DONE)
            self.assertEqual(
                left.get_commit(merge.hash).parent_hashes,
                sorted([left_head.hash, right_head.hash]),
            )
            resolution_record = next(
                item
                for item in left.entity_history("default", "card", CARD_ID)
                if item["event"]["source_operation"] == "sync.resolve_conflict"
            )
            self.assertEqual(resolution_record["event"]["field_intent"], ["lane"])
            self.assertEqual(resolution_record["parent_hashes"], merge.parent_hashes)


class CardLaneIntegrityHttpTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()

    def test_full_card_api_uses_version_fence_intent_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            headers = {"Authorization": f"Bearer {token}"}
            with TestClient(Kernel.boot(settings=settings).build_app()) as client:
                created = client.post(
                    "/api/cards",
                    json={"title": "API fence", "lane": "done"},
                    headers=headers,
                )
                self.assertEqual(created.status_code, 201, created.text)
                stale = created.json()
                card_id = stale["id"]
                advanced = client.patch(
                    f"/api/cards/{card_id}",
                    json={"body": "new body"},
                    headers=headers,
                )
                self.assertEqual(advanced.status_code, 200, advanced.text)

                rejected = client.patch(
                    f"/api/cards/{card_id}",
                    json={**stale, "title": "stale title", "lane": "inbox"},
                    headers=headers,
                )
                self.assertEqual(rejected.status_code, 409, rejected.text)
                self.assertEqual(
                    rejected.json()["detail"]["code"], "stale_card_version"
                )

                current = advanced.json()
                intentional = client.patch(
                    f"/api/cards/{card_id}",
                    json={
                        **current,
                        "title": "intentional title",
                        "lane": "inbox",
                        "field_intent": ["title"],
                    },
                    headers=headers,
                )
                self.assertEqual(intentional.status_code, 200, intentional.text)
                self.assertEqual(intentional.json()["lane"], "done")

                history = client.get(f"/api/cards/{card_id}/history", headers=headers)
                self.assertEqual(history.status_code, 200, history.text)
                final_event = history.json()["events"][-1]
                self.assertEqual(final_event["event"]["field_intent"], ["title"])
                self.assertEqual(
                    final_event["event"]["source_operation"], "card.update"
                )

    def test_fleet_principal_can_run_bounded_legacy_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                agent_enabled=False,
                auth_required=True,
                sync_token="shared-secret",
            )
            user_token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            with TestClient(Kernel.boot(settings=settings).build_app()) as client:
                stamp = datetime.now(UTC).isoformat()
                with sqlite3.connect(settings.db_path) as conn:
                    conn.execute(
                        "INSERT INTO items (id, kind, title, body, status, parent_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            CARD_ID,
                            "goal",
                            "Get agents working",
                            "",
                            "done",
                            None,
                            "[]",
                            stamp,
                            stamp,
                        ),
                    )

                repaired = client.post(
                    "/api/cards/repair-legacy-history",
                    json={"card_ids": [CARD_ID], "realm_id": "default"},
                    headers={"Authorization": "Bearer shared-secret"},
                )
                self.assertEqual(repaired.status_code, 200, repaired.text)
                self.assertEqual(repaired.json()["results"][0]["status"], "repaired")
                self.assertEqual(repaired.json()["results"][0]["lane"], "done")

                history = client.get(
                    f"/api/cards/{CARD_ID}/history",
                    headers={"Authorization": f"Bearer {user_token}"},
                )
                self.assertEqual(history.status_code, 200, history.text)
                event = history.json()["events"][-1]["event"]
                self.assertEqual(event["author_principal"], "instance:fleet")
                self.assertEqual(event["author_instance"], settings.instance_id)
                self.assertEqual(event["source_operation"], "repair.legacy_card_history")


if __name__ == "__main__":
    unittest.main()
