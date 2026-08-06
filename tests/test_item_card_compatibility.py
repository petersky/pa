"""Compatibility coverage for the item-to-card domain consolidation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import ValidationError

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import (
    Card,
    CardCreate,
    CardEvent,
    CardKind,
    CardLane,
    CardUpdate,
    EventType,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    legacy_status_from_lane,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


class ItemCardModelCompatibilityTests(unittest.TestCase):
    def test_kind_is_one_canonical_enum(self) -> None:
        self.assertIs(ItemKind, CardKind)

    def test_card_update_accepts_kind_for_enrichment(self) -> None:
        self.assertEqual(CardUpdate(kind="concern").kind, CardKind.CONCERN)

    def test_canonical_inputs_accept_legacy_status(self) -> None:
        self.assertEqual(
            CardCreate.model_validate({"title": "old", "status": "blocked"}).lane,
            CardLane.WAITING,
        )
        self.assertEqual(
            CardUpdate.model_validate({"status": "open"}).lane, CardLane.INBOX
        )

    def test_conflicting_status_and_lane_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "Conflicting lifecycle"):
            CardCreate.model_validate(
                {"title": "ambiguous", "lane": "active", "status": "blocked"}
            )

    def test_canonical_schema_marks_status_as_deprecated_input_only(self) -> None:
        schema = CardCreate.model_json_schema()
        self.assertTrue(schema["properties"]["status"]["deprecated"])
        payload = CardCreate.model_validate(
            {"title": "old", "status": "blocked"}
        ).model_dump(mode="json")
        self.assertNotIn("status", payload)
        self.assertEqual(payload["lane"], "waiting")

    def test_item_adapter_round_trips_each_representable_lifecycle(self) -> None:
        for status in (
            ItemStatus.OPEN,
            ItemStatus.ACTIVE,
            ItemStatus.BLOCKED,
            ItemStatus.DONE,
        ):
            with self.subTest(status=status):
                create = ItemCreate(
                    kind=ItemKind.TASK, title=status.value, status=status
                )
                canonical = create.to_card_create()
                compatibility = Item.from_card(
                    Card(title=status.value, lane=canonical.lane)
                )
                self.assertEqual(compatibility.status, status)
                self.assertEqual(legacy_status_from_lane(canonical.lane), status)

    def test_archived_maps_to_done_without_inventing_persistent_state(self) -> None:
        canonical = ItemCreate(
            kind=ItemKind.TASK, title="archived", status=ItemStatus.ARCHIVED
        ).to_card_create()
        self.assertEqual(canonical.lane, CardLane.DONE)
        self.assertEqual(legacy_status_from_lane(canonical.lane), ItemStatus.DONE)


class DurableHistoryCompatibilityTests(unittest.TestCase):
    def test_legacy_status_events_migrate_and_sync_to_multiple_peers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            log = EventLog(objects, root, "legacy-writer")
            created = CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id="legacy-card",
                author_principal="user:legacy",
                author_instance="legacy-writer",
                payload={
                    "id": "legacy-card",
                    "kind": "task",
                    "title": "Legacy",
                    "status": "blocked",
                },
            )
            log.append_event(created)
            log.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="legacy-card",
                    author_principal="user:legacy",
                    author_instance="legacy-writer",
                    payload={"status": "active"},
                )
            )

            first = CardProjection(root / "first.db", log)
            second = CardProjection(root / "second.db", log)
            first.rebuild_from_log("default")
            second.rebuild_from_log("default")

            self.assertEqual(first.get_card("legacy-card").lane, CardLane.ACTIVE)
            self.assertEqual(second.get_card("legacy-card").lane, CardLane.ACTIVE)
            self.assertEqual(
                first.get_projection_head("default"), second.get_projection_head("default")
            )


class ItemCardHttpCompatibilityTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()

    def test_legacy_and_canonical_routes_round_trip_the_same_card(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            headers = {"Authorization": f"Bearer {token}"}
            with TestClient(Kernel.boot(settings=settings).build_app()) as client:
                created = client.post(
                    "/api/items",
                    json={"kind": "task", "title": "Compatible", "status": "blocked"},
                    headers=headers,
                )
                self.assertEqual(created.status_code, 201, created.text)
                self.assertEqual(created.headers["deprecation"], "true")
                card_id = created.json()["id"]

                canonical = client.get(f"/api/cards/{card_id}", headers=headers)
                self.assertEqual(canonical.json()["lane"], "waiting")
                moved = client.patch(
                    f"/api/cards/{card_id}",
                    json={"status": "active"},
                    headers={**headers, "Idempotency-Key": "compat-move"},
                )
                self.assertEqual(moved.status_code, 200, moved.text)
                self.assertEqual(moved.json()["lane"], "active")

                legacy = client.get(f"/api/items/{card_id}", headers=headers)
                self.assertEqual(legacy.json()["status"], "active")
                self.assertIn('rel="deprecation"', legacy.headers["link"])

                conflict = client.patch(
                    f"/api/cards/{card_id}",
                    json={"lane": "done", "status": "blocked"},
                    headers={**headers, "Idempotency-Key": "compat-conflict"},
                )
                self.assertEqual(conflict.status_code, 422)


if __name__ == "__main__":
    unittest.main()
