"""Card updated_at must survive sync so fleet dispatch versions stay aligned."""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import CardCreate, CardUpdate, EventType
from pa.domain.projection import CardProjection
from pa.execution.lease import LeaseManager
from pa.modules.fleet import DispatchMaterializeBody, materialize_dispatch
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore
from tests.test_dispatch_consistency import request_for


class CardVersionSyncTests(unittest.TestCase):
    def _pair(self, tmp: str) -> tuple[CardProjection, CardProjection, EventLog]:
        data_dir = Path(tmp)
        objects = ObjectStore(data_dir / "objects")
        log = EventLog(objects, data_dir, "authority")
        authority = CardProjection(data_dir / "authority.db", log)
        replica = CardProjection(data_dir / "replica.db", log)
        return authority, replica, log

    def test_create_and_update_preserve_updated_at_across_projections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            authority, replica, log = self._pair(tmp)
            created = authority.create_card(CardCreate(title="repos"))
            time.sleep(0.01)
            updated = authority.update_card(
                created.id, CardUpdate(lane=created.lane, title="repos v2")
            )
            assert updated is not None

            replica.rebuild_from_log("default")
            replica_card = replica.get_card(created.id, realm_id="default")
            assert replica_card is not None

            self.assertEqual(updated.updated_at, replica_card.updated_at)
            self.assertEqual(
                updated.updated_at.isoformat(), replica_card.updated_at.isoformat()
            )
            self.assertEqual(updated.title, replica_card.title)

            # Durable update events carry the authority stamp.
            head = log.get_head("default")
            assert head is not None
            commit = log.get_commit(head)
            assert commit is not None
            self.assertEqual(len(commit.event_hashes), 1)
            event = log.get_event(commit.event_hashes[0])
            assert event is not None
            self.assertEqual(event.type, EventType.CARD_UPDATED)
            self.assertEqual(
                event.payload.get("updated_at"), updated.updated_at.isoformat()
            )

    def test_synced_card_version_materializes_for_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            authority, replica, _log = self._pair(tmp)
            created = authority.create_card(CardCreate(title="dispatch me"))
            updated = authority.update_card(
                created.id, CardUpdate(body="ready for remote work")
            )
            assert updated is not None
            replica.rebuild_from_log("default")

            settings = Settings(data_dir=Path(tmp) / "target", instance_id="target")
            request = request_for(settings, replica, {"event_log": _log})
            result = materialize_dispatch(
                request,
                DispatchMaterializeBody(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card=updated.model_dump(mode="json"),
                    card_version=updated.updated_at.isoformat(),
                    realm_id="default",
                    authority_instance_id="authority",
                    authority_url="http://authority:8080",
                    target_instance_id="target",
                ),
            )
            self.assertTrue(result["resolvable"])
            self.assertEqual(result["card_version"], updated.updated_at.isoformat())

    def test_divergent_content_still_reports_stale_target_card(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            authority, replica, log = self._pair(tmp)
            created = authority.create_card(CardCreate(title="shared"))
            replica.rebuild_from_log("default")

            # Replica advances independently with a newer local stamp.
            replica.update_card(created.id, CardUpdate(title="replica-only"))
            authority_card = authority.get_card(created.id, realm_id="default")
            assert authority_card is not None

            settings = Settings(data_dir=Path(tmp) / "target", instance_id="target")
            request = request_for(settings, replica, {"event_log": log})
            with self.assertRaises(HTTPException) as raised:
                materialize_dispatch(
                    request,
                    DispatchMaterializeBody(
                        dispatch_id="dispatch-2",
                        mutation_id="mutation-2",
                        card=authority_card.model_dump(mode="json"),
                        card_version=authority_card.updated_at.isoformat(),
                        realm_id="default",
                        authority_instance_id="authority",
                        authority_url="http://authority:8080",
                        target_instance_id="target",
                    ),
                )
            self.assertEqual(raised.exception.detail["code"], "stale_target_card")

    def test_lease_events_preserve_updated_at_across_projections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            authority, replica, _log = self._pair(tmp)
            created = authority.create_card(CardCreate(title="leased"))
            leases = LeaseManager(authority, _log, "authority")
            self.assertTrue(
                leases.grant(
                    created.id,
                    "default",
                    holder_instance="authority",
                    holder_principal="user:test",
                )
            )
            leased = authority.get_card(created.id, realm_id="default")
            assert leased is not None

            replica.rebuild_from_log("default")
            replica_card = replica.get_card(created.id, realm_id="default")
            assert replica_card is not None
            self.assertEqual(leased.updated_at, replica_card.updated_at)
            self.assertEqual(leased.lease_holder_instance, "authority")
            self.assertEqual(replica_card.lease_holder_instance, "authority")


if __name__ == "__main__":
    unittest.main()
