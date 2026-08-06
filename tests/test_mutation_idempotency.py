from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardEvent, CardUpdate, EventType, SyncCommit
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.domain.projection import CardProjection, MutationOperationConflict
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.sync.event_log import EventHistoryError, EventLog
from pa.sync.object_store import ObjectStore


class SimulatedProcessCrash(BaseException):
    pass


class MutationReceiptCrashTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.log = EventLog(ObjectStore(self.root / "objects"), self.root, "instance")
        self.projection = CardProjection(self.root / "pa.db", self.log)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _claim(self, key: str, operation: str, fingerprint: str) -> None:
        replay = self.projection.begin_operation(
            idempotency_key=key,
            operation=operation,
            request_fingerprint=fingerprint,
            realm_id="default",
            correlation_id=f"correlation-{key}",
        )
        self.assertIsNone(replay)

    def _crash_after_append(self, event) -> None:
        self.log.append_event(event)
        raise SimulatedProcessCrash("after durable append")

    def test_create_recovers_after_crash_between_append_and_receipt(self) -> None:
        key = "create-after-append"
        fingerprint = "create-fingerprint"
        self._claim(key, "card.create", fingerprint)

        with (
            patch.object(
                self.projection,
                "commit_event",
                side_effect=self._crash_after_append,
            ),
            self.assertRaises(SimulatedProcessCrash),
        ):
            self.projection.create_card(
                CardCreate(title="Exactly once"),
                idempotency_key=key,
                request_fingerprint=fingerprint,
            )

        restarted = CardProjection(self.root / "pa.db", self.log)
        replay = restarted.begin_operation(
            idempotency_key=key,
            operation="card.create",
            request_fingerprint=fingerprint,
            realm_id="default",
        )

        self.assertIsNotNone(replay)
        self.assertEqual(replay["title"], "Exactly once")
        self.assertEqual(len(restarted.list_cards()), 1)
        history = self.log.entity_history("default", "card", replay["id"])
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["event"]["idempotency_key"], key)

    def test_fresh_projection_replays_create_without_a_duplicate(self) -> None:
        key = "fresh-projection-create"
        fingerprint = "fresh-projection-fingerprint"
        self._claim(key, "card.create", fingerprint)
        created = self.projection.create_card(
            CardCreate(title="Only one durable card"),
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
        result = created.model_dump(mode="json")
        self.projection.complete_operation(key, result)

        fresh = CardProjection(self.root / "fresh.db", self.log)
        replay = fresh.begin_operation(
            idempotency_key=key,
            operation="card.create",
            request_fingerprint=fingerprint,
            realm_id="default",
        )

        self.assertEqual(replay, result)
        self.assertEqual([card.id for card in fresh.list_cards()], [created.id])
        attributable = [
            item
            for item in self.log.entity_history(
                "default", "card", created.id
            )
            if item["event"]["idempotency_key"] == key
        ]
        self.assertEqual(len(attributable), 1)

    def test_rebuild_rejects_distinct_events_with_the_same_key(self) -> None:
        key = "duplicate-durable-key"
        fingerprint = "duplicate-durable-fingerprint"
        self._claim(key, "card.create", fingerprint)
        created = self.projection.create_card(
            CardCreate(title="First durable event"),
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
        durable = self.log.find_operation_event("default", key)
        assert durable is not None
        duplicate = durable[2].model_copy(
            update={
                "id": "distinct-duplicate-event",
                "card_id": created.id,
                "type": EventType.CARD_UPDATED,
                "payload": {"title": "Duplicate durable event"},
            }
        )
        self.log.append_event(duplicate)

        with self.assertRaises(MutationOperationConflict):
            CardProjection(self.root / "duplicate-rebuild.db", self.log)

    def test_rebuild_rejects_same_id_complete_operation_revision(self) -> None:
        key = "same-id-complete-revision"
        fingerprint = "same-id-complete-fingerprint"
        self._claim(key, "card.create", fingerprint)
        created = self.projection.create_card(
            CardCreate(title="Canonical durable event"),
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
        durable = self.log.find_operation_event("default", key)
        assert durable is not None
        duplicate = durable[2].model_copy(
            update={
                "type": EventType.CARD_UPDATED,
                "payload": {**durable[2].payload, "title": "Altered result"},
                "operation_result": {
                    **(durable[2].operation_result or {}),
                    "title": "Altered result",
                },
            }
        )
        self.log.append_event(duplicate)

        with self.assertRaises(EventHistoryError) as invalid:
            self.log.find_operation_event("default", key)
        self.assertEqual(invalid.exception.code, "invalid_operation_revision")
        with self.assertRaises(MutationOperationConflict):
            CardProjection(self.root / "same-id-revision.db", self.log)
        self.assertEqual(
            self.projection.get_card(created.id).title,
            "Canonical durable event",
        )

    def test_deep_log_rebuild_restores_exact_create_receipt(self) -> None:
        key = "deep-rebuild-create"
        fingerprint = "deep-rebuild-fingerprint"
        self._claim(key, "card.create", fingerprint)
        created = self.projection.create_card(
            CardCreate(title="Original"),
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
        original = created.model_dump(mode="json")
        self.projection.complete_operation(key, original)
        original_head = self.log.get_head("default")
        parent = original_head
        assert parent is not None
        for index in range(2_100):
            event = CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id="default",
                card_id=created.id,
                author_principal="user:test",
                author_instance="instance",
                payload={"title": f"Update {index}"},
            )
            event_hash = self.log.store.put_json(event.model_dump(mode="json"))
            commit = SyncCommit(
                hash="",
                realm_id="default",
                instance_id="instance",
                parent_hashes=[parent],
                event_hashes=[event_hash],
                author_principal="user:test",
            )
            parent = self.log.store.put_json(commit.model_dump(mode="json"))
        self.log.advance_ref("default", parent, expected_head=original_head)

        rebuilt = CardProjection(self.root / "deep-rebuilt.db", self.log)
        rebuilt.rebuild_from_log("default")
        replay = rebuilt.begin_operation(
            idempotency_key=key,
            operation="card.create",
            request_fingerprint=fingerprint,
            realm_id="default",
        )

        self.assertEqual(replay, original)
        self.assertEqual(rebuilt.get_card(created.id).title, "Update 2099")
        self.assertEqual(len(rebuilt.list_cards()), 1)

    def test_create_recovers_after_crash_during_projection_apply(self) -> None:
        key = "create-during-apply"
        fingerprint = "apply-fingerprint"
        self._claim(key, "card.create", fingerprint)

        with (
            patch.object(
                self.projection,
                "apply_event",
                side_effect=SimulatedProcessCrash("during projection apply"),
            ),
            self.assertRaises(SimulatedProcessCrash),
        ):
            self.projection.create_card(
                CardCreate(title="Recovered projection"),
                idempotency_key=key,
                request_fingerprint=fingerprint,
            )

        restarted = CardProjection(self.root / "pa.db", self.log)
        outcome = restarted.get_operation_outcome(key)
        self.assertEqual(outcome["status"], "succeeded")
        self.assertTrue(outcome["durable"])
        self.assertEqual(outcome["result"]["title"], "Recovered projection")
        self.assertEqual(
            restarted.get_projection_head("default"),
            self.log.get_head("default"),
        )

    def test_update_retry_returns_original_result_without_overwriting_newer_edit(
        self,
    ) -> None:
        card = self.projection.create_card(CardCreate(title="Before"))
        key = "update-after-append"
        fingerprint = "update-fingerprint"
        self._claim(key, "card.update", fingerprint)

        with (
            patch.object(
                self.projection,
                "commit_event",
                side_effect=self._crash_after_append,
            ),
            self.assertRaises(SimulatedProcessCrash),
        ):
            self.projection.update_card(
                card.id,
                CardUpdate(title="Original update"),
                idempotency_key=key,
                request_fingerprint=fingerprint,
            )

        restarted = CardProjection(self.root / "pa.db", self.log)
        restarted.update_card(card.id, CardUpdate(title="Intervening edit"))
        replay = restarted.begin_operation(
            idempotency_key=key,
            operation="card.update",
            request_fingerprint=fingerprint,
            realm_id="default",
        )

        self.assertEqual(replay["title"], "Original update")
        self.assertEqual(restarted.get_card(card.id).title, "Intervening edit")
        attributable = [
            item
            for item in self.log.entity_history("default", "card", card.id)
            if item["event"]["idempotency_key"] == key
        ]
        self.assertEqual(len(attributable), 1)

    def test_failed_receipt_still_recovers_a_durable_event(self) -> None:
        key = "failed-but-durable"
        fingerprint = "failed-fingerprint"
        self._claim(key, "card.create", fingerprint)
        with (
            patch.object(
                self.projection,
                "commit_event",
                side_effect=self._crash_after_append,
            ),
            self.assertRaises(SimulatedProcessCrash),
        ):
            self.projection.create_card(
                CardCreate(title="Durable"),
                idempotency_key=key,
                request_fingerprint=fingerprint,
            )
        self.projection.fail_operation(key, "simulated_failure")

        restarted = CardProjection(self.root / "pa.db", self.log)
        replay = restarted.begin_operation(
            idempotency_key=key,
            operation="card.create",
            request_fingerprint=fingerprint,
            realm_id="default",
        )
        self.assertEqual(replay["title"], "Durable")
        self.assertEqual(len(restarted.list_cards()), 1)

    def test_reconcile_reclaims_pending_receipt_after_restart(self) -> None:
        self.projection.create_card(CardCreate(title="Reconcile durable head"))
        key = "reconcile-after-projection"
        fingerprint = "reconcile-fingerprint"
        self._claim(key, "sync.reconcile", fingerprint)

        self.projection.rebuild_from_log("default")
        durable_head = self.log.get_head("default")

        restarted = CardProjection(self.root / "pa.db", self.log)
        pending = restarted.get_operation_outcome(key)
        self.assertEqual(pending["status"], "retryable")
        self.assertFalse(pending["durable"])
        self.assertEqual(
            pending["recovery_state"], "safe_to_retry_with_same_key"
        )
        self.assertEqual(
            pending["recovery_action"], "retry_same_operation_with_same_key"
        )

        replay = restarted.begin_operation(
            idempotency_key=key,
            operation="sync.reconcile",
            request_fingerprint=fingerprint,
            realm_id="default",
        )
        self.assertIsNone(replay)
        restarted.rebuild_from_log("default")
        result = {
            "realm_id": "default",
            "head": durable_head,
            "projection_head": restarted.get_projection_head("default"),
            "rebuilt": True,
            "consistent": True,
        }
        restarted.complete_operation(key, result)

        outcome = restarted.get_operation_outcome(key)
        self.assertEqual(outcome["status"], "succeeded")
        self.assertEqual(outcome["result"], result)
        self.assertEqual(restarted.get_projection_head("default"), durable_head)



class DispatchOperationLookupTests(unittest.TestCase):
    def test_admission_and_control_keys_have_authoritative_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            record = DispatchRecord(
                mutation_id="mutation-1",
                idempotency_key="dispatch-admission",
                card_id="card-1",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
            )
            record = store.put(record)

            admission = store.find_operation_by_idempotency(
                "dispatch-admission"
            )
            self.assertIsNotNone(admission)
            self.assertEqual(admission[0], "dispatch.create")
            self.assertEqual(admission[1].dispatch_id, record.dispatch_id)

            record.control_operations["dispatch-cancel"] = "cancel"
            store.put(record)
            control = store.find_operation_by_idempotency(
                "dispatch-cancel"
            )
            self.assertIsNotNone(control)
            self.assertEqual(control[0], "dispatch.cancel")
            self.assertEqual(control[1].dispatch_id, record.dispatch_id)
            self.assertIsNone(
                store.find_operation_by_idempotency(
                    "dispatch-admission", realm_id="other-realm"
                )
            )
            store.close()


class MutationHttpIdempotencyTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()

    def test_create_update_response_loss_and_outcome_lookup_are_idempotent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            authorization = {"Authorization": f"Bearer {token}"}
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                create_headers = {
                    **authorization,
                    "Idempotency-Key": "http-create-once",
                }
                payload = {
                    "title": "HTTP exactly once",
                    "summary": "Stable summary",
                    "auto_enrich": False,
                }
                first = client.post(
                    "/api/cards", json=payload, headers=create_headers
                )
                replay = client.post(
                    "/api/cards", json=payload, headers=create_headers
                )
                self.assertEqual(first.status_code, 201, first.text)
                self.assertEqual(replay.status_code, 201, replay.text)
                self.assertEqual(first.json(), replay.json())
                self.assertEqual(
                    replay.headers["X-PA-Operation-Replayed"], "true"
                )

                conflict = client.post(
                    "/api/cards",
                    json={**payload, "title": "Different"},
                    headers=create_headers,
                )
                self.assertEqual(conflict.status_code, 409)
                self.assertEqual(
                    conflict.json()["detail"]["code"], "idempotency_conflict"
                )

                card_id = first.json()["id"]
                update_headers = {
                    **authorization,
                    "Idempotency-Key": "http-update-once",
                }
                updated = client.patch(
                    f"/api/cards/{card_id}",
                    json={"lane": "active"},
                    headers=update_headers,
                )
                updated_replay = client.patch(
                    f"/api/cards/{card_id}",
                    json={"lane": "active"},
                    headers=update_headers,
                )
                self.assertEqual(updated.status_code, 200, updated.text)
                self.assertEqual(updated.json(), updated_replay.json())
                self.assertEqual(
                    updated_replay.headers["X-PA-Operation-Replayed"], "true"
                )

                outcome = client.get(
                    "/api/operations/http-create-once",
                    headers=authorization,
                )
                self.assertEqual(outcome.status_code, 200, outcome.text)
                self.assertEqual(outcome.json()["status"], "succeeded")
                self.assertEqual(outcome.json()["result"]["id"], card_id)
                reconcile_headers = {
                    **authorization,
                    "Idempotency-Key": "http-reconcile-once",
                }
                reconciled = client.post(
                    "/api/sync/reconcile",
                    json={"realm_id": "default"},
                    headers=reconcile_headers,
                )
                reconciled_replay = client.post(
                    "/api/sync/reconcile",
                    json={"realm_id": "default"},
                    headers=reconcile_headers,
                )
                self.assertEqual(reconciled.status_code, 200, reconciled.text)
                self.assertEqual(reconciled.json(), reconciled_replay.json())
                self.assertEqual(
                    reconciled_replay.headers["X-PA-Operation-Replayed"],
                    "true",
                )


                log = app.state.ctx.require_service("event_log")
                attributable = [
                    item
                    for item in log.entity_history(
                        "default", "card", card_id
                    )
                    if item["event"]["idempotency_key"]
                    in {"http-create-once", "http-update-once"}
                ]
                self.assertEqual(len(attributable), 2)

    def test_dispatch_operation_lookup_is_fenced_to_the_requested_realm(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                agent_enabled=False,
                subscribed_realms=["realm-a", "realm-b"],
            )
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            authorization = {"Authorization": f"Bearer {token}"}
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                dispatch_store = app.state.ctx.require_service("dispatch_store")
                dispatch_store.put(
                    DispatchRecord(
                        mutation_id="realm-a-mutation",
                        idempotency_key="realm-a-dispatch",
                        realm_id="realm-a",
                        card_id="realm-a-card",
                        authority_instance_id="authority",
                        authority_url="https://authority.example",
                        target_instance_id="target",
                    )
                )

                cross_realm = client.get(
                    "/api/operations/realm-a-dispatch",
                    params={"realm": "realm-b"},
                    headers=authorization,
                )
                same_realm = client.get(
                    "/api/operations/realm-a-dispatch",
                    params={"realm": "realm-a"},
                    headers=authorization,
                )

            self.assertEqual(cross_realm.status_code, 200, cross_realm.text)
            self.assertEqual(cross_realm.json()["status"], "not_found")
            self.assertEqual(same_realm.status_code, 200, same_realm.text)
            self.assertEqual(same_realm.json()["status"], "succeeded")
            self.assertEqual(
                same_realm.json()["result"]["card_id"], "realm-a-card"
            )


if __name__ == "__main__":
    unittest.main()
