from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
from pa.domain.models import (
    CardCreate,
    CardEvent,
    CardLane,
    EventType,
    ItemKind,
    SyncCommit,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.execution.lease import LeaseManager
from pa.instance.agent_session import reset_instance_agent
from pa.mcp.local_api import (
    LocalPARequestError,
    LocalPAServerUnavailable,
    LocalPAUnknownOutcome,
    request_local_pa,
)
from pa.modules.items import ItemsModule
from pa.modules.sync import _ensure_projection_at_head
from pa.sync.event_log import EventHistoryCycleError, EventLog, StaleSyncHeadError
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
    def test_commit_and_entity_history_handle_more_than_5000_commits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            log = EventLog(ObjectStore(data_dir / "objects"), data_dir, "instance")
            parent: str | None = None
            commits: dict[str, SyncCommit] = {}
            events: dict[str, CardEvent] = {}
            expected: list[str] = []

            for index in range(5_200):
                event_id = f"event-{index}"
                commit_hash = f"commit-{index}"
                event_hash = f"event-hash-{index}"
                events[event_hash] = CardEvent(
                    id=event_id,
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="deep-card",
                    author_principal="user:test",
                    author_instance="test",
                    payload={"title": event_id},
                )
                commits[commit_hash] = SyncCommit(
                    hash=commit_hash,
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=[parent] if parent else [],
                    event_hashes=[event_hash],
                    author_principal="user:test",
                )
                parent = commit_hash
                expected.append(event_id)

            replayed: list[str] = []
            seen: set[str] = set()
            with (
                patch.object(log, "get_head", return_value=parent),
                patch.object(log, "get_commit", side_effect=commits.get),
                patch.object(log, "get_event", side_effect=events.get),
            ):
                log.apply_commit_chain(
                    parent or "",
                    lambda event: replayed.append(event.id),
                    seen=seen,
                )
                history = log.entity_history("default", "card", "deep-card")

            self.assertEqual(replayed, expected)
            self.assertEqual(len(seen), 5_200)
            self.assertEqual(
                [item["event"]["id"] for item in history],
                expected,
            )

    def test_create_card_after_deep_history_is_iterative(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")
            parent: str | None = None

            for index in range(1_500):
                event = _event(f"event-{index}")
                event_hash = objects.put_json(event.model_dump(mode="json"))
                commit = SyncCommit(
                    hash="",
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=[parent] if parent else [],
                    event_hashes=[event_hash],
                    author_principal="user:test",
                )
                commit.hash = objects.put_json(commit.model_dump(mode="json"))
                parent = commit.hash

            log.advance_ref("default", parent or "", expected_head=None)
            projection = CardProjection(data_dir / "pa.db", log)

            created = projection.create_card(CardCreate(title="After deep history"))

            self.assertEqual(projection.get_card(created.id).title, created.title)
            history = log.entity_history("default", "card", created.id)
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["event"]["card_id"], created.id)
            self.assertEqual(history[0]["event"]["type"], EventType.CARD_CREATED.value)
            self.assertEqual(history[0]["projection_effect"], "applied")

    def test_entity_history_preserves_merge_parent_order_and_shared_ancestors(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")

            def commit(event: CardEvent, parents: list[str]) -> str:
                event_hash = objects.put_json(event.model_dump(mode="json"))
                item = SyncCommit(
                    hash="",
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=parents,
                    event_hashes=[event_hash],
                    author_principal="user:test",
                )
                item.hash = objects.put_json(item.model_dump(mode="json"))
                return item.hash

            base = commit(
                CardEvent(
                    id="base",
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id="merge-card",
                    author_principal="user:test",
                    author_instance="instance",
                    payload={"id": "merge-card", "title": "base"},
                ),
                [],
            )
            left = commit(
                CardEvent(
                    id="left",
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="merge-card",
                    author_principal="user:test",
                    author_instance="left",
                    payload={"title": "left"},
                ),
                [base],
            )
            right = commit(
                CardEvent(
                    id="right",
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="merge-card",
                    author_principal="user:test",
                    author_instance="right",
                    payload={"body": "right"},
                ),
                [base],
            )
            merge = commit(
                CardEvent(
                    id="merge",
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="merge-card",
                    author_principal="sync:auto",
                    author_instance="instance",
                    payload={"lane": "done"},
                ),
                [left, right],
            )
            log.advance_ref("default", merge, expected_head=None)

            history = log.entity_history("default", "card", "merge-card")

            self.assertEqual(
                [item["event"]["id"] for item in history],
                ["base", "left", "right", "merge"],
            )
            self.assertEqual(history[-1]["parent_hashes"], [left, right])
            self.assertEqual(
                log.entity_snapshot(merge, "card", "merge-card"),
                {
                    "id": "merge-card",
                    "title": "left",
                    "body": "right",
                    "lane": "done",
                },
            )

    def test_entity_history_rejects_a_corrupt_parent_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            log = EventLog(ObjectStore(data_dir / "objects"), data_dir, "instance")
            commits = {
                "a": SyncCommit(
                    hash="a",
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=["b"],
                    event_hashes=[],
                    author_principal="user:test",
                ),
                "b": SyncCommit(
                    hash="b",
                    realm_id="default",
                    instance_id="instance",
                    parent_hashes=["a"],
                    event_hashes=[],
                    author_principal="user:test",
                ),
            }

            with (
                patch.object(log, "get_head", return_value="a"),
                patch.object(log, "get_commit", side_effect=commits.get),
                self.assertRaisesRegex(EventHistoryCycleError, "commit a"),
            ):
                log.entity_history("default", "card", "card-1")

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
            resolved = projection.get_card("card-1")
            resolution_event = left.get_event(merge.event_hashes[0])
            self.assertIsNotNone(resolution_event)
            self.assertIn("updated_at", resolution_event.payload)

            replica_log = EventLog(objects, data_dir / "replica", "replica")
            replica_log.advance_ref("default", merge.hash, expected_head=None)
            replica = CardProjection(data_dir / "replica.db", replica_log)
            replica.rebuild_from_log("default")
            replica_card = replica.get_card("card-1")
            self.assertEqual(replica_card.title, "resolved")
            self.assertEqual(replica_card.updated_at, resolved.updated_at)

    def test_list_hashes_returns_sharded_object_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            objects = ObjectStore(Path(tmp) / "objects")
            first = objects.put(b"one")
            second = objects.put(b"two")
            self.assertEqual(sorted(objects.list_hashes()), sorted([first, second]))

    def test_rebuild_replays_delete_without_appending_another_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")
            projection = CardProjection(data_dir / "pa.db", log)
            card = projection.create_card(CardCreate(title="delete me"))
            self.assertTrue(projection.delete_card(card.id, realm_id="default"))
            deleted_head = log.get_head("default")
            object_count = len(objects.list_hashes())

            projection.rebuild_from_log("default")

            self.assertIsNone(projection.get_card(card.id, realm_id="default"))
            self.assertEqual(log.get_head("default"), deleted_head)
            self.assertEqual(len(objects.list_hashes()), object_count)

    def test_lease_mutations_advance_projection_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")
            projection = CardProjection(data_dir / "pa.db", log)
            card = projection.create_card(CardCreate(title="leased"))
            leases = LeaseManager(projection, log, "instance")

            self.assertTrue(
                leases.grant(
                    card.id,
                    "default",
                    holder_instance="instance",
                    holder_principal="user:test",
                )
            )
            self.assertEqual(
                projection.get_projection_head("default"), log.get_head("default")
            )
            self.assertTrue(
                leases.release(
                    card.id,
                    "default",
                    principal_id="user:test",
                )
            )
            self.assertEqual(
                projection.get_projection_head("default"), log.get_head("default")
            )

    def test_conflict_preparation_repairs_stale_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            objects = ObjectStore(data_dir / "objects")
            log = EventLog(objects, data_dir, "instance")
            projection = CardProjection(data_dir / "pa.db", log)
            card = projection.create_card(CardCreate(title="before"))
            log.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id=card.id,
                    author_principal="user:test",
                    author_instance="instance",
                    payload={"title": "durable"},
                )
            )
            head = log.get_head("default")

            _ensure_projection_at_head(projection, log, "default", head)

            self.assertEqual(projection.get_card(card.id).title, "durable")
            self.assertEqual(projection.get_projection_head("default"), head)


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


class LocalMcpApiTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()

    def test_not_found_can_preserve_optional_mcp_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                404,
                request=httpx.Request("GET", "http://127.0.0.1/api/items/missing"),
            )
            with patch("httpx.request", return_value=response):
                result = request_local_pa(
                    settings,
                    "GET",
                    "/api/items/missing",
                    allow_not_found=True,
                )
            self.assertIsNone(result)

    def test_read_timeout_is_reported_without_masking_attribute_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            with (
                patch(
                    "httpx.request",
                    side_effect=httpx.ReadTimeout("slow owner"),
                ) as request,
                self.assertRaises(LocalPAUnknownOutcome) as raised,
            ):
                request_local_pa(settings, "POST", "/api/fleet/dispatch", json={})
            self.assertIn("operation=POST", str(raised.exception))
            self.assertNotIn("has no attribute", str(raised.exception))
            self.assertIn("same idempotency key", str(raised.exception))
            self.assertEqual(raised.exception.recovery_action, "get_operation_outcome")
            self.assertEqual(raised.exception.recovery_state, "lookup_required")
            sent_key = request.call_args.kwargs["headers"]["Idempotency-Key"]
            self.assertEqual(raised.exception.idempotency_key, sent_key)

    def test_http2_cancel_retries_mcp_read_with_same_correlation_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            success = httpx.Response(
                200,
                json={"status": "converged"},
                request=httpx.Request("GET", "http://127.0.0.1/api/sync/status"),
            )
            cancelled = httpx.RemoteProtocolError(
                "partial response body: http/2 stream closed with error code CANCEL (0x8)"
            )
            with patch("httpx.request", side_effect=[cancelled, success]) as request:
                result = request_local_pa(settings, "GET", "/api/sync/status")

            self.assertEqual(result["status"], "converged")
            self.assertEqual(request.call_count, 2)
            correlations = [
                call.kwargs["headers"]["X-Request-ID"] for call in request.call_args_list
            ]
            self.assertEqual(len(set(correlations)), 1)

    def test_http2_cancel_exhaustion_names_read_retry_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            cancelled = httpx.RemoteProtocolError(
                "response headers: http/2 stream closed with error code CANCEL (0x8)"
            )
            with (
                patch("httpx.request", side_effect=cancelled),
                self.assertRaises(LocalPAServerUnavailable) as raised,
            ):
                request_local_pa(settings, "GET", "/api/sync/status")
            self.assertIn("CANCEL (0x8)", str(raised.exception))
            self.assertIn("operation=GET", str(raised.exception))
            self.assertIn("safe_to_retry=True", str(raised.exception))
            self.assertIn("correlation_id=", str(raised.exception))

    def test_http2_cancel_retries_keyed_mcp_write_without_changing_key(self) -> None:
        phases = ("request body", "response headers", "partial response body")
        for phase in phases:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                settings = Settings(data_dir=Path(tmp), agent_enabled=False)
                success = httpx.Response(
                    200,
                    json={"effect_id": "one-durable-effect"},
                    request=httpx.Request("POST", "http://127.0.0.1/api/cards"),
                )
                cancelled = httpx.RemoteProtocolError(
                    f"{phase}: http/2 stream closed with error code CANCEL (0x8)"
                )
                with patch("httpx.request", side_effect=[cancelled, success]) as request:
                    result = request_local_pa(
                        settings,
                        "POST",
                        "/api/cards",
                        json={"title": "once"},
                        headers={"Idempotency-Key": "stable-mcp-write"},
                    )

                self.assertEqual(result["effect_id"], "one-durable-effect")
                keys = [
                    call.kwargs["headers"]["Idempotency-Key"]
                    for call in request.call_args_list
                ]
                self.assertEqual(keys, ["stable-mcp-write", "stable-mcp-write"])

    def test_mutation_server_errors_are_unknown_without_noncommit_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            for status in (500, 503):
                with self.subTest(status=status):
                    detail = {
                        "code": "server_failure",
                        "message": "failure after an unknown boundary",
                    }
                    response = httpx.Response(
                        status,
                        json={"detail": detail},
                        headers={"X-Request-ID": f"server-{status}"},
                        request=httpx.Request(
                            "POST", "http://127.0.0.1/api/cards"
                        ),
                    )
                    with (
                        patch("httpx.request", return_value=response) as request,
                        self.assertRaises(LocalPAUnknownOutcome) as raised,
                    ):
                        request_local_pa(
                            settings,
                            "POST",
                            "/api/cards",
                            json={"title": "Ambiguous"},
                            headers={"Idempotency-Key": "stable-mutation-key"},
                        )
                    error = raised.exception
                    self.assertEqual(error.status, status)
                    self.assertEqual(error.detail, detail)
                    self.assertEqual(error.idempotency_key, "stable-mutation-key")
                    self.assertEqual(error.correlation_id, f"server-{status}")
                    self.assertEqual(
                        error.recovery_action, "get_operation_outcome"
                    )
                    self.assertEqual(error.recovery_state, "lookup_required")
                    self.assertIn("same idempotency key", str(error))
                    self.assertEqual(
                        request.call_args.kwargs["headers"]["Idempotency-Key"],
                        "stable-mutation-key",
                    )

    def test_lost_mutation_response_preserves_supplied_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            with (
                patch(
                    "httpx.request",
                    side_effect=httpx.ReadError("response stream lost"),
                ),
                self.assertRaises(LocalPAUnknownOutcome) as raised,
            ):
                request_local_pa(
                    settings,
                    "PATCH",
                    "/api/cards/card-1",
                    json={"title": "Possibly committed"},
                    headers={"Idempotency-Key": "lost-response-key"},
                )
            self.assertEqual(
                raised.exception.idempotency_key, "lost-response-key"
            )
            self.assertEqual(
                raised.exception.recovery_action, "get_operation_outcome"
            )
            self.assertIn("same idempotency key", str(raised.exception))

    def test_malformed_success_response_is_a_recoverable_unknown_outcome(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                200,
                content=b'{"committed":',
                headers={"X-Request-ID": "server-correlation"},
                request=httpx.Request("POST", "http://127.0.0.1/api/cards"),
            )
            with (
                patch("httpx.request", return_value=response),
                self.assertRaises(LocalPAUnknownOutcome) as raised,
            ):
                request_local_pa(
                    settings,
                    "POST",
                    "/api/cards",
                    json={"title": "Possibly committed"},
                    headers={"Idempotency-Key": "malformed-success-key"},
                )

            error = raised.exception
            self.assertEqual(error.idempotency_key, "malformed-success-key")
            self.assertEqual(error.status, 200)
            self.assertEqual(error.correlation_id, "server-correlation")
            self.assertEqual(error.recovery_state, "lookup_required")
            self.assertEqual(error.recovery_action, "get_operation_outcome")
            self.assertEqual(error.detail["code"], "invalid_success_response")
            self.assertNotIn('{"committed":', str(error))

    def test_request_timeout_can_be_extended_for_durable_admission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                202,
                json={"accepted": True, "dispatch_id": "dispatch-1"},
                request=httpx.Request("POST", "http://127.0.0.1/api/fleet/dispatch"),
            )
            with patch("httpx.request", return_value=response) as request:
                result = request_local_pa(
                    settings,
                    "POST",
                    "/api/fleet/dispatch",
                    json={},
                    timeout_seconds=30.0,
                )

            self.assertTrue(result["accepted"])
            self.assertGreater(request.call_args.kwargs["timeout"], 29.0)
            self.assertLessEqual(request.call_args.kwargs["timeout"], 30.0)

    def test_default_owner_request_budget_tolerates_normal_write_contention(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                200,
                json={"outcome": "succeeded"},
                request=httpx.Request(
                    "GET", "http://127.0.0.1/api/operations/operation-1"
                ),
            )
            with patch("httpx.request", return_value=response) as request:
                request_local_pa(settings, "GET", "/api/operations/operation-1")

            self.assertGreater(request.call_args.kwargs["timeout"], 9.9)

    def test_no_content_mutation_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                204,
                request=httpx.Request(
                    "DELETE", "http://127.0.0.1/api/repositories/repo-1"
                ),
            )
            with patch("httpx.request", return_value=response):
                result = request_local_pa(
                    settings,
                    "DELETE",
                    "/api/repositories/repo-1",
                )
            self.assertIsNone(result)

    def test_validation_error_preserves_sanitized_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                422,
                json={
                    "detail": [
                        {
                            "type": "enum",
                            "loc": ["body", "lane"],
                            "msg": "Input should be 'todo'",
                            "input": "secret-value",
                        }
                    ]
                },
                headers={"X-Request-ID": "server-correlation"},
                request=httpx.Request("POST", "http://127.0.0.1/api/cards"),
            )
            with (
                patch("httpx.request", return_value=response),
                self.assertRaises(LocalPARequestError) as raised,
            ):
                request_local_pa(
                    settings, "POST", "/api/cards", json={"lane": "invalid"}
                )
            error = raised.exception
            self.assertEqual(error.operation, "POST")
            self.assertEqual(error.endpoint, "/api/cards")
            self.assertEqual(error.status, 422)
            self.assertEqual(error.correlation_id, "server-correlation")
            self.assertEqual(
                error.validation,
                [
                    {
                        "type": "enum",
                        "loc": ["body", "lane"],
                        "msg": "Input should be 'todo'",
                    }
                ],
            )
            self.assertNotIn("secret-value", str(error))
            self.assertNotIn("instance mismatch", str(error))

    def test_malformed_id_validation_is_not_an_instance_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                422,
                json={
                    "detail": [
                        {
                            "type": "uuid_parsing",
                            "loc": ["path", "card_id"],
                            "msg": "Input should be a valid UUID",
                        }
                    ]
                },
                request=httpx.Request("GET", "http://127.0.0.1/api/cards/bad"),
            )
            with (
                patch("httpx.request", return_value=response),
                self.assertRaisesRegex(LocalPARequestError, "uuid_parsing") as raised,
            ):
                request_local_pa(settings, "GET", "/api/cards/bad")
            self.assertIsNotNone(raised.exception.validation)
            self.assertNotIn("instance mismatch", str(raised.exception))

    def test_verified_instance_mismatch_has_specific_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            response = httpx.Response(
                422,
                json={"detail": "request rejected"},
                headers={
                    "X-PA-Instance-ID": "other-instance",
                    "X-Request-ID": "mismatch-correlation",
                },
                request=httpx.Request("POST", "http://127.0.0.1/api/cards"),
            )
            with (
                patch.dict(os.environ, {"PA_INSTANCE_ID": "bridge-instance"}),
                patch("httpx.request", return_value=response),
                self.assertRaisesRegex(
                    LocalPARequestError,
                    "bridge instance 'bridge-instance' reached server instance "
                    "'other-instance'",
                ) as raised,
            ):
                request_local_pa(settings, "POST", "/api/cards", json={})
            self.assertEqual(raised.exception.correlation_id, "mismatch-correlation")

    def test_auth_and_server_errors_keep_context_without_mismatch_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            for status in (401, 403, 500, 503):
                response = httpx.Response(
                    status,
                    json={"detail": "internal or auth detail"},
                    request=httpx.Request("GET", "http://127.0.0.1/api/cards"),
                )
                with (
                    patch("httpx.request", return_value=response),
                    self.assertRaises(LocalPARequestError) as raised,
                ):
                    request_local_pa(settings, "GET", "/api/cards")
                self.assertEqual(raised.exception.status, status)
                self.assertIn("operation=GET", str(raised.exception))
                self.assertIn("endpoint=/api/cards", str(raised.exception))
                self.assertNotIn("instance mismatch", str(raised.exception))
                self.assertNotIn("internal or auth detail", str(raised.exception))

    def test_structured_remote_error_survives_mcp_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            detail = {
                "code": "target_projection_not_ready",
                "message": "Target projection is catching up.",
                "recoverable": True,
                "retry_after": 1,
                "dispatch_id": "dispatch-1",
                "target_instance_id": "target-1",
                "target_correlation_id": "target-correlation",
                "committed": False,
                "recovery_state": "safe_to_retry_with_same_key",
            }
            response = httpx.Response(
                503,
                json={"detail": detail},
                headers={"X-Request-ID": "authority-correlation"},
                request=httpx.Request("POST", "http://127.0.0.1/api/fleet/dispatch"),
            )
            with (
                patch("httpx.request", return_value=response),
                self.assertRaises(LocalPARequestError) as raised,
            ):
                request_local_pa(settings, "POST", "/api/fleet/dispatch", json={})
            error = raised.exception
            self.assertEqual(error.detail, detail)
            self.assertEqual(error.code, "target_projection_not_ready")
            self.assertTrue(error.recoverable)
            self.assertEqual(error.retry_after, 1)
            self.assertEqual(error.correlation_id, "authority-correlation")

    def test_placement_409_surfaces_rejected_candidates_on_mcp_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=False)
            detail = {
                "code": "no_eligible_instance",
                "message": "No eligible instance remains after policy filters.",
                "recoverable": True,
                "rejected_candidates": [
                    {
                        "instance_id": "0c7d8ecb-7e45-4579-8fa0-35159492d3f1",
                        "name": "macbook",
                        "rejection_codes": ["policy_unknown_on_mixed_version_peer"],
                    }
                ],
            }
            response = httpx.Response(
                409,
                json={"detail": detail},
                headers={"X-Request-ID": "placement-correlation"},
                request=httpx.Request(
                    "POST", "http://127.0.0.1/api/fleet/placement/preview"
                ),
            )
            with (
                patch("httpx.request", return_value=response),
                self.assertRaises(LocalPARequestError) as raised,
            ):
                request_local_pa(
                    settings, "POST", "/api/fleet/placement/preview", json={}
                )
            error = raised.exception
            self.assertEqual(error.detail, detail)
            self.assertEqual(error.rejected_candidates, detail["rejected_candidates"])
            self.assertEqual(
                error.rejection_codes, ["policy_unknown_on_mixed_version_peer"]
            )
            self.assertIn("rejected_candidates=", str(error))
            self.assertIn("policy_unknown_on_mixed_version_peer", str(error))
            self.assertIn("rejection_codes=", str(error))

    def test_server_identity_and_correlation_headers_are_exposed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="owner",
                agent_enabled=False,
            )
            with TestClient(Kernel.boot(settings=settings).build_app()) as client:
                response = client.get(
                    "/api/ready", headers={"X-Request-ID": "client-correlation"}
                )
            self.assertEqual(response.headers["X-PA-Instance-ID"], "owner")
            self.assertEqual(response.headers["X-Request-ID"], "client-correlation")

    def test_list_tools_omit_empty_filters_and_forward_supplied_filters(self) -> None:
        class FakeMcp:
            def __init__(self) -> None:
                self.functions: dict[str, object] = {}

            def tool(self):
                def register(fn):
                    self.functions[fn.__name__] = fn
                    return fn

                return register

        mcp = FakeMcp()
        ctx = MagicMock()
        response = httpx.Response(
            200,
            json=[],
            request=httpx.Request("GET", "http://127.0.0.1/api/cards"),
        )
        with (
            patch.dict(os.environ, {"PA_LOCAL_API_TOKEN": "test-token"}),
            patch("httpx.request", return_value=response) as local_request,
        ):
            ItemsModule().register_mcp(mcp, ctx)

            self.assertEqual(mcp.functions["list_cards"](), [])
            self.assertIsNone(local_request.call_args.kwargs["params"])
            self.assertEqual(mcp.functions["list_items"](), [])
            self.assertIsNone(local_request.call_args.kwargs["params"])

            self.assertEqual(
                mcp.functions["list_cards"](
                    realm="team",
                    lane=CardLane.ACTIVE,
                    kind="task",
                ),
                [],
            )
            self.assertEqual(
                local_request.call_args.kwargs["params"],
                {"realm": "team", "lane": "active", "kind": "task"},
            )
            self.assertEqual(
                mcp.functions["list_items"](kind=ItemKind.TASK, status="done"),
                [],
            )
            self.assertEqual(
                local_request.call_args.kwargs["params"],
                {"kind": "task", "status": "done"},
            )

    def test_explicit_owner_target_survives_cold_start_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="owner",
                port=9876,
                agent_enabled=False,
            )
            token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
            headers = {"Authorization": f"Bearer {token}"}

            def exercise(app) -> tuple[str, str]:
                with TestClient(app) as client:
                    ready = client.get("/api/ready", headers=headers)
                    self.assertEqual(ready.status_code, 200, ready.text)
                    self.assertEqual(ready.json()["instance_id"], "owner")
                    card = client.post(
                        "/api/cards",
                        json={"title": "owner card"},
                        headers={
                            **headers, "Idempotency-Key": "owner-card-create"
                        },
                    )
                    self.assertEqual(card.status_code, 201, card.text)
                    item = client.post(
                        "/api/items",
                        json={"kind": "task", "title": "owner item"},
                        headers=headers,
                    )
                    self.assertEqual(item.status_code, 201, item.text)
                    project = client.post(
                        "/api/projects",
                        json={"title": "owner project"},
                        headers=headers,
                    )
                    self.assertEqual(project.status_code, 201, project.text)
                    self.assertEqual(
                        client.get("/api/cards", headers=headers).status_code, 200
                    )
                    self.assertEqual(
                        client.get("/api/items", headers=headers).status_code, 200
                    )
                    self.assertEqual(
                        client.get("/api/projects", headers=headers).status_code, 200
                    )
                    sync = client.get("/api/sync/status", headers=headers)
                    self.assertEqual(sync.status_code, 200, sync.text)
                    workspaces = client.get("/api/workspaces", headers=headers)
                    self.assertEqual(workspaces.status_code, 200, workspaces.text)
                    return card.json()["id"], project.json()["id"]

            first_card, first_project = exercise(
                Kernel.boot(settings=settings).build_app()
            )
            reset_instance_agent()
            reset_store()

            with TestClient(Kernel.boot(settings=settings).build_app()) as client:
                ready = client.get("/api/ready", headers=headers)
                self.assertEqual(ready.status_code, 200, ready.text)
                self.assertEqual(
                    client.get(
                        f"/api/cards/{first_card}", headers=headers
                    ).status_code,
                    200,
                )
                self.assertEqual(
                    client.get(
                        f"/api/projects/{first_project}", headers=headers
                    ).status_code,
                    200,
                )
                self.assertGreaterEqual(
                    len(client.get("/api/items", headers=headers).json()), 2
                )
                self.assertEqual(
                    client.get("/api/sync/status", headers=headers).status_code, 200
                )


if __name__ == "__main__":
    unittest.main()
