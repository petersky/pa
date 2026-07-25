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
from pa.domain.models import CardCreate, CardEvent, CardLane, EventType, ItemKind
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.execution.lease import LeaseManager
from pa.instance.agent_session import reset_instance_agent
from pa.mcp.local_api import LocalPARequestError, request_local_pa
from pa.modules.items import ItemsModule
from pa.modules.sync import _ensure_projection_at_head
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
                        "/api/cards", json={"title": "owner card"}, headers=headers
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
