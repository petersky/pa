from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pa.acp.final_message import likely_user_input_request
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.models import AgentSession, CardEvent, EventType
from pa.domain.notifications import (
    InteractionChoice,
    InteractionKind,
    InteractionRequest,
    InteractionResponse,
    InteractionState,
    Notification,
    NotificationCreate,
    NotificationType,
    NotificationVisibility,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord
from pa.instance.agent_session import AgentSessionRuntime, reset_instance_agent
from pa.modules.fleet import _create_operator_input_notification
from pa.notifications import NotificationConflict
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@pytest.fixture(autouse=True)
def _reset_singletons():
    reset_kernel()
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()
    reset_kernel()


def _kernel(tmp_path: Path) -> Kernel:
    return Kernel.boot(
        settings=Settings(
            data_dir=tmp_path,
            instance_id="local",
            instance_name="Local PA",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default", "engineering"],
            primary_realm="default",
            peers=[],
        )
    )


def _interaction(
    *,
    request_id: str = "request-1",
    schema: dict | None = None,
    choices: list[InteractionChoice] | None = None,
    deadline: datetime | None = None,
    sensitive: bool = False,
    continuation_mode: str = "protocol",
) -> InteractionRequest:
    return InteractionRequest(
        request_id=request_id,
        kind=InteractionKind.ACP_ELICITATION,
        prompt="Choose a deployment target",
        response_schema=schema,
        choices=choices or [],
        allow_freeform=schema is None and not choices,
        allow_cancel=True,
        sensitive=sensitive,
        deadline=deadline,
        continuation_mode=continuation_mode,
    )


def _create(service, **updates):
    data = {
        "realm_id": "default",
        "type": NotificationType.INTERACTION,
        "title": "Input required",
        "summary": "Choose a deployment target",
        "deduplication_key": "interaction:request-1",
        "interaction": _interaction(),
    }
    data.update(updates)
    return service.create(NotificationCreate(**data), principal_id="user:local")


def _agent_runtime(kernel: Kernel) -> AgentSessionRuntime:
    session = AgentSession(
        agent_name="codex",
        status="connected",
        realm_id="default",
        principal_id="user:local",
        card_id="card-1",
        project_id="project-1",
    )
    kernel.ctx.store.save_session(session)
    manager = MagicMock()
    manager.settings = kernel.ctx.settings
    manager.store = kernel.ctx.store
    manager.browser = MagicMock()
    manager.async_runtime = None
    manager.notification_service = kernel.ctx.require_service("notifications")
    manager.progress_handler = None
    manager.quiescing = False
    manager.should_auto_approve_async = AsyncMock(return_value=False)
    manager.set_auto_approve_async = AsyncMock()
    return AgentSessionRuntime(manager, session)


def test_durable_lifecycle_dedup_audit_and_idempotent_delivery(tmp_path: Path) -> None:
    kernel = _kernel(tmp_path)
    service = kernel.ctx.require_service("notifications")
    delivered: list[InteractionResponse] = []
    notice = _create(
        service,
        interaction=_interaction(
            choices=[InteractionChoice(id="prod", label="Production", value="prod")]
        ),
    )
    duplicate = _create(
        service,
        interaction=_interaction(
            choices=[InteractionChoice(id="prod", label="Production", value="prod")]
        ),
    )
    assert duplicate.id == notice.id
    assert duplicate.coalesced_count == 2

    service.register_delivery_handler(notice.id, delivered.append)
    result = asyncio.run(
        service.respond(
            notice,
            InteractionResponse(idempotency_key="answer-1", choice_id="prod"),
            principal_id="user:local",
        )
    )
    assert result.interaction.state == InteractionState.DELIVERED
    assert result.resolved_at is not None
    assert delivered[0].choice_id == "prod"

    repeated = asyncio.run(
        service.respond(
            notice,
            InteractionResponse(idempotency_key="answer-1", choice_id="prod"),
            principal_id="user:local",
        )
    )
    assert repeated.version == result.version
    assert len(delivered) == 1
    audit = kernel.ctx.store.list_notification_audit(notice.id)
    assert len(audit) >= 4
    assert {entry["action"] for entry in audit}.issuperset(
        {"created", "interaction.answered", "interaction.delivered"}
    )
    assert (
        kernel.ctx.store.count_outstanding_notifications(
            realm_id="default", principal_id="user:local"
        )
        == 0
    )


def test_concurrent_creation_uses_one_deterministic_notification(
    tmp_path: Path,
) -> None:
    service = _kernel(tmp_path).ctx.require_service("notifications")
    with ThreadPoolExecutor(max_workers=8) as pool:
        notices = list(pool.map(lambda _index: _create(service), range(24)))
    assert len({item.id for item in notices}) == 1
    persisted = service.get_authorized(
        notices[0].id, principal_id="user:local", realms={"default"}
    )
    assert persisted.coalesced_count == 24

    general = service.create(
        NotificationCreate(
            realm_id="default", title="Concurrent receipt", deduplication_key="receipt"
        ),
        principal_id="user:local",
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        read = pool.submit(
            service.mark_read,
            general,
            principal_id="user:local",
            idempotency_key="read",
        )
        acknowledged = pool.submit(
            service.acknowledge,
            general,
            principal_id="user:local",
            idempotency_key="ack",
        )
        read.result()
        acknowledged.result()
    receipt = service.get_authorized(
        general.id, principal_id="user:local", realms={"default"}
    )
    assert receipt.read_at is not None
    assert receipt.acknowledged_at is not None


def test_acp_permission_chat_and_bell_share_one_correlated_lifecycle(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    runtime = _agent_runtime(kernel)

    async def run():
        pending = asyncio.create_task(
            runtime._on_permission(
                "external-session",
                {
                    "request_id": "permission-1",
                    "tool_call": {"toolCallId": "tool-1", "title": "Run tests"},
                    "options": [
                        {
                            "optionId": "allow_once",
                            "kind": "allow_once",
                            "name": "Allow once",
                        },
                        {
                            "optionId": "reject_once",
                            "kind": "reject_once",
                            "name": "Reject",
                        },
                    ],
                },
            )
        )
        for _ in range(100):
            if runtime._permission_notification_ids.get("permission-1"):
                break
            if pending.done():
                await pending
            await asyncio.sleep(0.01)
        notification_id = runtime._permission_notification_ids["permission-1"]
        assert await runtime.respond_permission(
            "permission-1",
            allow=True,
            option_id="allow_once",
            principal_id="user:local",
        )
        return notification_id, await pending

    notification_id, protocol_result = asyncio.run(run())
    assert protocol_result.outcome.option_id == "allow_once"
    notice = kernel.ctx.store.get_notification(notification_id)
    assert notice.interaction.state == InteractionState.DELIVERED
    events = kernel.ctx.store.list_transcript_events(runtime.session_id)
    assert [event.event_type for event in events].count("permission_request") == 1
    assert [event.event_type for event in events].count("permission_resolved") == 1


def test_acp_structured_elicitation_delivers_fields_to_waiting_provider(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    runtime = _agent_runtime(kernel)
    service = kernel.ctx.require_service("notifications")

    async def run():
        pending = asyncio.create_task(
            runtime._on_elicitation(
                "external-session",
                {
                    "request_id": "elicitation-1",
                    "method": "elicitation/create",
                    "message": "Choose an environment",
                    "requestedSchema": {
                        "type": "object",
                        "properties": {"environment": {"type": "string"}},
                        "required": ["environment"],
                    },
                    "allowFreeform": False,
                    "allowCancel": True,
                },
            )
        )
        notice = None
        for _ in range(100):
            records = service.list_authorized(
                principal_id="user:local",
                realms={"default"},
                realm_id="default",
                outstanding=True,
            )
            notice = next(
                (
                    item
                    for item in records
                    if item.interaction.request_id == "elicitation-1"
                ),
                None,
            )
            if notice:
                break
            if pending.done():
                await pending
            await asyncio.sleep(0.01)
        assert notice is not None
        await service.respond(
            notice,
            InteractionResponse(
                idempotency_key="elicitation-answer",
                fields={"environment": "staging"},
            ),
            principal_id="user:local",
        )
        return await pending

    assert asyncio.run(run()) == {
        "action": "accept",
        "content": {"environment": "staging"},
    }


def test_provider_cancel_supersedes_durable_elicitation(tmp_path: Path) -> None:
    kernel = _kernel(tmp_path)
    runtime = _agent_runtime(kernel)

    async def run():
        pending = asyncio.create_task(
            runtime._on_elicitation(
                "external-session",
                {
                    "request_id": "provider-cancel-1",
                    "method": "elicitation/create",
                    "message": "Provide a value",
                },
            )
        )
        for _ in range(100):
            notification_id = runtime._elicitation_notification_ids.get(
                "provider-cancel-1"
            )
            if notification_id:
                break
            await asyncio.sleep(0.01)
        await runtime._on_elicitation(
            "external-session",
            {
                "request_id": "provider-cancel-1",
                "method": "elicitation/cancel",
            },
        )
        return notification_id, await pending

    notification_id, result = asyncio.run(run())
    assert result == {"action": "cancel"}
    notice = kernel.ctx.store.get_notification(notification_id)
    assert notice.interaction.state == InteractionState.SUPERSEDED
    assert notice.resolved_at is not None


def test_structured_validation_sensitive_redaction_cancel_and_expiry(
    tmp_path: Path,
) -> None:
    service = _kernel(tmp_path).ctx.require_service("notifications")
    schema = {
        "type": "object",
        "properties": {"environment": {"type": "string"}},
        "required": ["environment"],
        "additionalProperties": False,
    }
    notice = _create(
        service,
        deduplication_key="interaction:structured",
        interaction=_interaction(schema=schema, sensitive=True),
    )
    service.register_delivery_handler(notice.id, lambda _response: None)
    with pytest.raises(NotificationConflict, match="required schema"):
        asyncio.run(
            service.respond(
                notice,
                InteractionResponse(idempotency_key="bad", fields={}),
                principal_id="user:local",
            )
        )
    result = asyncio.run(
        service.respond(
            notice,
            InteractionResponse(
                idempotency_key="good", fields={"environment": "production"}
            ),
            principal_id="user:local",
        )
    )
    public = result.public_dict()
    assert public["interaction"]["response"] is None
    assert public["interaction"]["response_summary"] == "Sensitive response recorded"

    cancelled = _create(
        service,
        deduplication_key="interaction:cancel",
        interaction=_interaction(request_id="cancel"),
    )
    service.register_delivery_handler(cancelled.id, lambda _response: None)
    cancelled = asyncio.run(
        service.respond(
            cancelled,
            InteractionResponse(idempotency_key="cancel", cancel=True),
            principal_id="user:local",
        )
    )
    assert cancelled.interaction.state == InteractionState.CANCELLED

    expired = _create(
        service,
        deduplication_key="interaction:expired",
        interaction=_interaction(
            request_id="expired", deadline=datetime.now(UTC) - timedelta(seconds=1)
        ),
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    expiry_deliveries: list[InteractionResponse] = []
    service.register_delivery_handler(expired.id, expiry_deliveries.append)
    assert asyncio.run(service.expire_due(realm_id="default")) == 1
    assert expiry_deliveries[0].cancel is True
    persisted_expired = service.get_authorized(
        expired.id, principal_id="user:local", realms={"default"}
    )
    assert persisted_expired.interaction.state == InteractionState.EXPIRED
    assert persisted_expired.delivery_state.value == "delivered"
    with pytest.raises(NotificationConflict) as error:
        asyncio.run(
            service.respond(
                expired,
                InteractionResponse(idempotency_key="late", value="too late"),
                principal_id="user:local",
            )
        )
    assert error.value.code == "interaction_expired"

    with pytest.raises(ValueError, match="64 KB"):
        InteractionResponse(idempotency_key="oversized", value="x" * 70_000)


def test_failed_delivery_can_only_retry_the_same_response(tmp_path: Path) -> None:
    service = _kernel(tmp_path).ctx.require_service("notifications")
    notice = _create(
        service,
        interaction=_interaction(
            choices=[
                InteractionChoice(id="yes", label="Yes", value=True),
                InteractionChoice(id="no", label="No", value=False),
            ]
        ),
    )

    def fail(_response):
        raise RuntimeError("provider disconnected")

    service.register_delivery_handler(notice.id, fail)
    with pytest.raises(NotificationConflict) as error:
        asyncio.run(
            service.respond(
                notice,
                InteractionResponse(idempotency_key="first", choice_id="yes"),
                principal_id="user:local",
            )
        )
    assert error.value.code == "delivery_failed"

    failed = service.get_authorized(
        notice.id, principal_id="user:local", realms={"default"}
    )
    assert failed.outstanding is True
    assert (
        service.store.count_outstanding_notifications(
            realm_id="default", principal_id="user:local"
        )
        == 1
    )
    with pytest.raises(NotificationConflict) as mismatch:
        asyncio.run(
            service.respond(
                failed,
                InteractionResponse(idempotency_key="changed", choice_id="no"),
                principal_id="user:local",
            )
        )
    assert mismatch.value.code == "delivery_retry_mismatch"

    delivered: list[str] = []
    service.register_delivery_handler(
        notice.id, lambda response: delivered.append(response.choice_id or "")
    )
    result = asyncio.run(
        service.respond(
            failed,
            InteractionResponse(idempotency_key="retry", choice_id="yes"),
            principal_id="user:local",
        )
    )
    assert result.interaction.state == InteractionState.DELIVERED
    assert delivered == ["yes"]


def test_prompt_continuation_is_correlated_to_recoverable_session(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    service = kernel.ctx.require_service("notifications")
    runtime = MagicMock()
    manager = SimpleNamespace(get=lambda session_id: runtime)
    kernel.ctx.register_service("instance_agent", manager)
    notice = _create(
        service,
        session_id="session-1",
        card_id="card-1",
        interaction=_interaction(continuation_mode="prompt"),
    )
    result = asyncio.run(
        service.respond(
            notice,
            InteractionResponse(idempotency_key="reply", value="Use staging"),
            principal_id="user:local",
        )
    )
    assert result.interaction.state == InteractionState.DELIVERED
    runtime.enqueue.assert_called_once()
    message = runtime.enqueue.call_args.args[0]
    assert result.interaction.request_id in message
    assert "Use staging" in message
    assert runtime.enqueue.call_args.kwargs["source"] == "notification-response"


def test_authorization_pagination_filters_and_http_remote_routing(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    service = kernel.ctx.require_service("notifications")
    private = _create(
        service,
        deduplication_key="private",
        visibility=NotificationVisibility.PRINCIPAL,
        principal_id="user:alice",
    )
    resolved_notice = service.create(
        NotificationCreate(
            realm_id="default",
            title="Resolved general notice",
            deduplication_key="resolved",
        ),
        principal_id="user:local",
    )
    resolved = service.resolve(
        resolved_notice,
        principal_id="user:local",
        idempotency_key="resolved",
    )
    _create(service, deduplication_key="visible")
    assert (
        service.list_authorized(
            principal_id="user:local",
            realms={"default"},
            realm_id="default",
            resolved=False,
            limit=1,
            offset=0,
        )[0].id
        != private.id
    )
    assert resolved.id in {
        item.id
        for item in service.list_authorized(
            principal_id="user:local",
            realms={"default"},
            realm_id="default",
            resolved=True,
        )
    }

    remote = _create(
        service,
        deduplication_key="remote",
        owner_instance_id="remote",
        owner_url="https://remote.example",
        destination_url="/agent?session=remote-session",
        distributable=False,
    )
    proxyable = _create(
        service,
        deduplication_key="proxyable",
        owner_instance_id="remote",
        owner_url="https://remote.example",
        destination_url="/agent?session=proxy-session",
        distributable=True,
    )
    offline = _create(
        service,
        deduplication_key="offline",
        owner_instance_id="offline-owner",
        owner_url=None,
        distributable=True,
    )
    service.create(
        NotificationCreate(
            realm_id="not-subscribed",
            type=NotificationType.INTERACTION,
            title="Hidden realm request",
            interaction=_interaction(request_id="hidden"),
        ),
        principal_id="user:local",
    )
    with TestClient(kernel.build_app()) as client:
        assert client.get("/").status_code == 200
        listed = client.get("/api/notifications?outstanding=true&limit=20")
        assert listed.status_code == 200
        assert all(item["id"] != private.id for item in listed.json()["items"])
        hidden_realm = client.get(
            "/api/notifications?realm=not-subscribed&outstanding=true"
        )
        response = client.post(
            f"/api/notifications/{remote.id}/respond",
            headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
            json={"idempotency_key": "remote-answer", "value": "answer"},
        )
        offline_response = client.post(
            f"/api/notifications/{offline.id}/respond",
            headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
            json={"idempotency_key": "offline-answer", "value": "answer"},
        )
        upstream = AsyncMock()
        upstream.__aenter__.return_value = upstream
        upstream.__aexit__.return_value = None
        upstream.post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"id": proxyable.id, "interaction": {"state": "delivered"}},
        )
        with patch("pa.modules.notifications.httpx.AsyncClient", return_value=upstream):
            proxied_response = client.post(
                f"/api/notifications/{proxyable.id}/respond",
                headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
                json={"idempotency_key": "proxy-answer", "value": "answer"},
            )
    assert response.status_code == 409
    assert hidden_realm.json()["items"] == []
    assert hidden_realm.json()["outstanding_count"] == 0
    assert response.json()["detail"]["code"] == "remote_authority_required"
    assert response.json()["detail"]["destination"].startswith(
        "https://remote.example/agent"
    )
    assert offline_response.status_code == 503
    assert offline_response.json()["detail"]["code"] == "owner_unreachable"
    assert proxied_response.status_code == 200
    assert proxied_response.json()["interaction"]["state"] == "delivered"
    upstream.post.assert_awaited_once()


def test_legacy_database_migrates_notification_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE notifications (
              realm_id TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL,
              type TEXT NOT NULL, priority TEXT NOT NULL, outstanding INTEGER NOT NULL,
              unread INTEGER NOT NULL, deduplication_key TEXT, updated_at TEXT NOT NULL,
              expires_at TEXT, payload TEXT NOT NULL, PRIMARY KEY(realm_id, id)
            )
            """
        )
    CardProjection(db_path)
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(notifications)")}
    assert {"visibility", "principal_id", "resolved"}.issubset(columns)


def test_structured_mcp_operator_input_creates_remote_owned_request(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    request = MagicMock()
    request.app.state.ctx = kernel.ctx
    request.state.principal_id = "user:operator"
    record = DispatchRecord(
        dispatch_id="dispatch-1",
        mutation_id="mutation-1",
        card_id="card-1",
        project_id="project-1",
        realm_id="default",
        authority_instance_id="local",
        authority_url="http://pa.test:8080",
        target_instance_id="worker",
        session_id="session-1",
        state="running",
    )
    result = asyncio.run(
        _create_operator_input_notification(
            request,
            record,
            {
                "request_id": "choose-target",
                "prompt": "Choose the target",
                "choices": [{"id": "staging", "label": "Staging", "value": "staging"}],
                "allow_freeform": False,
            },
            idempotency_key="checkpoint-1",
            kind=InteractionKind.MCP_OPERATOR_INPUT,
        )
    )
    assert result["interaction"]["request_id"] == "choose-target"
    assert result["interaction"]["choices"][0]["id"] == "staging"
    assert result["owner_instance_id"] == "worker"
    assert result["owner_url"] is None
    assert result["delivery_state"] == "remote"


def test_concurrent_notification_heads_merge_deterministically(tmp_path: Path) -> None:
    objects = ObjectStore(tmp_path / "objects")
    left = EventLog(objects, tmp_path / "left", "left")
    right = EventLog(objects, tmp_path / "right", "right")
    first_merger = EventLog(objects, tmp_path / "merge-a", "merge-a")
    second_merger = EventLog(objects, tmp_path / "merge-b", "merge-b")
    base = Notification(
        id="notice-1",
        realm_id="default",
        type=NotificationType.GENERAL,
        title="Base",
    )
    _, base_head = left.append_event(
        CardEvent(
            type=EventType.NOTIFICATION_UPSERTED,
            realm_id="default",
            author_principal="user:test",
            author_instance="left",
            payload=base.model_dump(mode="json"),
        )
    )
    right.advance_ref("default", base_head.hash)
    _, left_head = left.append_event(
        CardEvent(
            type=EventType.NOTIFICATION_UPSERTED,
            realm_id="default",
            author_principal="user:test",
            author_instance="left",
            payload=base.model_copy(
                update={
                    "title": "Left",
                    "version": 2,
                    "read_at": datetime(2026, 8, 2, 12, tzinfo=UTC),
                    "idempotency_keys": ["read-left"],
                }
            ).model_dump(mode="json"),
        )
    )
    _, right_head = right.append_event(
        CardEvent(
            type=EventType.NOTIFICATION_UPSERTED,
            realm_id="default",
            author_principal="user:test",
            author_instance="right",
            payload=base.model_copy(
                update={
                    "title": "Right",
                    "version": 2,
                    "acknowledged_at": datetime(2026, 8, 2, 13, tzinfo=UTC),
                    "idempotency_keys": ["ack-right"],
                }
            ).model_dump(mode="json"),
        )
    )
    compatible, health = first_merger.compatible_histories(
        left_head.hash, right_head.hash
    )
    reverse_compatible, reverse_health = second_merger.compatible_histories(
        right_head.hash, left_head.hash
    )
    assert compatible and reverse_compatible
    title_resolution = next(
        item for item in health["automatic_resolutions"] if item["field"] == "title"
    )
    assert title_resolution["version"] == 2
    first = first_merger.merge_heads(
        "default",
        left_head.hash,
        right_head.hash,
        "sync:auto",
        automatic_resolutions=health["automatic_resolutions"],
    )
    second = second_merger.merge_heads(
        "default",
        right_head.hash,
        left_head.hash,
        "sync:auto",
        automatic_resolutions=reverse_health["automatic_resolutions"],
    )
    assert first.hash == second.hash

    projection_a = CardProjection(tmp_path / "projection-a.db")
    projection_b = CardProjection(tmp_path / "projection-b.db")
    first_merger.apply_commit_chain(first.hash, projection_a.apply_event)
    second_merger.apply_commit_chain(second.hash, projection_b.apply_event)
    projected_a = projection_a.get_notification("notice-1")
    projected_b = projection_b.get_notification("notice-1")
    assert projected_a.title == projected_b.title
    assert projected_a.read_at == projected_b.read_at
    assert projected_a.acknowledged_at == projected_b.acknowledged_at
    assert set(projected_a.idempotency_keys) == {"read-left", "ack-right"}


def test_notification_polls_throttle_expiry_and_use_one_store_call(
    tmp_path: Path,
) -> None:
    kernel = _kernel(tmp_path)
    with TestClient(kernel.build_app()) as client:
        service = client.app.state.ctx.require_service("notifications")
        list_calls: list[int] = []
        original_inbox = service.list_inbox

        def wrapped_inbox(**kwargs):
            list_calls.append(1)
            return original_inbox(**kwargs)

        service.list_inbox = wrapped_inbox
        first = client.get("/api/notifications")
        stamped = service._expire_mono.get("default")
        second = client.get("/api/notifications")
        assert first.status_code == 200
        assert second.status_code == 200
        assert "outstanding_count" in first.json()
        assert stamped is not None
        assert service._expire_mono.get("default") == stamped
        assert len(list_calls) == 2


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Please run `gh auth login`, then tell me when it is complete.", True),
        ("I need you to choose one of these two environments.", True),
        ("No action is required; all tests passed.", False),
        ("Implemented the change and verified the focused tests.", False),
    ],
)
def test_final_output_input_fallback_is_conservative(text: str, expected: bool) -> None:
    assert bool(likely_user_input_request(text)) is expected
