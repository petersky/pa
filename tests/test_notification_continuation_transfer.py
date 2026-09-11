from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from tests.test_notifications import _kernel, _reset_singletons  # noqa: F401
from pa.domain.models import AgentSession
from pa.domain.notifications import (
    ContinuationTransferRequest, InteractionChoice, InteractionRequest,
    InteractionResponse, InteractionState, InteractionKind, NotificationCreate,
)
from pa.execution.dispatch import DispatchRecord
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.notifications import NotificationConflict


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    kernel = _kernel(tmp_path)
    store = kernel.ctx.store
    service = kernel.ctx.require_service("notifications")
    sessions = []
    records = {}
    for name in ("old", "successor"):
        session = AgentSession(
            id=name, agent_name="codex", external_session_id=f"provider-{name}",
            dispatch_id=f"dispatch-{name}", origin_instance_id="local",
            authority_instance_id="local", realm_id="default", card_id="card-1",
            project_id="project-1", principal_id="user:local",
            status="closed" if name == "old" else "connected",
            recovery_json={"blocked": True, "context_lost": True} if name == "old" else {},
            execution_binding={
                "execution_card_id": "card-1", "execution_project_id": "project-1",
                "dispatch_id": f"dispatch-{name}", "realm_id": "default",
                "principal_id": "user:local", "origin_instance_id": "local",
            },
        )
        store.save_session(session)
        sessions.append(session)
        record = DispatchRecord(
            dispatch_id=session.dispatch_id, mutation_id=name, session_id=name,
            authority_instance_id="local", authority_url="http://pa.test",
            target_instance_id="local", card_id="card-1", project_id="project-1",
            state="failed" if name == "old" else "running",
        )
        records[record.dispatch_id] = record
    ledger = SimpleNamespace(get=records.get)
    manager = AgentSessionManager(kernel.ctx.settings, store, dispatch_store=ledger)
    runtime = AgentSessionRuntime(manager, sessions[1])
    runtime.connection = SimpleNamespace(connected=True)
    runtime._queue_paused = True  # Exercise durable enqueue without spawning a provider.
    manager._runtimes[runtime.session_id] = runtime
    kernel.ctx.register_service("instance_agent", manager)
    kernel.ctx.register_service("dispatch_store", ledger)
    data = NotificationCreate(
        title="Scope decision", type="interaction", session_id="old",
        dispatch_id="dispatch-old", card_id="card-1", project_id="project-1",
        deduplication_key="operator-input:dispatch-old:request-1",
        interaction=InteractionRequest(
            request_id="request-1", kind="mcp_operator_input", prompt="Approve this scope?",
            protocol_method="pa/report_dispatch_progress.operator_input",
            protocol_request_id="request-1", continuation_mode="prompt",
            choices=[InteractionChoice(id="approve", label="Approve scope", value={"confirmation_id": "confirm-1", "scope": ["repo-1"]})],
        ),
    )
    notice = service.create(data, principal_id="user:local")
    request = ContinuationTransferRequest(
        idempotency_key="transfer-1", expected_version=notice.version,
        expected_session_id="old", expected_dispatch_id="dispatch-old",
        successor_session_id="successor", successor_dispatch_id="dispatch-successor",
        reason="Original provider context is lost; recover the same pending decision",
    )
    return SimpleNamespace(
        kernel=kernel, store=store, service=service, manager=manager,
        runtime=runtime, old=sessions[0], successor=sessions[1], records=records,
        notice=notice, data=data, request=request,
    )


async def transfer(r, request=None):
    return await r.service.transfer_continuation(
        r.notice, request or r.request, principal_id="user:local", realms={"default"}
    )


async def respond(r, *, key="answer-1", retry=False):
    return await r.service.respond(
        r.notice, InteractionResponse(idempotency_key=key, **({"retry": True} if retry else {"choice_id": "approve"})),
        principal_id="user:local",
    )


def test_transfer_preserves_question_provenance_and_durably_enqueues_once(recovery):
    r = recovery

    async def exercise():
        moved = await transfer(r)
        before = r.notice.model_dump(exclude={"version", "updated_at", "continuation_transfer"})
        assert moved.model_dump(exclude={"version", "updated_at", "continuation_transfer"}) == before
        assert not r.runtime._queue
        assert (await transfer(r)).version == moved.version
        assert r.store.list_notification_audit(moved.id)[0]["action"] == "continuation.transferred"
        delivered = await respond(r)
        assert delivered.interaction.state == InteractionState.DELIVERED
        assert (await respond(r)).version == delivered.version
        assert len(r.runtime._queue) == 1
        prompt = r.runtime._queue[0]
        assert prompt.id == f"notification-response:{r.notice.id}:request-1"
        assert r.store.get_prompt_acceptance("successor", prompt.id)
        assert r.store.get_prompt_acceptance("old", prompt.id) is None
        envelope = json.loads(prompt.message.split("\n", 1)[1])
        assert envelope["notification_id"] == r.notice.id
        assert envelope["request_id"] == "request-1"
        assert envelope["session_id"] == "old"
        assert envelope["dispatch_id"] == "dispatch-old"
        assert envelope["response"]["value"]["confirmation_id"] == "confirm-1"
        # A lost delivery acknowledgement must not create a second queue item.
        delivered.interaction.state = InteractionState.FAILED
        delivered.interaction.delivered_at = None
        delivered.resolved_at = None
        delivered.version += 1
        r.store.save_notification(delivered, principal_id="system:test", instance_id="local")
        assert (await respond(r, key="retry-1", retry=True)).interaction.state == InteractionState.DELIVERED
        assert len(r.runtime._queue) == 1
        # Durable receipt replays even after response/version advances.
        assert (await transfer(r)).continuation_transfer == moved.continuation_transfer

    asyncio.run(exercise())


def test_recorded_response_is_preserved_and_not_delivered_by_transfer(recovery):
    r = recovery
    r.notice.interaction.response = {"choice_id": "approve", "value": r.notice.interaction.choices[0].value}
    r.notice.interaction.response_principal = "user:local"
    r.notice.interaction.responded_at = r.notice.created_at
    r.notice.interaction.state = InteractionState.FAILED
    r.notice.interaction.delivery_attempts = 1
    r.notice.interaction.continuation_prompt_id = f"notification-response:{r.notice.id}:request-1"
    r.store.save_notification(r.notice, principal_id="user:local", instance_id="local")

    async def exercise():
        moved = await transfer(r)
        assert moved.interaction == r.notice.interaction
        assert not r.runtime._queue
        result = await respond(r, key="retry-recorded", retry=True)
        assert result.interaction.response == r.notice.interaction.response
        assert result.interaction.responded_at == r.notice.interaction.responded_at
        assert len(r.runtime._queue) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("field,value", [
    ("realm_id", "elsewhere"), ("card_id", "other-card"),
    ("project_id", "other-project"), ("principal_id", "user:other"),
    ("authority_instance_id", "remote"), ("origin_instance_id", "remote"),
    ("dispatch_id", "other-dispatch"), ("execution_binding", {}),
    ("status", "closed"), ("recovery_json", {"blocked": True}),
])
def test_rejects_successor_identity_and_recovery_mismatches(recovery, field, value):
    r = recovery
    setattr(r.successor, field, value)
    r.store.save_session(r.successor)
    if field == "execution_binding":
        # Existing bindings cannot be erased by save_session. Model a legacy
        # missing binding at the read boundary instead of bypassing that fence.
        get_session = r.store.get_session
        with patch.object(r.store, "get_session", side_effect=lambda key: r.successor if key == "successor" else get_session(key)):
            with pytest.raises(NotificationConflict):
                asyncio.run(transfer(r))
    else:
        with pytest.raises(NotificationConflict):
            asyncio.run(transfer(r))
    assert r.store.get_notification(r.notice.id).continuation_transfer is None


@pytest.mark.parametrize("case", ["live-old", "admitting-old", "admitting-successor", "recoverable-old", "nonterminal-old", "provider-permission", "provider-elicitation", "handler", "already-admitted", "pending-delivery"])
def test_rejects_live_ambiguous_or_provider_owned_origins(recovery, case):
    r = recovery
    if case == "live-old":
        r.manager._runtimes["old"] = SimpleNamespace(_closed=False)
    elif case.startswith("admitting-"):
        r.manager._admitting_sessions.add(case.removeprefix("admitting-"))
    elif case == "recoverable-old":
        r.old.recovery_json = {}
        r.store.save_session(r.old)
    elif case == "nonterminal-old":
        r.records["dispatch-old"].state = "running"
    elif case.startswith("provider-"):
        r.notice.interaction.kind = InteractionKind.ACP_PERMISSION if case.endswith("permission") else InteractionKind.ACP_ELICITATION
    elif case == "handler":
        r.service.register_delivery_handler(r.notice.id, lambda response: None)
    elif case == "already-admitted":
        r.old.config_json = {"durable_runtime": {"queued_prompts": [{"id": f"notification-response:{r.notice.id}:request-1"}]}}
        r.store.save_session(r.old)
    elif case == "pending-delivery":
        r.notice.interaction.state = InteractionState.DELIVERY_PENDING
    r.store.save_notification(r.notice, principal_id="user:local", instance_id="local")
    with pytest.raises(NotificationConflict):
        asyncio.run(transfer(r))
    assert not r.runtime._queue


def test_transfer_cas_origin_and_idempotency_conflicts(recovery):
    r = recovery
    for updates in ({"expected_version": 99}, {"expected_session_id": "wrong"}, {"expected_dispatch_id": "wrong"}):
        with pytest.raises(NotificationConflict):
            asyncio.run(transfer(r, r.request.model_copy(update=updates)))
    asyncio.run(transfer(r))
    with pytest.raises(NotificationConflict):
        asyncio.run(transfer(r, r.request.model_copy(update={"reason": "different operation"})))
    with pytest.raises(NotificationConflict):
        asyncio.run(respond(r, key="transfer-1"))


@pytest.mark.parametrize("writer", ["read", "coalesce"])
def test_stale_metadata_writer_reloads_transfer_and_recorded_response(recovery, writer):
    r = recovery
    entered = threading.Event()
    release = threading.Event()
    save = r.store.save_notification
    blocked_once = False

    def intercept(item, **kwargs):
        nonlocal blocked_once
        stale_metadata = item.read_at is not None if writer == "read" else item.coalesced_count > 1
        if stale_metadata and not blocked_once:
            blocked_once = True
            entered.set()
            assert release.wait(10)
        return save(item, **kwargs)

    def mutate():
        if writer == "read":
            return r.service.mark_read(r.notice, principal_id="user:local", idempotency_key="read-1")
        return r.service.create(r.data, principal_id="user:local")

    with patch.object(r.store, "save_notification", side_effect=intercept), ThreadPoolExecutor() as pool:
        pending = pool.submit(mutate)
        assert entered.wait(10)
        try:
            async def exercise():
                await transfer(r)
                return await respond(r)
            delivered = asyncio.run(exercise())
        finally:
            release.set()
        updated = pending.result(timeout=10)
    stored = r.store.get_notification(r.notice.id)
    assert stored.continuation_transfer == delivered.continuation_transfer
    assert stored.interaction == delivered.interaction
    assert updated.continuation_transfer == delivered.continuation_transfer
    assert len(r.runtime._queue) == 1
    if writer == "read":
        assert stored.read_at is not None
    # Coalescing a resolved notification is a no-op on retry.
    assert stored.version >= delivered.version


def test_response_wins_race_transfer_rejects_stale_version(recovery):
    r = recovery

    async def exercise():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def delivery(notice, response):
            entered.set()
            await release.wait()
            raise RuntimeError("Old provider context cannot be recovered")

        with patch.object(r.service, "_deliver", side_effect=delivery):
            response_task = asyncio.create_task(respond(r))
            await entered.wait()
            transfer_task = asyncio.create_task(transfer(r))
            await asyncio.sleep(0)
            assert not transfer_task.done()
            release.set()
            with pytest.raises(NotificationConflict):
                await response_task
            with pytest.raises(NotificationConflict, match="changed"):
                await transfer_task
        stored = r.store.get_notification(r.notice.id)
        assert stored.interaction.response["choice_id"] == "approve"
        assert stored.continuation_transfer is None

    asyncio.run(exercise())


def test_api_and_mcp_forward_exact_transfer_contract(recovery):
    r = recovery
    # Use ASGI routing without server lifecycle tasks or a provider process.
    app = r.kernel.build_app()
    app.state.ctx = r.kernel.ctx
    client = TestClient(app)
    client.get("/")
    url = f"/api/notifications/{r.notice.id}/transfer-continuation"
    result = client.post(url, json=r.request.model_dump(), headers={"X-CSRF-Token": client.cookies.get("pa_csrf")})
    assert result.status_code == 200, result.text
    assert result.json()["session_id"] == "old"
    assert result.json()["continuation_transfer"]["successor_session_id"] == "successor"
    from pa.modules.notifications import NotificationsModule
    captured = {}
    class MCP:
        def tool(self):
            def register(fn):
                captured[fn.__name__] = fn
                return fn
            return register
    with patch("pa.mcp.local_api.request_local_pa", return_value={"ok": True}) as call:
        NotificationsModule().register_mcp(MCP(), r.kernel.ctx)
        captured["transfer_notification_continuation"](notification_id=r.notice.id, **r.request.model_dump())
    assert call.call_args.args[1:3] == ("POST", url)
    assert call.call_args.kwargs["json"] == r.request.model_dump()


def test_coalescing_retries_on_transfer_without_answering(recovery):
    r = recovery
    entered, release = threading.Event(), threading.Event()
    save = r.store.save_notification
    blocked = False

    def intercept(item, **kwargs):
        nonlocal blocked
        if item.coalesced_count > 1 and not blocked:
            blocked = True
            entered.set()
            assert release.wait(10)
        return save(item, **kwargs)

    with patch.object(r.store, "save_notification", side_effect=intercept), ThreadPoolExecutor() as pool:
        pending = pool.submit(r.service.create, r.data, principal_id="user:local")
        assert entered.wait(10)
        try:
            moved = asyncio.run(transfer(r))
        finally:
            release.set()
        coalesced = pending.result(timeout=10)
    assert coalesced.id == moved.id
    assert coalesced.continuation_transfer == moved.continuation_transfer
    assert coalesced.coalesced_count == 2
    assert coalesced.version == moved.version + 1
    assert coalesced.interaction == r.notice.interaction


def test_transfer_wins_race_response_waits_for_committed_route(recovery):
    r = recovery

    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()
        original_save = r.service._save_async

        async def delayed_save(item, **kwargs):
            if item.version == r.notice.version + 1 and item.continuation_transfer:
                entered.set()
                await release.wait()
            return await original_save(item, **kwargs)

        with patch.object(r.service, "_save_async", side_effect=delayed_save):
            transfer_task = asyncio.create_task(transfer(r))
            await entered.wait()
            assert {"old", "successor"} <= r.manager._admitting_sessions
            response_task = asyncio.create_task(respond(r))
            await asyncio.sleep(0)
            assert not response_task.done()
            release.set()
            await transfer_task
            result = await response_task
        assert result.interaction.state == InteractionState.DELIVERED
        assert not r.manager._admitting_sessions
        assert len(r.runtime._queue) == 1

    asyncio.run(exercise())


def test_transfer_requires_originating_principal_and_authorized_realm(recovery):
    r = recovery
    with pytest.raises(NotificationConflict):
        asyncio.run(r.service.transfer_continuation(r.notice, r.request, principal_id="user:other", realms={"default"}))
    with pytest.raises(KeyError):
        asyncio.run(r.service.transfer_continuation(r.notice, r.request, principal_id="user:local", realms={"elsewhere"}))
    asyncio.run(transfer(r))
    with pytest.raises(NotificationConflict, match="principal"):
        asyncio.run(r.service.respond(r.notice, InteractionResponse(idempotency_key="foreign-answer", choice_id="approve"), principal_id="user:other"))
    assert r.store.get_notification(r.notice.id).interaction.response is None


def test_delivery_revalidates_successor_without_overwriting_recorded_response(recovery):
    r = recovery

    async def exercise():
        await transfer(r)
        r.successor.principal_id = "user:other"
        r.store.save_session(r.successor)
        with pytest.raises(NotificationConflict):
            await respond(r)
        result = r.store.get_notification(r.notice.id)
        assert result.interaction.response["choice_id"] == "approve"
        assert result.interaction.state == InteractionState.FAILED
        assert not r.runtime._queue

    asyncio.run(exercise())


@pytest.mark.parametrize("stage", ["answered", "delivery_pending", "delivered", "failed"])
@pytest.mark.parametrize("writer", ["read", "acknowledge", "coalesce"])
def test_response_stage_merges_only_concurrent_metadata(recovery, stage, writer):
    r = recovery

    async def exercise():
        await transfer(r)
        save = r.service._save_async
        changed = False

        async def intercept(item, **kwargs):
            nonlocal changed
            if item.interaction.state.value == stage and not changed:
                changed = True
                if writer == "coalesce":
                    await asyncio.to_thread(r.service.create, r.data, principal_id="user:local")
                else:
                    mutation = r.service.mark_read if writer == "read" else r.service.acknowledge
                    await asyncio.to_thread(mutation, r.notice, principal_id="user:local", idempotency_key=f"metadata-{stage}")
            return await save(item, **kwargs)

        with patch.object(r.service, "_save_async", side_effect=intercept):
            if stage == "failed":
                async def fail_delivery(*args):
                    raise RuntimeError("Temporary target outage")
                with patch.object(r.service, "_deliver", side_effect=fail_delivery):
                    with pytest.raises(NotificationConflict) as error:
                        await respond(r)
                assert error.value.code == "delivery_failed"
                stored = r.store.get_notification(r.notice.id)
                assert stored.interaction.state == InteractionState.FAILED
                assert stored.interaction.response["choice_id"] == "approve"
                result = await respond(r, key="retry-after-outage", retry=True)
            else:
                result = await respond(r)
        assert changed
        assert result.interaction.state == InteractionState.DELIVERED
        assert result.interaction.response["choice_id"] == "approve"
        assert len(r.runtime._queue) == 1
        assert (await respond(r)).interaction.state == InteractionState.DELIVERED
        stored = r.store.get_notification(r.notice.id)
        assert stored.continuation_transfer is not None
        if writer == "read":
            assert stored.read_at is not None
        elif writer == "acknowledge":
            assert stored.acknowledged_at is not None
        else:
            assert stored.coalesced_count == 2

    asyncio.run(exercise())


def test_response_stage_does_not_merge_a_semantic_conflict(recovery):
    r = recovery

    async def exercise():
        await transfer(r)
        save = r.service._save_async

        async def intercept(item, **kwargs):
            if item.interaction.state == InteractionState.DELIVERY_PENDING:
                r.service.supersede(r.notice, principal_id="user:local", idempotency_key="supersede")
            return await save(item, **kwargs)

        with patch.object(r.service, "_save_async", side_effect=intercept):
            with pytest.raises(NotificationConflict) as error:
                await respond(r)
        assert error.value.code == "notification_version_conflict"
        result = r.store.get_notification(r.notice.id)
        assert result.interaction.state == InteractionState.SUPERSEDED
        assert result.interaction.response["choice_id"] == "approve"
        assert result.continuation_transfer is not None
        assert not r.runtime._queue

    asyncio.run(exercise())


def test_late_provider_handler_cannot_take_over_a_transferred_response(recovery):
    r = recovery
    calls = []

    async def exercise():
        await transfer(r)
        r.service.register_delivery_handler(r.notice.id, calls.append)
        with pytest.raises(NotificationConflict):
            await respond(r)
        assert not calls
        assert not r.runtime._queue
        result = r.store.get_notification(r.notice.id)
        assert result.interaction.state == InteractionState.FAILED
        assert result.interaction.response["choice_id"] == "approve"

    asyncio.run(exercise())


@pytest.mark.parametrize("auth_required", [False, True])
def test_transfer_rejects_shared_sync_bearer_impersonation(recovery, auth_required):
    r = recovery
    r.kernel.ctx.settings.auth_required = auth_required
    r.kernel.ctx.settings.sync_token = "test-only-fleet-secret"
    app = r.kernel.build_app()
    app.state.ctx = r.kernel.ctx
    client = TestClient(app)
    client.get("/")
    response = client.post(
        f"/api/notifications/{r.notice.id}/transfer-continuation",
        json=r.request.model_dump(), headers={
            "Authorization": "Bearer test-only-fleet-secret",
            "X-PA-Acting-Principal": "user:local",
            "X-CSRF-Token": client.cookies.get("pa_csrf"),
        },
    )
    assert response.status_code == 403
    if not auth_required:
        assert response.json()["detail"]["code"] == "operator_identity_required"
    current = r.store.get_notification(r.notice.id)
    assert current.version == r.notice.version
    assert current.continuation_transfer is None
    assert current.interaction.response is None


@pytest.mark.parametrize("credential", ["ui", "user-bearer"])
@pytest.mark.parametrize("auth_required", [False, True])
def test_transfer_uses_actual_ui_or_mcp_user_identity(recovery, credential, auth_required):
    from pa.auth.sessions import SessionManager
    from pa.auth.users import UserDirectory

    r = recovery
    settings = r.kernel.ctx.settings
    settings.auth_required = auth_required
    settings.sync_token = "test-only-fleet-secret"
    user = UserDirectory(settings.data_dir).ensure_default_user()
    app = r.kernel.build_app()
    app.state.ctx = r.kernel.ctx
    client = TestClient(app)
    headers = {"X-PA-Acting-Principal": "user:forged"}
    if credential == "ui":
        client.cookies.set(SessionManager.COOKIE_NAME, SessionManager(settings.session_secret).create_token(user))
    else:
        # This is the user credential used by request_local_pa for bound MCP.
        headers["Authorization"] = "Bearer " + user.cli_token
    client.get("/", headers=headers)
    headers["X-CSRF-Token"] = client.cookies.get("pa_csrf")
    response = client.post(
        f"/api/notifications/{r.notice.id}/transfer-continuation",
        json=r.request.model_dump(), headers=headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["continuation_transfer"]["actor_principal"] == "user:local"


def test_retry_acknowledges_admitted_response_after_successor_closes(recovery):
    from unittest.mock import AsyncMock

    r = recovery

    async def exercise():
        await transfer(r)
        drain = r.runtime._drain_transcripts

        async def lose_acknowledgement():
            await drain()
            raise RuntimeError("Acknowledgement lost after durable admission")

        with patch.object(r.runtime, "enqueue", wraps=r.runtime.enqueue) as enqueue:
            with patch.object(r.runtime, "_drain_transcripts", side_effect=lose_acknowledgement):
                with pytest.raises(NotificationConflict) as error:
                    await respond(r)
            assert error.value.code == "delivery_failed"
            recorded = r.store.get_notification(r.notice.id)
            prompt_id = recorded.interaction.continuation_prompt_id
            assert r.store.get_prompt_acceptance("successor", prompt_id)
            successor = r.store.get_session("successor")
            successor.status = "closed"
            successor.external_session_id = None
            successor.recovery_json = {"blocked": True, "context_lost": True}
            r.store.save_session(successor)
            r.runtime._closed = True
            r.records["dispatch-successor"].state = "cancelled"
            r.records["dispatch-successor"].recoverable = False
            with patch.object(r.manager, "recover_session", new_callable=AsyncMock) as recover:
                delivered = await respond(r, key="retry-closed-successor", retry=True)
                recover.assert_not_called()
            enqueue.assert_called_once()
        assert delivered.interaction.state == InteractionState.DELIVERED
        assert delivered.interaction.response == recorded.interaction.response
        assert delivered.interaction.continuation_prompt_id == prompt_id
        assert len(r.runtime._queue) == 1
        admissions = [event for event in r.store.list_transcript_events("successor")
                      if event.event_type == "queue_enqueued" and event.payload.get("id") == prompt_id]
        assert len(admissions) == 1
        # A receipt does not bypass a changed routing identity.
        delivered.interaction.state = InteractionState.FAILED
        delivered.interaction.delivered_at = None
        delivered.resolved_at = None
        delivered.version += 1
        r.store.save_notification(delivered, principal_id="system:test", instance_id="local")
        successor.principal_id = "user:other"
        r.store.save_session(successor)
        with pytest.raises(NotificationConflict):
            await respond(r, key="retry-invalid-route", retry=True)
        assert len(r.runtime._queue) == 1

    asyncio.run(exercise())


def test_public_routing_and_rendered_progress_link_use_successor(recovery):
    import shutil
    import subprocess
    from pathlib import Path

    r = recovery
    r.notice.destination_url = "/agent?session=old"
    r.store.save_notification(r.notice, principal_id="user:local", instance_id="local")
    app = r.kernel.build_app()
    app.state.ctx = r.kernel.ctx
    client = TestClient(app)
    client.get("/")
    transferred = client.post(
        f"/api/notifications/{r.notice.id}/transfer-continuation", json=r.request.model_dump(),
        headers={"X-CSRF-Token": client.cookies.get("pa_csrf")},
    )
    assert transferred.status_code == 200
    assert transferred.json()["routing"]["destination"] == "/agent?session=successor"
    asyncio.run(respond(r))
    public = client.get(f"/api/notifications/{r.notice.id}").json()
    assert public["routing"]["destination"] == "/agent?session=successor"
    assert public["destination_url"] == "/agent?session=old"
    assert public["session_id"] == "old"
    assert public["dispatch_id"] == "dispatch-old"
    assert public["presentation"]["response_status"]["continuation"] == "Queued"
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for the rendered-link regression")
    harness = r'''
const assert = require("assert");
const list = { innerHTML: "", querySelectorAll: () => [], querySelector: () => null };
const root = { querySelector: (s) => s === "[data-notification-list]" ? list : null };
global.window = {};
global.document = {
  readyState: "loading", addEventListener: () => {},
  querySelector: (s) => s === "[data-notification-chrome]" ? root : null,
  createElement: () => ({
    set textContent(v) { this.text = String(v); },
    get innerHTML() { return this.text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
  })
};
require(process.argv[1]);
window.PANotificationsTest.render([JSON.parse(require("fs").readFileSync(0, "utf8"))], false);
assert.ok(list.innerHTML.includes('<a href="/agent?session=successor">View continuation and progress</a>'));
assert.ok(!list.innerHTML.includes('<a href="/agent?session=old">View continuation and progress</a>'));
'''
    script = Path(__file__).parents[1] / "src/pa/server/static/js/notifications.js"
    result = subprocess.run([node, "-e", harness, str(script)], input=json.dumps(public), text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_closed_successor_without_acceptance_still_rejects_new_delivery(recovery):
    r = recovery

    async def exercise():
        await transfer(r)
        r.successor.status = "closed"
        r.store.save_session(r.successor)
        r.runtime._closed = True
        with pytest.raises(NotificationConflict):
            await respond(r)
        assert not r.runtime._queue
        assert r.store.get_notification(r.notice.id).interaction.state == InteractionState.FAILED

    asyncio.run(exercise())
