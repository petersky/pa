"""Fresh dispatcher admission must produce transfer-ready immutable provenance."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.acp.providers.base import AgentProviderSpec
from pa.domain.models import (
    AgentSession, CardCreate, ProjectCreate, ProjectRepository, Repository,
)
from pa.domain.notifications import (
    ContinuationTransferRequest, InteractionChoice, InteractionRequest,
    InteractionResponse, InteractionState, NotificationCreate,
)
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.followup import PROMPT_IDENTITY_PROTOCOL
from pa.instance.agent_session import (
    AgentSessionManager, AgentSessionRuntime, WorkspaceBindingMismatch,
)
from pa.modules.agent_chat import CreateSessionBody, create_session
from pa.modules.fleet import _process_remote_dispatch
from pa.notifications import NotificationConflict
from tests.test_notifications import _kernel, _reset_singletons  # noqa: F401
from tests.test_repository_workspaces import cache_workspace_test_provider, make_remote


@pytest.fixture
def admitted(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    kernel = _kernel(tmp_path / "data")
    ctx = kernel.ctx
    ctx.settings.workspace_root = tmp_path / "workspace"
    ctx.settings.agent_enabled = True
    store = ctx.store
    project = store.create_project(ProjectCreate(title="Scope"), via_log=False)
    card = store.create_card(CardCreate(title="Pending scope", project_id=project.id), via_log=False)
    remote = make_remote(tmp_path)
    url = "https://github.com/pa-test/provenance.git"
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", f"url.{remote}.insteadOf")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", url)
    repository = Repository(id="repo-1", name="Scope", url=url)
    monkeypatch.setattr(store, "list_project_repositories", lambda *a, **kw: [
        (repository, ProjectRepository(project_id=project.id, repository_id=repository.id, branch="main"))
    ])
    ledger = DispatchStore(tmp_path / "dispatch")
    manager = AgentSessionManager(ctx.settings, store, dispatch_store=ledger)
    ctx.register_service("instance_agent", manager)
    ctx.register_service("dispatch_store", ledger)
    cache_workspace_test_provider(ctx.settings)
    record = ledger.put(DispatchRecord(
        dispatch_id="fresh-dispatch", mutation_id="fresh-mutation",
        authority_instance_id="local", authority_url="http://pa.test",
        target_instance_id="local", card_id=card.id, project_id=project.id,
        principal_id="user:local", realm_id="default",
        request_payload={"provider": "codex", "message": "Wait for the scope decision"},
        materialization_plan={"profile": "repository"},
    ))
    request = MagicMock()
    request.app.state.ctx = ctx
    request.state.user = None
    request.state.principal_id = "user:local"
    request.state.instance_authenticated = True
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="unused-test-provider")

    async def provider_start(runtime, **kwargs):
        # Only the external provider process is replaced. Admission and workspace
        # materialization, including the audited SQLite CAS, run normally.
        persisted = store.get_session(runtime.session_id)
        for field in ("dispatch_id", "realm_id", "principal_id"):
            assert persisted.execution_binding[field] == getattr(record, field)
        audit, = store.list_session_execution_binding_history(runtime.session_id)
        assert audit["binding"] == persisted.execution_binding
        runtime.session.external_session_id = "native-successor"
        runtime.session.status = "connected"
        store.save_session(runtime.session)
        runtime._queue_paused = True

    async def materialize(_request, instance_id, body):
        assert body["dispatch_id"] == record.dispatch_id
        assert body["principal_id"] == record.principal_id
        assert body["realm_id"] == record.realm_id
        return {"resolvable": True, "dispatch_id": body["dispatch_id"],
                "card_id": body["card"]["id"], "card_version": body["card_version"]}

    async def peer(_request, instance_id, method, path, *, body=None, **kwargs):
        if path == "sessions":
            # Invoke the actual target dispatcher admission endpoint; do not
            # assemble an AgentSession or execution_binding in the fixture.
            return await create_session(request, CreateSessionBody.model_validate(body))
        if method == "GET":
            return {"protocols": [PROMPT_IDENTITY_PROTOCOL]}
        assert path.endswith("/dispatch-prompts")
        return {"accepted": True, "accepted_event": "queue_enqueued",
                "session_id": ledger.get(record.dispatch_id).session_id,
                "dispatch_id": body["dispatch_id"], "prompt_id": body["dispatch_prompt_id"]}

    async def dispatch():
        with (
            patch("pa.modules.fleet._wait_for_dispatch_sync_health", AsyncMock(return_value={})),
            patch("pa.modules.fleet._peer_dispatch_json", side_effect=materialize),
            patch("pa.modules.fleet._peer_agent_json", side_effect=peer),
            patch("pa.instance.agent_session.resolve_agent_provider", return_value=SimpleNamespace(provider_id="codex", spec=spec, source="override")),
            patch("pa.acp.providers.resolve.resolve_provider_id", return_value=("codex", "instance")),
            patch.object(AgentSessionRuntime, "start", provider_start),
        ):
            await _process_remote_dispatch(request.app, record)

    asyncio.run(dispatch())
    successor = store.get_session(record.session_id)
    runtime = manager.get(successor.id)
    runtime.connection = SimpleNamespace(connected=True)
    old = store.save_session(AgentSession(
        id="lost-session", agent_name="codex", dispatch_id="lost-dispatch",
        origin_instance_id="local", authority_instance_id="local",
        realm_id=record.realm_id, principal_id=record.principal_id,
        card_id=card.id, project_id=project.id, status="closed",
    ))
    ledger.put(DispatchRecord(
        dispatch_id=old.dispatch_id, mutation_id="lost-mutation", session_id=old.id,
        authority_instance_id="local", authority_url="http://pa.test",
        target_instance_id="local", realm_id=old.realm_id, principal_id=old.principal_id,
        card_id=card.id, project_id=project.id, state="failed",
    ))
    service = ctx.require_service("notifications")
    notice = service.create(NotificationCreate(
        title="Scope decision", type="interaction", session_id=old.id,
        dispatch_id=old.dispatch_id, card_id=card.id, project_id=project.id,
        deduplication_key="operator-input:lost-dispatch:scope-1",
        interaction=InteractionRequest(
            request_id="scope-1", kind="mcp_operator_input", prompt="Approve this scope?",
            protocol_method="pa/report_dispatch_progress.operator_input",
            protocol_request_id="scope-1", continuation_mode="prompt",
            choices=[InteractionChoice(id="approve", label="Approve scope", value={"confirmation_id": "confirm-1", "scope": ["repo-1"]})],
        ),
    ), principal_id="user:local")
    transfer = ContinuationTransferRequest(
        idempotency_key="transfer-1", expected_version=notice.version,
        expected_session_id=old.id, expected_dispatch_id=old.dispatch_id,
        successor_session_id=successor.id, successor_dispatch_id=record.dispatch_id,
        reason="Original provider context is lost",
    )
    return SimpleNamespace(
        store=store, manager=manager, record=record, card=card,
        successor=successor, runtime=runtime, old=old,
        service=service, notice=notice, transfer=transfer,
    )


@pytest.mark.parametrize("recorded", [False, True])
def test_fresh_dispatch_binding_enables_same_question_transfer(admitted, recorded):
    r = admitted
    binding = r.successor.execution_binding
    for field in ("dispatch_id", "realm_id", "principal_id"):
        assert binding[field] == getattr(r.record, field)
    lease, = r.manager.workspace_manager.list(card_id=r.card.id)
    assert lease.state == "ready" and lease.fencing_token > 0
    assert binding["lease_ids"] == [lease.id]
    history = r.store.list_session_execution_binding_history(r.successor.id)
    assert len(history) == 1
    assert history[0]["prior_binding"] == {}
    assert history[0]["binding"] == binding
    assert history[0]["reason"] == "workspace_binding_initialized"
    if recorded:
        # Record a real test response through the service while old delivery fails.
        with pytest.raises(NotificationConflict) as error:
            asyncio.run(r.service.respond(r.notice, InteractionResponse(
                idempotency_key="answer-1", choice_id="approve"), principal_id="user:local"))
        assert error.value.code == "delivery_failed"
        failed = r.store.get_notification(r.notice.id)
        assert failed.interaction.state == InteractionState.FAILED
        r.notice = failed
        r.transfer.expected_version = failed.version
    before = r.notice.model_dump(exclude={"version", "updated_at", "continuation_transfer"})

    async def exercise():
        moved = await r.service.transfer_continuation(r.notice, r.transfer, principal_id="user:local", realms={"default"})
        assert moved.model_dump(exclude={"version", "updated_at", "continuation_transfer"}) == before
        assert not r.runtime._queue
        answer = InteractionResponse(idempotency_key="retry-1" if recorded else "answer-1", **({"retry": True} if recorded else {"choice_id": "approve"}))
        delivered = await r.service.respond(moved, answer, principal_id="user:local")
        assert delivered.interaction.state == InteractionState.DELIVERED
        await r.service.respond(delivered, answer, principal_id="user:local")
        await r.service.transfer_continuation(moved, r.transfer, principal_id="user:local", realms={"default"})
        prompt, = r.runtime._queue
        assert prompt.id == f"notification-response:{r.notice.id}:scope-1"
        envelope = json.loads(prompt.message.split("\n", 1)[1])
        assert envelope["notification_id"] == r.notice.id
        assert envelope["request_id"] == "scope-1"
        assert envelope["session_id"] == r.old.id
        assert envelope["dispatch_id"] == r.old.dispatch_id
        assert envelope["response"]["value"]["confirmation_id"] == "confirm-1"
        assert r.store.get_prompt_acceptance(r.successor.id, prompt.id)
        assert not r.store.get_prompt_acceptance(r.old.id, prompt.id)
        if recorded:
            assert delivered.interaction.response == r.notice.interaction.response
            assert delivered.interaction.responded_at == r.notice.interaction.responded_at

    asyncio.run(exercise())


@pytest.mark.parametrize("field,value", [("dispatch_id", "wrong-dispatch"), ("realm_id", "engineering"), ("principal_id", "user:other")])
@pytest.mark.parametrize("source", ["session", "binding"])
def test_fresh_binding_rejects_identity_conflicts_without_rewriting(admitted, field, value, source):
    r = admitted
    original = r.successor.execution_binding.copy()
    changed = r.successor.model_copy(deep=True)
    if source == "session":
        setattr(changed, field, value)
    else:
        changed.execution_binding[field] = value
        with pytest.raises(ValueError, match="immutable provenance"):
            r.store.set_session_execution_binding(
                changed.id, changed.execution_binding,
                reason="workspace_materialized", expected_binding=original,
            )
    with pytest.raises(WorkspaceBindingMismatch, match=field):
        asyncio.run(r.manager._prepare_workspace(changed, requested_cwd=None, provider_id="codex"))
    assert r.store.get_session(changed.id).execution_binding == original
    assert len(r.store.list_session_execution_binding_history(changed.id)) == 1
    # Inject conflicting evidence at the read boundary without bypassing the
    # durable binding's write fence. Both consumers must reject it.
    get_session = r.store.get_session
    with patch.object(r.store, "get_session", side_effect=lambda key: changed if key == changed.id else get_session(key)):
        with pytest.raises(NotificationConflict) as error:
            asyncio.run(r.service.transfer_continuation(r.notice, r.transfer, principal_id="user:local", realms={"default"}))
    if source == "binding":
        assert "immutable execution binding" in str(error.value)
    assert r.store.get_notification(r.notice.id).interaction == r.notice.interaction
    assert not r.runtime._queue


@pytest.mark.parametrize("empty", [False, True])
def test_legacy_incomplete_binding_is_not_backfilled(tmp_path, monkeypatch, empty):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    kernel = _kernel(tmp_path / "data")
    kernel.ctx.settings.workspace_root = tmp_path / "workspace"
    store = kernel.ctx.store
    legacy = {"version": 1, "execution_card_id": None,
              "execution_project_id": None, "origin_instance_id": "local"}
    if empty:
        legacy = {}
    session = store.save_session(AgentSession(
        id="legacy", agent_name="codex", external_session_id="historical-provider",
        origin_instance_id="local", dispatch_id="historical-dispatch",
        realm_id="default", principal_id="user:local", execution_binding=legacy,
    ))
    manager = AgentSessionManager(kernel.ctx.settings, store)
    asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
    binding = store.get_session(session.id).execution_binding
    assert all(binding[key] == value for key, value in legacy.items())
    assert not {"dispatch_id", "realm_id", "principal_id"}.intersection(binding)
    history, = store.list_session_execution_binding_history(session.id)
    assert history["reason"] == ("workspace_binding_initialized" if empty else "workspace_materialized")
    assert history["prior_binding"] == legacy
    assert history["binding"] == binding
