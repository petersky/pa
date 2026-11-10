"""Authority != target dispatch completion, with two independent ledgers.

Local dispatch hides target-side lifecycle defects because the authority and the
target share one ledger. These tests keep them separate and move bytes between
them only through the real HTTP-facing handlers.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.execution.dispatch import CompletionOutbox, DispatchRecord, DispatchStore
from pa.execution.followup import (
    PROMPT_IDENTITY_PROTOCOL, dispatch_prompt_identity, prompt_fingerprint,
)
from pa.execution.reconciliation import CompletionReconciler
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import PromptBody, session_prompt
from pa.modules.fleet import DispatchCompletionBody, complete_dispatch

AUTHORITY = "authority"
TARGET = "target"
DISPATCH = "dispatch-two-ledger"
SESSION = "session-two-ledger"
MESSAGE = "Implement the card."


def _record(state: str) -> DispatchRecord:
    return DispatchRecord(
        dispatch_id=DISPATCH, mutation_id="mutation-two-ledger", card_id="card-1",
        project_id="project-1", session_id=SESSION, authority_instance_id=AUTHORITY,
        authority_url="http://authority.invalid", target_instance_id=TARGET, state=state,
    )


@pytest.fixture
def pair(tmp_path):
    authority_ledger = DispatchStore(tmp_path / "authority")
    target_ledger = DispatchStore(tmp_path / "target")
    authority_ledger.put(_record("delivering_prompt"))
    # The target's mirror as left by materialize + session creation.
    target_ledger.put(_record("starting_session"))
    store = CardProjection(tmp_path / "target.db")
    settings = Settings(data_dir=tmp_path / "target", instance_id=TARGET, auth_required=False)
    session = store.save_session(AgentSession(
        id=SESSION, agent_name="codex", dispatch_id=DISPATCH,
        card_id="card-1", project_id="project-1",
    ))
    manager = AgentSessionManager(settings, store, dispatch_store=target_ledger)
    manager._quiescing = True  # Real queue admission without a provider.
    runtime = AgentSessionRuntime(manager, session)
    manager._runtimes[SESSION] = runtime
    target_request = MagicMock()
    target_request.state.instance_authenticated = True
    target_request.state.user = None
    target_request.state.principal_id = "user:local"
    target_request.app.state.ctx.settings = settings
    target_request.app.state.ctx.services = {
        "dispatch_store": target_ledger, "instance_agent": manager,
    }
    target_request.app.state.ctx.require_service = target_request.app.state.ctx.services.__getitem__

    authority_request = MagicMock()
    authority_request.app.state.ctx.settings = Settings(
        data_dir=tmp_path / "authority", instance_id=AUTHORITY, auth_required=False,
    )
    authority_request.app.state.ctx.services = {"dispatch_store": authority_ledger}
    authority_request.app.state.ctx.store.get_card.return_value = None  # card gone: simplest ack path
    authority_request.app.state.ctx.store.get_project.return_value = None
    with patch("pa.modules.agent_chat._runtime_or_404_async", new=AsyncMock(return_value=runtime)), patch(
        "pa.modules.agent_chat._record_web_intake", AsyncMock(return_value=None),
    ):
        yield SimpleNamespace(
            authority=authority_ledger, target=target_ledger, runtime=runtime,
            target_request=target_request, authority_request=authority_request,
        )
    authority_ledger.close()
    target_ledger.close()


async def _deliver_initial_prompt(pair):
    record = pair.target.get(DISPATCH)
    return await session_prompt(pair.target_request, SESSION, PromptBody(
        message=MESSAGE, dispatch_id=DISPATCH,
        dispatch_prompt_id=dispatch_prompt_identity(record, None),
        dispatch_prompt_protocol=PROMPT_IDENTITY_PROTOCOL,
    ))


def _authority_transport(pair, calls):
    """Route the target's outbox POST into the authority's real /complete handler."""
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert request.url.path == f"/api/fleet/dispatch/{DISPATCH}/complete"
        fake = MagicMock()
        fake.headers = {k.lower(): v for k, v in request.headers.items()}
        fake.app = pair.authority_request.app
        try:
            body = DispatchCompletionBody.model_validate(
                json.loads(request.content)
            )
            return httpx.Response(200, json=complete_dispatch(fake, DISPATCH, body))
        except HTTPException as exc:
            return httpx.Response(exc.status_code, json={"detail": exc.detail})
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_initial_prompt_ack_starts_target_mirror(pair):
    """The target's own record must learn that work is running."""
    result = await _deliver_initial_prompt(pair)
    assert result["accepted"] is True
    mirror = pair.target.get(DISPATCH)
    assert mirror.prompt_ack is not None
    assert mirror.state == "running"
    assert mirror.events[-1].state == "running"


@pytest.mark.asyncio
async def test_completion_travels_from_target_to_authority_with_separate_ledgers(pair):
    await _deliver_initial_prompt(pair)
    # The authority drives its own record, exactly as _process_remote_dispatch ends.
    authority_record = pair.authority.get(DISPATCH)
    pair.authority.transition(authority_record, "running", "Prompt accepted by the intended remote session.")

    outbox = CompletionOutbox(pair.target, "token", retry_seconds=0.01)
    calls: list[str] = []
    outbox._client = httpx.AsyncClient(transport=_authority_transport(pair, calls))
    reconciler = CompletionReconciler(
        pair.target, SimpleNamespace(async_runtime=None, store=MagicMock(), get=lambda _s: pair.runtime),
        outbox, MagicMock(), lambda: None, retry_seconds=0.01,
    )
    payload = {
        "stop_reason": "end_turn",
        "card_disposition": {
            "contract": "pa.card-disposition/v1", "lane": "active",
            "outcome": "Still working.", "evidence": {"integration_required": False, "references": ["card:card-1"]},
        },
    }
    assert await reconciler.handle_completion(SESSION, payload) is True
    queued = pair.target.get(DISPATCH)
    assert queued.state == "completion_pending"

    await outbox._send(queued)
    assert calls == [f"http://authority.invalid/api/fleet/dispatch/{DISPATCH}/complete"]
    assert pair.target.get(DISPATCH).completion_delivery_class == "acknowledged"
    delivered = pair.authority.get(DISPATCH)
    assert delivered.state == "completed"
    assert delivered.acknowledged_at is not None
    await outbox._client.aclose()
