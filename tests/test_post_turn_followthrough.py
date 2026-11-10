"""Post-turn follow-through: evidence survives, actions execute, stalls escalate."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.domain.models import CardCreate
from pa.execution.dispatch import CompletionOutbox, DispatchRecord


def _boot(tmp_path, **overrides):
    from pa.core.kernel import Kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    reset_store()
    reset_instance_agent()
    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        agent_enabled=False,
        peers=[],
        sync_token="isolated-peer-token",
        **overrides,
    )
    return Kernel.boot(settings=settings).build_app(), settings


def test_outbox_ack_on_shared_store_keeps_turn_end_evidence(tmp_path):
    """Authority and target share one DispatchStore: the outbox's ack write must
    not erase the snapshot/evaluation the /complete handler just persisted."""
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    app, settings = _boot(tmp_path)
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title="worker outcome", lane="active"))
        ledger = app.state.ctx.services["dispatch_store"]
        ledger.put(
            DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="m-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                card_snapshot=card.model_dump(mode="json"),
                authority_instance_id=settings.instance_id,
                authority_url="http://testserver",
                target_instance_id=settings.instance_id,
                session_id="worker-session",
                state="completion_pending",
                completion_payload={"stop_reason": "end_turn"},
            )
        )
        outbox = CompletionOutbox(ledger, "isolated-peer-token")

        async def forward(url, **kwargs):
            return client.post(url, **kwargs)

        with patch.object(
            outbox, "_http_client", return_value=SimpleNamespace(post=forward)
        ):
            asyncio.run(outbox._send(ledger.get("dispatch-1")))
        done = ledger.get("dispatch-1")
        assert done.state == "completed"
        assert done.completion_delivery_class == "acknowledged"
        assert len(done.turn_end_snapshots) == 1
        assert len(done.post_turn_evaluations) == 1
        assert done.post_turn_context_digests
    reset_store()
    reset_instance_agent()
