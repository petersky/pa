"""Attachment publication remains responsive and recoverable after response loss."""
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


@pytest.fixture
def publication(tmp_path):
    reset_settings()
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path, agent_enabled=False, telemetry_enabled=False)
    token = UserDirectory(tmp_path).ensure_default_user().cli_token
    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        client.headers["Authorization"] = f"Bearer {token}"
        card = app.state.ctx.store.create_card(CardCreate(title="Isolated attachment receipts"))
        yield app, client, card
    reset_instance_agent()
    reset_store()
    reset_settings()


def upload(client, card, key, content=b"numeric evidence"):
    return client.post(f"/api/cards/{card.id}/attachments",
                       headers={"Idempotency-Key": key},
                       files={"file": ("evidence.txt", content, "text/plain")})


def test_same_key_replays_at_capacity_and_rejects_changed_bytes(publication):
    app, client, card = publication
    first = upload(client, card, "attachment-once")
    assert first.status_code == 201, first.text
    for number in range(9):
        response = upload(client, card, f"other-{number}", str(number).encode())
        assert response.status_code == 201, response.text
    replay = upload(client, card, "attachment-once")
    assert replay.status_code == 201, replay.text
    assert replay.json() == first.json()
    assert len(app.state.ctx.store.get_card(card.id).attachments) == 10
    conflict = upload(client, card, "attachment-once", b"different")
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["detail"]["code"] == "idempotency_conflict"
    outcome = client.get("/api/operations/attachment-once", params={"owner": "canonical"})
    assert outcome.status_code == 200, outcome.text
    assert outcome.json()["durable"] is True
    assert outcome.json()["result"] == first.json()


def test_lost_postcommit_response_recovers_from_canonical_event(publication, tmp_path):
    app, client, card = publication
    store = app.state.ctx.store
    with patch.object(store, "complete_operation", side_effect=RuntimeError("isolated lost response")):
        failed = upload(client, card, "lost-attachment-response")
        assert failed.status_code == 500
    attached = store.get_card(card.id).attachments
    assert len(attached) == 1
    expected = attached[0].model_dump(mode="json")
    # A fresh disposable projection has no derived receipt from the original
    # process. The immutable event must carry both request identity and result.
    rebuilt = CardProjection(tmp_path / "receipt-rebuild.db", store.event_log)
    rebuilt.rebuild_from_log("default")
    outcome = rebuilt.get_operation_outcome("lost-attachment-response")
    assert outcome["durable"] is True
    assert outcome["operation"] == "card.attachment.create"
    assert outcome["result"] == expected
    replay = upload(client, card, "lost-attachment-response")
    assert replay.status_code == 201, replay.text
    assert replay.json() == expected
    assert len(store.get_card(card.id).attachments) == 1


def test_slow_publication_does_not_block_unrelated_api(publication):
    app, client, card = publication
    store = app.state.ctx.store
    entered, release = threading.Event(), threading.Event()
    original = store.add_attachment

    def held(*args, **kwargs):
        entered.set()
        assert release.wait(5), "test failed to release isolated worker"
        return original(*args, **kwargs)

    with patch.object(store, "add_attachment", side_effect=held), ThreadPoolExecutor(2) as pool:
        pending = pool.submit(upload, client, card, "held-publication")
        try:
            assert entered.wait(5)
            # Functional ordering assertion, not a host-dependent latency SLO:
            # the unrelated API must finish while publication is still held.
            independent = pool.submit(client.get, "/api/instance")
            assert independent.result(timeout=2).status_code == 200
            assert not pending.done()
        finally:
            release.set()
        assert pending.result(timeout=5).status_code == 201


def test_mcp_attachment_exposes_and_forwards_stable_key(tmp_path):
    from pa.mcp.tools.items import register_mcp
    from tests.test_goal_assigned_service_mcp import FakeMcp

    mcp = FakeMcp()
    source = tmp_path / "evidence.txt"
    source.write_bytes(b"fixture")
    with patch("pa.mcp.local_api.request_local_pa", return_value={}) as request:
        register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace()))
        mcp.functions["create_card_attachment"](
            "card", str(source), idempotency_key=" attachment-stable "
        )
        assert request.call_args.kwargs["headers"] == {"Idempotency-Key": "attachment-stable"}
        with pytest.raises(ValueError, match="empty"):
            mcp.functions["create_card_attachment"]("card", str(source), idempotency_key=" ")
