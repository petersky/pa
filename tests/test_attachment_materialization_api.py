"""Same-instance admission uses the real authenticated materialization API."""
import io
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from pa.attachments import AttachmentStore, manifest_digest
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardAttachment, CardCreate
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.agent_session import reset_instance_agent

LOCAL = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"


@pytest.fixture
def materialization(tmp_path):
    reset_settings()
    reset_store()
    reset_instance_agent()
    app = Kernel.boot(settings=Settings(data_dir=tmp_path, instance_id=LOCAL,
        agent_enabled=False, telemetry_enabled=False, sync_token="test-only")).build_app()
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title="Attached work"))
        blobs = AttachmentStore(tmp_path)
        content = b"Verified work attached before local dispatch"
        digest, size = blobs.ingest(io.BytesIO(content))
        attachment = CardAttachment(card_id=card.id, filename="work.txt", media_type="text/plain",
                                    size=size, sha256=digest, blob_ref=f"sha256:{digest}",
                                    created_by_principal="user:local", created_by_instance=LOCAL)
        card = store.add_attachment(attachment, principal_id="user:local", instance_id=LOCAL)
        record = DispatchRecord(mutation_id=str(uuid4()), card_id=card.id,
            card_version=card.updated_at.isoformat(), card_snapshot=card.model_dump(mode="json"),
            authority_instance_id=LOCAL, target_instance_id=LOCAL, authority_url="http://local",
            state="materializing")
        app.state.ctx.services["dispatch_store"].put(record)
        body = dict(dispatch_id=record.dispatch_id, mutation_id=record.mutation_id,
            card=record.card_snapshot, card_version=record.card_version, realm_id="default",
            authority_instance_id=LOCAL, target_instance_id=LOCAL, authority_url="http://local",
            attachment_manifest=[attachment.model_dump(mode="json")], attachment_digest=manifest_digest([attachment]),
            progress_versions=[1])
        client.headers.update({"Authorization": "Bearer test-only", "X-PA-Origin-Instance-ID": LOCAL})
        yield app, client, body, record, blobs, attachment, content
    reset_instance_agent()
    reset_store()
    reset_settings()


def post(client, body):
    return client.post("/api/fleet/dispatch/materialize", json=body)


def test_verified_evidence_survives_local_admission_replay_and_ledger_restart(materialization):
    app, client, body, record, _, attachment, content = materialization
    response = post(client, body)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["duplicate"] and result["resolvable"]
    evidence = result["attachment_evidence"]
    assert evidence["verified"] and evidence["digest"] == manifest_digest([attachment])
    assert Path(evidence["attachments"][0]["local_path"]).read_bytes() == content
    restarted = DispatchStore(app.state.ctx.settings.data_dir)
    assert restarted.get(record.dispatch_id).attachment_evidence == evidence
    app.state.ctx.services["dispatch_store"] = restarted
    replay = post(client, body)
    assert replay.status_code == 200, replay.text
    assert replay.json()["attachment_evidence"] == evidence
    assert restarted.get(record.dispatch_id).session_id is None


@pytest.mark.parametrize("mutation", ["authority", "realm", "version", "card", "manifest", "digest"])
def test_replay_cannot_substitute_snapshot_or_provenance(materialization, mutation):
    app, client, body, record, _, _, _ = materialization
    assert post(client, body).status_code == 200
    original = app.state.ctx.services["dispatch_store"].get(record.dispatch_id).attachment_evidence
    if mutation == "authority":
        body["authority_instance_id"] = str(uuid4())
        client.headers["X-PA-Origin-Instance-ID"] = body["authority_instance_id"]
    elif mutation == "realm":
        body["realm_id"] = "another"
    elif mutation == "version":
        body["card_version"] = "2026-09-01T00:00:00+00:00"
    elif mutation == "card":
        body["card"]["id"] = str(uuid4())
    elif mutation == "manifest":
        body["attachment_manifest"] = []
        body["attachment_digest"] = manifest_digest([])
    else:
        body["attachment_digest"] = "0" * 64
    response = post(client, body)
    assert response.status_code in {403, 409}, response.text
    assert app.state.ctx.services["dispatch_store"].get(record.dispatch_id).attachment_evidence == original


@pytest.mark.parametrize("damage", ["missing_blob", "bad_size", "bad_hash", "missing_materialized", "replaced_materialized"])
def test_unverified_bytes_never_return_resolvable_evidence(materialization, damage):
    app, client, body, record, blobs, attachment, content = materialization
    blob = blobs.blob_path(attachment.sha256)
    if damage.endswith("materialized"):
        first = post(client, body)
        assert first.status_code == 200
        path = Path(first.json()["attachment_evidence"]["attachments"][0]["local_path"])
        path.unlink()
        if damage == "replaced_materialized":
            path.write_bytes(b"x" * len(content))
    elif damage == "missing_blob":
        blob.unlink()
    elif damage == "bad_size":
        blob.write_bytes(b"bad")
    else:
        blob.write_bytes(b"x" * len(content))
    response = post(client, body)
    assert response.status_code == 409 or not response.json().get("resolvable"), response.text
    assert app.state.ctx.services["dispatch_store"].get(record.dispatch_id).session_id is None


def test_verification_cannot_overwrite_concurrent_dispatch_control(materialization, monkeypatch):
    app, client, body, record, _, _, _ = materialization
    ledger = app.state.ctx.services["dispatch_store"]
    original = AttachmentStore.materialize

    def concurrent_cancel(store, dispatch_id, manifest):
        evidence = original(store, dispatch_id, manifest)
        ledger.mutate_current(dispatch_id, mutate=lambda current: setattr(current, "cancel_requested", True))
        return evidence

    monkeypatch.setattr(AttachmentStore, "materialize", concurrent_cancel)
    response = post(client, body)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "dispatch_materialization_changed"
    durable = ledger.get(record.dispatch_id)
    assert durable.cancel_requested
    assert durable.attachment_evidence is None
