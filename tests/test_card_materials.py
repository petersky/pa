"""Rich description discovery and attachment mutations preserve scope and history."""
import json
from types import SimpleNamespace
from unittest.mock import patch

from pa.domain.models import CardUpdate
from pa.domain.models import CardEvent, EventType
from pa.domain.projection import CardProjection
from tests.test_attachment_operation_receipts import publication, upload
from tests.test_completion_records import envelope, submit


def test_material_discovery_omits_full_spec_and_preserves_canonical_edit_history(publication):
    app, client, card = publication
    body = "<script>untrusted()</script>" + " specification" * 600
    changed = client.patch(f"/api/cards/{card.id}", headers={"Idempotency-Key": "description-once"},
                           json={"body": body, "expected_version": card.updated_at.isoformat()})
    assert changed.status_code == 200, changed.text
    assert changed.json()["lane"] == "inbox"
    artifact = upload(client, card, "supporting-once").json()
    materials = client.get(f"/api/cards/{card.id}/materials").json()
    assert materials["description_characters"] == len(body)
    assert body not in json.dumps(materials) and "body" not in materials
    assert materials["limits"]["card_attachments"] == 64
    assert materials["attachments"][0]["sha256"] == artifact["sha256"]
    assert client.get(materials["attachments"][0]["url"]).content == b"numeric evidence"
    stale = client.patch(f"/api/cards/{card.id}", headers={"Idempotency-Key": "stale-description"},
                         json={"body": "stale replacement", "expected_version": card.updated_at.isoformat()})
    assert stale.status_code == 409
    full = client.get(materials["card_url"]).json()
    assert full["body"] == body
    history = client.get(materials["history_url"]).json()
    assert any(row["event"]["payload"].get("body") == body for row in history["events"])
    page = client.get(f"/card-history?card_id={card.id}")
    assert page.status_code == 200, page.text
    assert "&lt;script&gt;untrusted()&lt;/script&gt;" in page.text
    assert "<script>untrusted()</script>" not in page.text
    assert "Description and material history" in page.text
    assert client.get(f"/api/cards/{card.id}/materials?realm=other").status_code == 404
    assert client.get(f"/card-history?card_id={card.id}&realm=other").status_code == 404
    assert app.state.ctx.services["dispatch_store"].list(card_id=card.id) == []


def test_removal_cas_and_durable_replay_after_lost_response(publication, tmp_path):
    app, client, card = publication
    artifact = upload(client, card, "remove-me").json()
    url = f"/api/cards/{card.id}/attachments/{artifact['attachment_id']}"
    stale = client.delete(url, params={"expected_version": card.updated_at.isoformat()}, headers={"Idempotency-Key": "stale-remove"})
    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "stale_card_version"
    assert len(app.state.ctx.store.get_card(card.id).attachments) == 1
    version = client.get(f"/api/cards/{card.id}").json()["updated_at"]
    headers = {"Idempotency-Key": "remove-once"}
    with patch.object(app.state.ctx.store, "complete_operation", side_effect=RuntimeError("lost removal acknowledgement")):
        failed = client.delete(url, params={"expected_version": version}, headers=headers)
    assert failed.status_code == 500
    assert not app.state.ctx.store.get_card(card.id).attachments
    replay = client.delete(url, params={"expected_version": version}, headers=headers)
    assert replay.status_code == 200, replay.text
    assert replay.json()["attachment_removal"]["retained_for_evidence"] is False
    rebuilt = CardProjection(tmp_path / "removal-rebuilt.db", app.state.ctx.store.event_log)
    rebuilt.rebuild_from_log("default")
    receipt = rebuilt.get_operation_outcome("remove-once")
    assert receipt["durable"] and receipt["result"] == replay.json()
    assert receipt["operation"] == "card.attachment.remove"
    different = client.delete(url, headers=headers)
    assert different.status_code == 409 and different.json()["detail"]["code"] == "idempotency_conflict"


def test_material_history_keeps_legacy_events_without_field_intent(publication):
    app, client, card = publication
    app.state.ctx.store.commit_event(CardEvent(type=EventType.CARD_UPDATED, card_id=card.id, realm_id=card.realm_id,
        author_principal="legacy:author", author_instance="legacy:instance", payload={"body": "Retained legacy specification"}))
    page = client.get(f"/card-history?card_id={card.id}")
    assert page.status_code == 200, page.text
    assert "Retained legacy specification" in page.text and "legacy:author" in page.text


def test_referenced_attachment_removal_explains_evidence_retention(publication):
    app, client, card = publication
    artifact = upload(client, card, "referenced").json()
    url = f"/api/cards/{card.id}/attachments/{artifact['attachment_id']}"
    app.state.ctx.store.update_card(card.id, CardUpdate(body=f"[Supporting evidence]({url})"))
    denied = client.delete(url, headers={"Idempotency-Key": "referenced-remove"})
    assert denied.status_code == 409 and denied.json()["detail"]["code"] == "attachment_referenced"
    assert client.get(url).status_code == 200
    result = submit(client, card, "retain-evidence", envelope(references=[{
        "kind": "attachment", "label": "Original evidence", "attachment_id": artifact["attachment_id"], "sha256": artifact["sha256"],
    }]))
    assert result.status_code == 201, result.text
    removed = client.delete(url, headers={"Idempotency-Key": "retained-remove"})
    assert removed.status_code == 200, removed.text
    assert removed.json()["attachment_removal"]["retained_for_evidence"] is True
    assert "original remains accessible" in removed.json()["attachment_removal"]["message"]
    assert client.get(url).content == b"numeric evidence"


def test_mcp_material_reads_and_removal_use_canonical_api():
    from pa.mcp.tools.items import register_mcp
    from tests.test_goal_assigned_service_mcp import FakeMcp
    mcp = FakeMcp()
    with patch("pa.mcp.local_api.request_local_pa", return_value={}) as request:
        register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace()))
        mcp.functions["get_card"]("card")
        assert request.call_args.args[2] == "/api/cards/card"
        mcp.functions["get_card_materials"]("card")
        assert request.call_args.args[2] == "/api/cards/card/materials"
        mcp.functions["remove_card_attachment"]("card", "attachment", expected_version="version", idempotency_key=" remove-stable ")
        assert request.call_args.kwargs["headers"] == {"Idempotency-Key": "remove-stable"}
        assert request.call_args.kwargs["params"]["expected_version"] == "version"
