"""Files and links share canonical revisions, retries, permissions and previews."""
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.domain.models import Membership, RealmRole
from pa.domain.projection import CardProjection
from tests.test_attachment_operation_receipts import publication, upload
from tests.test_completion_records import envelope, submit


def attachment_url(card, attachment):
    return f"/api/cards/{card.id}/attachments/{attachment['attachment_id']}"


def test_file_lifecycle_stable_identity_history_and_retry_after_response_loss(publication, tmp_path):
    app, client, card = publication
    original = upload(client, card, "original", b"first version").json()
    url = attachment_url(card, original)
    updated = client.patch(url, json={"name": "Assessment", "description": "Useful context", "expected_revision": 1}, headers={"Idempotency-Key": "metadata"})
    assert updated.status_code == 200, updated.text
    assert updated.json()["attachment"]["revision"] == 2
    assert client.patch(url, json={"name": "Stale", "expected_revision": 1}).status_code == 409
    params = {"expected_revision": 2}
    files = {"file": ("new.txt", b"replacement version", "text/plain")}
    headers = {"Idempotency-Key": "replace-once"}
    with patch.object(app.state.ctx.store, "complete_operation", side_effect=RuntimeError("lost response")):
        assert client.put(url + "/replace", params=params, files=files, headers=headers).status_code == 500
    replay = client.put(url + "/replace", params=params, files=files, headers=headers)
    assert replay.status_code == 200, replay.text
    attachment = replay.json()["attachment"]
    assert attachment["attachment_id"] == original["attachment_id"] and attachment["revision"] == 3
    assert attachment["name"] == "Assessment" and attachment["description"] == "Useful context"
    assert attachment["modified_by_principal"] == "user:local"
    assert client.get(url).content == b"replacement version"
    assert client.get(url, params={"revision": 1}).content == b"first version"
    assert client.get(url, params={"revision": 2}).content == b"first version"
    editable = client.get(url + "/content").json()
    assert editable["content"] == "replacement version"
    edited = client.put(url + "/content", json={**editable, "content": "edited source"}, headers={"Idempotency-Key": "edit-once"})
    assert edited.status_code == 200, edited.text
    assert edited.json()["attachment"]["revision"] == 4
    assert client.get(url).content == b"edited source"
    rebuilt = CardProjection(tmp_path / "revision-rebuilt.db", app.state.ctx.store.event_log)
    rebuilt.rebuild_from_log("default")
    assert rebuilt.get_operation_outcome("replace-once")["result"] == replay.json()
    assert rebuilt.get_card(card.id).attachments[0].revision == 4
    with rebuilt._conn() as conn:
        assert conn.execute("SELECT count(*) FROM card_attachment_revisions").fetchone()[0] == 4
    manifest = client.get(f"/api/cards/{card.id}/materials").json()
    assert "edited source" not in json.dumps(manifest)
    assert manifest["attachments"][0]["editable"]
    assert client.get(f"/partials/cards/{card.id}/detail").text.count("Assessment") >= 1
    assert client.delete(url, headers={"Idempotency-Key": "remove"}, params={"expected_version": manifest["updated_at"]}).status_code == 200
    assert client.get(url).status_code == 404
    assert client.get(url, params={"revision": 1}).status_code == 404
    assert client.get(f"/api/cards/{card.id}/materials").json()["attachments"] == []
    assert app.state.ctx.services["dispatch_store"].list(card_id=card.id) == []


def test_replacement_uses_current_quota_and_preserves_old_manifest_digest(publication):
    import hashlib
    from pa.attachments import manifest_digest
    from pa.domain.models import CardAttachment
    app, client, card = publication
    artifact = upload(client, card, "quota-file", b"old").json()
    legacy = {k: v for k, v in artifact.items() if k not in {
        "name", "description", "kind", "resource_url", "revision", "modified_at", "modified_by_principal", "modified_by_instance"}}
    legacy_digest = hashlib.sha256(json.dumps([legacy], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert manifest_digest([CardAttachment.model_validate(legacy)]) == legacy_digest
    assert manifest_digest([artifact]) == legacy_digest
    url = attachment_url(card, artifact)
    with patch("pa.domain.attachment_revisions.CARD_ATTACHMENT_BYTES", 4):
        rejected = client.put(url + "/replace?expected_revision=1", files={"file": ("big.txt", b"large", "text/plain")})
        assert rejected.status_code == 422
        assert client.get(url).content == b"old"
        accepted = client.put(url + "/replace?expected_revision=1", files={"file": ("fits.txt", b"four", "text/plain")})
        assert accepted.status_code == 200, accepted.text
    # History is retained outside the current-card byte quota.
    from pa.attachments import AttachmentStore
    blobs = AttachmentStore(app.state.ctx.settings.data_dir)
    blobs.garbage_collect(app.state.ctx.store.get_card(card.id).attachments, minimum_age_seconds=0)
    assert client.get(url, params={"revision": 1}).content == b"old"


def test_resource_links_share_manifest_revisions_and_safe_urls(publication):
    app, client, card = publication
    endpoint = f"/api/cards/{card.id}/attachment-links"
    data = {"name": "Specification", "description": "Current source", "resource_url": "https://example.org/spec?a=1&b=2"}
    first = client.post(endpoint, json=data, headers={"Idempotency-Key": "link-once"})
    assert first.status_code == 201, first.text
    assert client.post(endpoint, json=data, headers={"Idempotency-Key": "link-once"}).json() == first.json()
    attachment = first.json()
    url = attachment_url(card, attachment)
    assert attachment["kind"] == "resource_link"
    assert client.get(url).text == data["resource_url"]
    assert 'rel="noopener noreferrer"' in client.get(url + "/preview").text
    update = client.patch(url, json={"expected_revision": 1, "resource_url": "https://example.org/new", "name": "Revised specification"}, headers={"Idempotency-Key": "url-edit"})
    assert update.status_code == 200, update.text
    assert update.json()["attachment"]["revision"] == 2
    assert client.get(url).text == "https://example.org/new"
    assert client.get(url, params={"revision": 1}).text == data["resource_url"]
    for bad in ["javascript:alert(1)", "data:text/html,bad", "file:///tmp/spec", "https://user:pass@example.org", "https://example.org/\npath"]:
        assert client.post(endpoint, json={**data, "resource_url": bad}).status_code == 422
        assert client.patch(url, json={"resource_url": bad, "expected_revision": 2}).status_code == 422
    assert client.put(url + "/content", json={"content": "no", "expected_revision": 2}).status_code == 422
    assert client.put(url + "/replace?expected_revision=2", files={"file": ("x.txt", b"x", "text/plain")}).status_code == 422
    assert client.delete(url).status_code == 200
    assert client.get(url + "/preview").status_code == 404


def test_safe_previews_edit_validation_and_unavailable_blob(publication):
    app, client, card = publication
    dangerous = b'<h1>Report</h1><style>h1 {color:red}</style><script>parent.evil=1</script><meta http-equiv="refresh" content="0;url=https://example.org"><img src="https://example.org/track" onerror="alert(1)"><a href="https://example.org">go</a>'
    html = client.post(f"/api/cards/{card.id}/attachments", files={"file": ("report.html", dangerous, "text/html")}).json()
    url = attachment_url(card, html)
    assert 'sandbox=""' in client.get(url + "/preview").text
    inline = client.get(url, params={"view": True})
    assert inline.status_code == 200
    assert "<h1>Report</h1>" in inline.text and "color:red" in inline.text
    assert "<script" not in inline.text and "<meta" not in inline.text
    assert "https://example.org" not in inline.text and "onerror" not in inline.text
    assert inline.headers["content-security-policy"].startswith("sandbox; default-src 'none'")
    assert client.get(url, params={"download": True}).content == dangerous
    audit = client.post(f"/api/cards/{card.id}/attachments", files={"file": ("audit.json", b'{"ok":true}', "application/json")}).json()
    audit_url = attachment_url(card, audit)
    assert client.put(audit_url + "/content", json={"content": "invalid JSON", "expected_revision": 1}).status_code == 422
    assert client.get(audit_url).content == b'{"ok":true}'
    binary = client.post(f"/api/cards/{card.id}/attachments", files={"file": ("binary.xyz", b"\xff", "application/octet-stream")}).json()
    binary_url = attachment_url(card, binary)
    fallback = client.get(binary_url + "/preview")
    assert fallback.status_code == 200 and "No browser preview" in fallback.text and "Download original" in fallback.text
    assert client.get(binary_url + "/content").status_code == 415
    assert client.put(binary_url + "/content", json={"expected_revision": 1, "content": "no"}).status_code == 415
    with patch("pa.modules.items._ensure_attachment_available", side_effect=__import__('fastapi').HTTPException(503, "unavailable")):
        unavailable = client.get(url + "/preview")
    assert unavailable.status_code == 503 and "saved attachment reference has been kept" in unavailable.text
    assert len(client.get(f"/api/cards/{card.id}/materials").json()["attachments"]) == 3


def test_completion_evidence_selects_original_bytes_after_multiple_replacements(publication):
    app, client, card = publication
    first = upload(client, card, "evidence-v1", b"original evidence").json()
    url = attachment_url(card, first)
    for revision, artifact in [(1, first), (2, None)]:
        artifact = artifact or client.get(f"/api/cards/{card.id}/materials").json()["attachments"][0]
        result = submit(client, card, f"result-{revision}", envelope(references=[{
            "kind": "attachment", "label": f"Evidence {revision}", "attachment_id": artifact["attachment_id"], "sha256": artifact["sha256"]}]))
        assert result.status_code == 201, result.text
        changed = client.put(url + f"/replace?expected_revision={revision}", files={"file": ("proof.txt", f"version {revision+1}".encode(), "text/plain")})
        assert changed.status_code == 200, changed.text
    assert client.get(url, params={"sha256": first["sha256"]}).content == b"original evidence"
    # Both evidence hashes remain resolvable after current reference removal.
    assert client.delete(url).status_code == 200
    assert client.get(url, params={"sha256": first["sha256"]}).content == b"original evidence"
    assert client.get(url, params={"sha256": artifact["sha256"]}).content == b"version 2"
    assert client.get(url, params={"revision": 2}).content == b"version 2"


def test_removal_receipt_distinguishes_current_content_from_earlier_evidence(publication):
    app, client, card = publication
    first = upload(client, card, "earlier-evidence").json()
    url = attachment_url(card, first)
    assert submit(client, card, "retain-earlier", envelope(references=[{
        "kind": "attachment", "label": "Original", "attachment_id": first["attachment_id"], "sha256": first["sha256"]}])).status_code == 201
    assert client.put(url + "/replace?expected_revision=1", files={"file": ("later.txt", b"later", "text/plain")}).status_code == 200
    headers = {"Idempotency-Key": "remove-later"}
    removed = client.delete(url, headers=headers)
    assert removed.status_code == 200, removed.text
    assert removed.json()["attachment_removal"]["retained_for_evidence"] is False
    assert removed.json()["attachment_removal"]["prior_evidence_retained"] is True
    assert "earlier revisions" in removed.json()["attachment_removal"]["message"]
    assert client.delete(url, headers=headers).json() == removed.json()
    assert client.get(url, params={"sha256": first["sha256"]}).status_code == 200


def test_concurrent_revision_writers_commit_once(publication):
    app, client, card = publication
    artifact = upload(client, card, "race").json()
    url = attachment_url(card, artifact)
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(client.patch, url, json={"name": f"writer-{i}", "expected_revision": 1}, headers={"Idempotency-Key": f"writer-{i}"}) for i in range(2)]
        assert sorted(f.result().status_code for f in futures) == [200, 409]
    assert app.state.ctx.store.get_card(card.id).attachments[0].revision == 2


def test_authenticated_permissions_and_browser_csrf(publication):
    app, client, card = publication
    artifact = upload(client, card, "protected").json()
    url = attachment_url(card, artifact)
    users = app.state.ctx.require_service("users")
    viewer = users.create_user("reader", "reader-password", role="viewer")
    membership = app.state.ctx.require_service("membership")
    membership.add_membership(Membership(realm_id="default", principal_type="user", principal_id=viewer.id, role=RealmRole.VIEWER))
    app.state.ctx.settings.auth_required = True
    other = TestClient(app)
    assert other.get(url).status_code == 401
    other.headers["Authorization"] = "Bearer " + viewer.cli_token
    assert other.get(url).status_code == 200
    assert "Upload a file" not in other.get(f"/partials/cards/{card.id}/detail").text
    assert other.patch(url, json={"name": "denied", "expected_revision": 1}).status_code == 403
    assert other.delete(url).status_code == 403
    outsider = users.create_user("outsider", "outsider-password")
    other.headers["Authorization"] = "Bearer " + outsider.cli_token
    assert other.get(url).status_code == 403
    assert other.get(url + "/preview").status_code == 403
    assert other.get(f"/api/cards/{card.id}/materials").status_code == 403
    app.state.ctx.settings.auth_required = False
    browser = TestClient(app)
    browser.get("/api/health")
    assert browser.patch(url, json={"name": "csrf denied", "expected_revision": 1}).status_code == 403
    assert browser.patch(url, headers={"X-CSRF-Token": browser.cookies.get("pa_csrf"), "Origin": "https://hostile.example"}, json={"name": "origin denied", "expected_revision": 1}).status_code == 403


def test_mcp_management_routes_use_same_writer(tmp_path):
    from pa.mcp.tools.items import register_mcp
    from tests.test_goal_assigned_service_mcp import FakeMcp
    mcp = FakeMcp()
    source = tmp_path / "replacement.txt"
    source.write_text("new")
    with patch("pa.mcp.local_api.request_local_pa", return_value={}) as request:
        register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace()))
        mcp.functions["create_card_resource_link"]("card", "Spec", "https://example.org", " link-key ")
        assert request.call_args.args[1:3] == ("POST", "/api/cards/card/attachment-links")
        assert request.call_args.kwargs["headers"]["Idempotency-Key"] == "link-key"
        mcp.functions["update_card_attachment"]("card", "attachment", 2, "metadata-key", name="New")
        assert request.call_args.kwargs["json"]["expected_revision"] == 2
        mcp.functions["get_card_attachment_content"]("card", "attachment")
        assert request.call_args.args[1] == "GET"
        mcp.functions["edit_card_attachment_content"]("card", "attachment", "New", 2, "content-key")
        assert request.call_args.args[1] == "PUT"
        mcp.functions["replace_card_attachment"]("card", "attachment", str(source), 2, "replace-key")
        assert request.call_args.args[2].endswith("/replace")
        with pytest.raises(ValueError, match="empty"):
            mcp.functions["update_card_attachment"]("card", "attachment", 2, " ")
