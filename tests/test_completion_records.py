"""Substantive results survive retries/replay without granting acceptance."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.domain.completion_records import CompletionRecordInput, get_record, list_records
from pa.domain.models import AgentSession, CardCreate, CardUpdate
from pa.domain.projection import CardProjection
from tests.test_attachment_operation_receipts import publication
from tests.test_completion_provider_transport import transport


def envelope(**changes):
    return {"schema_version": 1, "title": "Audit findings", "summary": "Two gaps found; deployment was not assessed.",
            "sections": [{"heading": "Findings", "content": "**Important:** a reproducible gap."},
                         {"heading": "Validation and limits", "content": "Inspected fixtures only."}], **changes}


def submit(client, card, key, data=None, **kwargs):
    return client.post(f"/api/cards/{card.id}/completion-records", json=data or envelope(),
                       headers={"Idempotency-Key": key, **kwargs})


def test_distinct_retry_revision_reload_and_no_acceptance(publication, tmp_path):
    app, client, card = publication
    store = app.state.ctx.store
    before = store.get_card(card.id).model_dump(mode="json")
    first = submit(client, card, "audit-one")
    assert first.status_code == 201, first.text
    record = first.json()
    assert record["source_session_id"] is None and record["author_principal"].startswith("user:")
    assert submit(client, card, "audit-one").json() == record
    assert submit(client, card, "audit-one", envelope(summary="changed")).status_code == 409
    design = envelope(title="Design handoff", sections=[{"heading": "Requirements and next steps", "content": "Review before implementation."}],
                      supersedes_record_id=record["record_id"])
    second = submit(client, card, "design-two", design)
    assert second.status_code == 201, second.text
    third = submit(client, card, "another-attempt", design)
    assert third.status_code == 201 and third.json()["record_id"] != second.json()["record_id"]
    listing = client.get(f"/api/cards/{card.id}/completion-records?limit=1").json()
    assert listing["total"] == 3 and listing["next_offset"] == 1
    assert "sections" not in listing["items"][0] and "retained_attachments" not in listing["items"][0]
    assert store.get_card(card.id).model_dump(mode="json") == before
    rebuilt = CardProjection(tmp_path / "rebuilt-results.db", store.event_log)
    rebuilt.rebuild_from_log("default")
    assert list_records(rebuilt, card.id, "default")["total"] == 3
    assert get_record(rebuilt, card.id, record["record_id"], "default").model_dump(mode="json") == record
    assert rebuilt.get_operation_outcome("audit-one")["result"] == record
    # Replay twice has no duplicates, and ordinary card updates cannot erase results.
    rebuilt.rebuild_from_log("default")
    store.update_card(card.id, CardUpdate(body="Administrative reconciliation note"))
    assert list_records(store, card.id, "default")["total"] == 3
    for result in (record, second.json(), third.json()):
        assert client.get(f"/api/cards/{card.id}/completion-records/{result['record_id']}").json() == result


def test_lost_response_and_concurrent_retry(publication, tmp_path):
    app, client, card = publication
    store = app.state.ctx.store
    with patch.object(store, "complete_operation", side_effect=RuntimeError("lost postcommit response")):
        assert submit(client, card, "lost-result").status_code == 500
    saved = list_records(store, card.id, "default")["items"][0]
    with ThreadPoolExecutor(3) as pool:
        replies = list(pool.map(lambda _: submit(client, card, "lost-result"), range(3)))
    assert all(r.status_code == 201 for r in replies)
    assert all(r.json()["record_id"] == saved["record_id"] for r in replies)
    assert list_records(store, card.id, "default")["total"] == 1
    rebuilt = CardProjection(tmp_path / "recovered-results.db", store.event_log)
    rebuilt.rebuild_from_log("default")
    assert rebuilt.get_operation_outcome("lost-result")["result"] == replies[0].json()


def test_initial_concurrency_and_protected_card_unchanged(publication):
    app, client, _card = publication
    store = app.state.ctx.store
    card = store.create_card(CardCreate(title="Protected", lane="active", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "Independent verification", "acceptance_principals": ["user:local"]}))
    before = card.model_dump(mode="json")
    with ThreadPoolExecutor(4) as pool:
        replies = list(pool.map(lambda _: submit(client, card, "initial-race"), range(4)))
    assert any(r.status_code == 201 for r in replies)
    assert all(r.status_code in {201, 409} for r in replies)
    final = submit(client, card, "initial-race")
    assert final.status_code == 201
    assert list_records(store, card.id, "default")["total"] == 1
    assert store.get_card(card.id).model_dump(mode="json") == before
    assert submit(client, card, " ").status_code == 422
    assert client.post(f"/api/cards/{card.id}/completion-records", json=envelope()).status_code == 422
    # A predecessor from a different card is not a valid revision/resource.
    assert submit(client, _card, "cross-card", envelope(supersedes_record_id=final.json()["record_id"])).status_code == 422


def test_attachment_survives_removal_gc_and_closed_session(publication):
    app, client, card = publication
    attached = client.post(f"/api/cards/{card.id}/attachments", headers={"Idempotency-Key": "result-evidence"},
                           files={"file": ("evidence.txt", b"retained numeric evidence", "text/plain")}).json()
    data = envelope(references=[{"kind": "attachment", "label": "Durable evidence", "attachment_id": attached["attachment_id"], "sha256": attached["sha256"]}])
    result = submit(client, card, "with-evidence", data)
    assert result.status_code == 201, result.text
    url = f"/api/cards/{card.id}/attachments/{attached['attachment_id']}"
    assert client.delete(url).status_code == 200
    assert app.state.ctx.store.get_card(card.id).attachments == []
    blobs = app.state.ctx.require_service("attachment_store")
    assert blobs.garbage_collect([], minimum_age_seconds=0)["removed_blobs"] == 0
    assert client.get(url).content == b"retained numeric evidence"
    assert client.get(url + "?realm=other").status_code == 404
    # Revision can retain exactly the original immutable bytes after removal.
    assert submit(client, card, "retained-revision", {**data, "supersedes_record_id": result.json()["record_id"]}).status_code == 201
    assert "original evidence retained" in client.get(f"/results?card_id={card.id}&record={result.json()['record_id']}").text


def test_slow_evidence_fetch_does_not_hold_canonical_writer(publication):
    app, client, card = publication
    attachment = client.post(f"/api/cards/{card.id}/attachments", headers={"Idempotency-Key": "slow-evidence"},
        files={"file": ("evidence.txt", b"evidence", "text/plain")}).json()
    data = envelope(references=[{"kind": "attachment", "label": "Evidence", "attachment_id": attachment["attachment_id"], "sha256": attachment["sha256"]}])
    entered, release = threading.Event(), threading.Event()
    from pa.modules.items import _ensure_attachment_available

    def held(*args):
        entered.set()
        assert release.wait(5)
        return _ensure_attachment_available(*args)

    with patch("pa.modules.items._ensure_attachment_available", side_effect=held), ThreadPoolExecutor(2) as pool:
        pending = pool.submit(submit, client, card, "slow-result", data)
        try:
            assert entered.wait(5)
            independent = pool.submit(app.state.ctx.store.create_card, CardCreate(title="Independent write"))
            assert independent.result(timeout=2).title == "Independent write"
            assert not pending.done()
        finally:
            release.set()
        assert pending.result(timeout=5).status_code == 201


@pytest.mark.parametrize("change", [
    {"schema_version": 2}, {"title": " "}, {"summary": ""}, {"sections": []},
    {"author_principal": "forged"}, {"source_session_id": "forged"}, {"created_at": "2000-01-01"},
    {"sections": [{"heading": "one", "content": " "}]},
    {"sections": [{"heading": "one", "content": "x" * 32000}] * 3},
    {"references": [{"kind": "external", "label": "bad", "url": "file:///tmp/report"}]},
    {"references": [{"kind": "external", "label": "bad", "url": "https://user:password@example.org"}]},
    {"supersedes_record_id": "nonexistent"},
    {"references": [{"kind": "completion_record", "label": "other", "record_id": "missing"}]},
])
def test_validation_does_not_persist(publication, change):
    app, client, card = publication
    response = submit(client, card, "invalid", envelope(**change))
    assert response.status_code == 422, response.text
    assert list_records(app.state.ctx.store, card.id, "default")["total"] == 0


def bound_headers(app, card, session_id, dispatch_id):
    from pa.acp.environment import assigned_service_session_capability
    from pa.execution.dispatch import DispatchRecord
    ctx = app.state.ctx
    ctx.store.save_session(AgentSession(id=session_id, agent_name="codex", dispatch_id=dispatch_id,
                                        authority_instance_id=ctx.settings.instance_id, status="closed"))
    ctx.require_service("dispatch_store").put(DispatchRecord(mutation_id=dispatch_id, dispatch_id=dispatch_id,
        session_id=session_id, target_instance_id=ctx.settings.instance_id, authority_instance_id=ctx.settings.instance_id,
        authority_url="http://testserver", state="completed", principal_id="user:local", card_id=card.id))
    token = assigned_service_session_capability(secret=ctx.settings.session_secret, dispatch_id=dispatch_id,
        session_id=session_id, target_instance_id=ctx.settings.instance_id, purpose="completion-record")
    return {"Authorization": "SessionResult " + token, "X-PA-Assigned-Session-ID": session_id, "X-PA-Assigned-Dispatch-ID": dispatch_id}


def test_bound_attempt_provenance_scope_and_credential_isolation(publication):
    app, client, card = publication
    other = app.state.ctx.store.create_card(CardCreate(title="Unrelated"))
    first = submit(client, card, "bound-one", **bound_headers(app, card, "attempt-one", "dispatch-one"))
    assert first.status_code == 201, first.text
    assert (first.json()["source_session_id"], first.json()["source_dispatch_id"]) == ("attempt-one", "dispatch-one")
    second_headers = bound_headers(app, card, "attempt-two", "dispatch-two")
    second = submit(client, card, "bound-two", **second_headers)
    assert second.status_code == 201, second.text
    assert second.json()["source_session_id"] == "attempt-two"
    assert submit(client, other, "wrong-scope", **second_headers).status_code == 403
    assert client.get(f"/api/cards/{other.id}/completion-records", headers=second_headers).status_code == 403
    assert client.patch(f"/api/cards/{card.id}", json={"lane": "done"}, headers=second_headers).status_code >= 400
    assert submit(client, card, "forged-token", **{**second_headers, "Authorization": "SessionResult invalid"}).status_code == 403
    first_record = first.json()["record_id"]
    assert client.get(f"/api/cards/{card.id}/completion-records/{first_record}", headers=second_headers).status_code == 200
    assert app.state.ctx.store.get_card(card.id).lane.value == "inbox"


def test_mcp_contract_and_forwarding():
    from pa.mcp.tools.items import register_mcp
    from tests.test_goal_assigned_service_mcp import FakeMcp
    mcp = FakeMcp()
    with patch("pa.mcp.local_api.request_local_pa", return_value={}) as request:
        register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace()))
        mcp.functions["submit_completion_record"]("card", CompletionRecordInput(**envelope()), " stable ")
        assert request.call_args.kwargs["headers"] == {"Idempotency-Key": "stable"}
        assert request.call_args.kwargs["bound_result"] is True
        assert request.call_args.kwargs["json"]["sections"]
        mcp.functions["list_completion_records"]("card")
        assert request.call_args.args[1] == "GET"
        mcp.functions["get_completion_record"]("card", "record")
        assert request.call_args.args[2].endswith("/completion-records/record")
        with pytest.raises(ValueError):
            mcp.functions["submit_completion_record"]("card", CompletionRecordInput(**envelope()), " ")


def test_actual_mcp_bridge_stamps_provenance_and_replays(transport):
    t = transport
    arguments = dict(card_id=t.card.id, realm="default", envelope=envelope(), idempotency_key="mcp-result-once")
    first = t.client.portal.call(t.mcp.call_tool, "submit_completion_record", arguments)
    assert not first.is_error
    result = first.structured_content or json.loads(first.content[0].text)
    # MCP structured results may wrap the tool's dictionary under result.
    result = result.get("result", result)
    assert result["source_session_id"] == t.session.id
    assert result["source_dispatch_id"] == t.record.dispatch_id
    assert t.sent[-1]["Authorization"].startswith("SessionResult ")
    replay = t.client.portal.call(t.mcp.call_tool, "submit_completion_record", arguments)
    assert (replay.structured_content or json.loads(replay.content[0].text)) == (first.structured_content or json.loads(first.content[0].text))
    assert list_records(t.ctx.store, t.card.id, "default")["total"] == 1
