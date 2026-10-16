"""Reported stale previews and pre-session health failures, on isolated stores."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import pytest

from pa.domain.models import Card, CardCreate, CardUpdate
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.health_journal.models import Assessment, Policy
from pa.health_journal.store import Journal
from tests.test_execution_selection_api import selection_app  # noqa: F401
from tests.test_health_journal import api, fleet, observation  # noqa: F401


def test_summary_retry_preserves_preview_but_not_edit_cas(selection_app):
    client, app, _service = selection_app
    store = app.state.ctx.store
    card = store.create_card(CardCreate(title="Stable task", body="Original scope", auto_enrich=False))
    token = client.get(f"/api/cards/{card.id}").json()["execution_version"]
    defaults = client.get(f"/api/execution/defaults?card_id={card.id}").json()
    assert defaults["card_execution_version"] == token
    # The historical mutations were summary retries and summary_too_long failure
    # bookkeeping, not changes to the task or its execution settings.
    store.update_card(card.id, CardUpdate(summary_status="pending", summary_attempt_count=2))
    store.update_card(card.id, CardUpdate(summary_status="failed", summary_failure_code="summary_too_long"))
    fresh = store.get_card(card.id)
    assert fresh.updated_at != card.updated_at
    assert fresh.execution_version == token
    preview = client.post("/api/execution/preview", json={
        "card_id":card.id, "expected_execution_version":token})
    assert preview.status_code == 200, preview.text
    assert preview.json()["context"]["card_execution_version"] == token
    # Legacy edit/preview CAS stays strict; the new token never permits a stale
    # full-card mutation, even when it would be safe for execution selection.
    stale = client.post("/api/execution/preview", json={
        "card_id":card.id, "expected_card_version":card.updated_at.isoformat()})
    assert stale.status_code == 409
    store.update_card(card.id, CardUpdate(body="Changed execution scope"))
    rejected = client.post("/api/execution/preview", json={
        "card_id":card.id, "expected_execution_version":token})
    assert rejected.status_code == 409
    assert rejected.json()["detail"]["code"] == "stale_card_preferences"


@pytest.mark.parametrize("change", [
    {"title":"Changed"}, {"body":"Changed"}, {"project_id":"other"}, {"lane":"done"},
    {"owner_principal":"user:other"}, {"preferred_instance":"other"},
    {"preferred_capabilities":["browser"]}, {"lease_holder_instance":"other"},
    {"visibility":"private"}, {"tags":["high-risk"]},
    {"execution_preferences":{"harness":{"intent":"required", "value":"cursor"}}},
    {"completion_requirement":{"mode":"explicit_acceptance", "milestones":["verified"]}},
    {"attachments":[{"card_id":"card", "filename":"evidence", "sha256":"a"*64, "size":1,
                     "blob_ref":"sha256:" + "a"*64, "created_by_principal":"user:local", "created_by_instance":"local"}]},
])
def test_execution_snapshot_covers_real_execution_changes(change):
    card = Card(title="Task")
    changed = Card.model_validate(card.model_dump(mode="json") | change)
    assert changed.execution_version != card.execution_version


def test_fleet_dispatch_consumes_preview_token_and_replays_same_identity(selection_app, monkeypatch):
    from tests.test_fleet_placement import _candidate

    client, app, _service = selection_app
    ctx = app.state.ctx
    client.portal.call(ctx.require_service("dispatch_worker").close)
    monkeypatch.setattr(ctx.require_service("dispatch_worker"), "start", lambda:None)
    monkeypatch.setattr(ctx.require_service("dispatch_worker"), "wake", lambda:None)
    card = ctx.store.create_card(CardCreate(title="Dispatch unchanged scope", auto_enrich=False))
    body = {"card_id":card.id, "target_instance_id":"local", "message":"Perform the task",
            "execution_contract":{"version":1, "profile":"research", "confirmed":True, "requirements":{}}}
    with patch("pa.modules.fleet._placement_candidates", AsyncMock(return_value=[_candidate("local", local=True)])):
        preview = client.post("/api/fleet/placement/preview", json=body)
        assert preview.status_code == 200, preview.text
        body["expected_execution_version"] = preview.json()["card_execution_version"]
        ctx.store.update_card(card.id, CardUpdate(summary_status="failed", summary_failure_code="summary_too_long"))
        headers = {"Idempotency-Key":"stable-preview-dispatch"}
        admitted = client.post("/api/fleet/dispatch", json=body, headers=headers)
        assert admitted.status_code == 202, admitted.text
        replay = client.post("/api/fleet/dispatch", json=body, headers=headers)
        assert replay.status_code == 202, replay.text
        assert replay.json()["dispatch_id"] == admitted.json()["dispatch_id"]
        ctx.store.update_card(card.id, CardUpdate(body="A different task"))
        rejected = client.post("/api/fleet/dispatch", json=body, headers={"Idempotency-Key":"real-edit-dispatch"})
        assert rejected.status_code == 409, rejected.text
        assert rejected.json()["detail"]["code"] == "stale_card_preferences"


def test_failed_start_uses_real_retry_and_card_apis_with_protection(selection_app, monkeypatch):
    from pa.domain.models import ProjectCreate
    from pa.health_journal.models import Observation
    from tests.test_fleet_placement import _candidate

    client, app, _selection = selection_app
    ctx = app.state.ctx
    worker = ctx.require_service("dispatch_worker")
    client.portal.call(worker.close)
    monkeypatch.setattr(worker, "start", lambda:None)
    monkeypatch.setattr(worker, "wake", lambda:None)
    health = ctx.require_service("health_journal")
    project = ctx.store.create_project(ProjectCreate(title="Isolated repair"))
    health.journal.configure(Policy(authority_id="local", enabled=True, project_id=project.id,
        principal_id="user:local"), expected_version=1, actor="user:local")
    health.journal.append(Observation(**observation()), principal="user:local", realm="default", key="fixture")
    lease = health.journal.lease(health.owner)
    entry = health.journal.page(realms=["default"])["items"][0]
    health.journal.ingest(entry, source_id="local", realms=["default"], owner=health.owner, fence=lease["fence"])
    action = health.journal.action(owner=health.owner, fence=lease["fence"])
    card = ctx.store.create_card(CardCreate(title="Canonical repair", project_id=project.id, lane="active",
        completion_requirement={"mode":"explicit_acceptance", "milestones":["verified"],
            "criteria":"health-group:" + action["group_id"]}, auto_enrich=False))
    protected = card.completion_requirement.model_dump(mode="json")
    candidate = _candidate("local", local=True)
    candidate.capabilities.append("completion-requirements:v1")
    with patch("pa.modules.fleet._placement_candidates", AsyncMock(return_value=[candidate])):
        admitted = client.post("/api/fleet/dispatch", headers={"Idempotency-Key":"canonical-start"}, json={
            "card_id":card.id, "target_instance_id":"local", "message":"Investigate this failure",
            "execution_contract":{"version":1, "profile":"research", "confirmed":True, "requirements":{}}})
        assert admitted.status_code == 202, admitted.text
        dispatch_id = admitted.json()["dispatch_id"]
        ledger = ctx.require_service("dispatch_store")
        record = ledger.get(dispatch_id)
        selected = record.request_payload["execution_selection"]
        # Isolated fixture for the terminal outcome seen before session creation.
        record.error_code = "no_compatible_execution"
        record.recoverable = True
        ledger.transition(record, "failed", "Isolated target readiness rejection")
        health.journal.action_result(action["id"], state="dispatched", result={
            "card_id":card.id, "dispatch_id":dispatch_id}, owner=health.owner, fence=lease["fence"])
        client.portal.call(health.dispatch_one, lease)
        waiting = ctx.store.get_card(card.id)
        assert waiting.lane.value == "waiting"
        assert waiting.completion_requirement.model_dump(mode="json") == protected
        clock = time.time() + 31
        monkeypatch.setattr("pa.health_journal.service.time.time", lambda:clock)
        lease = health.journal.lease(health.owner)
        client.portal.call(health.dispatch_one, lease)
        retried = ledger.get(dispatch_id)
        assert retried.state == "queued"
        assert retried.mutation_id == record.mutation_id
        assert retried.control_operations[f"health-start_retry:{action['id']}"] == "retry"
        assert retried.request_payload["execution_selection"] == selected
        assert retried.request_payload["provider"] == selected["selected"]["harness"]
        assert retried.session_id is None
        assert ctx.store.get_card(card.id).completion_requirement.model_dump(mode="json") == protected


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery", ["accepted", "lost_reply", "unknown", "rejected", "started"])
async def test_health_failed_start_is_bounded_and_never_creates_a_second_worker(fleet, monkeypatch, delivery):
    authority, source, _network = fleet
    await api(source, "POST", "/reports", body=observation())
    await authority.service.cycle(manual=True)
    policy = authority.journal.status()["policy"]
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={
        "project_id":"project", "principal_id":"user:local"}), expected_version=2, actor="user:local")
    lease = authority.journal.lease(authority.service.owner)
    action = authority.journal.action(owner=authority.service.owner, fence=lease["fence"])
    ledger = DispatchStore(authority.settings.data_dir / "isolated-dispatches")
    record = DispatchRecord(mutation_id=str(uuid4()), card_id="canonical", state="failed",
        authority_instance_id=authority.settings.instance_id, authority_url="http://authority",
        target_instance_id=authority.settings.instance_id, recoverable=True,
        error_code="no_compatible_execution", reconciliation_state="not_requested")
    ledger.put(record)
    authority.ctx.services.update(dispatch_store=ledger, session_lifecycle=object(),
        pr_supervisor_store=SimpleNamespace(list_watches_for_cards=lambda *args, **kwargs:[]))
    authority.journal.action_result(action["id"], state="dispatched", result={
        "card_id":"canonical", "dispatch_id":record.dispatch_id}, owner=authority.service.owner, fence=lease["fence"])
    requirement = {"mode":"explicit_acceptance", "revision":"protected", "milestones":["verified"]}
    card = {"id":"canonical", "lane":"active", "summary":"Investigate", "updated_at":"initial",
            "completion_requirement":requirement}
    mutations = []
    retries = []
    now = time.time()
    monkeypatch.setattr("pa.health_journal.service.time.time", lambda:now)

    async def local(payload, method, path, **kwargs):
        if method == "GET" and "/dispatch-jobs/" in path:
            return {"dispatch":ledger.get(record.dispatch_id).model_dump(mode="json")}
        if method == "GET" and "/cards/" in path:
            return dict(card)
        mutations.append((method, path, kwargs))
        if method == "PATCH":
            assert "completion_requirement" not in kwargs["body"]
            card.update({k:v for k, v in kwargs["body"].items() if k in {"summary", "lane"}})
            card["updated_at"] = "checkpoint"
            return dict(card)
        assert path == f"/api/fleet/dispatch-jobs/{record.dispatch_id}/retry"
        assert kwargs["body"] == {}
        retries.append(kwargs["key"])
        if delivery == "unknown":
            raise httpx.ReadError("isolated unknown reply")
        if delivery == "rejected":
            response = httpx.Response(409, request=httpx.Request("POST", "http://authority" + path))
            raise httpx.HTTPStatusError("isolated policy rejection", request=response.request, response=response)
        current = ledger.get(record.dispatch_id)
        current.control_operations[kwargs["key"]] = "retry"
        current.state = "queued"
        if delivery == "started":
            current.state = "running"
            current.session_id = "investigator"
        ledger.put(current)
        if delivery == "lost_reply":
            raise httpx.ReadError("isolated lost committed reply")
        return {"dispatch_id":record.dispatch_id, "dispatch":current.model_dump(mode="json")}

    monkeypatch.setattr(authority.service, "local_api", local)
    try:
        await authority.service.dispatch_one(lease)
        assert card["lane"] == "waiting" and "no agent" in card["summary"].lower()
        assert not retries  # Backoff does not start a worker immediately.
        for _ in range(5):
            now += 31
            lease = authority.service.journal.lease(authority.service.owner)
            await authority.service.dispatch_one(lease)
            # Reopen the durable journal at every delivery boundary.
            authority.service.journal = Journal(authority.journal.path, authority.settings.instance_id)
        current = authority.service.journal.action(owner=authority.service.owner, fence=lease["fence"])
        assert current["id"] == action["id"]
        assert current["result"]["card_id"] == "canonical"
        assert current["result"]["dispatch_id"] == record.dispatch_id
        assert card["completion_requirement"] == requirement
        assert len(set(retries)) == 1
        assert len(retries) == (3 if delivery == "unknown" else 1)
        assert all(method == "PATCH" or path.endswith("/retry") for method, path, _ in mutations)
        if delivery == "started":
            assert card["lane"] == "active" and "is running" in card["summary"]
            group = authority.service.journal.group(action["group_id"], realms=["default"])
            assert group["data"]["next_actor"] == "agent"
            return
        if delivery in {"accepted", "lost_reply"}:
            assert current["state"] == "dispatched"
            assert not current["result"]["retry_pending"]
            # A failed automatic retry holds the existing obligation visibly;
            # another wake must not create another dispatch or retry key.
            failed = ledger.get(record.dispatch_id)
            failed.state = "failed"
            ledger.put(failed)
            await authority.service.dispatch_one(lease)
        else:
            assert current["state"] == "blocked"
        group = authority.service.journal.group(action["group_id"], realms=["default"])
        assert group["data"]["next_actor"] == "project_owner"
        assert group["data"]["phase"] == "blocked"
        assert "no agent" in card["summary"].lower() or "could not confirm" in card["summary"].lower()
    finally:
        ledger.close()


@pytest.mark.asyncio
async def test_explicit_repair_owner_prevents_rival_automatic_retry(fleet):
    authority, source, _network = fleet
    await api(source, "POST", "/reports", body=observation())
    await authority.service.cycle(manual=True)
    policy = authority.journal.status()["policy"]
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={
        "project_id":"project", "principal_id":"user:local"}), expected_version=2, actor="user:local")
    lease = authority.journal.lease(authority.service.owner)
    action = authority.journal.action(owner=authority.service.owner, fence=lease["fence"])
    group = authority.journal.group(action["group_id"], realms=["default"])
    authority.journal.assess(group["id"], Assessment(expected_version=group["version"],
        disposition="in_progress", reason="Canonical repair worker owns implementation", card_id="canonical"),
        actor="user:local", realms=["default"])
    authority.service.local_api = AsyncMock(side_effect=AssertionError("Must not start or alter a rival workflow"))
    await authority.service.failed_start(action, lease, {"state":"failed"})
    authority.service.local_api.assert_not_called()
    group = authority.journal.group(group["id"], realms=["default"])
    assert group["data"]["phase"] == "repairing"
    assert group["data"]["next_actor"] == "repair_owner"
    from pa.health_journal.store import JournalError
    with pytest.raises(JournalError, match="repair_owned"):
        authority.journal.reserve_effect(action["id"], "start_retry", {"method":"POST"},
            owner=authority.service.owner, fence=lease["fence"])
    authority.journal.action_result(action["id"], state="blocked", result={
        "reason":"Late automatic checkpoint", "next_actor":"pa"},
        owner=authority.service.owner, fence=lease["fence"])
    group = authority.journal.group(group["id"], realms=["default"])
    assert group["data"]["phase"] == "repairing"
    assert group["data"]["next_actor"] == "repair_owner"
    assert group["data"]["reason"] == "Canonical repair worker owns implementation"
