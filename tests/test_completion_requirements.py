from datetime import timedelta

import pytest

from pa.config import Settings
from pa.domain.completion import CompletionConflict
from pa.domain.models import CardCreate, CardUpdate, CardLane, CardEvent, EventType, CompletionEvidence
from pa.domain.projection import CardProjection, CardVersionConflict
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore
from tests.test_pr_supervisor import _FakeGitHub, _DedupeDispatcher, snapshot, watch
from tests.test_repository_workspaces import manager_for
from pa.pr_supervisor.models import PRPolicy, utcnow


def projection(tmp_path):
    objects = ObjectStore(tmp_path / "objects")
    log = EventLog(objects, tmp_path, "instance-a")
    return CardProjection(tmp_path / "cards.db", event_log=log)


def protected(store, *, milestones=None):
    return store.create_card(CardCreate(title="release", lane="waiting", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "Publish and verify production",
        "milestones": milestones or [], "acceptance_principals": ["instance:acceptance"],
    }))


def test_automation_human_and_replay(tmp_path):
    store = projection(tmp_path)
    card = protected(store)
    with pytest.raises(CompletionConflict, match="acceptance_pending"):
        store.update_card(card.id, CardUpdate(lane="done"), principal_id="instance:old-supervisor")
    # An immutable delayed legacy lane claim is retained but cannot bypass the declaration.
    event = CardEvent(type=EventType.CARD_UPDATED, realm_id="default", author_instance="old-instance", card_id=card.id, author_principal="instance:old-supervisor", payload={"lane": "done"})
    store.commit_event(event)
    assert store.get_card(card.id).lane == CardLane.WAITING
    history = store.event_log.entity_history_page("default", "card", card.id)
    claim = next(item for item in history["events"] if item["event"]["id"] == event.id)
    assert claim["event"]["payload"]["lane"] == "done"
    assert claim["projection_effect"] == "completion_claim_preserved_pending"
    head = store.event_log.get_head("default")
    assert store.event_log.entity_snapshot(head, "card", card.id)["lane"] == "waiting"
    store.rebuild_from_log("default")
    current = store.get_card(card.id)
    done = store.update_card(card.id, CardUpdate(lane="done", expected_version=current.updated_at), direct_human=True, principal_id="user:alice", idempotency_key="human-done")
    assert done.lane == CardLane.DONE
    assert done.completion_evidence[-1].outcome == "human_override"
    assert done.completion_evidence[-1].actor == "user:alice"
    store.rebuild_from_log("default")
    assert store.get_card(card.id).completion_evidence[-1].idempotency_key == "human-done"


def test_acceptance_authority_and_stale_requirement(tmp_path):
    store = projection(tmp_path)
    card = protected(store, milestones=["verified"])
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision, subject_revision="build-a", milestones=["verified"], references=["artifact:sha256:abc"])
    update = CardUpdate(lane="done", expected_version=card.updated_at, completion_acceptance=evidence)
    with pytest.raises(CompletionConflict, match="unauthorized"):
        store.update_card(card.id, update, principal_id="instance:worker", idempotency_key="fake-accept")
    changed = store.update_card(card.id, CardUpdate(completion_requirement={"mode": "explicit_acceptance", "criteria": "new build"}, expected_version=card.updated_at, field_intent=["completion_requirement"]))
    with pytest.raises(CardVersionConflict):
        store.update_card(card.id, update, principal_id="instance:acceptance", idempotency_key="stale")
    with pytest.raises(CompletionConflict, match="stale_completion_requirement"):
        store.update_card(card.id, update.model_copy(update={"expected_version": changed.updated_at}), principal_id="instance:acceptance", idempotency_key="stale-revision")


def test_source_only_legacy_and_unknown_schema(tmp_path):
    store = projection(tmp_path)
    card = store.create_card(CardCreate(title="source only"))
    echoed = CardUpdate.model_validate({**card.model_dump(mode="json"), "body": "ordinary full-card edit"})
    card = store.update_card(card.id, echoed)
    assert card.body == "ordinary full-card edit"
    assert store.update_card(card.id, CardUpdate(lane="done"), principal_id="instance:legacy").lane == CardLane.DONE
    card = protected(store)
    requirement = card.completion_requirement.model_copy(update={"schema_version": 2})
    from pa.execution.disposition import decide_card_disposition
    disposition = {"contract": "pa.card-disposition/v1", "lane": "done", "outcome": "finished", "evidence": {"integration_required": False}}
    decision = decide_card_disposition(disposition, current_lane=card.lane, completion_requirement=requirement)
    assert decision.applied_lane == CardLane.WAITING
    assert decision.reason_code == "unsupported_completion_requirement"


def test_actual_api_proxy_rejected_human_done_audited_and_ui(tmp_path):
    from fastapi.testclient import TestClient
    from pa.core.kernel import Kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path, agent_enabled=False, peers=[])
    app = Kernel.boot(settings=settings).build_app()
    try:
        with TestClient(app) as client:
            card = protected(app.state.ctx.store)
            page = client.get(f"/partials/cards/{card.id}/detail?realm=default")
            assert page.status_code == 200
            assert 'data-completion-reason="acceptance_pending"' in page.text
            assert "Next: acceptance" in page.text
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf"), "Idempotency-Key": "legacy-proxy-done", "X-PA-MCP-Instance-ID": settings.instance_id}
            response = client.patch(f"/api/cards/{card.id}", json={"lane": "done", "expected_version": card.updated_at.isoformat()}, headers=headers)
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["code"] == "acceptance_pending"
            headers.pop("X-PA-MCP-Instance-ID")
            headers["Idempotency-Key"] = "human-override"
            response = client.patch(f"/api/cards/{card.id}", json={"lane": "done", "expected_version": card.updated_at.isoformat()}, headers=headers)
            assert response.status_code == 200, response.text
            assert response.json()["completion_evidence"][-1]["outcome"] == "human_override"
            replay = client.patch(f"/api/cards/{card.id}", json={"lane": "done", "expected_version": card.updated_at.isoformat()}, headers=headers)
            assert replay.status_code == 200
            assert replay.json() == response.json()
            stale = client.post(f"/partials/cards/{card.id}/move?realm=default", data={"lane": "waiting", "expected_version": card.updated_at.isoformat()}, headers={"X-CSRF-Token": client.cookies.get("pa_csrf")})
            assert stale.status_code == 409
            assert stale.json()["detail"]["code"] == "stale_card_version"
    finally:
        reset_store()
        reset_instance_agent()


@pytest.mark.asyncio
async def test_real_merge_pending_acceptance_preserves_workspace(tmp_path):
    store = projection(tmp_path)
    card = protected(store, milestones=["integrated", "verified"])
    workspace_root = tmp_path / "git-fixture"
    workspace_root.mkdir()
    manager, _, linked = manager_for(workspace_root)
    manager.store = store
    lease = manager.provision_repository(linked, project_id="project-1", session_id="session-1", card_id=card.id)
    settings = Settings(data_dir=tmp_path, instance_id="instance-a", instance_url="http://instance-a", fleet_owner_url="http://instance-a", peers=[])
    watches = PRSupervisorStore(tmp_path / "supervisor.db")
    service = PRSupervisor(settings, store, supervisor_store=watches, github_client=_FakeGitHub([snapshot(), snapshot(state="merged", merge_commit_sha="c" * 40)]), dispatcher=_DedupeDispatcher(), workspace_manager=manager)
    await service.refresh_capability(force=True)
    item = watch(policy=PRPolicy(stable_head_seconds=0, stable_observations=1))
    item.card_id = card.id
    await service.register_watch(item, replicate=False)
    await service.run_once()
    watches.schedule_now(watch_id=item.id)
    await service.run_once()
    current = store.get_card(card.id)
    assert current.lane == CardLane.WAITING
    assert current.completion_evidence[0].outcome == "integrated"
    assert current.completion_status["missing"] == ["verified", "acceptance"]
    assert not manager.list()[0].completed
    assert manager.mark_card_completed(card.id, merged=True) == 0
    assert watches.get_watch(item.id).state["card_disposition"]["reason_code"] == "acceptance_pending"
    reopened = PRSupervisorStore(watches.db_path)
    assert reopened.list_card_completion_due(now=utcnow() + timedelta(days=1)) == []
    await service._complete_merged_card(reopened.get_watch(item.id))
    assert len(store.get_card(card.id).completion_evidence) == 1
    done = store.update_card(card.id, CardUpdate(lane="done", expected_version=current.updated_at, completion_acceptance=CompletionEvidence(requirement_revision=current.completion_requirement.revision, subject_revision="c" * 40, milestones=["verified"])), principal_id="instance:acceptance", idempotency_key="accept-build-c")
    assert done.lane == CardLane.DONE
    assert manager.mark_card_completed(card.id, merged=True) == 1
    # Even accepted completion cannot collect an active execution.
    assert manager.collect_garbage(now=utcnow() + timedelta(days=2), active_session_ids={lease.session_id})["retained"] == 1
    from pathlib import Path
    (Path(lease.worktree_path) / "uncommitted.txt").write_text("preserve this evidence")
    assert manager.collect_garbage(now=utcnow() + timedelta(days=2))["blocked"] == 1
    assert (Path(lease.worktree_path) / "uncommitted.txt").exists()
    (Path(lease.worktree_path) / "uncommitted.txt").unlink()
    assert manager.collect_garbage(now=utcnow() + timedelta(days=2))["cleaned"] == 1


def test_legacy_snapshot_omission_and_stale_requirement_keep_protection(tmp_path):
    store = projection(tmp_path)
    card = protected(store)
    old = card.model_dump(mode="json")
    changed = store.update_card(card.id, CardUpdate(completion_requirement={"mode": "explicit_acceptance", "criteria": "accept newer build"}, expected_version=card.updated_at, field_intent=["completion_requirement"]))
    event = CardEvent(type=EventType.CARD_UPSERTED, realm_id="default", author_instance="old", author_principal="instance:legacy", card_id=card.id, payload={**old, "lane": "done"}, causal_card_version=card.updated_at.isoformat(), field_intent=["completion_requirement", "lane"])
    store.commit_event(event)
    assert store.get_card(card.id).completion_requirement.revision == changed.completion_requirement.revision
    assert store.get_card(card.id).lane == CardLane.WAITING
    old.pop("completion_requirement")
    old.pop("completion_evidence")
    store.commit_event(event.model_copy(update={"id": "omitted-snapshot", "payload": {**old, "lane": "done"}}))
    store.rebuild_from_log("default")
    current = store.get_card(card.id)
    assert current.completion_requirement.revision == changed.completion_requirement.revision
    assert current.lane == CardLane.WAITING
    snapshot = store.event_log.entity_snapshot(store.event_log.get_head("default"), "card", card.id)
    assert snapshot["completion_requirement"]["revision"] == current.completion_requirement.revision
    assert snapshot["lane"] == "waiting"
    from pa.core.ui.work_presentation import present_work_item
    presentation = present_work_item(current, dispatch={"state": "completed"})
    assert presentation["state_label"] == "Waiting"
    assert presentation["state"] == "acceptance_pending"


@pytest.mark.asyncio
async def test_declared_integration_only_completes_from_real_merge(tmp_path):
    store = projection(tmp_path)
    card = store.create_card(CardCreate(title="source", lane="waiting", completion_requirement={"mode": "integration_only"}))
    settings = Settings(data_dir=tmp_path, instance_id="instance-a", instance_url="http://instance-a", fleet_owner_url="http://instance-a", peers=[])
    watches = PRSupervisorStore(tmp_path / "supervisor.db")
    service = PRSupervisor(settings, store, supervisor_store=watches, github_client=_FakeGitHub([snapshot(), snapshot(state="merged", merge_commit_sha="c" * 40)]), dispatcher=_DedupeDispatcher())
    await service.refresh_capability(force=True)
    item = watch(policy=PRPolicy(stable_head_seconds=0, stable_observations=1))
    item.card_id = card.id
    await service.register_watch(item, replicate=False)
    await service.run_once()
    watches.schedule_now(watch_id=item.id)
    await service.run_once()
    result = store.get_card(card.id)
    assert result.lane == CardLane.DONE
    assert result.completion_evidence[0].outcome == "integrated"
    assert result.completion_status["missing"] == []
