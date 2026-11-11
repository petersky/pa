"""Completion last mile: declaration defaults, the `active` migration, and owner acceptance."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.completion import completion_state
from pa.domain.models import CardCreate, CompletionRequirement
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


@pytest.fixture(autouse=True)
def _reset():
    for fn in (reset_kernel, reset_settings, reset_store, reset_instance_agent):
        fn()
    yield
    for fn in (reset_instance_agent, reset_store, reset_settings, reset_kernel):
        fn()


def _app(tmp_path, **overrides):
    settings = Settings(data_dir=tmp_path, agent_enabled=False, peers=[], **overrides)
    kernel = Kernel.boot(settings=settings)
    return kernel, kernel.build_app()


def _create(client, key, requirement):
    headers = {"X-CSRF-Token": client.cookies.get("pa_csrf") or "", "Idempotency-Key": key}
    return client.post("/api/cards", json={"title": "declared " + key, "lane": "waiting",
                                           "completion_requirement": requirement}, headers=headers)


def _requirement(**extra):
    return {"mode": "explicit_acceptance", "criteria": "ship it",
            "milestones": ["integrated", "active", "verified"],
            "acceptance_principals": ["user:local"], **extra}


def test_active_is_not_a_default_milestone_but_stays_selectable(tmp_path):
    kernel, app = _app(tmp_path)
    with TestClient(app) as client:
        client.get("/api/cards")
        default = _create(client, "default", _requirement())
        assert default.status_code == 201, default.text
        assert default.json()["completion_requirement"]["milestones"] == ["integrated", "verified"]
        chosen = _create(client, "chosen", _requirement(include_active=True))
        assert chosen.json()["completion_requirement"]["milestones"] == ["integrated", "active", "verified"]


def test_setting_restores_active_as_default(tmp_path):
    kernel, app = _app(tmp_path, completion_active_milestone_default=True)
    with TestClient(app) as client:
        client.get("/api/cards")
        created = _create(client, "legacy", _requirement())
        assert created.json()["completion_requirement"]["milestones"] == ["integrated", "active", "verified"]


def test_resubmitting_an_unchanged_requirement_does_not_edit_it(tmp_path):
    kernel, app = _app(tmp_path)
    store = kernel.ctx.store
    card = store.create_card(CardCreate(title="legacy", lane="waiting", completion_requirement=_requirement()))
    assert "active" in card.completion_requirement.milestones  # stored before the new default
    with TestClient(app) as client:
        client.get("/api/cards")
        headers = {"X-CSRF-Token": client.cookies.get("pa_csrf") or "", "Idempotency-Key": "resave"}
        response = client.patch(f"/api/cards/{card.id}", headers=headers, json={
            "title": "renamed", "expected_version": card.updated_at.isoformat(),
            "completion_requirement": card.completion_requirement.model_dump(mode="json")})
        assert response.status_code == 200, response.text
        assert response.json()["completion_requirement"]["revision"] == card.completion_requirement.revision
        assert "active" in response.json()["completion_requirement"]["milestones"]


# --------------------------------------------------------------- revision carry-forward

def _accepted_card(tmp_path, **req):
    from tests.test_completion_requirements import projection
    from pa.domain.models import CardUpdate, CompletionEvidence
    store = projection(tmp_path)
    card = store.create_card(CardCreate(title="shipped", lane="waiting", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "Verify production", "milestones": ["verified", "active"],
        "include_active": True, "acceptance_principals": ["instance:acceptance"], **req}))
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
                                  subject_revision="build-a", milestones=["verified", "active"], references=["artifact:x"])
    card = store.update_card(card.id, CardUpdate(expected_version=card.updated_at, completion_acceptance=evidence),
                             principal_id="instance:acceptance", actor_session_id="verifier-session",
                             actor_dispatch_id="verifier-dispatch", idempotency_key="accept-a")
    assert card.completion_status["accepted"]
    return store, card


def _edit(store, card, **changes):
    from pa.domain.models import CardUpdate
    card = store.get_card(card.id)
    requirement = card.completion_requirement.model_copy(update=changes)
    return store.update_card(card.id, CardUpdate(expected_version=card.updated_at, field_intent=["completion_requirement"],
                                                 completion_requirement=requirement))


def test_a_non_substantive_requirement_edit_keeps_finished_verification(tmp_path):
    store, card = _accepted_card(tmp_path)
    edited = _edit(store, card, verification_paused=True, milestones=["verified"])
    assert edited.completion_requirement.revision != card.completion_requirement.revision
    assert edited.completion_requirement.prior_revisions == [card.completion_requirement.revision]
    assert edited.completion_status["accepted"], edited.completion_status
    again = _edit(store, edited, verification_paused=False)
    assert again.completion_status["accepted"]
    assert len(again.completion_requirement.prior_revisions) == 2


def test_a_substantive_requirement_edit_still_requires_fresh_acceptance(tmp_path):
    for index, change in enumerate((
            {"criteria": "Verify something else"}, {"milestones": ["verified", "active", "published"]},
            {"acceptance_principals": ["instance:acceptance", "user:other"]})):
        store, card = _accepted_card(tmp_path / str(index))
        edited = _edit(store, card, **change)
        assert edited.completion_requirement.prior_revisions == []
        assert not edited.completion_status["accepted"], change


def test_clients_cannot_forge_inherited_revisions(tmp_path):
    from tests.test_completion_requirements import projection
    store = projection(tmp_path)
    card = store.create_card(CardCreate(title="forged", lane="waiting", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "x", "prior_revisions": ["anything"]}))
    assert card.completion_requirement.prior_revisions == []
    edited = _edit(store, card, criteria="y", prior_revisions=["anything"])
    assert edited.completion_requirement.prior_revisions == []


# --------------------------------------------------------------- drop-active migration

def _legacy_cards(store):
    from pa.domain.models import CardUpdate, CompletionEvidence
    stuck = store.create_card(CardCreate(title="waiting for activation", lane="waiting", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "ship", "milestones": ["integrated", "active", "verified"],
        "acceptance_principals": ["instance:acceptance"]}))
    stuck = store.update_card(stuck.id, CardUpdate(expected_version=stuck.updated_at), principal_id="instance:pr-supervisor",
        integration_evidence=CompletionEvidence(requirement_revision=stuck.completion_requirement.revision,
                                                subject_revision="build-a", outcome="integrated"), idempotency_key="int")
    stuck = store.update_card(stuck.id, CardUpdate(expected_version=stuck.updated_at, completion_acceptance=CompletionEvidence(
        requirement_revision=stuck.completion_requirement.revision, subject_revision="build-a", milestones=["verified"])),
        principal_id="instance:acceptance", actor_session_id="verifier", actor_dispatch_id="verifier-d", idempotency_key="acc")
    chosen = store.create_card(CardCreate(title="really needs activation", lane="waiting", completion_requirement={
        "mode": "explicit_acceptance", "criteria": "ship", "milestones": ["active"], "include_active": True,
        "acceptance_principals": ["instance:acceptance"]}))
    plain = store.create_card(CardCreate(title="no requirement", lane="waiting"))
    done = store.create_card(CardCreate(title="finished", lane="waiting", completion_requirement={
        "mode": "integration_only", "milestones": ["active"], "acceptance_principals": []}))
    return stuck, chosen, plain, done


def test_drop_active_migration_dry_run_then_apply(tmp_path):
    from pa.execution import completion_migration
    kernel, app = _app(tmp_path)
    stuck, chosen, plain, other = _legacy_cards(kernel.ctx.store)
    assert stuck.completion_status["missing"] == ["active"]
    with TestClient(app) as client:
        client.get("/api/cards")

        def request(method, path, *, params=None, body=None, headers=None):
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf") or "", **(headers or {})}
            response = client.request(method, path, params=params, json=body, headers=headers)
            assert response.status_code < 400, response.text
            return response.json()

        dry = completion_migration.run(request, ["default"])
        assert dry["apply"] is False and dry["results"] == []
        ids = {entry["card_id"]: entry for entry in dry["plan"]}
        assert set(ids) == {stuck.id, other.id}  # the explicit include_active card and plain card are left alone
        assert ids[stuck.id]["milestones_after"] == ["integrated", "verified"] and ids[stuck.id]["completes"] is True
        assert kernel.ctx.store.get_card(stuck.id).completion_requirement.revision == stuck.completion_requirement.revision

        applied = completion_migration.run(request, ["default"], apply=True)
        by_id = {item["card_id"]: item for item in applied["results"]}
        assert by_id[stuck.id]["edited"] and by_id[stuck.id]["finished"] and by_id[stuck.id]["lane"] == "done"
        assert kernel.ctx.store.get_card(chosen.id).completion_requirement.milestones == ["active"]
        again = completion_migration.run(request, ["default"], apply=True)
        assert again["eligible"] == 0  # idempotent: nothing is left to change
        assert kernel.ctx.store.get_card(stuck.id).completion_requirement.milestones == ["integrated", "verified"]


def test_drop_active_cli_defaults_to_a_dry_run(tmp_path, monkeypatch):
    from typer.testing import CliRunner
    from pa.cli import card as card_cli
    kernel, app = _app(tmp_path)
    stuck, *_ = _legacy_cards(kernel.ctx.store)
    monkeypatch.setattr(card_cli, "get_settings", lambda: kernel.ctx.settings)
    with TestClient(app) as client:
        client.get("/api/cards")

        def fake_request(settings, method, path, *, params=None, body=None, headers=None, **_):
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf") or "", **(headers or {})}
            return client.request(method, path, params=params, json=body, headers=headers).json()
        monkeypatch.setattr(card_cli, "_request", fake_request)
        result = CliRunner().invoke(card_cli.card_app, ["drop-active"])
        assert result.exit_code == 0, result.output
        assert '"apply": false' in result.output and stuck.id in result.output
        assert kernel.ctx.store.get_card(stuck.id).completion_requirement.milestones == ["integrated", "active", "verified"]
        applied = CliRunner().invoke(card_cli.card_app, ["drop-active", "--apply"])
        assert applied.exit_code == 0, applied.output
        assert kernel.ctx.store.get_card(stuck.id).lane == "done"


def test_goal_supervisor_follows_the_independence_setting(tmp_path):
    kernel, _ = _app(tmp_path / "default")
    supervisor = kernel.ctx.require_service("goal_supervisor")
    assert supervisor.verifier_independence() == "relaxed"
    strict, _ = _app(tmp_path / "strict", completion_verifier_independence="strict")
    assert strict.ctx.require_service("goal_supervisor").verifier_independence() == "strict"
