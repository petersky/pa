"""Exercise operator identity through real auth, card APIs, and HTMX forms."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


@pytest.fixture
def operator_app(tmp_path):
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                        agent_enabled=False, peers=[], sync_token="test-instance-token")
    app = Kernel.boot(settings=settings).build_app()
    try:
        yield app
    finally:
        reset_store()
        reset_instance_agent()


def make_card(app):
    store = app.state.ctx.store
    card = store.create_card(CardCreate(title="Operator acceptance regression", summary="Existing successor evidence", lane="active",
        completion_requirement={"mode": "explicit_acceptance", "criteria": "Verify the successor evidence",
                                "acceptance_principals": ["user:local"]}))
    # Earlier integration evidence must survive acceptance unchanged.
    store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
        integration_evidence=CompletionEvidence(requirement_revision=card.completion_requirement.revision,
            subject_revision="build-a", references=["https://example.test/pr/1"], outcome="integrated"),
        principal_id="instance:pr-supervisor", instance_id="test", idempotency_key="prior-integration")
    return store.get_card(card.id)


def csrf(client):
    client.get("/api/health")
    return {"X-CSRF-Token": client.cookies.get("pa_csrf"), "Origin": "http://testserver"}


def configure_operator(client, app):
    user = app.state.ctx.require_service("users").get("local")
    response = client.post("/api/auth/users/local/password", json={"password": "test-operator-password"},
                           headers={"Authorization": "Bearer " + user.cli_token})
    assert response.status_code == 200, response.text
    assert "pa_session" not in client.cookies  # Credential setup is not acceptance or login.


def sign_in(client):
    client.get("/login")
    response = client.post("/login", data={"username": "local", "password": "test-operator-password",
                                         "csrf": client.cookies.get("pa_csrf")}, follow_redirects=False)
    assert response.status_code == 303
    assert "pa_session" in client.cookies


@pytest.mark.parametrize("surface", ["form", "api"])
def test_local_refusal_then_password_sign_in_persists_one_receipt(operator_app, surface):
    app = operator_app
    with TestClient(app) as client:
        card = make_card(app)
        original = card.model_dump(mode="json")
        headers = csrf(client) | {"Idempotency-Key": "operator-done"}
        data = {"lane": "done", "expected_version": card.updated_at.isoformat()}
        def submit():
            if surface == "form":
                return client.post(f"/partials/cards/{card.id}?realm=default", data=data, headers=headers)
            return client.patch(f"/api/cards/{card.id}", json=data, headers=headers)
        refused = submit()
        assert refused.status_code == 409
        assert refused.json()["detail"]["code"] == "acceptance_pending"
        assert "pa user set-password local" in refused.json()["detail"]["message"]
        assert refused.json()["detail"]["operator_login_url"] == "/login"
        assert app.state.ctx.store.get_card(card.id).model_dump(mode="json") == original
        page = client.get(f"/partials/cards/{card.id}/detail")
        assert 'href="/login"' in page.text
        assert "no default password" in client.get("/login").text
        configure_operator(client, app)
        sign_in(client)
        headers["Idempotency-Key"] = "signed-in-done"
        response = submit()
        assert response.status_code == 200, response.text
        repeated = submit()
        assert repeated.status_code == 200
        stored = client.get(f"/api/cards/{card.id}").json()
        assert stored["lane"] == "done"
        assert stored["completion_requirement"] == original["completion_requirement"]
        assert stored["completion_evidence"][:-1] == original["completion_evidence"]
        receipt = stored["completion_evidence"][-1]
        assert (receipt["actor"], receipt["actor_kind"], receipt["outcome"]) == ("user:local", "human", "human_override")
        assert receipt["requirement_revision"] == card.completion_requirement.revision
        assert receipt["recorded_at"] and receipt["idempotency_key"]
        assert receipt["actor_session_id"] is None and receipt["actor_dispatch_id"] is None
        app.state.ctx.store.rebuild_from_log("default")
        assert client.get(f"/api/cards/{card.id}").json() == stored
        assert 'value="done" selected' in client.get(f"/partials/cards/{card.id}/detail").text


@pytest.mark.parametrize("producer", ["cli", "instance", "mcp", "session", "completion", "invalid-authorization"])
def test_producers_with_browser_cookie_cannot_claim_human_override(operator_app, producer):
    app = operator_app
    with TestClient(app) as client:
        card = make_card(app)
        configure_operator(client, app)
        sign_in(client)
        headers = csrf(client) | {"Idempotency-Key": producer}
        headers.update({
            "cli": {"Authorization": "Bearer " + app.state.ctx.require_service("users").get("local").cli_token},
            "instance": {"Authorization": "Bearer test-instance-token"},
            "mcp": {"X-PA-MCP-Instance-ID": app.state.ctx.settings.instance_id},
            "session": {"X-PA-Assigned-Session-ID": "session-forged"},
            "completion": {"X-PA-Completion-Producer": "human"},
            "invalid-authorization": {"Authorization": "GoalSession invalid"},
        }[producer])
        if producer != "cli":
            denied = client.post("/api/auth/users/local/password", json={"password": "forged-operator"}, headers=headers)
            assert denied.status_code == 403
            assert app.state.ctx.require_service("users").authenticate("local", "test-operator-password")
        for method, url, body in [(client.patch, f"/api/cards/{card.id}", "json"),
                                  (client.post, f"/partials/cards/{card.id}", "data")]:
            response = method(url, headers=headers, **{body: {"lane": "done", "expected_version": card.updated_at.isoformat()}})
            assert response.status_code in {403, 409}, response.text
        assert app.state.ctx.store.get_card(card.id).completion_evidence == card.completion_evidence
        assert app.state.ctx.store.get_card(card.id).lane.value == "active"


def test_password_setup_and_human_acceptance_keep_csrf_origin_and_version_checks(operator_app):
    app = operator_app
    with TestClient(app) as client:
        card = make_card(app)
        headers = csrf(client)
        for extra in [{}, {"Authorization": "Bearer test-instance-token"}]:
            denied = client.post("/api/auth/users/local/password", json={"password": "untrusted"}, headers=headers | extra)
            assert denied.status_code == 403
        assert not app.state.ctx.require_service("users").get("local").password_hash
        configure_operator(client, app)
        sign_in(client)
        payload = {"lane": "done", "expected_version": card.updated_at.isoformat()}
        assert client.post(f"/partials/cards/{card.id}", data=payload).status_code == 403
        assert client.post(f"/partials/cards/{card.id}", data=payload, headers=headers | {"Origin": "https://hostile.test"}).status_code == 403
        changed = app.state.ctx.store.update_card(card.id, CardUpdate(body="new evidence", expected_version=card.updated_at))
        stale = client.post(f"/partials/cards/{card.id}", data=payload, headers=headers)
        assert stale.status_code == 409
        assert stale.json()["detail"]["code"] == "stale_card_version"
        assert "Review" in stale.json()["detail"]["message"]
        assert app.state.ctx.store.get_card(card.id).completion_evidence == changed.completion_evidence


def test_set_password_cli_uses_live_server_without_offline_writes(operator_app, monkeypatch):
    import httpx
    from typer.testing import CliRunner
    from pa.cli.main import app as cli_app

    app = operator_app
    with TestClient(app) as client:
        settings = app.state.ctx.settings
        monkeypatch.setattr("pa.cli.main.get_settings", lambda: settings)
        def request(method, url, **kwargs):
            return client.request(method, url, **{k: v for k, v in kwargs.items() if k != "timeout"})
        monkeypatch.setattr(httpx, "request", request)
        result = CliRunner().invoke(cli_app, ["user", "set-password", "local"], input="cli-operator-password\ncli-operator-password\n")
        assert result.exit_code == 0, result.output
        assert "cli-operator-password" not in result.output
        assert app.state.ctx.require_service("users").authenticate("local", "cli-operator-password")
        assert client.post("/api/auth/login", data={"username": "local", "password": "cli-operator-password"}).status_code == 200
        before = (settings.data_dir / "users.json").read_bytes()
        def offline(*args, **kwargs):
            raise httpx.ConnectError("offline")
        monkeypatch.setattr(httpx, "request", offline)
        result = CliRunner().invoke(cli_app, ["user", "set-password", "local"], input="new-password\nnew-password\n")
        assert result.exit_code == 1
        assert "offline CLI writes are intentionally disabled" in result.output
        assert (settings.data_dir / "users.json").read_bytes() == before


def test_password_cli_without_credentials_does_not_create_user_files(tmp_path, monkeypatch):
    from typer.testing import CliRunner
    from pa.cli.main import app as cli_app

    settings = Settings(data_dir=tmp_path / "uninitialized", workspace_root=tmp_path / "workspaces", agent_enabled=False)
    monkeypatch.setattr("pa.cli.main.get_settings", lambda: settings)
    result = CliRunner().invoke(cli_app, ["user", "set-password", "local"], input="new-password\nnew-password\n")
    assert result.exit_code == 1
    assert "No local PA administrator credential" in result.output
    assert not (settings.data_dir / "users.json").exists()
