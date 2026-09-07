"""Repository scope is an explicit, authenticated, atomic local setting."""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.pr_supervisor import scope
from pa.pr_supervisor.github import GitHubAPIError, GitHubClient, GitHubCredentials


@pytest.fixture
def document(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("PA_GITHUB_WEBHOOK_SECRET", raising=False)
    path = tmp_path / "integrations" / "github.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"token": "credential-secret", "webhook_secret": "webhook-secret",
        "allowed_repositories": ["petersky/pa"], "custom": {"nested": "preserve-me"}}))
    return path


def proposal(document, **changes):
    return {"allowed_repositories": ["petersky/pa", "git@github.com:PeterSky/Eschaton.git"],
            "expected_revision": scope.snapshot(document.parent.parent)["revision"],
            "confirmed_additions": ["petersky/eschaton"], "confirmation_id": "operator-scope-1",
            "idempotency_key": "scope-update-1", **changes}


def prepared(document, body):
    return scope.prepare(document.parent.parent, repositories=body["allowed_repositories"],
        expected_revision=body["expected_revision"], actor="user:local",
        confirmed_additions=body["confirmed_additions"], confirmation_id=body["confirmation_id"],
        idempotency_key=body["idempotency_key"])


def commit(document, body, preparation):
    result, original, credentials, fingerprint = preparation
    return scope.commit(document.parent.parent, original=original, credentials=credentials,
        repositories=result["candidate"], actor="user:local", instance_id="scope-test",
        idempotency_key=body["idempotency_key"], fingerprint=fingerprint,
        confirmation_id=body["confirmation_id"])


def test_atomic_preservation_audit_replay_and_cas(document):
    body = proposal(document)
    result = commit(document, body, prepared(document, body))
    assert result["allowed_repositories"] == ["petersky/eschaton", "petersky/pa"]
    payload = json.loads(document.read_text())
    assert payload["token"] == "credential-secret"
    assert payload["webhook_secret"] == "webhook-secret"
    assert payload["custom"] == {"nested": "preserve-me"}
    assert document.stat().st_mode & 0o777 == 0o600
    replay, *_ = prepared(document, body)
    assert replay == {**result, "duplicate": True}
    audit = scope.audit(document.parent.parent)
    assert len(audit) == 1
    assert audit[0]["actor"] == "user:local"
    assert audit[0]["confirmation_id"] == "operator-scope-1"
    assert "secret" not in json.dumps([result, audit, scope.snapshot(document.parent.parent)])
    with pytest.raises(scope.ScopeError, match="different scope"):
        prepared(document, {**body, "allowed_repositories": ["petersky/other"]})
    with pytest.raises(scope.ScopeError, match="changed"):
        prepared(document, {**body, "idempotency_key": "new-request"})


def test_concurrent_cas_has_one_winner(document):
    one = proposal(document)
    two = proposal(document, idempotency_key="scope-update-2", allowed_repositories=["petersky/pa", "petersky/other"], confirmed_additions=["petersky/other"])
    first, second = prepared(document, one), prepared(document, two)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(commit, document, body, prep) for body, prep in [(one, first), (two, second)]]
    assert sum(job.exception() is None for job in jobs) == 1
    assert len(scope.audit(document.parent.parent)) == 1


@pytest.mark.parametrize("field,value", [("token", "rotated"), ("custom", {"updated": True})])
def test_changes_during_validation_are_preserved(document, field, value):
    body = proposal(document)
    preparation = prepared(document, body)
    changed = json.loads(document.read_text())
    changed[field] = value
    document.write_text(json.dumps(changed))
    with pytest.raises(scope.ScopeError, match="during validation"):
        commit(document, body, preparation)
    assert json.loads(document.read_text()) == changed


def test_environment_token_is_used_without_persisting_it(document, monkeypatch):
    monkeypatch.setenv("PA_GITHUB_TOKEN", "environment-secret")
    body = proposal(document)
    preparation = prepared(document, body)
    assert preparation[2].token == "environment-secret"
    commit(document, body, preparation)
    assert "environment-secret" not in document.read_text()
    assert json.loads(document.read_text())["token"] == "credential-secret"


@pytest.mark.parametrize("value", ["https://github.com/PeterSky/PA.git", "git@github.com:PeterSky/PA.git", "ssh://git@github.com/PeterSky/PA", "PeterSky/PA/"])
def test_normalization(value):
    assert scope.normalize_repositories([value, "petersky/pa"]) == ["petersky/pa"]


@pytest.mark.parametrize("value", ["*", "petersky/*", "https://evil.test/petersky/pa", "petersky/pa?x", "petersky/../pa", "https://token@github.com/petersky/pa", "petersky/pa\nextra"])
def test_invalid_identities(value):
    with pytest.raises(scope.ScopeError):
        scope.normalize_repositories([value])


def test_no_empty_wildcard_or_unconfirmed_expansion(document):
    original = document.read_bytes()
    for changes in [{"allowed_repositories": []}, {"confirmed_additions": []}, {"confirmation_id": ""}]:
        with pytest.raises(scope.ScopeError):
            prepared(document, proposal(document, **changes))
    assert document.read_bytes() == original


def test_corrupt_document_never_overwritten(document):
    document.write_text("{broken credential document")
    with pytest.raises(scope.ScopeError, match="safely"):
        scope.snapshot(document.parent.parent)
    assert document.read_text() == "{broken credential document"


@pytest.fixture
def api(document):
    reset_settings(); reset_store(); reset_instance_agent()
    async def github_request(client, method, path, **kwargs):
        if path == "/user":
            return 200, {"login": "petersky"}
        assert client.credentials.token == "credential-secret"
        return 200, {"full_name": path.removeprefix("/repos/"), "private": True, "token": "provider-secret"}
    with patch.object(GitHubClient, "_request", github_request):
        app = Kernel.boot(settings=Settings(data_dir=document.parent.parent, instance_id="scope-test",
            instance_name="scope test", agent_enabled=False, sync_token="fleet-secret", peers=[])).build_app()
        with TestClient(app) as client:
            client.get("/settings?section=github")
            yield client, app, {"X-CSRF-Token": client.cookies.get("pa_csrf")}
    reset_instance_agent(); reset_store(); reset_settings()


def test_preview_update_refresh_and_private_choices(api, document):
    client, app, headers = api
    body = proposal(document)
    response = client.post("/api/github/supervision-scope/preview", headers=headers,
        json={k: body[k] for k in ["allowed_repositories", "expected_revision"]})
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["additions"] == ["petersky/eschaton"]
    assert preview["validated_repositories"][0]["private"]
    choices = preview["operator_input"]["choices"]
    assert [c["id"] for c in choices] == ["apply_scope", "keep_scope"]
    assert preview["operator_input"]["allow_freeform"] is False
    assert scope.snapshot(document.parent.parent)["allowed_repositories"] == ["petersky/pa"]
    approved = {**choices[0]["value"], "idempotency_key": "approved-operation"}
    approved.pop("instance_id")
    result = client.put("/api/github/supervision-scope", headers=headers, json=approved)
    assert result.status_code == 200, result.text
    assert result.json()["capability_refresh"] == {"state": "ready", "authenticated": True}
    assert app.state.ctx.require_service("pr_supervisor").credentials.allowed_repositories == ["petersky/eschaton", "petersky/pa"]
    replay = client.put("/api/github/supervision-scope", headers=headers, json=approved)
    assert replay.json()["duplicate"] is True
    for path in ["", "/audit"]:
        response = client.get("/api/github/supervision-scope" + path)
        assert response.status_code == 200
        assert "secret" not in response.text
    page = client.get("/settings?section=github")
    assert 'id="pa-github-scope-apply"' in page.text
    assert 'id="pa-github-scope-keep"' in page.text


@pytest.mark.parametrize("error", [GitHubAPIError(404, "scope", "credential-secret"),
    GitHubAPIError(403, "scope", "provider-secret"), httpx.ReadTimeout("webhook-secret")])
def test_access_failure_is_redacted_and_does_not_write(api, document, error):
    client, _, headers = api
    before = document.read_bytes()
    with patch.object(GitHubClient, "_request", AsyncMock(side_effect=error)):
        response = client.put("/api/github/supervision-scope", headers=headers, json=proposal(document))
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "repository_access_failed"
    assert "secret" not in response.text
    assert document.read_bytes() == before


def test_auth_csrf_schema_boundaries(api, document):
    client, _, headers = api
    before = document.read_bytes()
    assert client.put("/api/github/supervision-scope", json=proposal(document)).status_code == 403
    for method, path in [("GET", ""), ("GET", "/audit"), ("POST", "/preview"), ("PUT", "")]:
        response = client.request(method, "/api/github/supervision-scope" + path,
            headers={"Authorization": "Bearer fleet-secret"}, json=proposal(document) if method != "GET" else None)
        assert response.status_code in {401, 403}, response.text
        response = client.request(method, "/api/github/supervision-scope" + path,
            headers={"Authorization": "Bearer invalid"}, json=proposal(document) if method != "GET" else None)
        assert response.status_code in {401, 403}, response.text
    for changes in [{"idempotency_key": ""}, {"token": "replacement-secret"}, {"allowed_repositories": []}]:
        response = client.put("/api/github/supervision-scope", headers=headers, json=proposal(document, **changes))
        assert response.status_code == 422
        assert "secret" not in response.text
    assert document.read_bytes() == before


def test_refresh_failure_is_durable_and_replay_repairs(api, document):
    client, app, headers = api
    body = proposal(document)
    service = app.state.ctx.require_service("pr_supervisor")
    with patch.object(service, "refresh_capability", AsyncMock(side_effect=RuntimeError("secret"))):
        response = client.put("/api/github/supervision-scope", headers=headers, json=body)
    assert response.status_code == 200
    assert response.json()["capability_refresh"]["state"] == "refresh_pending"
    assert "secret" not in response.text
    replay = client.put("/api/github/supervision-scope", headers=headers, json=body)
    assert replay.json()["duplicate"] is True
    assert replay.json()["capability_refresh"]["state"] == "ready"


def test_required_auth_rejects_anonymous_and_accepts_user_token(api, document):
    client, app, _ = api
    app.state.ctx.settings.auth_required = True
    try:
        client.cookies.clear()
        assert client.get("/api/github/supervision-scope").status_code == 401
        user = app.state.ctx.require_service("users").ensure_default_user()
        response = client.get("/api/github/supervision-scope",
            headers={"Authorization": f"Bearer {user.cli_token}"})
        assert response.status_code == 200
        response = client.put("/api/github/supervision-scope", json=proposal(document),
            headers={"Authorization": f"Bearer {user.cli_token}"})
        assert response.status_code == 200, response.text
    finally:
        app.state.ctx.settings.auth_required = False


def test_mcp_scope_tools_proxy_only_to_running_server(document):
    from pa.core.context import AppContext
    from pa.modules.pr_supervisor import PRSupervisorModule

    class FakeMcp:
        functions = {}

        def tool(self):
            def register(fn):
                self.functions[fn.__name__] = fn
                return fn
            return register

    ctx = AppContext(settings=Settings(data_dir=document.parent.parent), hooks=None, store=None)
    ctx.register_service("async_runtime", object())
    mcp = FakeMcp()
    with patch("pa.mcp.local_api.request_local_pa") as local:
        PRSupervisorModule().register_mcp(mcp, ctx)
        mcp.functions["github_supervision_scope"]()
        assert local.call_args.args[1:] == ("GET", "/api/github/supervision-scope")
        mcp.functions["preview_github_supervision_scope"](["petersky/pa"], "revision")
        assert local.call_args.args[1:] == ("POST", "/api/github/supervision-scope/preview")
        mcp.functions["update_github_supervision_scope"](["petersky/pa"], "revision", "key", [], "confirmation")
        assert local.call_args.args[1:] == ("PUT", "/api/github/supervision-scope")
        assert local.call_args.kwargs["json"]["confirmation_id"] == "confirmation"
        mcp.functions["github_supervision_scope_audit"]()
        assert local.call_args.args[1:] == ("GET", "/api/github/supervision-scope/audit")
