"""Repository scope is an explicit, authenticated, atomic local setting."""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.pr_supervisor import scope
from pa.pr_supervisor.github import GitHubAPIError, GitHubClient, GitHubCredentials

_REAL_GITHUB_REQUEST = GitHubClient._request


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
    assert result.json()["capability_refresh"] == {"state": "ready", "authenticated": True,
        "policy_revision": result.json()["revision"]}
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


@pytest.mark.parametrize('failure,reason', [(httpx.ReadTimeout('secret'), 'authority_unreachable'),
    ({'instances': 'secret'}, 'authority_response_invalid')])
def test_real_service_failure_watch_api_and_ui(api, document, failure, reason):
    from pa.pr_supervisor.models import PRWatch
    client, app, _ = api
    service = app.state.ctx.require_service('pr_supervisor')
    client.portal.call(service._loop_supervisor.close)
    service.settings.fleet_owner_url = 'http://authority'
    service.settings.instance_url = 'http://local'
    watch = service.store.upsert_watch(PRWatch(id='eligibility-ui', repository='petersky/eschaton',
        pr_number=42, pr_url='https://github.com/petersky/eschaton/pull/42'))
    read = AsyncMock(side_effect=failure) if isinstance(failure, Exception) else AsyncMock(return_value=failure)
    with patch.object(service, '_get_json', read), patch.object(service, '_post_json', AsyncMock(return_value={})), patch.object(service, '_reconcile_merged_cards', AsyncMock()):
        client.portal.call(service.run_once)
        result = client.get('/api/pr-supervisor/watches/' + watch.id)
        assert result.status_code == 200
        assert result.json()['watch']['state']['eligibility']['reason_code'] == reason
        assert 'secret' not in result.text
        assert 'Configure instance-local GitHub authentication' not in result.text
        page = client.get('/pull-requests?watch=' + watch.id)
        assert page.status_code == 200
        assert reason in page.text
        assert 'Automatic retry is scheduled' in page.text
        comparison = client.get('/api/github/supervision-scope/comparison')
        assert comparison.json()['evaluation_state'] == 'unavailable'
        assert comparison.json()['reason_code'] == reason
        assert read.await_count == 1


def test_instance_comparison_is_read_only_and_publication_is_separate(api, document):
    from datetime import timedelta
    from pa.pr_supervisor.models import GitHubCapability, utcnow
    client, app, headers = api
    service = app.state.ctx.require_service('pr_supervisor')
    client.portal.call(service._loop_supervisor.close)
    service.store.save_capability(GitHubCapability(instance_id='macmini', instance_name='Macmini',
        authenticated=True, allowed_repositories=['petersky/pa'], pr_watch_protocol_version=2,
        policy_revision='mini-revision', scope_mode='allowlist', policy_source='configured'))
    service.store.save_capability(GitHubCapability(instance_id='old-peer', authenticated=True,
        allowed_repositories=[], checked_at=utcnow()-timedelta(seconds=121)))
    original = document.read_bytes()
    response = client.get('/api/github/supervision-scope/comparison')
    assert response.status_code == 200
    data = response.json()
    rows = {row['instance_id']: row for row in data['candidates']}
    assert rows['macmini']['repositories'] == ['petersky/pa']
    assert rows['macmini']['policy_revision'] == 'mini-revision'
    assert rows['old-peer']['freshness'] == 'stale'
    assert rows['old-peer']['policy_revision'] is None
    assert data['inventory_kind'] == 'authority_received_advertisements'
    assert document.read_bytes() == original
    assert client.get('/api/github/supervision-scope/comparison', headers={'Authorization': 'Bearer fleet-secret'}).status_code in {401, 403}
    service._authority_last_error = 'authority_unreachable'
    saved = client.get('/api/github/supervision-scope').json()
    assert saved['published_capability']['state'] == 'publication_pending'
    assert saved['revision'] == scope.snapshot(document.parent.parent)['revision']
    page = client.get('/settings?section=github')
    assert 'Advertised scope by instance' in page.text
    assert 'not direct audits' in page.text


def test_missing_environment_only_installation_uses_existing_consent_flow(api, document, monkeypatch):
    client, _, headers = api
    document.unlink()
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'credential-secret')
    current = client.get('/api/github/supervision-scope').json()
    assert current['configuration_status'] == 'missing'
    assert current['scope_mode'] == 'none'
    response = client.post('/api/github/supervision-scope/preview', headers=headers,
        json={'allowed_repositories': ['petersky/pa'], 'expected_revision': current['revision']})
    assert response.status_code == 200, response.text
    assert not document.exists()
    body = response.json()['operator_input']['choices'][0]['value']
    body.pop('instance_id')
    body['idempotency_key'] = 'establish-exact-scope'
    result = client.put('/api/github/supervision-scope', headers=headers, json=body)
    assert result.status_code == 200, result.text
    assert result.json()['scope_mode'] == 'allowlist'
    assert json.loads(document.read_text())['allowed_repositories'] == ['petersky/pa']
    assert 'secret' not in document.read_text()


def test_save_before_failed_authority_publication_keeps_receipt(api, document):
    client, app, headers = api
    service = app.state.ctx.require_service('pr_supervisor')
    client.portal.call(service._loop_supervisor.close)
    service.settings.fleet_owner_url = 'http://authority'
    service.settings.instance_url = 'http://local'
    body = proposal(document)
    with patch.object(service, '_post_json', AsyncMock(side_effect=httpx.ReadTimeout('secret'))):
        saved = client.put('/api/github/supervision-scope', headers=headers, json=body)
    assert saved.status_code == 200
    assert saved.json()['capability_refresh']['state'] == 'refresh_pending'
    revision = saved.json()['revision']
    assert scope.snapshot(document.parent.parent)['revision'] == revision
    assert 'secret' not in saved.text
    with patch.object(service, '_post_json', AsyncMock(return_value={})):
        replay = client.put('/api/github/supervision-scope', headers=headers, json=body)
    assert replay.json()['duplicate']
    assert replay.json()['revision'] == revision
    assert replay.json()['capability_refresh']['state'] == 'ready'
    assert len(scope.audit(document.parent.parent)) == 1


def test_heartbeat_rejects_invalid_stale_and_forged_identity(api):
    from datetime import timedelta
    from pa.pr_supervisor.models import GitHubCapability, utcnow
    client, app, _ = api
    service = app.state.ctx.require_service('pr_supervisor')
    client.portal.call(service._loop_supervisor.close)
    headers = {'Authorization': 'Bearer fleet-secret', 'X-PA-Origin-Instance-ID': 'peer'}
    capability = GitHubCapability(instance_id='peer', authenticated=True, pr_watch_protocol_version=2,
        allowed_repositories=['petersky/pa']).model_dump(mode='json')
    assert client.post('/api/pr-supervisor/instances/heartbeat', json=capability, headers=headers).status_code == 200
    assert client.post('/api/pr-supervisor/instances/heartbeat', json={**capability, 'instance_id':'forged'}, headers=headers).status_code == 403
    for changed in [{'checked_at': (utcnow()-timedelta(seconds=121)).isoformat()},
                    {'checked_at': (utcnow()+timedelta(seconds=60)).isoformat()},
                    {'allowed_repositories': 'secret'}, {'authenticated':'true'}]:
        result = client.post('/api/pr-supervisor/instances/heartbeat', json={**capability, **changed}, headers=headers)
        assert result.status_code == 422
        assert 'secret' not in result.text
    assert not any(row.instance_id == 'forged' for row in service.store.list_capabilities())


@pytest.mark.parametrize('failure,reason', [(401, 'credentials_rejected'),
    (503, 'verification_unavailable'), ('timeout', 'verification_unavailable'),
    ('deadline', 'verification_unavailable')])
def test_actual_probe_cause_reaches_capability_watch_api_ui_and_recovers(api, failure, reason):
    import asyncio
    from datetime import timedelta
    from pa.pr_supervisor.models import PRWatch, utcnow
    from tests.test_pr_supervisor import snapshot
    client, app, _ = api
    service = app.state.ctx.require_service('pr_supervisor')
    runtime = app.state.ctx.require_service('async_runtime')
    client.portal.call(service._loop_supervisor.close)
    failed, user_calls = [True], []
    async def transport(request):
        assert request.url.path == '/user'
        user_calls.append(request.url.path)
        if failed[0]:
            if failure == 'timeout':
                raise httpx.ReadTimeout('private-secret')
            if failure == 'deadline':
                await asyncio.Event().wait()
            return httpx.Response(failure, json={'message': 'private-secret'})
        return httpx.Response(200, json={'login': 'test'})
    http = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    real_observe = runtime.observe
    async def short_probe_observe(operation, awaitable, *, timeout=None):
        return await real_observe(operation, awaitable,
            timeout=0.01 if operation == 'http.github' else timeout)
    runtime.observe = short_probe_observe
    service.github._provided_client = http
    service.github._request = _REAL_GITHUB_REQUEST.__get__(service.github, GitHubClient)
    service.eligibility_journal_hook = MagicMock()
    watch = service.store.upsert_watch(PRWatch(id='probe-cause-ui', repository='petersky/pa',
        pr_number=42, pr_url='https://github.com/petersky/pa/pull/42'))
    try:
        client.portal.call(lambda: service.refresh_capability(force=True))
        client.portal.call(service.run_once)
        assert len(user_calls) == 1  # Not reprobed by the next run_once.
        capability = client.get('/api/pr-supervisor/capabilities').json()['local']
        assert capability['state'] == reason
        assert not capability['authenticated']
        result = client.get('/api/pr-supervisor/watches/' + watch.id)
        assert reason in result.text and 'private-secret' not in result.text
        if reason == 'verification_unavailable':
            assert 'credentials_unavailable' not in result.text
            assert 'credentials_rejected' not in result.text
        assert reason in client.get('/pull-requests?watch=' + watch.id).text
        comparison = client.get('/api/github/supervision-scope/comparison')
        assert comparison.json()['candidates'][0]['reason_code'] == reason
        failed[0] = False
        service._capability_checked_at = utcnow() - timedelta(seconds=service.CAPABILITY_ERROR_RETRY_SECONDS + 1)
        service.github.snapshot = AsyncMock(return_value=snapshot().model_copy(update={
            'repository': 'petersky/pa', 'number': 42}))
        service._notify = AsyncMock()
        service.store.schedule_now(watch_id=watch.id)
        client.portal.call(service.run_once)
        assert len(user_calls) == 2
        assert service.capability.state == 'ready'
        assert service.capability.supports('petersky/pa')
        assert service.store.get_watch(watch.id).last_error is None
        inventory = [call.args[0]['report'] for call in service.eligibility_journal_hook.call_args_list
            if call.args[0]['report']['dependency'] == 'capability_inventory']
        assert inventory[-1]['eligible'] == ['scope-test']
    finally:
        runtime.observe = real_observe
        client.portal.call(http.aclose)


def test_actual_authority_deadline_reaches_watch_api_and_ui(api):
    import asyncio
    from pa.pr_supervisor.models import PRWatch
    client, app, _ = api
    service = app.state.ctx.require_service('pr_supervisor')
    runtime = app.state.ctx.require_service('async_runtime')
    client.portal.call(service._loop_supervisor.close)
    service.settings.instance_url = 'http://local'
    service.settings.fleet_owner_url = 'http://authority'
    calls = []
    async def transport(request):
        if request.method == 'POST':
            return httpx.Response(200, json={})
        calls.append(request.url.path)
        await asyncio.Event().wait()
    http = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    original_http, real_observe = service.http_client, runtime.observe
    async def short_peer_observe(operation, awaitable, *, timeout=None):
        return await real_observe(operation, awaitable,
            timeout=0.01 if operation == 'http.pr_supervisor_peer' else timeout)
    runtime.observe = short_peer_observe
    service.http_client = http
    try:
        for number in (1, 2):
            service.store.upsert_watch(PRWatch(id=f'deadline-{number}', repository='petersky/eschaton',
                pr_number=number, pr_url=f'https://github.com/petersky/eschaton/pull/{number}'))
        client.portal.call(service.run_once)
        assert len(calls) == 1
        for number in (1, 2):
            response = client.get(f'/api/pr-supervisor/watches/deadline-{number}')
            assert response.json()['watch']['state']['eligibility']['reason_code'] == 'authority_unreachable'
            assert 'credentials_unavailable' not in response.text
        page = client.get('/pull-requests?watch=deadline-1')
        assert 'authority_unreachable' in page.text
        assert 'Automatic retry is scheduled' in page.text
        assert runtime.snapshot()['operations']['http.pr_supervisor_peer']['timed_out'] == 1
    finally:
        service.http_client = original_http
        runtime.observe = real_observe
        client.portal.call(http.aclose)
