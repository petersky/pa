"""Ordinary registered producer -> private bridge -> auth -> durable receipt."""
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate
from pa.execution.dispatch import DispatchRecord
from pa.acp.environment import COMPLETION_DISPATCH_ENV, COMPLETION_SESSION_ENV, sanitize_provider_environment


@pytest.fixture
def transport(tmp_path, monkeypatch):
    from pa.core.kernel import Kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent
    from pa.modules.fleet import _assigned_mcp_environment_for_session
    from pa.acp.mcp_config import pa_mcp_servers
    from pa.mcp import server, local_api
    reset_store()
    reset_instance_agent()
    for key in ('PA_ASSIGNED_SERVICE_MODE', 'PA_ASSIGNED_SERVICE_SESSION_ID', 'PA_ASSIGNED_SERVICE_DISPATCH_ID', 'PA_LOCAL_API_SOCKET'):
        monkeypatch.delenv(key, raising=False)
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces', agent_enabled=False, auth_required=True, peers=[], instance_id='acceptance-owner')
    from pa.domain.instance_config import update_instance_config
    update_instance_config(settings.data_dir, session_secret=settings.session_secret, instance_id=settings.instance_id)
    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app) as client:
        ctx = app.state.ctx
        card = ctx.store.create_card(CardCreate(title='verify build', lane='waiting', completion_requirement={
            'mode': 'explicit_acceptance', 'milestones': ['verified'], 'acceptance_principals': ['user:local'],
            'originating_session_id': 'repair-session', 'originating_dispatch_id': 'repair-dispatch',
        }))
        session = ctx.store.save_session(AgentSession(id='verifier-session', agent_name='codex', dispatch_id='verifier-dispatch', authority_instance_id=settings.instance_id, status='active'))
        ledger = ctx.services['dispatch_store']
        record = ledger.put(DispatchRecord(mutation_id='verifier-test', dispatch_id=session.dispatch_id, session_id=session.id,
            authority_instance_id=settings.instance_id, authority_url='http://testserver', target_instance_id=settings.instance_id,
            principal_id='user:local', card_id=card.id, realm_id='default', state='running'))
        assert record.goal_provenance is None
        manager = ctx.services['instance_agent']
        runtime = SimpleNamespace(_closed=False, connected=True)
        monkeypatch.setattr(manager, 'get', lambda key: runtime if key == session.id else None)
        binding = _assigned_mcp_environment_for_session(settings, ledger, session)
        fresh = client.portal.call(manager._new_runtime, session)
        recovered = client.portal.call(manager._new_runtime, session)
        assert fresh.mcp_private_env == recovered.mcp_private_env == binding
        descriptor = pa_mcp_servers(settings, private_environment=binding)[0]
        env = {value.name: value.value for value in descriptor.env}
        assert env[COMPLETION_SESSION_ENV] == session.id
        assert env[COMPLETION_DISPATCH_ENV] == record.dispatch_id
        assert settings.session_secret not in json.dumps(env)
        assert not any(value.startswith('pas1.') for value in env.values())
        assert not ({COMPLETION_SESSION_ENV, COMPLETION_DISPATCH_ENV} & sanitize_provider_environment(env).keys())
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        monkeypatch.delenv('PA_LOCAL_API_SOCKET', raising=False)
        monkeypatch.setenv('PA_LOCAL_API_URL', 'http://testserver')
        monkeypatch.setattr(server, 'mcp', None)
        sent = []
        def bridge(method, url, **kwargs):
            sent.append(kwargs['headers'].copy())
            kwargs.pop("timeout", None)
            return client.request(method, url, **kwargs)
        monkeypatch.setattr(local_api.httpx, 'request', bridge)
        yield SimpleNamespace(ctx=ctx, card=card, session=session, record=record, ledger=ledger, runtime=runtime,
                              mcp=server._get_mcp(), sent=sent, client=client, settings=settings)
    reset_store()
    reset_instance_agent()


def arguments(t, **updates):
    return dict(card_id=t.card.id, realm='default', expected_version=t.card.updated_at.isoformat(),
        requirement_revision=t.card.completion_requirement.revision, subject_revision='build-a',
        milestones=['verified'], references=['artifact:build-a'], idempotency_key='accept-once', **updates)


def invoke(t, args):
    result = t.client.portal.call(t.mcp.call_tool, 'record_card_acceptance', args)
    # MCPServer returns text blocks and structured content for structured tools.
    if isinstance(result, tuple):
        return result[1]
    return result.structured_content or json.loads(result.content[0].text)


def test_ordinary_registered_acceptance_receipt_and_terminal_replay(transport, caplog):
    t = transport
    names = {tool.name for tool in t.client.portal.call(t.mcp.list_tools)}
    from pa.mcp.server import ASSIGNED_SERVICE_TOOL_ALLOWLIST
    assert 'record_card_acceptance' in names
    assert 'record_card_acceptance' not in ASSIGNED_SERVICE_TOOL_ALLOWLIST
    result = invoke(t, arguments(t))
    receipt = result['completion_evidence'][-1]
    assert receipt['actor_kind'] == 'bound_session'
    assert receipt['actor'] == 'user:local'
    assert receipt['actor_session_id'] == t.session.id
    assert receipt['actor_dispatch_id'] == t.record.dispatch_id
    assert result['lane'] == 'waiting'
    assert result['completion_status']['accepted']
    assert t.sent[-1]['Authorization'].startswith('SessionAcceptance pas1.')
    # Merging the journal routes must not expand this acceptance-purpose token.
    for method, path, body in (
        ('GET', '/api/health-journal/reports', None),
        ('GET', '/api/health-journal/status', None),
        ('POST', '/api/health-journal/reports', {
            'subsystem':'synthetic', 'error_code':'purpose-test', 'summary':'Synthetic scope check',
            'occurrence_key':'purpose-check'}),
    ):
        denied = t.client.request(method, path, json=body,
            headers={**t.sent[-1], 'Idempotency-Key':'no-report-authority'})
        assert denied.status_code in {401, 403}
    assert 'pas1.' not in json.dumps(result)
    # The authenticated producer cannot use its private credential for mutation
    # outside acceptance, and neither tool results nor logs disclose it.
    response = t.client.patch(f'/api/cards/{t.card.id}', json={'lane': 'done'},
        headers={**t.sent[-1], 'Idempotency-Key': 'no-lane-authority'})
    assert response.status_code == 403
    assert response.json()['detail']['code'] == 'completion_actor_scope_mismatch'
    for credential in ('SessionAcceptance ', 'SessionAcceptance forged'):
        response = t.client.patch(f'/api/cards/{t.card.id}', json={'lane': 'done'},
            headers={'Authorization': credential, 'Idempotency-Key': 'invalid-credential'})
        assert response.status_code == 403
        assert response.json()['detail']['code'] == 'invalid_assigned_session_capability'
    assert t.ctx.store.get_card(t.card.id).lane.value == 'waiting'
    assert 'pas1.' not in caplog.text
    assert t.settings.session_secret not in caplog.text
    t.ledger.put(t.record.model_copy(update={'state': 'completed'}))
    t.runtime.connected = False
    assert invoke(t, arguments(t)) == result
    with pytest.raises(Exception, match='invalid_assigned_session_capability'):
        invoke(t, {**arguments(t), 'idempotency_key': 'new-after-terminal'})


@pytest.mark.parametrize('fault,code', [
    ('origin', 'completion_self_acceptance_forbidden'),
    ('wrong-session', 'invalid_assigned_session_capability'),
    ('stale', 'invalid_assigned_session_capability'),
    ('realm', 'completion_actor_scope_mismatch'),
    ('card', 'completion_actor_scope_mismatch'),
    ('bearer', 'completion_actor_unbound'),
])
def test_actual_bridge_rejects_untrusted_or_ineligible_binding(transport, monkeypatch, fault, code):
    t = transport
    args = arguments(t)
    if fault == 'origin':
        # Durable repair origin, not caller-supplied actor claims.
        from pa.domain.models import CardUpdate
        req = t.card.completion_requirement.model_copy(update={'originating_session_id': t.session.id})
        t.card = t.ctx.store.update_card(t.card.id, CardUpdate(completion_requirement=req, expected_version=t.card.updated_at, field_intent=['completion_requirement']))
        args = arguments(t)
    elif fault == 'wrong-session':
        monkeypatch.setenv(COMPLETION_SESSION_ENV, 'someone-else')
    elif fault == 'stale':
        t.runtime.connected = False
    elif fault == 'realm':
        args['realm'] = 'other'
    elif fault == 'card':
        args['card_id'] = t.ctx.store.create_card(CardCreate(title='another')).id
    else:
        from pa.mcp.local_api import request_local_pa
        with pytest.raises(Exception, match=code):
            request_local_pa(t.settings, 'PATCH', f'/api/cards/{t.card.id}', json={'expected_version': args['expected_version'], 'completion_acceptance': {k: args[k] for k in ('requirement_revision', 'subject_revision', 'milestones', 'references')}}, headers={'Idempotency-Key': 'bearer-forgery', 'X-PA-Assigned-Session-ID': t.session.id, 'X-PA-Assigned-Dispatch-ID': t.record.dispatch_id})
        assert not t.ctx.store.get_card(t.card.id).completion_evidence
        return
    with pytest.raises(Exception, match=code):
        invoke(t, args)
    assert not t.ctx.store.get_card(t.card.id).completion_evidence
