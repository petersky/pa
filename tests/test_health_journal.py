"""Actual independent owner stores and authenticated ASGI producer/consumer tests."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.core.kernel import _IdentityHeadersMiddleware
from pa.health_journal.models import Assessment, Observation, Policy, digest
from pa.health_journal.service import HealthService
from pa.health_journal.store import Journal, JournalError
from pa.modules.health_journal import router, ui_router


def observation(**extra):
    return {'subsystem': 'sync', 'error_code': 'repeated_timeout', 'summary': 'Bounded read repeatedly timed out',
            'occurrence_key': 'operation-123', 'evidence': ['retry=3 duration_ms=300'], **extra}


class Network(httpx.AsyncBaseTransport):
    def __init__(self):
        self.apps, self.offline, self.drop_ack = {}, set(), False

    async def handle_async_request(self, request):
        host = request.url.host
        if host in self.offline:
            raise httpx.ConnectError('fixture offline', request=request)
        response = await httpx.ASGITransport(app=self.apps[host]).handle_async_request(request)
        if self.drop_ack and request.url.path.endswith('/gathered'):
            self.drop_ack = False
            await response.aread()
            raise httpx.ReadError('fixture lost acknowledgment', request=request)
        return response


@pytest_asyncio.fixture
async def fleet(tmp_path):
    network = Network()
    nodes = []
    for name in ('authority', 'source'):
        settings = Settings(data_dir=tmp_path/name, instance_id=str(uuid4()), auth_required=True,
                            sync_token='synthetic-shared-fleet-token', subscribed_realms=['default'], agent_enabled=False)
        settings.data_dir.mkdir()
        users = UserDirectory(settings.data_dir)
        user = users.ensure_default_user()
        settings.session_secret = 'synthetic-session-secret'
        sessions = SessionManager(settings.session_secret)
        ctx = AppContext(settings, HookBus(), store=SimpleNamespace())
        journal = Journal(settings.data_dir/'health.db', settings.instance_id)
        service = HealthService(ctx, journal, transport=network)
        ctx.register_service('health_journal', service)
        ctx.register_service('sync_recovery', SimpleNamespace(admission_view=lambda realm: (False, {})))
        app = FastAPI()
        app.state.ctx = ctx
        app.include_router(router, prefix='/api')
        app.include_router(ui_router)
        app.add_middleware(AuthMiddleware, settings=settings, users=users, sessions=sessions)
        app.add_middleware(_IdentityHeadersMiddleware, instance_id=settings.instance_id)
        service.app = app
        network.apps[name] = app
        nodes.append(SimpleNamespace(name=name, settings=settings, ctx=ctx, app=app, service=service, journal=journal,
                                     headers={'Authorization': f'Bearer {user.cli_token}'}, users=users))
    authority, source = nodes
    peers = {n.settings.instance_id: SimpleNamespace(instance_id=n.settings.instance_id, url=f'http://{n.name}', lifecycle_state='active') for n in nodes}
    for node in nodes:
        node.ctx.services['fleet_registry'] = SimpleNamespace(get_instance=peers.get, list_instances=lambda: list(peers.values()))
        node.journal.configure(Policy(authority_id=authority.settings.instance_id, enabled=True), expected_version=1, actor='user:local')
    yield authority, source, network
    for node in nodes:
        await node.service.stop()


async def api(node, method, path, *, body=None, key='report-1', headers=None):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=node.app), base_url=f'http://{node.name}') as client:
        return await client.request(method, '/api/health-journal'+path, json=body,
                                    headers=(node.headers | {'Idempotency-Key': key}) if headers is None else headers)


@pytest.mark.asyncio
async def test_report_without_canonical_store_and_replay(fleet):
    authority, source, _ = fleet
    # ctx.store has no methods: journal admission and reads cannot touch a DAG/index.
    response = await api(source, 'POST', '/reports', body=observation())
    assert response.status_code == 201, response.text
    first = response.json()
    assert (await api(source, 'POST', '/reports', body=observation())).json() == first
    conflict = await api(source, 'POST', '/reports', body=observation(summary='changed'))
    assert conflict.status_code == 409
    second = await api(source, 'POST', '/reports', body=observation(summary='changed'), key='revision-2')
    assert second.json()['report_id'] == first['report_id']
    assert second.json()['revision'] == 2
    await authority.service.cycle(manual=True)
    history = (await api(source, 'GET', '/reports/'+first['report_id'])).json()['history']
    assert len(history) == 2
    assert all(r['delivery'] == 'gathered' for r in history)
    assert all(r['custody']['disposition'] == 'new' for r in history)


@pytest.mark.asyncio
async def test_offline_return_lost_ack_and_restart(fleet):
    authority, source, network = fleet
    first = (await api(source, 'POST', '/reports', body=observation())).json()
    network.offline.add('source')
    await authority.service.cycle(manual=True)
    assert authority.journal.status()['sources'][source.settings.instance_id]['phase'] == 'unknown'
    assert source.journal.status()['pending_revisions'] == 1
    network.offline.clear()
    network.drop_ack = True
    await authority.service.cycle(manual=True)
    assert authority.journal.status()['inbox_revisions'] == 1
    # Reopen owner database, simulating restart after commit and lost network reply.
    authority.service.journal = Journal(authority.journal.path, authority.settings.instance_id)
    await authority.service.cycle(manual=True)
    assert authority.service.journal.status()['inbox_revisions'] == 1
    assert source.journal.status()['pending_revisions'] == 0
    history = (await api(source, 'GET', '/reports/'+first['report_id'])).json()['history']
    assert history[0]['custody']['receipt_id']


@pytest.mark.asyncio
async def test_new_revision_during_snapshot_paging(fleet):
    _, source, _ = fleet
    for i in range(3):
        await api(source, 'POST', '/reports', body=observation(occurrence_key=f'op-{i}'), key=f'key-{i}')
    page = source.journal.page(realms=['default'], limit=1)
    await api(source, 'POST', '/reports', body=observation(occurrence_key='op-new'), key='new')
    entries = page['items']
    while page['cursor']:
        page = source.journal.page(realms=['default'], limit=1, cursor=page['cursor'])
        entries += page['items']
    assert len(entries) == 3
    assert len(source.journal.page(realms=['default'])['items']) == 4


@pytest.mark.asyncio
async def test_auth_principal_realm_redaction_and_injection(fleet):
    authority, source, _ = fleet
    unauthorized = await api(source, 'POST', '/reports', body=observation(), headers={})
    assert unauthorized.status_code == 401
    assert (await api(source, 'POST', '/reports', body=observation(principal='user:other'))).status_code == 422
    assert (await api(source, 'POST', '/reports', body=observation(realm='private'))).status_code == 403
    assert (await api(source, 'POST', '/reports', body=observation(), headers=source.headers | {'Idempotency-Key':'x', 'X-PA-Health-Session-ID':'forged'})).status_code == 403
    secret = 'sk-this-is-a-secret-synthetic-value'
    body = observation(summary='<script>alert(1)</script>', evidence=[f'Authorization: Bearer {secret}', 'password=synthetic-private-answer'])
    response = await api(source, 'POST', '/reports', body=body)
    assert response.status_code == 201
    stored = source.journal.page(realms=['default'])['items'][0]
    assert secret not in json.dumps(stored)
    assert 'synthetic-private-answer' not in json.dumps(stored)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=source.app), base_url='http://source') as client:
        ui = await client.get('/health-journal', headers=source.headers)
        assert '&lt;script&gt;' in ui.text and '<script>' not in ui.text
    assert (await api(source, 'POST', '/gathered', body={'receipt_id': str(uuid4())})).status_code == 403
    assert (await api(source, 'GET', '/outbox')).status_code == 403
    assert (await api(source, 'POST', '/reports', body=observation(evidence=['x'*33000]))).status_code == 413


@pytest.mark.asyncio
async def test_fences_dual_configuration_expiry_and_dispositions(fleet):
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    lease = authority.journal.lease('first')
    with pytest.raises(JournalError, match='owned'):
        authority.journal.lease('second')
    with pytest.raises(JournalError, match='inactive'):
        source.journal.lease('partition-election')
    authority.journal.release('first', lease['fence'])
    successor = authority.journal.lease('second')
    assert successor['fence'] > lease['fence']
    entry = source.journal.page(realms=['default'])['items'][0]
    with pytest.raises(JournalError, match='stale_health_fence'):
        authority.journal.ingest(entry, source_id=source.settings.instance_id, realms=['default'], owner='first', fence=lease['fence'])
    receipt = authority.journal.ingest(entry, source_id=source.settings.instance_id, realms=['default'], owner='second', fence=successor['fence'])
    authority.journal.release('second', successor['fence'])
    await authority.service.cycle(manual=True)
    updated = await api(authority, 'PATCH', f'/groups/{receipt["group_id"]}', body={'expected_version':1, 'disposition':'no_fix', 'reason':'Evidence describes an expected test condition'})
    assert updated.status_code == 200, updated.text
    await authority.service.cycle(manual=True)
    report = source.journal.report(entry['payload']['report_id'], realms=['default'])
    assert report['history'][0]['custody']['disposition'] == 'no_fix'
    assert (await api(authority, 'PATCH', f'/groups/{receipt["group_id"]}', body={'expected_version':2, 'disposition':'deployed_verified', 'reason':'merged'})).status_code == 409
    assert len(authority.journal.group_history(receipt['group_id'])) == 2


@pytest.mark.asyncio
async def test_hash_conflict_backpressure_and_concurrent_append(fleet):
    authority, source, _ = fleet
    responses = await asyncio.gather(*[api(source, 'POST', '/reports', body=observation()) for _ in range(8)])
    assert all(r.status_code == 201 for r in responses)
    assert len({r.json()['report_id'] for r in responses}) == 1
    lease = authority.journal.lease('collector')
    entry = source.journal.page(realms=['default'])['items'][0]
    args = dict(source_id=source.settings.instance_id, realms=['default'], owner='collector', fence=lease['fence'])
    receipt = authority.journal.ingest(entry, **args)
    assert authority.journal.ingest(entry, **args) == receipt
    entry['payload']['observation']['summary'] = 'different bytes same identity'
    entry['hash'] = digest(entry['payload'])
    with pytest.raises(JournalError, match='revision_hash_conflict'):
        authority.journal.ingest(entry, **args)
    source.journal.max_bytes = 1
    refused = await api(source, 'POST', '/reports', body=observation(), key='capacity')
    assert refused.status_code == 507
    assert source.journal.status()['pending_revisions'] == 1


@pytest.mark.asyncio
async def test_explicit_transfer_requires_fencing_and_durable_handover(fleet):
    authority, source, network = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    lease = authority.journal.lease('active-cycle')
    rejected = await api(authority, 'POST', '/transfer', body={'action':'freeze', 'target_id':source.settings.instance_id, 'expected_version':2})
    assert rejected.status_code == 409
    authority.journal.release('active-cycle', lease['fence'])
    frozen = await api(authority, 'POST', '/transfer', body={'action':'freeze', 'target_id':source.settings.instance_id, 'expected_version':2})
    assert frozen.status_code == 200, frozen.text
    with pytest.raises(JournalError, match='inactive'):
        authority.journal.lease('stale-owner')
    with pytest.raises(JournalError, match='permanently_fenced'):
        authority.journal.configure(Policy(authority_id=authority.settings.instance_id, enabled=True), expected_version=3, actor='user:local')
    received = await api(source, 'POST', '/transfer', body={'action':'receive'})
    assert received.status_code == 200, received.text
    assert received.json()['paused'] is True
    assert source.journal.status()['inbox_revisions'] == 1
    followed = await api(authority, 'POST', '/transfer', body={'action':'follow'})
    assert followed.status_code == 200, followed.text
    assert followed.json()['authority_id'] == source.settings.instance_id
    updated_policy = Policy.model_validate(received.json()).model_copy(update={'paused':False})
    source.journal.configure(updated_policy, expected_version=received.json()['version'], actor='user:local')
    await source.service.cycle(manual=True)
    report = source.journal.page(realms=['default'])['items'][0]
    assert report['custody']['authority_id'] == source.settings.instance_id
    assert report['custody']['epoch'] == 2
    assert report['custody']['ingested_authority_id'] == authority.settings.instance_id


@pytest.mark.asyncio
async def test_restricted_session_reporting_derives_principal(fleet):
    from pa.acp.environment import assigned_service_session_capability
    _, source, _ = fleet
    session = SimpleNamespace(id=str(uuid4()), dispatch_id=str(uuid4()), principal_id='user:restricted',
                              card_id='card-bound', project_id='project-bound', realm_id='default')
    source.ctx.services['instance_agent'] = SimpleNamespace(get=lambda sid: SimpleNamespace(session=session, _closed=False) if sid == session.id else None)
    capability = assigned_service_session_capability(secret=source.settings.session_secret,
        dispatch_id=session.dispatch_id, session_id=session.id, target_instance_id=source.settings.instance_id)
    headers = {'Authorization':f'GoalSession {capability}', 'X-PA-Assigned-Session-ID':session.id,
               'X-PA-Assigned-Dispatch-ID':session.dispatch_id, 'Idempotency-Key':'restricted-key'}
    response = await api(source, 'POST', '/reports', body=observation(), headers=headers)
    assert response.status_code == 201, response.text
    entry = source.journal.page(realms=['default'])['items'][0]
    assert entry['payload']['principal'] == session.principal_id
    assert entry['payload']['context']['card_id'] == session.card_id
    forged = await api(source, 'POST', '/reports', body=observation(), headers=headers | {'X-PA-Assigned-Dispatch-ID':'forged'})
    assert forged.status_code == 403
    assert (await api(source, 'GET', '/reports', headers=headers)).status_code == 200
    assert (await api(source, 'PATCH', '/config', body={}, headers=headers)).status_code == 401


@pytest.mark.asyncio
async def test_ack_before_inbox_commit_is_never_accepted(fleet):
    authority, source, _ = fleet
    first = await api(source, 'POST', '/reports', body=observation())
    response = await api(source, 'POST', '/gathered', body={'receipt_id':str(uuid4())},
        headers={'Authorization':f'Bearer {source.settings.sync_token}'})
    # A configured-authority lookup has no receipt; no source bookkeeping changes.
    assert response.status_code >= 400
    assert source.journal.status()['pending_revisions'] == 1


@pytest.mark.asyncio
async def test_recurring_after_declared_acceptance_and_delayed_replay(fleet):
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    group = authority.journal.groups(['default'])[0]
    authority.journal.assess(group['id'], Assessment(expected_version=1, disposition='deployed_verified',
        reason='Synthetic lifecycle acceptance', card_id='card', acceptance_reference='acceptance-1', accepted_instances=[source.settings.instance_id]),
        actor='system:lifecycle', realms=['default'], accepted=True)
    # Exact late replay does not reopen accepted state.
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    assert authority.journal.groups(['default'])[0]['disposition'] == 'deployed_verified'
    await api(source, 'POST', '/reports', body=observation(recurrence_after_acceptance='acceptance-1'), key='recurrence')
    await authority.service.cycle(manual=True)
    assert authority.journal.groups(['default'])[0]['disposition'] == 'reopened'


@pytest.mark.asyncio
async def test_actual_mcp_stdio_http_report_roundtrip(fleet, monkeypatch):
    import os
    import socket
    import sys
    import uvicorn
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    authority, _, _ = fleet
    source = authority
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(source.app, log_level='error', lifespan='off'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(0.01)
        from pa.acp.mcp_config import pa_mcp_servers
        from pa.domain.models import AgentSession
        bound = AgentSession(agent_name='fixture', principal_id='user:local', realm_id='default')
        source.ctx.services['instance_agent'] = SimpleNamespace(
            get=lambda selector: SimpleNamespace(session=bound) if selector == bound.id else None)
        descriptor = pa_mcp_servers(source.settings, principal_id=bound.principal_id,
            owner_environment={'PA_OWNER_API_URL':f'http://127.0.0.1:{port}'})[0]
        env = {k:v for k,v in os.environ.items() if not k.startswith('PA_')}
        env.update({item.name:item.value for item in descriptor.env})
        env.update(PA_EXECUTION_CONTEXT=json.dumps({'session_id':bound.id}), PA_AGENT_ENABLED='false')
        async with asyncio.timeout(30):
            async with stdio_client(StdioServerParameters(command=sys.executable, args=['-m','pa','mcp'], env=env)) as streams:
                async with ClientSession(*streams) as session:
                    await session.initialize()
                    names = {t.name for t in (await session.list_tools()).tools}
                    assert {'report_pa_problem','list_pa_problems','get_pa_problem','update_pa_problem'} <= names
                    result = await session.call_tool('report_pa_problem', {'idempotency_key':'real-mcp', 'observation':observation()})
                    assert not result.is_error, result
                    result = await session.call_tool('list_pa_problems', {})
                    assert not result.is_error
                    assert source.journal.status()['pending_revisions'] == 1
                    await authority.service.cycle(manual=True)
                    group = authority.journal.groups(['default'])[0]
                    result = await session.call_tool('get_pa_problem_group', {'group_id':group['id']})
                    assert not result.is_error, result
                    current = json.loads(result.content[0].text)
                    assert current['version'] == group['version']
                    result = await session.call_tool('update_pa_problem', {'group_id':group['id'], 'assessment':{
                        'expected_version':current['version'], 'disposition':'no_fix', 'reason':'Synthetic harmless transport acceptance'}})
                    assert not result.is_error, result
                    assert authority.journal.groups(['default'])[0]['disposition'] == 'no_fix'
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 5)
        listener.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('recovered', [False, True])
async def test_ordinary_bound_connection_mcp_report(fleet, monkeypatch, recovered):
    import os
    import socket
    import uvicorn
    from unittest.mock import MagicMock
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from pa.acp.client import AgentConnection
    from pa.acp.providers.base import AgentProviderSpec
    from pa.acp import mcp_config
    from pa.domain.models import AgentSession

    _, source, _ = fleet
    source.settings.subscribed_realms.append('secondary')
    source.settings.agent_enabled = True
    owner = source.users.create_user('ordinary', 'synthetic-password')
    principal = 'user:' + owner.id
    source.ctx.services['membership'] = SimpleNamespace(
        has_role=lambda realm, actor, **kw: realm == 'secondary' and actor == principal)
    bound = AgentSession(agent_name='fixture', principal_id=principal, realm_id='secondary')
    foreign = AgentSession(agent_name='fixture', principal_id='user:local', realm_id='secondary')
    runtimes = {s.id: SimpleNamespace(session=s) for s in (bound, foreign)}
    source.ctx.services['instance_agent'] = SimpleNamespace(get=runtimes.get)
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    endpoint = f'http://127.0.0.1:{listener.getsockname()[1]}'
    server = uvicorn.Server(uvicorn.Config(source.app, log_level='error', lifespan='off'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    # The selector is ordinary provider context, not authentication. Preserve it
    # through the real MCP child; HTTP verifies it against the selected user.
    execution = json.dumps({'session_id': bound.id})
    monkeypatch.setenv('PA_EXECUTION_CONTEXT', execution)
    monkeypatch.setenv('PA_OWNER_API_URL', endpoint)
    monkeypatch.setenv('PA_LOCAL_API_TOKEN', 'synthetic-untrusted-provider-token')
    descriptors = []
    real_servers = mcp_config.pa_mcp_servers
    def descriptor(settings, **kwargs):
        assert kwargs['principal_id'] == principal
        result = real_servers(settings, **kwargs)
        descriptors.append(result[0])
        return result
    monkeypatch.setattr('pa.acp.client.pa_mcp_servers', descriptor)
    connection = AgentConnection(source.settings, MagicMock(), agent_name='fixture',
        provider_spec=AgentProviderSpec(id='fixture', display_name='Fixture', command='unused-fixture'))
    class AdmissionObserved(Exception):
        pass
    async def offload(label, fn, *args, **kwargs):
        if label == 'acp.pa_mcp_owner_probe':
            assert fn.keywords['principal_id'] == principal
            return {'state':'connected'}  # Readiness route is outside this isolated app.
        if label == 'acp.pa_mcp_stdio_probe':
            assert fn.keywords['principal_id'] == principal
            # Real same-owner bootstrap; no external provider is started.
            result = await asyncio.to_thread(fn, *args)
            assert result['state'] == 'connected'
            raise AdmissionObserved
        raise AssertionError(label)
    monkeypatch.setattr(connection, '_offload', offload)
    try:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(.01)
        with pytest.raises(AdmissionObserved):
            await connection.connect(existing_session=bound if recovered else None,
                principal_id=principal if not recovered else 'user:ignored-caller',
                resume_external_id='fixture-recovered' if recovered else None)
        descriptor_env = {item.name:item.value for item in descriptors[0].env}
        assert descriptor_env['PA_LOCAL_API_TOKEN'] == owner.cli_token
        env = {k:v for k,v in os.environ.items() if not k.startswith('PA_')}
        env.update(descriptor_env, PA_EXECUTION_CONTEXT=execution, PA_AGENT_ENABLED='false')
        async def exchange(selector, *, deny=False):
            params = StdioServerParameters(command=descriptors[0].command,
                args=list(descriptors[0].args), env=env | {'PA_EXECUTION_CONTEXT': json.dumps({'session_id':selector})})
            async with stdio_client(params) as streams:
                async with ClientSession(*streams) as client:
                    await client.initialize()
                    args = {'idempotency_key':'ordinary-report', 'observation':observation()}
                    result = await client.call_tool('report_pa_problem', args)
                    if deny:
                        assert result.is_error
                        return
                    assert not result.is_error, result
                    receipt = json.loads(result.content[0].text)
                    replay = await client.call_tool('report_pa_problem', args)
                    assert not replay.is_error and json.loads(replay.content[0].text) == receipt
                    listed = await client.call_tool('list_pa_problems', {})
                    assert not listed.is_error
                    assert len(json.loads(listed.content[0].text)['items']) == 1
                    read = await client.call_tool('get_pa_problem', {'report_id':receipt['report_id']})
                    assert not read.is_error
                    payload = json.loads(read.content[0].text)['history'][0]['payload']
                    assert payload['principal'] == principal
                    assert payload['realm'] == 'secondary'
                    assert payload['source_instance_id'] == source.settings.instance_id
                    assert payload['context']['session_id'] == bound.id
                    denied = await client.call_tool('report_pa_problem', {
                        'idempotency_key':'forbidden', 'observation':observation(realm='default')})
                    assert denied.is_error
        async with asyncio.timeout(60):
            await exchange(bound.id)
            await exchange(foreign.id, deny=True)
        assert source.journal.status()['pending_revisions'] == 1
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 5)
        listener.close()


@pytest.mark.parametrize('principal', [None, '', 'user:missing', 'system:provider', 'user:'])
def test_explicit_ordinary_mcp_owner_fails_closed(tmp_path, principal):
    from pa.acp.mcp_config import pa_mcp_servers, probe_pa_mcp_stdio, probe_owner_channel
    settings = Settings(data_dir=tmp_path)
    UserDirectory(tmp_path).ensure_default_user()
    for operation in (pa_mcp_servers, probe_pa_mcp_stdio, probe_owner_channel):
        with pytest.raises(ValueError, match='ordinary MCP session'):
            operation(settings, principal_id=principal)


def test_empty_ordinary_mcp_credential_fails_closed(tmp_path):
    from pa.acp.mcp_config import pa_mcp_servers, probe_pa_mcp_stdio, probe_owner_channel
    users = UserDirectory(tmp_path)
    users.ensure_default_user()
    user = users.create_user('empty', 'synthetic-password')
    user.cli_token = ''
    users._save()
    for operation in (pa_mcp_servers, probe_pa_mcp_stdio, probe_owner_channel):
        with pytest.raises(ValueError, match='credential is unavailable'):
            operation(Settings(data_dir=tmp_path), principal_id='user:'+user.id)
    assert pa_mcp_servers(Settings(data_dir=tmp_path))  # Separate standalone bootstrap.


@pytest.mark.parametrize('decision', ['repair', 'no_fix', 'integration_only', 'stale_requirement'])
def test_real_card_and_dispatch_admission_recovers_lost_reply(tmp_path, monkeypatch, decision):
    pytest.importorskip('pa.domain.completion', reason='Actual repair producer requires companion lifecycle declaration contract')
    from unittest.mock import AsyncMock
    from fastapi.testclient import TestClient
    from pa.config import reset_settings
    from pa.core.kernel import Kernel
    from pa.domain.models import ProjectCreate, RepositoryCreate
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent
    from tests.test_fleet_placement import _candidate
    reset_settings(); reset_store(); reset_instance_agent()
    from pa.instance.session_lifecycle import SessionLifecyclePolicy
    monkeypatch.setattr(SessionLifecyclePolicy, 'start', lambda self: None)
    settings = Settings(data_dir=tmp_path/'owner', workspace_root=tmp_path/'workspaces',
                        instance_id=str(uuid4()), agent_enabled=False,
                        auth_required=True, subscribed_realms=['default'], peers=[], sync_token='isolated-health-peer')
    from pa.domain.instance_config import update_instance_config
    update_instance_config(settings.data_dir, session_secret=settings.session_secret, instance_id=settings.instance_id)
    kernel = Kernel.boot(settings=settings)
    app = kernel.build_app()
    try:
        with TestClient(app) as client:
            ctx = app.state.ctx
            # Keep normal durable admission; this component test supplies the provider runtime.
            client.portal.call(ctx.require_service('dispatch_worker').close)
            monkeypatch.setattr(ctx.require_service('dispatch_worker'), 'start', lambda: None)
            project = ctx.store.create_project(ProjectCreate(title='Synthetic health project'))
            repository = ctx.store.create_repository(RepositoryCreate(url='https://example.test/owner/pa.git'))
            ctx.store.link_project_repository(project.id, repository.id)
            candidate = _candidate(settings.instance_id, local=True, repositories=[repository.id])
            candidate.capabilities.append('completion-requirements:v1')
            monkeypatch.setattr('pa.modules.fleet._placement_candidates', AsyncMock(return_value=[candidate]))
            service = ctx.require_service('health_journal')
            supervisor = ctx.require_service('pr_supervisor')
            if hasattr(supervisor, 'eligibility_journal_hook'):
                assert supervisor.eligibility_journal_hook.__self__ is service
            journal = service.journal
            journal.configure(Policy(authority_id=settings.instance_id, enabled=True,
                project_id=project.id, principal_id='user:local'), expected_version=1, actor='user:local')
            journal.append(Observation(**observation()), principal='user:local', realm='default', key='seed')
            original = service.local_api
            lost = False
            async def lose_dispatch_reply(action, method, path, **kwargs):
                nonlocal lost
                result = await original(action, method, path, **kwargs)
                if path == '/api/fleet/dispatch' and not lost:
                    lost = True
                    raise httpx.ReadError('Synthetic interruption after actual durable admission')
                return result
            monkeypatch.setattr(service, 'local_api', lose_dispatch_reply)
            client.portal.call(service.cycle)
            assert lost, journal.status()
            first = ctx.require_service('dispatch_store').by_authority_idempotency(
                settings.instance_id, 'health-dispatch:'+journal.status()['active_actions'][0]['id'])
            assert first is not None
            # Reopen journal; the next cycle repeats the exact canonical API key.
            service.journal = Journal(journal.path, settings.instance_id)
            client.portal.call(service.cycle)
            action = service.journal.status()['active_actions'][0]
            assert json.loads(action['result'])['dispatch_id'] == first.dispatch_id
            cards = [c for c in ctx.store.list_cards() if c.project_id == project.id]
            assert len(cards) == 1
            assert cards[0].completion_requirement is None
            assert service.journal.status()['inbox_revisions'] == 1
            from pa.domain.models import CardUpdate, CompletionEvidence
            from pa.health_journal.acceptance import verify_acceptance
            card = cards[0]
            group = service.journal.groups(['default'])[0]
            commit = 'c'*40
            with pytest.raises(JournalError, match='awaiting_acceptance'):
                verify_acceptance(card, group, Assessment(expected_version=group['version'],
                    disposition='deployed_verified', reason='No repair has been declared'))
            user = UserDirectory(settings.data_dir).get('local')
            if decision == 'no_fix':
                from pa.domain.models import AgentSession
                from pa.execution.dispatch import DispatchStore, CompletionOutbox
                session = ctx.store.save_session(AgentSession(id='no-fix-worker', agent_name='codex',
                    card_id=card.id, dispatch_id=first.dispatch_id, authority_instance_id=settings.instance_id,
                    status='idle'))
                assert ctx.require_service('instance_agent').get(session.id) is None
                ledger = ctx.require_service('dispatch_store')
                record = ledger.put(first.model_copy(update={'session_id':session.id, 'state':'running'}))
                nofix = client.patch('/api/health-journal/groups/'+group['id'],
                    headers={'Authorization':f'Bearer {user.cli_token}'},
                    json={'expected_version':group['version'], 'disposition':'no_fix',
                          'reason':'Evidence describes expected behavior; no repository work needed'})
                assert nofix.status_code == 200, nofix.text
                target = DispatchStore(tmp_path/'no-fix-target')
                try:
                    target.put(record)
                    assert target.queue_completion_payload(session.id, {'summary':'No fix required',
                        'card_disposition':{'contract':'pa.card-disposition/v1', 'lane':'done',
                            'outcome':'Reasoned no-fix investigation complete',
                            'evidence':{'integration_required':False}}})
                    outbox = CompletionOutbox(target, settings.sync_token)
                    async def forward(url, **kwargs):
                        return client.post(url, **kwargs)
                    monkeypatch.setattr(outbox, '_http_client', lambda:SimpleNamespace(post=forward))
                    asyncio.run(outbox._send(target.get(record.dispatch_id)))
                    assert target.get(record.dispatch_id).completion_delivery_class == 'acknowledged'
                    assert ctx.store.get_card(card.id).lane.value == 'done'
                    assert ctx.store.get_card(card.id).completion_requirement is None
                    actual = ledger.get(record.dispatch_id)
                    assert actual.acknowledged_at is not None
                    assert actual.reconciliation_state in {'applied', 'not_applicable', 'operator_state_preserved'}
                    assert ctx.store.get_session(session.id).status == 'idle'
                    lifecycle = ctx.require_service('session_lifecycle')
                    client.portal.call(lifecycle.run_once)
                    assert ctx.store.get_session(session.id).status == 'closed'
                    assert lifecycle.diagnostics[session.id]['close_reason'] == 'dispatch_completed'
                    client.portal.call(service.cycle)
                    assert not service.journal.status()['active_actions']
                    assert service.journal.groups(['default'])[0]['disposition'] == 'no_fix'
                    service.journal.append(Observation(**observation(error_code='separate_bounded_issue',
                        occurrence_key='next-investigation')), principal='user:local', realm='default', key='next-investigation')
                    client.portal.call(service.cycle)
                    next_action = service.journal.status()['active_actions'][0]
                    assert next_action['id'] != action['id']
                    assert json.loads(next_action['result'])['dispatch_id'] != first.dispatch_id
                finally:
                    target.close()
                return
            if decision in {'integration_only', 'stale_requirement'}:
                headers = {'Authorization':f'Bearer {user.cli_token}'}
                if decision == 'integration_only':
                    declaration = client.patch('/api/cards/'+card.id, params={'realm':'default'},
                        headers=headers | {'Idempotency-Key':'existing-integration-only'}, json={
                            'expected_version':card.updated_at.isoformat(), 'field_intent':['completion_requirement'],
                            'completion_requirement':{'mode':'integration_only', 'milestones':[],
                                'criteria':'Keep source integration', 'acceptance_principals':['user:local']}})
                    assert declaration.status_code == 200, declaration.text
                else:
                    original_api = service.local_api
                    attempts, stale_statuses = [], []
                    async def stale_patch(action_payload, method, path, **kwargs):
                        if method == 'PATCH' and '/api/cards/' in path:
                            attempts.append((kwargs['key'], kwargs['body']))
                            await original_api(action_payload, method, path, key='concurrent-card-change',
                                body={'expected_version':kwargs['body']['expected_version'],
                                      'title':'Deliberate concurrent canonical update'})
                        try:
                            return await original_api(action_payload, method, path, **kwargs)
                        except httpx.HTTPStatusError as exc:
                            stale_statuses.append(exc.response.status_code)
                            raise
                    monkeypatch.setattr(service, 'local_api', stale_patch)
                transition = {'expected_version':group['version'], 'disposition':'reproduced',
                              'reason':'Bounded canonical protection', 'card_id':card.id}
                response = client.patch('/api/health-journal/groups/'+group['id'], headers=headers, json=transition)
                if decision == 'stale_requirement':
                    assert response.status_code >= 400, response.text
                    assert len(attempts) == 1 and stale_statuses == [409]
                    saved = json.loads(service.journal.status()['active_actions'][0]['result'])['effects']['requirement']
                    assert saved['payload']['body'] == attempts[0][1]
                    assert service.journal.groups(['default'])[0]['version'] == group['version']
                    current = ctx.store.get_card(card.id)
                    assert current.completion_requirement is None
                    correction = client.patch('/api/cards/'+card.id, params={'realm':'default'},
                        headers=headers | {'Idempotency-Key':'deliberate-canonical-correction'}, json={
                            **saved['payload']['body'], 'expected_version':current.updated_at.isoformat()})
                    assert correction.status_code == 200, correction.text
                    corrected = ctx.store.get_card(card.id).completion_requirement.model_dump(mode='json')
                    response = client.patch('/api/health-journal/groups/'+group['id'], headers=headers, json=transition)
                    assert len(attempts) == 1  # Never resend the historical stale PATCH.
                    same_action = service.journal.status()['active_actions'][0]
                    assert same_action['id'] == action['id']
                    assert json.loads(same_action['result'])['effects']['requirement'] == saved
                    assert ctx.store.get_card(card.id).completion_requirement.model_dump(mode='json') == corrected
                assert response.status_code == 200, response.text
                if decision == 'integration_only':
                    from pa.domain.completion import completion_state
                    requirement = ctx.store.get_card(card.id).completion_requirement
                    assert requirement.acceptance_principals == ['user:local']
                    assert requirement.criteria.startswith('Keep source integration')
                    assert set(requirement.milestones) == {'integrated', 'verified'}
                    evidence = CompletionEvidence(requirement_revision=requirement.revision,
                        subject_revision=commit, milestones=['verified'], outcome='accepted',
                        actor='user:local', recorded_at=card.updated_at.isoformat())
                    status = completion_state(requirement, [evidence])
                    assert not status['accepted'] and 'integrated' in status['missing']
                return
            # Every repair-entry disposition must protect the card before success.
            original_api = service.local_api
            lost_requirement = False
            patches = []
            async def lose_requirement_reply(action, method, path, **kwargs):
                nonlocal lost_requirement
                response = await original_api(action, method, path, **kwargs)
                if method == 'PATCH' and '/api/cards/' in path:
                    patches.append((kwargs['key'], kwargs['body']))
                    if not lost_requirement:
                        lost_requirement = True
                        raise httpx.ReadError('Synthetic lost canonical declaration reply')
                return response
            monkeypatch.setattr(service, 'local_api', lose_requirement_reply)
            assessment_body = {'expected_version':group['version'], 'disposition':'linked',
                'reason':'Source fix ready; deployment owner is still undeclared',
                'card_id':card.id, 'commit':commit, 'pr_url':'https://github.com/example/pa/pull/1'}
            failed_transition = client.patch('/api/health-journal/groups/'+group['id'],
                headers={'Authorization':f'Bearer {user.cli_token}'}, json=assessment_body)
            assert lost_requirement, failed_transition.text
            assert failed_transition.status_code >= 400
            assert service.journal.groups(['default'])[0]['version'] == group['version']
            protected_revision = ctx.store.get_card(card.id).completion_requirement.revision
            for disposition in ('reproduced', 'linked', 'in_progress', 'merged'):
                group = service.journal.groups(['default'])[0]
                linked = client.patch('/api/health-journal/groups/'+group['id'],
                    headers={'Authorization':f'Bearer {user.cli_token}'},
                    json=assessment_body | {'expected_version':group['version'], 'disposition':disposition})
                assert linked.status_code == 200, linked.text
                assert ctx.store.get_card(card.id).completion_requirement.revision == protected_revision
            assert all(patch == patches[0] for patch in patches)
            group = service.journal.groups(['default'])[0]
            card = ctx.store.get_card(card.id)
            assert card.completion_requirement.mode == 'explicit_acceptance'
            assert 'verified' in card.completion_requirement.milestones
            assert group['data']['pr_url'].endswith('/pull/1')
            assert not card.completion_status['accepted']
            with pytest.raises(JournalError, match='acceptance_owner_unconfigured'):
                verify_acceptance(card, group, Assessment(expected_version=group['version'],
                    disposition='deployed_verified', reason='Owner must be explicitly declared'))
            # The isolated provider stub binds the actual admitted repair dispatch.
            # The coordinator reads this durable evidence, never guesses an origin.
            from pa.domain.models import AgentSession
            from pa.execution.dispatch import DispatchRecord
            from pa.modules.fleet import _assigned_mcp_environment_for_session
            from pa.mcp import server as mcp_server, local_api
            from pa.acp.environment import COMPLETION_SESSION_ENV, COMPLETION_DISPATCH_ENV
            from pa.acp.mcp_config import pa_mcp_servers
            ledger = ctx.require_service('dispatch_store')
            repair_session = ctx.store.save_session(AgentSession(id='health-repair-session',
                agent_name='codex', dispatch_id=first.dispatch_id,
                authority_instance_id=settings.instance_id, status='active'))
            ledger.put(first.model_copy(update={'session_id':repair_session.id, 'state':'running'}))
            origin_ids = service.journal.repair_dispatches(group['id'])
            assert origin_ids == [first.dispatch_id]
            origin = ledger.get(origin_ids[0])
            assert origin.goal_provenance is None
            assert not card.completion_requirement.originating_session_id
            assert not card.completion_requirement.originating_dispatch_id
            user = UserDirectory(settings.data_dir).get('local')
            declaration = client.patch('/api/cards/'+card.id, params={'realm':'default'},
                headers={'Authorization':f'Bearer {user.cli_token}', 'Idempotency-Key':'synthetic-owner-declaration'},
                json={'expected_version':card.updated_at.isoformat(), 'field_intent':['completion_requirement'],
                    'completion_requirement':card.completion_requirement.model_copy(update={
                        'acceptance_principals':['user:local'],
                        'originating_session_id':origin.session_id,
                        'originating_dispatch_id':origin.dispatch_id}).model_dump(mode='json')})
            assert declaration.status_code == 200, declaration.text
            card = ctx.store.get_card(card.id)
            declared = card.completion_requirement.model_dump(mode='json')
            current_group = service.journal.groups(['default'])[0]
            still_protected = client.patch('/api/health-journal/groups/'+current_group['id'],
                headers={'Authorization':f'Bearer {user.cli_token}'}, json={
                    'expected_version':current_group['version'], 'disposition':'linked',
                    'reason':'Reuse protected repair with declared owner', 'card_id':card.id})
            assert still_protected.status_code == 200, still_protected.text
            assert ctx.store.get_card(card.id).completion_requirement.model_dump(mode='json') == declared
            current_group = service.journal.groups(['default'])[0]
            nofix = client.patch('/api/health-journal/groups/'+current_group['id'],
                headers={'Authorization':f'Bearer {user.cli_token}'}, json={
                    'expected_version':current_group['version'], 'disposition':'no_fix',
                    'reason':'No additional source change; prior protected repair still needs acceptance'})
            assert nofix.status_code == 200
            from pa.domain.completion import completion_state
            preserved = ctx.store.get_card(card.id)
            assert preserved.completion_requirement.model_dump(mode='json') == declared
            assert not completion_state(preserved.completion_requirement, preserved.completion_evidence)['accepted']
            group = service.journal.groups(['default'])[0]
            evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
                subject_revision=commit, milestones=['verified'], references=[f'health-group:{group["id"]}',
                    'scenario:synthetic-provider-smoke', f'instance:{settings.instance_id}'])
            verifier = ctx.store.save_session(AgentSession(id='health-independent-verifier',
                agent_name='codex', dispatch_id='health-verifier-dispatch',
                authority_instance_id=settings.instance_id, status='active'))
            ledger.put(DispatchRecord(mutation_id=verifier.dispatch_id, dispatch_id=verifier.dispatch_id,
                session_id=verifier.id, authority_instance_id=settings.instance_id,
                authority_url='http://testserver', target_instance_id=settings.instance_id,
                principal_id='user:local', card_id=card.id, realm_id='default', state='running'))
            manager = ctx.require_service('instance_agent')
            monkeypatch.setattr(manager, 'get', lambda key: SimpleNamespace(_closed=False, connected=True)
                if key in (repair_session.id, verifier.id) else None)
            # Exercise the registered ordinary producer and its private HTTP bridge.
            for name in ('PA_ASSIGNED_SERVICE_MODE', 'PA_ASSIGNED_SERVICE_SESSION_ID',
                         'PA_ASSIGNED_SERVICE_DISPATCH_ID', 'PA_LOCAL_API_SOCKET'):
                monkeypatch.delenv(name, raising=False)
            monkeypatch.setenv('PA_DATA_DIR', str(settings.data_dir))
            monkeypatch.setenv('PA_LOCAL_API_URL', 'http://testserver')
            monkeypatch.setattr(mcp_server, 'mcp', None)
            sent = []
            def bridge(method, url, **kwargs):
                sent.append(kwargs['headers'].copy())
                kwargs.pop('timeout', None)
                return client.request(method, url, **kwargs)
            monkeypatch.setattr(local_api.httpx, 'request', bridge)
            mcp = mcp_server._get_mcp()
            args = dict(card_id=card.id, realm='default', expected_version=card.updated_at.isoformat(),
                requirement_revision=card.completion_requirement.revision, subject_revision=commit,
                milestones=evidence.milestones, references=evidence.references,
                idempotency_key='synthetic-current-acceptance')
            for session in (repair_session, verifier):
                binding = _assigned_mcp_environment_for_session(settings, ledger, session)
                assert binding[COMPLETION_SESSION_ENV] == session.id
                assert binding[COMPLETION_DISPATCH_ENV] == session.dispatch_id
                descriptor = pa_mcp_servers(settings, private_environment=binding)[0]
                for value in descriptor.env:
                    monkeypatch.setenv(value.name, value.value)
                monkeypatch.delenv('PA_LOCAL_API_SOCKET', raising=False)
                monkeypatch.setenv('PA_LOCAL_API_URL', 'http://testserver')
                if session == repair_session:
                    with pytest.raises(Exception, match='completion_self_acceptance_forbidden'):
                        client.portal.call(mcp.call_tool, 'record_card_acceptance', args | {'idempotency_key':'repair-rejected'})
                else:
                    client.portal.call(mcp.call_tool, 'record_card_acceptance', args)
            assert sent[-1]['Authorization'].startswith('SessionAcceptance ')
            accepted = ctx.store.get_card(card.id)
            receipt = accepted.completion_evidence[-1]
            assert receipt.actor_kind == 'bound_session'
            assert receipt.actor_session_id == verifier.id and receipt.actor_dispatch_id == verifier.dispatch_id
            assessment = Assessment(expected_version=group['version'], disposition='deployed_verified',
                reason='Synthetic isolated acceptance', card_id=card.id, commit=commit,
                accepted_subject_revision=commit, acceptance_reference='synthetic-current-acceptance',
                acceptance_scenario='synthetic-provider-smoke', accepted_instances=[settings.instance_id])
            verify_acceptance(accepted, group, assessment)
            user = UserDirectory(settings.data_dir).get('local')
            verified = client.patch('/api/health-journal/groups/'+group['id'],
                json=assessment.model_dump(mode='json'), headers={'Authorization':f'Bearer {user.cli_token}'})
            assert verified.status_code == 200, verified.text
    finally:
        reset_instance_agent(); reset_store(); reset_settings()


@pytest.mark.parametrize('provider', ['codex', 'cursor', 'openinterpreter', 'claude', 'future-provider'])
def test_provider_neutral_prompt_admission_matrix(tmp_path, provider):
    from unittest.mock import MagicMock
    from pa.agent.context import compose_session_prompt
    from pa.domain.models import AgentSession, Project
    from pa.prompts import PROMPTS
    store = MagicMock()
    project = Project(id='synthetic-project', title='Synthetic')
    store.get_project.return_value = project
    store.get_card.return_value = None
    session = AgentSession(agent_name=provider, project_id=project.id, principal_id='user:local')
    settings = Settings(data_dir=tmp_path, instance_id='synthetic')
    message = 'Continue the authorized task.'
    composed = compose_session_prompt(store, settings, session, message)
    instruction = PROMPTS.render('health.problem_reporting', provider=provider)
    assert composed.text.count(instruction.text) == 1
    audit = [p for p in composed.audit_records() if p['key'] == 'health.problem_reporting']
    assert len(audit) == 1 and audit[0]['provider'] == provider and audit[0]['version'] == 1
    for phrase in ('report_pa_problem', 'occurrence_key', 'correlation/operation IDs', 'private user answers', 'do not recursively'):
        assert phrase in composed.text


@pytest.mark.asyncio
async def test_reporting_failure_does_not_block_primary_startup(tmp_path, monkeypatch):
    import sqlite3
    from pa.modules.health_journal import HealthJournalModule
    ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=tmp_path, instance_id='synthetic'), services={})
    ctx.register_service = ctx.services.__setitem__
    def full(*args, **kwargs):
        raise sqlite3.OperationalError('synthetic full disk')
    monkeypatch.setattr('pa.modules.health_journal.Journal', full)
    await HealthJournalModule().on_startup(None, ctx)
    assert ctx.services == {'health_journal_error':'journal_storage_unavailable'}


@pytest.mark.asyncio
async def test_append_replay_survives_changed_runtime_identity(fleet):
    _, source, _ = fleet
    first = (await api(source, 'POST', '/reports', body=observation())).json()
    source.service.runtime_build = {'loaded_module_version':'synthetic-next', 'pid':9999, 'startup_id':str(uuid4())}
    replay = await api(source, 'POST', '/reports', body=observation())
    assert replay.status_code == 201 and replay.json() == first


@pytest.mark.asyncio
async def test_source_revision_races_ack_exact_old_revision(fleet):
    authority, source, _ = fleet
    first = (await api(source, 'POST', '/reports', body=observation())).json()
    old = source.journal.page(realms=['default'])['items'][0]
    lease = authority.journal.lease('collector')
    receipt = authority.journal.ingest(old, source_id=source.settings.instance_id, realms=['default'], owner='collector', fence=lease['fence'])
    second = (await api(source, 'POST', '/reports', body=observation(summary='additional evidence'), key='new-observation')).json()
    assert second['revision'] == 2
    await source.service.verify_gathered(receipt['receipt_id'])
    assert source.journal.status()['pending_revisions'] == 1
    assert source.journal.report(first['report_id'], realms=['default'])['history'][0]['delivery'] == 'pending'


@pytest.mark.asyncio
async def test_journal_reserved_capacity_ignores_canonical_worker_saturation(fleet):
    import threading
    from pa.core.async_runtime import AsyncRuntime
    _, source, _ = fleet
    runtime = AsyncRuntime(max_workers=1, max_queue=1)
    gate = threading.Event()
    blocked = asyncio.create_task(runtime.run_blocking('synthetic.blocked', gate.wait, timeout=3))
    try:
        async with asyncio.timeout(1):
            response = await api(source, 'POST', '/reports', body=observation())
            assert response.status_code == 201
            assert (await api(source, 'GET', '/status')).status_code == 200
    finally:
        gate.set()
        await blocked
        await runtime.close()


@pytest.mark.asyncio
async def test_cookie_reporting_keeps_csrf_and_hides_invalid_values(fleet):
    _, source, _ = fleet
    from pa.auth.sessions import SessionManager
    user = source.users.get('local')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=source.app), base_url='http://source') as client:
        client.cookies.set('pa_session', SessionManager(source.settings.session_secret).create_token(user))
        assert (await client.get('/api/health-journal/status')).status_code == 200
        denied = await client.post('/api/health-journal/reports', json=observation(), headers={'Idempotency-Key':'cookie-report'})
        assert denied.status_code == 403
        accepted = await client.post('/api/health-journal/reports', json=observation(),
            headers={'Idempotency-Key':'cookie-report', 'X-CSRF-Token':client.cookies['pa_csrf']})
        assert accepted.status_code == 201
    invalid = await api(source, 'POST', '/reports', body=observation(private_content='synthetic-private-answer'))
    assert invalid.status_code == 422 and 'synthetic-private-answer' not in invalid.text


@pytest.mark.asyncio
async def test_scheduler_recovers_after_transient_owner_read(fleet, monkeypatch):
    import sqlite3
    _, source, _ = fleet
    original = source.journal.status
    reads, sleeps = 0, 0
    observed = []
    def transient():
        nonlocal reads
        reads += 1
        if reads == 1:
            raise sqlite3.OperationalError('synthetic busy')
        return original()
    async def tick(seconds):
        nonlocal sleeps
        sleeps += 1
        observed.append(dict(source.service.scheduler_status))
        if sleeps == 2:
            raise asyncio.CancelledError
    monkeypatch.setattr(source.journal, 'status', transient)
    monkeypatch.setattr('pa.health_journal.service.asyncio.sleep', tick)
    with pytest.raises(asyncio.CancelledError):
        await source.service.run()
    assert reads == 2 and observed[0]['phase'] == 'backoff' and observed[1]['phase'] == 'idle'
    assert observed[0]['diagnostic_persistence'] == 'unknown'


@pytest.mark.asyncio
async def test_effect_admission_is_fenced_and_durable_across_pause(fleet):
    authority, source, _ = fleet
    policy = authority.journal.status()['policy']
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={'project_id':'project', 'principal_id':'user:local'}), expected_version=2, actor='user:local')
    await api(source, 'POST', '/reports', body=observation())
    lease = authority.journal.lease('coordinator')
    authority.journal.ingest(source.journal.page(realms=['default'])['items'][0], source_id=source.settings.instance_id,
                             realms=['default'], owner='coordinator', fence=lease['fence'])
    action = authority.journal.action(owner='coordinator', fence=lease['fence'])
    with pytest.raises(JournalError, match='repair_requirement_not_safely_replayable'):
        authority.journal.reserve_effect(action['id'], 'requirement',
            {'criteria':'Authorization: Bearer synthetic-private-value'},
            owner='coordinator', fence=lease['fence'])
    assert 'requirement' not in json.loads(authority.journal.status()['active_actions'][0]['result'] or '{}').get('effects', {})
    admitted = authority.journal.reserve_effect(action['id'], 'card', {'exact':'payload'}, owner='coordinator', fence=lease['fence'])
    policy = authority.journal.status()['policy']
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={'paused':True}), expected_version=policy['version'], actor='user:local')
    # Already admitted work remains a truthful in-flight obligation after pause.
    assert authority.journal.reserve_effect(action['id'], 'card', {'exact':'payload'}, owner='coordinator', fence=lease['fence']) == admitted
    with pytest.raises(JournalError, match='stale_health_fence'):
        authority.journal.reserve_effect(action['id'], 'dispatch', {'new':'effect'}, owner='coordinator', fence=lease['fence'])
    assert authority.journal.status()['active_actions'][0]['id'] == action['id']


@pytest.mark.asyncio
async def test_pending_revision_after_accepted_boundary_is_triaged_without_false_reopen(fleet):
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    policy = authority.journal.status()['policy']
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={'project_id':'project', 'principal_id':'user:local'}), expected_version=2, actor='user:local')
    lease = authority.journal.lease('coordinator')
    first = authority.journal.action(owner='coordinator', fence=lease['fence'])
    group = authority.journal.group(first['group_id'], realms=['default'])
    authority.journal.assess(group['id'], Assessment(expected_version=group['version'], disposition='deployed_verified', reason='Synthetic verified boundary', card_id='canonical-repair-card', accepted_instances=[source.settings.instance_id]), actor='system:lifecycle', realms=['default'], accepted=True)
    authority.journal.action_result(first['id'], state='terminal', result={}, owner='coordinator', fence=lease['fence'])
    await api(source, 'POST', '/reports', body=observation(summary='Delayed evidence requiring bounded triage'), key='delayed')
    entry = source.journal.page(realms=['default'])['items'][-1]
    before = authority.journal.group(first['group_id'], realms=['default'])['version']
    authority.journal.ingest(entry, source_id=source.settings.instance_id, realms=['default'], owner='coordinator', fence=lease['fence'])
    assert authority.journal.group(first['group_id'], realms=['default'])['version'] == before + 1
    second = authority.journal.action(owner='coordinator', fence=lease['fence'])
    assert second['id'] != first['id'] and second['group_id'] == first['group_id']
    assert second['result'] == {'card_id':'canonical-repair-card'}
    assert authority.journal.group(first['group_id'], realms=['default'])['disposition'] == 'awaiting_acceptance'
    assert authority.journal.action(owner='coordinator', fence=lease['fence'])['id'] == second['id']


@pytest.mark.asyncio
async def test_normal_lifecycle_obligations_keep_single_health_slot(fleet):
    from datetime import UTC, datetime
    from pa.execution.dispatch import DispatchStore, DispatchRecord
    from pa.instance.session_lifecycle import SessionLifecyclePolicy
    from pa.domain.models import AgentSession
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    policy = authority.journal.status()['policy']
    authority.journal.configure(Policy.model_validate(policy).model_copy(update={'project_id':'project', 'principal_id':'user:local'}), expected_version=2, actor='user:local')
    lease = authority.journal.lease(authority.service.owner)
    action = authority.journal.action(owner=authority.service.owner, fence=lease['fence'])
    ledger = DispatchStore(authority.settings.data_dir / 'dispatch-fixture')
    record = DispatchRecord(mutation_id=str(uuid4()), card_id='card', authority_instance_id=authority.settings.instance_id, authority_url='http://authority',
        target_instance_id=authority.settings.instance_id, state='failed', recoverable=True, reconciliation_state='not_required')
    ledger.put(record)
    authority.journal.action_result(action['id'], state='dispatched', result={'card_id':'card','dispatch_id':record.dispatch_id}, owner=authority.service.owner, fence=lease['fence'])
    action = authority.journal.action(owner=authority.service.owner, fence=lease['fence'])
    watches = []
    manager = SimpleNamespace(get=lambda sid:None, workspace_manager=SimpleNamespace(list=lambda **kw:[]), settings=authority.settings)
    lifecycle = SessionLifecyclePolicy(manager, {})
    authority.ctx.services.update(dispatch_store=ledger, session_lifecycle=lifecycle,
        pr_supervisor_store=SimpleNamespace(list_watches_for_cards=lambda *a, **kw:watches), instance_agent=manager)
    try:
        from datetime import timedelta
        record.created_at = datetime.now(UTC) - timedelta(days=3)
        for active_state in ('queued', 'running', 'waiting'):
            record.state = active_state
            ledger.put(record)
            calls = []
            async def get_active(payload, method, path, **kwargs):
                calls.append((method, path))
                return {'dispatch':record.model_dump(mode='json')}
            authority.service.local_api = get_active
            await authority.service.dispatch_one(lease)
            assert len(calls) == 1 and calls[0][0] == 'GET'
            assert authority.journal.action(owner=authority.service.owner, fence=lease['fence'])['id'] == action['id']
        record.state = 'failed'
        ledger.put(record)
        assert await authority.service.repair_obligation(action) == 'dispatch_recoverable'
        assert authority.journal.action(owner=authority.service.owner, fence=lease['fence'])['id'] == action['id']
        record.state = 'completed'
        record.recoverable = False
        record.acknowledged_at = datetime.now(UTC)
        record.completion_delivery_class = 'acknowledged'
        ledger.put(record)
        # Completed delivery still holds on an actionable PR watch.
        watches.append(SimpleNamespace())
        assert await authority.service.repair_obligation(action) == 'actionable_pr_watch'
        watches.clear()
        assert await authority.service.repair_obligation(action) is None
    finally:
        ledger.close()
        await lifecycle.close()


@pytest.mark.asyncio
async def test_journal_uses_current_canonical_acceptance_in_bound_realm(fleet, tmp_path, monkeypatch):
    pytest.importorskip('pa.domain.completion', reason='Companion lifecycle component required; also exercised in recorded combined integration overlay')
    from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence
    from pa.domain.projection import CardProjection
    from pa.sync.event_log import EventLog
    from pa.sync.object_store import ObjectStore
    authority, source, _ = fleet
    authority.settings.subscribed_realms.append('secondary')
    source.settings.subscribed_realms.append('secondary')
    await api(source, 'POST', '/reports', body=observation(realm='secondary'))
    await api(authority, 'POST', '/reports', body=observation(realm='secondary'), key='second-source')
    await authority.service.cycle(manual=True)
    group = authority.journal.groups(['secondary'])[0]
    root = tmp_path/'canonical'
    objects = ObjectStore(root/'objects')
    log = EventLog(objects, root, authority.settings.instance_id)
    projection = CardProjection(root/'cards.db', event_log=log)
    authority.ctx.store = projection
    card = projection.create_card(CardCreate(title='Scoped repair', realm_id='secondary', completion_requirement={
        'mode':'explicit_acceptance', 'criteria':'Verify journal scenario on affected instance',
        'milestones':['verified'], 'acceptance_principals':['user:local'],
        'originating_session_id':'repair-session', 'originating_dispatch_id':'repair-dispatch'}))
    commit = 'a'*40
    linked = authority.journal.assess(group['id'], Assessment(
        expected_version=group['version'], disposition='linked', reason='Canonical scoped repair',
        card_id=card.id, commit=commit), actor='fixture:protected-repair', realms=['secondary'])
    body = {'expected_version':linked['version'], 'disposition':'deployed_verified', 'reason':'Declared scenario verified',
        'card_id':card.id, 'commit':commit, 'accepted_subject_revision':commit,
        'acceptance_reference':'canonical-acceptance', 'acceptance_scenario':'journal-smoke',
        'accepted_instances':[source.settings.instance_id]}
    assert (await api(authority, 'PATCH', f'/groups/{group["id"]}', body=body)).status_code == 409
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision=commit, milestones=['verified'], references=[f'health-group:{group["id"]}',
            'scenario:journal-smoke', f'instance:{source.settings.instance_id}'])
    # Use the actual authenticated canonical HTTP consumer, not body actor claims.
    from pa.modules.items import router as card_router
    from pa.domain.models import AgentSession
    from pa.execution.dispatch import DispatchStore, DispatchRecord
    from pa.acp.environment import assigned_service_session_capability
    authority.app.include_router(card_router, prefix='/api')
    monkeypatch.setattr('pa.modules.items.get_store', lambda:projection)
    ledger = DispatchStore(tmp_path/'acceptance-dispatches')
    authority.ctx.services['dispatch_store'] = ledger
    runtimes = {}
    authority.ctx.services['instance_agent'] = SimpleNamespace(get=runtimes.get)
    await authority.service.cycle(manual=True)
    assert source.journal.page(realms=['secondary'])['items'][0]['custody']['disposition'] == 'linked'
    try:
        for origin in (True, False):
            session_id = 'repair-session' if origin else 'verifier-session'
            dispatch_id = 'repair-dispatch' if origin else 'verifier-dispatch'
            session = projection.save_session(AgentSession(id=session_id, agent_name='codex', realm_id='secondary',
                dispatch_id=dispatch_id, authority_instance_id=authority.settings.instance_id, status='active'))
            runtimes[session_id] = SimpleNamespace(session=session, _closed=False, connected=True)
            ledger.put(DispatchRecord(mutation_id=dispatch_id, dispatch_id=dispatch_id, session_id=session_id,
                target_instance_id=authority.settings.instance_id, authority_instance_id=authority.settings.instance_id,
                authority_url='http://authority', state='running', principal_id='user:local', card_id=card.id, realm_id='secondary'))
            token = assigned_service_session_capability(secret=authority.settings.session_secret,
                dispatch_id=dispatch_id, session_id=session_id, target_instance_id=authority.settings.instance_id,
                purpose='completion-acceptance')
            headers = {'Authorization':f'SessionAcceptance {token}', 'X-PA-Assigned-Session-ID':session_id,
                'X-PA-Assigned-Dispatch-ID':dispatch_id, 'Idempotency-Key':'origin-attempt' if origin else 'canonical-acceptance'}
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=authority.app), base_url='http://authority') as client:
                response = await client.patch('/api/cards/'+card.id, params={'realm':'secondary'}, headers=headers,
                    json={'expected_version':card.updated_at.isoformat(), 'completion_acceptance':evidence.model_dump(mode='json')})
            if origin:
                assert response.status_code == 409, response.text
                assert response.json()['detail']['code'] == 'completion_self_acceptance_forbidden'
            else:
                assert response.status_code == 200, response.text
                receipt = response.json()['completion_evidence'][-1]
                assert receipt['actor_kind'] == 'bound_session'
                assert receipt['actor_session_id'] == session_id and receipt['actor_dispatch_id'] == dispatch_id
        partial = await api(authority, 'PATCH', f'/groups/{group["id"]}', body=body)
        assert partial.status_code == 409 and partial.json()['detail']['code'] == 'acceptance_scope_incomplete'
        assert source.journal.page(realms=['secondary'])['items'][0]['custody']['disposition'] == 'linked'
        complete_evidence = evidence.model_copy(update={'references':[*evidence.references, f'instance:{authority.settings.instance_id}']})
        current_card = projection.get_card(card.id, realm_id='secondary')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=authority.app), base_url='http://authority') as client:
            complete = await client.patch('/api/cards/'+card.id, params={'realm':'secondary'},
                headers=headers | {'Idempotency-Key':'canonical-acceptance-full'},
                json={'expected_version':current_card.updated_at.isoformat(), 'completion_acceptance':complete_evidence.model_dump(mode='json')})
        assert complete.status_code == 200, complete.text
        body.update(acceptance_reference='canonical-acceptance-full', accepted_instances=[source.settings.instance_id, authority.settings.instance_id])
    finally:
        ledger.close()
    assert (await api(authority, 'PATCH', f'/groups/{group["id"]}', body=body | {'acceptance_reference':'forged'})).status_code == 409
    observed = (await api(authority, 'GET', f'/groups/{group["id"]}')).json()
    assert observed['version'] == body['expected_version']
    previous_watermark = observed['data'].get('acceptance_watermark')
    await api(source, 'POST', '/reports', body=observation(realm='secondary', summary='Evidence arrived after verifier read'), key='racing-revision')
    await authority.service.cycle(manual=True)
    stale = await api(authority, 'PATCH', f'/groups/{group["id"]}', body=body)
    assert stale.status_code == 409 and stale.json()['detail']['code'] == 'assessment_version_conflict'
    fresh = (await api(authority, 'GET', f'/groups/{group["id"]}')).json()
    assert fresh['version'] == observed['version'] + 1
    assert fresh['data'].get('acceptance_watermark') == previous_watermark
    entry = source.journal.page(realms=['secondary'])['items'][-1]
    lease = authority.journal.lease(authority.service.owner)
    authority.journal.ingest(entry, source_id=source.settings.instance_id, realms=['secondary'],
        owner=authority.service.owner, fence=lease['fence'])
    authority.journal.release(authority.service.owner, lease['fence'])
    replayed = (await api(authority, 'GET', f'/groups/{group["id"]}')).json()
    assert replayed == fresh
    body['expected_version'] = fresh['version']
    accepted = await api(authority, 'PATCH', f'/groups/{group["id"]}', body=body)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()['data']['acceptance_watermark'] > (previous_watermark or 0)
    await authority.service.cycle(manual=True)
    source_report = source.journal.page(realms=['secondary'])['items'][0]
    assert source_report['custody']['disposition'] == 'deployed_verified'
    assert source_report['custody']['fix']['acceptance_reference'] == 'canonical-acceptance-full'
    assert authority.journal.page(realms=['secondary'])['items'][0]['custody']['disposition'] == 'deployed_verified'
    late = (await api(source, 'POST', '/reports', body=observation(realm='secondary', occurrence_key='new-member'), key='late-member')).json()
    await authority.service.cycle(manual=True)
    entries = source.journal.page(realms=['secondary'])['items']
    original, uncovered = entries[0], next(e for e in entries if e['payload']['report_id'] == late['report_id'])
    assert original['custody']['disposition'] == 'deployed_verified'
    assert uncovered['custody']['disposition'] == 'awaiting_acceptance'
    assert uncovered['custody']['fix']['acceptance_reference'] is None
    assert uncovered['custody']['fix']['acceptance_scope'] == 'uncovered'


@pytest.mark.asyncio
async def test_actual_scope_producer_shared_journal_dependency_recovery(fleet):
    pytest.importorskip('pa.pr_supervisor.eligibility', reason='Companion scope component required; recorded combined integration overlay exercises it')
    from pa.pr_supervisor.service import PRSupervisor
    from pa.pr_supervisor.eligibility import EligibilityReport, EligibilityCandidate
    from pa.pr_supervisor.models import PRWatch
    from datetime import UTC, datetime
    authority, source, _ = fleet
    watch = PRWatch(id='synthetic-scope-watch', repository='owner/pa', pr_number=1, pr_url='https://github.com/owner/pa/pull/1', realm_id='default', card_id='synthetic-card')
    source.ctx.services['pr_supervisor_store'] = SimpleNamespace(get_watch=lambda wid:watch)
    producer = PRSupervisor.__new__(PRSupervisor)
    producer.eligibility_journal_hook = source.service.eligibility_hook
    affected = source.settings.instance_id
    def report(dependency, reason=None, *, instance_id=affected, auth=authority.settings.instance_id):
        return EligibilityReport(dependency=dependency, authority_instance_id=auth, candidates=[EligibilityCandidate(
            instance_id=instance_id, scope_mode='allowlist', repositories=['owner/pa'],
            policy_source='synthetic', policy_revision='1', configuration_status='valid', authenticated=True,
            observed_at=datetime.now(UTC), freshness='fresh', reason_code=reason)], eligible=[] if reason else [instance_id])
    failed = report('github_repository_observation', 'repository_access_denied')
    await producer._emit_eligibility_diagnostic(watch, failed)
    await producer._emit_eligibility_diagnostic(watch, failed)
    assert source.journal.status()['pending_revisions'] == 1
    await producer._emit_eligibility_diagnostic(watch, report('capability_inventory'))
    await producer._emit_eligibility_diagnostic(watch, report('github_repository_observation', instance_id='other-instance'))
    await producer._emit_eligibility_diagnostic(watch, report('github_repository_observation', auth='other-authority'))
    assert source.journal.status()['pending_revisions'] == 1
    source.settings.subscribed_realms.append('secondary')
    authority.settings.subscribed_realms.append('secondary')
    second_watch = watch.model_copy(update={'id':'secondary-watch', 'realm_id':'secondary'})
    source.ctx.services['pr_supervisor_store'] = SimpleNamespace(
        get_watch=lambda wid: second_watch if wid == second_watch.id else watch)
    await producer._emit_eligibility_diagnostic(second_watch, failed)
    assert source.journal.status()['pending_revisions'] == 2
    await producer._emit_eligibility_diagnostic(watch, report('github_repository_observation'))
    assert source.journal.status()['pending_revisions'] == 3
    await authority.service.cycle(manual=True)
    page = source.journal.page(realms=['default'])
    assert len({e['payload']['report_id'] for e in page['items']}) == 1
    assert page['items'][1]['payload']['observation']['actual'].startswith('same dependency')
    assert all(e['delivery'] == 'gathered' for e in page['items'])
    assert page['items'][0]['payload']['context']['card_id'] == watch.card_id

    secondary = source.journal.page(realms=['secondary'])['items']
    assert len(secondary) == 1
    assert secondary[0]['payload']['observation']['actual'] == 'failure observed'
    assert secondary[0]['payload']['report_id'] != page['items'][0]['payload']['report_id']
    assert secondary[0]['payload']['observation']['correlation_ids'] == page['items'][0]['payload']['observation']['correlation_ids']


@pytest.mark.asyncio
async def test_authenticated_custody_responses_cannot_regress_source(fleet, monkeypatch):
    authority, source, network = fleet
    created = (await api(source, 'POST', '/reports', body=observation())).json()
    await authority.service.cycle(manual=True)
    current = source.journal.report(created['report_id'], realms=['default'])['history'][0]['custody']
    original = network.handle_async_request
    captured, release = asyncio.Event(), asyncio.Event()
    held = False
    async def reordered(request):
        nonlocal held
        response = await original(request)
        if request.url.path.endswith('/receipts/'+current['receipt_id']) and not held:
            held = True
            await response.aread()
            captured.set()
            await release.wait()
        return response
    monkeypatch.setattr(network, 'handle_async_request', reordered)
    headers = {'Authorization':'Bearer synthetic-shared-fleet-token'}
    delayed = asyncio.create_task(api(source, 'POST', '/gathered', body={'receipt_id':current['receipt_id']}, headers=headers))
    try:
        await asyncio.wait_for(captured.wait(), 3)
        authority.journal.assess(current['group_id'], Assessment(
            expected_version=current['version'], disposition='linked', reason='Current repair owner',
            card_id=str(uuid4()), pr_url='https://github.com/owner/pa/pull/7', commit='d'*40),
            actor='fixture:protected-repair', realms=['default'])
        latest = await api(source, 'POST', '/gathered', body={'receipt_id':current['receipt_id']}, headers=headers)
        assert latest.status_code == 200 and latest.json()['custody'] == 'advanced'
        release.set()
        stale = await asyncio.wait_for(delayed, 3)
        assert stale.status_code == 200 and stale.json()['custody'] == 'stale_ignored'
        result = (await api(source, 'GET', '/reports/'+created['report_id'])).json()
        assert result['history'][0]['custody']['disposition'] == 'linked'
        assert result['history'][0]['custody']['fix']['commit'] == 'd'*40
        assert len(result['custody_history']) == 2
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=source.app), base_url='http://source') as client:
            page = await client.get('/health-journal', headers=source.headers)
        assert 'Current repair owner' in page.text and 'pull/7' in page.text
        receipt = authority.journal.receipt(current['receipt_id'])
        assert source.journal.gathered(receipt)['custody'] == 'replayed'
        with pytest.raises(JournalError, match='custody_version_conflict'):
            source.journal.gathered(receipt | {'disposition':'no_fix'})
    finally:
        release.set()
        await asyncio.gather(delayed, return_exceptions=True)


@pytest.mark.asyncio
async def test_shared_realm_reads_and_exact_signed_nonprimary_session(fleet):
    from pa.acp.environment import assigned_service_session_capability
    from pa.domain.models import AgentSession
    _, source, _ = fleet
    source.settings.subscribed_realms.append('secondary')
    first = source.users.create_user('first', 'synthetic-password')
    second = source.users.create_user('second', 'synthetic-password')
    source.ctx.services['membership'] = SimpleNamespace(has_role=lambda realm, principal, **kw:realm == 'secondary')
    first_headers = {'Authorization':f'Bearer {first.cli_token}', 'Idempotency-Key':'first-report'}
    second_headers = {'Authorization':f'Bearer {second.cli_token}', 'Idempotency-Key':'second-report'}
    authored = await api(source, 'POST', '/reports', body=observation(realm='secondary'), headers=first_headers)
    assert authored.status_code == 201
    assert (await api(source, 'GET', '/reports/'+authored.json()['report_id'], headers=second_headers)).status_code == 200
    shared = (await api(source, 'GET', '/reports', headers=second_headers)).json()['items']
    assert len(shared) == 1 and shared[0]['payload']['principal'] == 'user:'+first.id
    foreign = (await api(source, 'POST', '/reports', body=observation(), key='admin-default')).json()
    assert (await api(source, 'GET', '/reports/'+foreign['report_id'], headers=second_headers)).status_code == 404
    session = AgentSession(agent_name='codex', realm_id='secondary', principal_id='user:'+second.id,
        dispatch_id=str(uuid4()), card_id=str(uuid4()))
    other = session.model_copy(update={'id':str(uuid4()), 'dispatch_id':str(uuid4()), 'realm_id':'default'})
    runtimes = {s.id:SimpleNamespace(session=s, _closed=False) for s in (session, other)}
    source.ctx.services['instance_agent'] = SimpleNamespace(get=runtimes.get)
    token = assigned_service_session_capability(secret=source.settings.session_secret,
        dispatch_id=session.dispatch_id, session_id=session.id, target_instance_id=source.settings.instance_id)
    signed = {'Authorization':f'GoalSession {token}', 'X-PA-Assigned-Session-ID':session.id,
        'X-PA-Assigned-Dispatch-ID':session.dispatch_id, 'Idempotency-Key':'signed-secondary'}
    accepted = await api(source, 'POST', '/reports', body=observation(), headers=signed)
    assert accepted.status_code == 201, accepted.text
    record = (await api(source, 'GET', '/reports/'+accepted.json()['report_id'], headers=signed)).json()['history'][0]['payload']
    assert record['realm'] == 'secondary' and record['context']['session_id'] == session.id
    assert record['context']['dispatch_id'] == session.dispatch_id
    assert (await api(source, 'POST', '/reports', body=observation(realm='default'), headers=signed)).status_code == 403
    assert (await api(source, 'POST', '/reports', body=observation(), headers=signed | {'X-PA-Health-Session-ID':other.id})).status_code == 403
    assert (await api(source, 'GET', '/reports/'+authored.json()['report_id'], headers=signed)).status_code == 200
    assert (await api(source, 'GET', '/reports/'+foreign['report_id'], headers=signed)).status_code == 404
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=source.app), base_url='http://source') as client:
        page = await client.get('/health-journal', headers=second_headers)
    assert authored.json()['report_id'] in page.text and foreign['report_id'] not in page.text


@pytest.mark.asyncio
async def test_large_inbox_receipt_lookup_uses_unique_index(fleet, monkeypatch):
    from contextlib import contextmanager
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    journal = authority.journal
    with journal.connection(write=True) as db:
        row = dict(db.execute('SELECT * FROM inbox LIMIT 1').fetchone())
        original = json.loads(row['receipt'])
        # Bulk fixture data isolates lookup complexity from collection throughput.
        db.executemany('INSERT INTO inbox(identity,hash,payload,receipt,group_id) VALUES(?,?,?,?,?)',
            [(f'fixture-{i}', row['hash'], row['payload'],
              json.dumps(original | {'receipt_id':f'fixture-receipt-{i}', 'identity':f'fixture-{i}'}), row['group_id'])
             for i in range(10_000)])
        assert db.execute('SELECT count(*) FROM inbox').fetchone()[0] == 10_001
    connection = journal.connection
    plans = []
    @contextmanager
    def traced_connection(**kwargs):
        with connection(**kwargs) as db:
            def trace(sql):
                if sql.startswith('SELECT * FROM inbox WHERE'):
                    plans.extend(r['detail'] for r in db.execute('EXPLAIN QUERY PLAN '+sql))
            db.set_trace_callback(trace)
            yield db
    monkeypatch.setattr(journal, 'connection', traced_connection)
    found = journal.receipt('fixture-receipt-9999')
    assert found['identity'] == 'fixture-9999'
    with pytest.raises(JournalError, match='receipt_not_found'):
        journal.receipt('absent-receipt')
    assert len(plans) == 2
    assert all('SEARCH inbox USING INDEX receipt_ids' in plan for plan in plans)
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        with journal.connection(write=True) as db:
            db.execute('INSERT INTO inbox(identity,hash,payload,receipt,group_id) VALUES(?,?,?,?,?)',
                ('different-identity', row['hash'], row['payload'], row['receipt'], row['group_id']))


@pytest.mark.asyncio
async def test_all_repair_entries_require_bound_protection(fleet):
    authority, source, _ = fleet
    await api(source, 'POST', '/reports', body=observation())
    await authority.service.cycle(manual=True)
    group = authority.journal.groups(['default'])[0]
    for disposition in ('reproduced', 'linked', 'in_progress', 'merged'):
        denied = await api(authority, 'PATCH', f'/groups/{group["id"]}', body={
            'expected_version':group['version'], 'disposition':disposition, 'reason':'Cannot bypass canonical protection'})
        assert denied.status_code == 409 and denied.json()['detail']['code'] == 'repair_action_required'
    denied = await api(authority, 'PATCH', f'/groups/{group["id"]}', body={
        'expected_version':group['version'], 'disposition':'no_fix', 'reason':'Cannot smuggle a repair link', 'commit':'a'*40})
    assert denied.status_code == 409
    assert authority.journal.groups(['default'])[0] == group


@pytest.mark.asyncio
async def test_kernel_journal_local_operations_and_bound_recovery_admission(tmp_path, monkeypatch):
    from pa.core.kernel import Kernel, _SyncRecoveryAdmissionMiddleware
    from pa.config import reset_settings
    from pa.domain.store import reset_store
    from pa.modules.items import router as items_router
    from tests.test_sync_recovery_owned import Harness

    reset_settings(); reset_store()
    h = Harness(tmp_path/'isolated-history')
    h.settings.workspace_root = tmp_path/'workspaces'
    h.settings.auth_required = True
    h.settings.subscribed_realms = ['default', 'healthy']
    h.services['membership'].ensure_owner_membership('healthy', 'local')
    h.objects.put(h.raw)  # Boot with complete fixture history, then damage it below.
    kernel = Kernel.boot(settings=h.settings, load_modules=False)
    h.objects._path_for(h.hash).unlink()
    kernel.ctx.store = h.store
    kernel.ctx.services.update(h.services)
    journal = Journal(h.settings.data_dir/'health.db', h.settings.instance_id)
    current = HealthService(kernel.ctx, journal)
    kernel.ctx.register_service('health_journal', current)
    app = kernel.build_app()
    app.state.ctx = kernel.ctx
    app.include_router(router, prefix='/api')
    app.include_router(items_router, prefix='/api')
    current.app = app
    assert any(m.cls is _SyncRecoveryAdmissionMiddleware for m in app.user_middleware)
    monkeypatch.setattr('pa.modules.items.get_store', lambda: h.store)
    h.fetch_release.clear()
    h.diagnose()  # Real missing canonical event, reported by actual traversal.
    assert h.recovery.admission_view('default')[0]
    assert not h.recovery.admission_view('healthy')[0]
    user = UserDirectory(h.settings.data_dir).get('local')
    auth = {'Authorization':f'Bearer {user.cli_token}'}
    peer = {'Authorization':f'Bearer {h.settings.sync_token}'}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://local') as client:
            async def request(method, path, body=None, *, headers=auth, key='local-test'):
                return await client.request(method, '/api/health-journal'+path, json=body,
                    headers=headers | {'Idempotency-Key':key})
            assert (await request('POST', '/reports', observation(), headers={})).status_code in {401, 403}
            assert (await request('POST', '/reports', observation(realm='unknown'))).status_code == 403
            configured = await request('PATCH', '/config', {'expected_version':1,
                'policy':{'authority_id':h.settings.instance_id, 'enabled':True}})
            assert configured.status_code == 200, configured.text
            for realm in ['default', 'healthy']:
                reported = await request('POST', '/reports', observation(realm=realm), key='report-'+realm)
                assert reported.status_code == 201, reported.text
            assert (await request('POST', '/reports', observation(source_instance_id='forged'), key='forged')).status_code == 422
            collected = await request('POST', '/run')
            assert collected.status_code == 200, collected.text
            entries = journal.page(realms=['default', 'healthy'])['items']
            assert len(entries) == 2 and all(e['delivery'] == 'gathered' for e in entries)
            receipt_id = entries[0]['custody']['receipt_id']
            gathered = await request('POST', '/gathered', {'receipt_id':receipt_id}, headers=peer)
            assert gathered.status_code == 200, gathered.text
            assert (await request('POST', '/gathered', {'receipt_id':receipt_id}, headers=auth)).status_code == 403
            bound = {g['realm']:g for g in journal.groups(['default', 'healthy'])}
            for disposition in ['no_fix', 'duplicate', 'needs_input', 'reopened']:
                group = journal.group(bound['default']['id'], realms=['default'])
                result = await request('PATCH', '/groups/'+group['id'], {
                    'expected_version':group['version'], 'disposition':disposition, 'reason':'Local reasoned triage'})
                assert result.status_code == 200, result.text
            group = journal.group(bound['default']['id'], realms=['default'])
            for disposition in ['reproduced', 'linked', 'in_progress', 'merged', 'deployed_verified', 'no_fix', 'duplicate']:
                body = {'expected_version':group['version'], 'disposition':disposition, 'reason':'Requires canonical history'}
                if disposition == 'no_fix':
                    body['commit'] = 'a'*40
                if disposition == 'duplicate':
                    body['pr_url'] = 'https://github.com/example/pa/pull/1'
                blocked = await request('PATCH', '/groups/'+group['id']+'?realm=healthy', body)
                assert blocked.status_code == 503 and blocked.json()['detail']['code'] == 'sync_history_recovery', blocked.text
            assert journal.group(group['id'], realms=['default'])['version'] == group['version']
            healthy = bound['healthy']
            allowed = await request('PATCH', '/groups/'+healthy['id']+'?realm=default', {
                'expected_version':healthy['version'], 'disposition':'reproduced', 'reason':'Healthy bound realm'})
            assert allowed.status_code == 409 and allowed.json()['detail']['code'] == 'repair_action_required', allowed.text
            # Inner canonical admission remains gated by its own normal contract.
            for realm, expected in [('default',503), ('healthy',201)]:
                card = await client.post('/api/cards', headers=auth | {'Idempotency-Key':'card-'+realm},
                    json={'realm_id':realm, 'title':'Isolated card', 'summary':'provided', 'auto_enrich':False})
                assert card.status_code == expected, card.text
            kernel.ctx.services.pop('sync_recovery')
            unknown = await request('PATCH', '/groups/'+healthy['id'], {
                'expected_version':healthy['version'], 'disposition':'deployed_verified', 'reason':'Unknown admission'})
            assert unknown.status_code == 503 and unknown.json()['detail']['code'] == 'canonical_admission_unavailable'
            kernel.ctx.services['sync_recovery'] = h.recovery
            h.settings.subscribed_realms = ['healthy']
            denied = await request('PATCH', '/groups/'+group['id']+'?realm=healthy', {
                'expected_version':group['version'], 'disposition':'reproduced', 'reason':'Unavailable bound realm'})
            assert denied.status_code == 404
            h.settings.subscribed_realms = ['default', 'healthy']
            client.cookies.set('pa_session', SessionManager(h.settings.session_secret).create_token(user))
            await client.get('/api/health-journal/status')
            assert (await request('POST', '/reports', observation(), headers={}, key='csrf')).status_code == 403
            assert (await request('POST', '/reports', observation(),
                headers={'X-CSRF-Token':client.cookies['pa_csrf']}, key='csrf')).status_code == 201
    finally:
        await current.stop()
        await h.close()
        reset_store(); reset_settings()
