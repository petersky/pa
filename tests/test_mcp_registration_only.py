"""Actual child protocol, store exclusion, and provider startup evidence."""
import asyncio
import json
import os
import sqlite3
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from pa.acp.client import AgentConnection, PAClient
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.mcp.server import ASSIGNED_SERVICE_TOOL_ALLOWLIST


@pytest.mark.asyncio
@pytest.mark.parametrize('assigned', [False, True])
@pytest.mark.parametrize('entrypoint', ['module', 'console'])
async def test_actual_stdio_locked_existing_data_is_registration_only(tmp_path, assigned, entrypoint):
    data = tmp_path / 'data'
    data.mkdir()
    db = sqlite3.connect(data / 'pa.db')
    db.execute('create table sentinel(value)')
    db.commit()
    db.execute('begin exclusive')
    # Sparse large service file makes accidental store traversal visible without
    # consuming test disk space; the SQLite writer lock remains held throughout.
    with (data / 'large-history').open('wb') as f:
        f.truncate(128 * 1024 * 1024)
    (data / 'config.json').write_text(json.dumps({'session_secret': 'test-only', 'instance_id': 'owner-test'}))
    before = {p.name: (p.stat().st_size, p.stat().st_mtime_ns) for p in data.iterdir()}
    guard = tmp_path / 'guard'
    guard.mkdir()
    (guard / 'sitecustomize.py').write_text('''
import os, sys
root = os.path.realpath(os.environ['PA_DATA_DIR'])
def audit(event, args):
    if event == 'sqlite3.connect':
        raise RuntimeError('MCP child attempted SQLite initialization')
    if event == 'open' and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC) and isinstance(args[0], (str, bytes)):
        path = os.path.realpath(os.fsdecode(args[0]))
        if path.startswith(root + os.sep) and (args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC)):
            raise RuntimeError('MCP child attempted service write')
    if event in ('os.mkdir', 'os.remove', 'os.rename', 'os.rmdir'):
        if any(isinstance(p, str) and os.path.realpath(p).startswith(root) for p in args):
            raise RuntimeError('MCP child attempted service mutation')
sys.addaudithook(audit)
''')
    env = {k: v for k, v in os.environ.items() if not k.startswith('PA_')}
    env.update(PA_DATA_DIR=str(data), PA_AGENT_ENABLED='false', PA_INSTANCE_ID='owner-test',
               PA_LOCAL_API_URL='http://127.0.0.1:1', PA_LOCAL_API_TOKEN='isolated-test-only-token',
               PA_WORKSPACE_ROOT=str(tmp_path / 'workspaces'), PA_BROWSER_SESSION_ID='session-test',
               PYTHONPATH=str(guard), PYTHONDONTWRITEBYTECODE='1')
    if assigned:
        env.update(PA_ASSIGNED_SERVICE_MODE='1', PA_ASSIGNED_SERVICE_SESSION_ID='session-test', PA_ASSIGNED_SERVICE_DISPATCH_ID='dispatch-test')
    command = sys.executable if entrypoint == 'module' else str(Path(sys.executable).parent / 'pa')
    args = ['-m', 'pa', 'mcp'] if entrypoint == 'module' else ['mcp']
    started = time.monotonic()
    try:
        async with AsyncExitStack() as stack:
            # Match the real bootstrap probe: the 25s deadline covers spawn,
            # initialize, and tools/list; SDK teardown has its own bounded waits.
            async with asyncio.timeout(25):
                streams = await stack.enter_async_context(stdio_client(
                    StdioServerParameters(command=command, args=args, env=env)
                ))
                session = await stack.enter_async_context(ClientSession(*streams))
                await session.initialize()
                tools = (await session.list_tools()).tools
                assert time.monotonic() - started < 25
        names = {t.name for t in tools}
        if assigned:
            assert names == ASSIGNED_SERVICE_TOOL_ALLOWLIST
        else:
            assert {'instance_info', 'agent_providers_list', 'preview_agent_restart_handoff'} <= names
            assert len(names) == 205
        assert {p.name: (p.stat().st_size, p.stat().st_mtime_ns) for p in data.iterdir()} == before
    finally:
        db.rollback()
        db.close()


def bind(connection, native='native-current'):
    client = PAClient(MagicMock())
    connection._client = client
    connection.session = AgentSession(agent_name='codex', external_session_id=native)
    connection._mcp_observer = (client, native)
    client.on_mcp_startup = lambda key: connection._publish_mcp_startup(client, key)
    connection._publish_mcp_startup(client, native)
    return client


async def emit(client, status, detail='', native='native-current', kind='tool_call'):
    await client.session_update(native, {'sessionUpdate': kind, 'toolCallId': 'mcp_startup.pa',
        'status': status, 'content': [{'type': 'text', 'text': detail}]})


@pytest.mark.asyncio
async def test_two_second_silence_then_real_thirty_second_failure_and_recovery(tmp_path):
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    client = bind(connection)
    started = time.monotonic()
    assert await client.wait_for_pa_mcp_startup_failure('native-current', timeout=2.0) is None
    connection._publish_mcp_startup(client, 'native-current')
    assert 2 <= time.monotonic() - started < 4
    assert connection.pa_mcp_health['state'] == 'checking'
    assert connection.pa_mcp_health['last_success'] is None
    # Exercise the actual elapsed provider deadline, with no external provider.
    await asyncio.sleep(max(0, 30 - (time.monotonic() - started)))
    await emit(client, 'failed', 'PA MCP client timed out after 30 seconds', kind='tool_call_update')
    assert connection.pa_mcp_health['state'] == 'disconnected'
    assert '30 seconds' in connection.pa_mcp_health['detail']
    assert connection.pa_mcp_health['retry_state'] == 'provider_startup_failed'
    assert connection.session.status != 'disconnected'
    await emit(client, 'completed')
    assert connection.pa_mcp_health['state'] == 'disconnected'
    await emit(client, 'in_progress')
    await emit(client, 'completed')
    assert connection.pa_mcp_health['state'] == 'connected'
    assert connection.pa_mcp_health['provider_context_probe']['classification'] == 'startup_confirmed'
    assert connection.pa_mcp_health['last_failure'] is None
    assert 'detail' not in connection.pa_mcp_health


@pytest.mark.asyncio
async def test_stale_generation_and_cancelled_only_cannot_override_health(tmp_path):
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    old = bind(connection)
    client = bind(connection)  # Same native ID, different provider generation.
    await emit(old, 'completed')
    assert connection.pa_mcp_health['state'] == 'checking'
    await emit(client, 'failed', 'MCP startup was cancelled')
    assert connection.pa_mcp_health['state'] == 'checking'
    await emit(client, 'completed', native='stale-native')
    assert connection.pa_mcp_health['state'] == 'checking'
    await emit(client, 'completed')
    await emit(old, 'failed', 'MCP failed to start: timeout')
    await emit(client, 'failed', 'MCP startup was cancelled')
    assert connection.pa_mcp_health['state'] == 'connected'
    await emit(client, 'failed', 'MCP failed to start: connection refused')
    assert connection.pa_mcp_health['state'] == 'disconnected'
    connection._mcp_observer = None
    await emit(client, 'completed')
    assert connection.pa_mcp_health['state'] == 'disconnected'


def test_registration_never_calls_service_lifecycle(tmp_path, monkeypatch):
    from pa.mcp import server
    from pa.core.kernel import Kernel
    from pa.core.registry import BUILTIN_MODULE_NAMES, ModuleRegistry
    from pa.mcp.context import registration_context
    monkeypatch.setenv('PA_DATA_DIR', str(tmp_path))
    monkeypatch.setattr(server, 'mcp', None)
    with patch.object(Kernel, 'boot', side_effect=AssertionError('service boot')):
        ctx = registration_context()
        registry = ModuleRegistry(ctx, registration_only=True)
        registry.load_all()
        assert {entry.module.name for entry in registry.modules if entry.source == "builtin"} == BUILTIN_MODULE_NAMES
        proxy_registry = ModuleRegistry(ctx, registration_only=True, reserved_names=BUILTIN_MODULE_NAMES)
        with pytest.raises(ValueError, match="already registered"):
            proxy_registry.register(registry.modules[0].module, source="entrypoint:duplicate")
        for entry in registry.modules:
            monkeypatch.setattr(type(entry.module), 'on_load', lambda *_: pytest.fail('module lifecycle'))
        server._get_mcp()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('env', [
    {'PA_ASSIGNED_SERVICE_MODE': '1'},
    {'PA_ASSIGNED_SERVICE_MODE': '2'},
    {'PA_ASSIGNED_SERVICE_SESSION_ID': 's'},
    {'PA_ASSIGNED_SERVICE_MODE': '1', 'PA_ASSIGNED_SERVICE_SESSION_ID': 's'},
])
def test_bad_binding_never_caches_broad_server(env):
    from pa.mcp import server
    with patch.dict(os.environ, env, clear=True), patch.object(server, 'mcp', None):
        with pytest.raises(RuntimeError, match='binding'):
            server._get_mcp()
        assert server.mcp is None

@pytest.mark.asyncio
async def test_all_ordinary_tool_schemas_match_pre_repair_contract(tmp_path, monkeypatch):
    import hashlib
    from pa.mcp import server
    monkeypatch.setenv('PA_DATA_DIR', str(tmp_path))
    monkeypatch.setattr(server, 'mcp', None)
    tools = await server._get_mcp().list_tools()
    actual = {t.name: hashlib.sha256(json.dumps(t.input_schema, sort_keys=True).encode()).hexdigest() for t in tools}
    baseline = json.loads((Path(__file__).parent / 'fixtures/mcp_tool_schemas_cb2588d3.json').read_text())
    assert actual == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize('peer', [None, 'peer-test'])
@pytest.mark.parametrize('tool,arguments,method,suffix', [
    ('agent_providers_list', {}, 'GET', ''),
    ('agent_provider_status', {'provider_id': 'codex'}, 'GET', '/codex'),
    ('agent_provider_install', {'provider_id': 'codex'}, 'POST', '/codex/install'),
    ('agent_provider_update', {'provider_id': 'codex'}, 'POST', '/codex/update'),
    ('agent_provider_configure', {'provider_id': 'codex', 'model': 'test-model'}, 'POST', '/codex/configure'),
    ('agent_provider_probe', {'provider_id': 'codex'}, 'POST', '/codex/probe'),
    ('agent_provider_login_start', {'provider_id': 'codex', 'consent': True}, 'POST', '/codex/login-jobs'),
    ('agent_provider_login_status', {'provider_id': 'codex', 'job_id': 'job-test'}, 'GET', '/codex/login-jobs/job-test'),
    ('agent_provider_login_cancel', {'provider_id': 'codex', 'job_id': 'job-test'}, 'POST', '/codex/login-jobs/job-test/cancel'),
])
async def test_provider_tools_forward_authenticated_to_owner(tmp_path, tool, arguments, method, suffix, peer):
    import httpx
    from pa.mcp.context import registration_context
    from pa.modules.agent_providers import AgentProvidersModule
    from tests.test_goal_assigned_service_mcp import FakeMcp
    calls = []
    def respond(verb, url, **kwargs):
        calls.append((verb, url, kwargs))
        return httpx.Response(200, request=httpx.Request(verb, url), headers={'X-PA-Instance-ID': 'owner-test'}, json={'forwarded': True})
    with patch.dict(os.environ, {'PA_DATA_DIR': str(tmp_path), 'PA_LOCAL_API_URL': 'http://owner.test', 'PA_LOCAL_API_TOKEN': 'test-only-token', 'PA_INSTANCE_ID': 'owner-test'}, clear=True):
        ctx = registration_context()
        mcp = FakeMcp()
        AgentProvidersModule().register_mcp(mcp, ctx)
        with patch('pa.mcp.local_api.httpx.request', side_effect=respond), patch('pa.mcp.local_api.UserDirectory', side_effect=AssertionError('credential mutation')):
            assert await mcp.functions[tool](**arguments, instance_id=peer) == {'forwarded': True}
    assert len(calls) == 1
    verb, url, kwargs = calls[0]
    base = '/api/agent/providers' if peer is None else '/api/fleet/instances/peer-test/agent-providers'
    assert (verb, url) == (method, 'http://owner.test' + base + suffix)
    assert kwargs['headers']['Authorization'] == 'Bearer test-only-token'
    if peer is None and suffix in {'/codex/install', '/codex/update'}:
        assert 900 < kwargs['timeout'] <= 910
    else:
        assert kwargs['timeout'] <= 120
    if method == 'POST':
        assert kwargs['headers']['Idempotency-Key']
    if tool == 'agent_provider_configure':
        assert kwargs['json']['model'] == 'test-model'
    assert list(tmp_path.iterdir()) == []


def test_missing_credential_never_initializes_user_directory(tmp_path):
    from pa.mcp.local_api import request_local_pa, LocalPAServerUnavailable
    with patch.dict(os.environ, {}, clear=True), patch('pa.mcp.local_api.httpx.request') as http:
        with pytest.raises(LocalPAServerUnavailable, match='credential'):
            request_local_pa(Settings(data_dir=tmp_path), 'GET', '/api/instance')
    http.assert_not_called()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_mcp_health_is_attached_to_live_ui_event(tmp_path):
    from unittest.mock import AsyncMock
    from pa.instance.agent_session import AgentSessionRuntime
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    client = bind(connection)
    runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
    runtime.session = connection.session
    runtime.connection = connection
    runtime._in_flight = None
    runtime._append_transcript = MagicMock()
    runtime._report_progress = AsyncMock()
    client.on_update = runtime._on_acp_update
    await emit(client, 'failed', 'PA MCP client timed out after 30 seconds')
    payload = runtime._append_transcript.call_args.args[1]
    assert payload['pa_mcp']['state'] == 'disconnected'
    assert '30 seconds' in payload['pa_mcp']['detail']
    contract = json.loads((Path(__file__).parent / 'fixtures/codex_acp_1_11_mcp_events.json').read_text())
    await client.session_update('native-current', {**contract['success'], 'sessionUpdate': 'tool_call', 'status': 'in_progress', 'rawOutput': None})
    await client.session_update('native-current', contract['success'])
    assert runtime._append_transcript.call_args.args[1]['pa_mcp']['state'] == 'connected'


def test_browser_displays_pending_late_failure_and_recovery():
    import shutil
    import subprocess
    node = shutil.which('node')
    if not node:
        pytest.skip('node is required for browser health regression')
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([node, str(root / 'tests/mcp_health_node_harness.js'),
                             str(root / 'src/pa/server/static/js/agent-chat.js')],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.asyncio
async def test_actual_acp_wire_late_startup_failure_and_recovery(tmp_path):
    from acp import PROTOCOL_VERSION
    from pa.acp.transport import spawn_agent
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    client = bind(connection)
    failed, recovered = asyncio.Event(), asyncio.Event()
    evidence = []
    async def update(_native, _update):
        evidence.append(dict(connection.pa_mcp_health))
        if connection.pa_mcp_health['state'] == 'disconnected':
            failed.set()
        if connection.pa_mcp_health['state'] == 'connected':
            recovered.set()
    client.on_update = update
    fixture = Path(__file__).parent / 'fixtures/mcp_startup_provider.py'
    env = {k: v for k, v in os.environ.items() if not k.startswith('PA_')}
    async with asyncio.timeout(40):
        async with spawn_agent(client, sys.executable, str(fixture), env=env) as (protocol, _process):
            await protocol.initialize(protocol_version=PROTOCOL_VERSION)
            created = await protocol.new_session(cwd=str(tmp_path), mcp_servers=[])
            started = time.monotonic()
            assert created.session_id == 'native-current'
            assert await client.wait_for_pa_mcp_startup_failure(created.session_id, timeout=2.0) is None
            assert connection.pa_mcp_health['state'] == 'checking'
            await failed.wait()
            assert time.monotonic() - started >= 29.5
            assert '30 seconds' in connection.pa_mcp_health['detail']
            await recovered.wait()
    assert [item['state'] for item in evidence] == ['disconnected', 'disconnected', 'connected']
    assert list(tmp_path.iterdir()) == []


def test_stdio_logs_redact_messages_and_exceptions_without_shared_files():
    import io
    import logging
    from pa.mcp import server
    stdout, stderr = io.StringIO(), io.StringIO()
    root = logging.getLogger()
    def run(**kwargs):
        assert kwargs == {'transport': 'stdio'}
        assert len(root.handlers) == 1
        assert not isinstance(root.handlers[0], logging.FileHandler)
        logging.warning('Authorization: Bearer test-sensitive-value')
        try:
            raise RuntimeError('secret=test-sensitive-exception')
        except RuntimeError:
            logging.exception('provider failure')
    candidate = MagicMock()
    candidate.run.side_effect = run
    with patch.object(root, 'handlers', []), patch.object(root, 'level', logging.WARNING), patch('sys.stdout', stdout), patch('sys.stderr', stderr), patch.object(server, '_get_mcp', return_value=candidate):
        server.run_stdio()
    assert stdout.getvalue() == ''
    assert 'test-sensitive-value' not in stderr.getvalue()
    assert 'test-sensitive-exception' not in stderr.getvalue()
    assert '[redacted]' in stderr.getvalue()


def test_codex_adapter_error_only_contract_fixture():
    """Execute the extracted installed adapter functions, including dropped ready."""
    import subprocess
    fixture = Path(__file__).parent / 'fixtures/codex_acp_1_11_mcp_contract.js'
    result = subprocess.run(['node', str(fixture)], capture_output=True, text=True, check=True, timeout=10)
    actual = json.loads(result.stdout)
    assert actual == json.loads(fixture.with_name('codex_acp_1_11_mcp_events.json').read_text())
    assert actual['ready'] == []
    assert actual['success']['sessionUpdate'] == 'tool_call_update'


@pytest.mark.asyncio
async def test_error_only_adapter_confirms_only_current_session_pa_tool_success(tmp_path):
    import copy
    events = json.loads((Path(__file__).parent / 'fixtures/codex_acp_1_11_mcp_events.json').read_text())
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    old = bind(connection)
    client = bind(connection)
    success = events['success']
    assert events['ready'] == []
    assert await client.wait_for_pa_mcp_startup_failure('native-current', timeout=2.0) is None
    assert connection.pa_mcp_health['state'] == 'checking'
    await old.session_update('native-current', success)
    await client.session_update('other-session', success)
    assert connection.pa_mcp_health['state'] == 'checking'
    for change in [
        {'rawInput': {'server': 'another', 'tool': 'list_items'}},
        {'rawOutput': {'result': {'isError': True}, 'error': None}},
        {'rawOutput': {'result': {}, 'error': {'message': 'failed'}}},
        {'rawOutput': None},
        {'status': 'in_progress'},
        {'rawInput': None, 'title': 'mcp.pa.list_items'},
    ]:
        candidate = copy.deepcopy(success)
        candidate.update(change)
        await client.session_update('native-current', candidate)
        assert connection.pa_mcp_health['state'] == 'checking'
    await client.session_update('native-current', success)
    assert connection.pa_mcp_health['state'] == 'connected'
    await client.session_update('native-current', events['failed'][0])
    assert connection.pa_mcp_health['state'] == 'disconnected'
    await client.session_update('native-current', success)
    assert connection.pa_mcp_health['state'] == 'disconnected'
    await client.session_update('native-current', {**success, 'sessionUpdate': 'tool_call', 'status': 'in_progress', 'rawOutput': None})
    await client.session_update('native-current', success)
    assert connection.pa_mcp_health['state'] == 'connected'
    assert 'detail' not in connection.pa_mcp_health
    await old.session_update('native-current', events['failed'][0])
    assert connection.pa_mcp_health['state'] == 'connected'


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid', ['', '..', '../codex', 'codex/install', 'codex?x', 'codex#x', '%2e%2e', 'a\\b', 'a\n'])
@pytest.mark.parametrize('field', ['provider_id', 'job_id'])
async def test_provider_path_segments_rejected_before_http(tmp_path, invalid, field):
    from pa.mcp.context import registration_context
    from pa.mcp.tools.agent_providers import register_mcp
    from tests.test_goal_assigned_service_mcp import FakeMcp
    with patch.dict(os.environ, {'PA_DATA_DIR': str(tmp_path)}, clear=True):
        mcp = FakeMcp()
        register_mcp(mcp, registration_context())
        arguments = {'provider_id': 'codex', 'job_id': 'job-test', field: invalid}
        with patch('pa.mcp.local_api.request_local_pa') as request:
            with pytest.raises(ValueError, match='Invalid provider or job identifier'):
                await mcp.functions['agent_provider_login_cancel'](**arguments)
            request.assert_not_called()


@pytest.mark.asyncio
async def test_completion_started_before_new_failure_cannot_recover(tmp_path):
    events = json.loads((Path(__file__).parent / 'fixtures/codex_acp_1_11_mcp_events.json').read_text())
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), agent_name='codex')
    client = bind(connection)
    success = events['success']
    start = {**success, 'sessionUpdate': 'tool_call', 'status': 'in_progress', 'rawOutput': None}
    await client.session_update('native-current', events['failed'][0])
    await client.session_update('native-current', start)
    # A newer failure invalidates the first recovery attempt.
    await client.session_update('native-current', events['failed'][0])
    await client.session_update('native-current', success)
    await emit(client, 'completed')  # No new startup attempt either.
    assert connection.pa_mcp_health['state'] == 'disconnected'
    fresh = {**start, 'toolCallId': 'fresh-recovery'}
    await client.session_update('native-current', fresh)
    await client.session_update('native-current', {**success, 'toolCallId': 'fresh-recovery'})
    assert connection.pa_mcp_health['state'] == 'connected'
