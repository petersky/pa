import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pa.domain.models import AgentSession
from pa.instance.recovery_custody import inspect_recovery_custody
from tests.test_workspace_binding_inspection import fixture


def read(manager, args):
    return inspect_recovery_custody(manager, 's', **args)


def replace_config(store, value):
    # Disposable adversarial fixture, never a production state mutation.
    with store._conn() as conn:
        conn.execute('UPDATE agent_sessions SET config_json=? WHERE id=?', (value, 's'))


def test_checkpoint_queue_order_native_and_all_state_are_preserved(tmp_path):
    mgr, session, leases, args = fixture(tmp_path)
    before = mgr.store.get_session('s')
    with patch.object(mgr.store, 'get_session', side_effect=AssertionError('private hydration')), \
         patch.object(mgr.store, 'save_session', side_effect=AssertionError('mutation')):
        result = read(mgr, args)
    assert result['checkpoint'] == {'state': 'object', 'in_flight': {'state': 'null'},
        'queue': {'state': 'observed', 'count': 2, 'prompt_ids': ['q1', 'q2'], 'duplicate_ids': False}}
    assert result['native_registry']['unique_in_observed_registry'] is True
    assert result['native_registry']['matching_sessions_lower_bound'] == 1
    assert result['native_exclusive_ownership'] == 'unknown'
    assert result['runtime_inactivity'] == 'not_observed'
    assert result['effect'] == 'none' and not result['restored'] and not result['delivery_allowed']
    assert result['historical_effect'] == 'unknown'
    assert 'PRIVATE' not in json.dumps(result) and 'DO_NOT_EMIT' not in json.dumps(result)
    assert mgr.store.get_session('s') == before and mgr.store.list_transcript_events('s') == []
    assert leases[0]['fencing_token'] == 373


@pytest.mark.parametrize('config,state,flight,queue', [
    ({}, 'absent', 'unavailable', 'unavailable'),
    ({'durable_runtime': None}, 'null', 'unavailable', 'unavailable'),
    ({'durable_runtime': {}}, 'object', 'absent', 'absent'),
    ({'durable_runtime': {'in_flight': None, 'queued_prompts': None}}, 'object', 'null', 'null'),
    ({'durable_runtime': {'in_flight': {}, 'queued_prompts': []}}, 'object', 'invalid', 'observed'),
    ({'durable_runtime': {'in_flight': {'id': 'flight', 'text': 'PRIVATE'}, 'queued_prompts': []}}, 'object', 'identified', 'observed'),
    ({'durable_runtime': []}, 'invalid', 'unavailable', 'unavailable'),
    ({'durable_runtime': {'in_flight': False, 'queued_prompts': [False]}}, 'object', 'invalid', 'invalid'),
])
def test_missing_null_and_invalid_are_not_conflated(tmp_path, config, state, flight, queue):
    mgr, _, _, args = fixture(tmp_path)
    replace_config(mgr.store, json.dumps(config))
    r = read(mgr, args)['checkpoint']
    assert r['state'] == state and r['in_flight']['state'] == flight and r['queue']['state'] == queue
    if flight == 'identified':
        assert r['in_flight']['prompt_id'] == 'flight'
    assert 'PRIVATE' not in json.dumps(r)


@pytest.mark.parametrize('raw,state', [('{', 'unavailable'), ('null', 'unavailable'),
    (json.dumps({'secret': 'PRIVATE' * 50000}), 'oversized')])
def test_malformed_and_large_private_config_never_escape_sql(tmp_path, raw, state):
    mgr, _, _, args = fixture(tmp_path)
    replace_config(mgr.store, raw)
    metadata = mgr.store.recovery_custody_metadata('s')
    assert 'PRIVATE' not in json.dumps(metadata)
    assert read(mgr, args)['checkpoint']['state'] == state


@pytest.mark.parametrize('queue,state', [([{'id': 'q'}] * 33, 'capacity_exceeded'),
    ([{'id': 'q' * 201}], 'invalid'), ([{'id': 1}], 'invalid'), ([{'id': 'bad\nID'}], 'invalid')])
def test_queue_capacity_and_identity_shape_remain_explicit(tmp_path, queue, state):
    mgr, _, _, args = fixture(tmp_path)
    replace_config(mgr.store, json.dumps({'durable_runtime': {'queued_prompts': queue}}))
    r = read(mgr, args)['checkpoint']['queue']
    assert r['state'] == state and 'prompt_ids' not in r


def test_duplicate_ids_remain_in_exact_order_without_deduplication(tmp_path):
    mgr, _, _, args = fixture(tmp_path)
    replace_config(mgr.store, json.dumps({'durable_runtime': {'queued_prompts': [{'id': 'q'}] * 2}}))
    queue = read(mgr, args)['checkpoint']['queue']
    assert queue['prompt_ids'] == ['q', 'q'] and queue['duplicate_ids']


@pytest.mark.parametrize('raw', [
    '{"durable_runtime":{"in_flight":null,"in_flight":{"id":"flight"}}}',
    '{"durable_runtime":{"queued_prompts":[],"queued_prompts":[{"id":"queued"}]}}',
    '{"durable_runtime":{"queued_prompts":[{"id":"first","id":"last"}]}}',
    '{"durable_runtime":{"in_flight":null,"in_\\u0066light":{"id":"flight"}}}',
])
def test_duplicate_json_keys_cannot_create_false_null_or_queue_identity_proof(tmp_path, raw):
    mgr, _, _, args = fixture(tmp_path)
    replace_config(mgr.store, raw)
    result = read(mgr, args)['checkpoint']
    assert result['state'] == 'unavailable'
    assert result['in_flight']['state'] == result['queue']['state'] == 'unavailable'


def alias(store, id, *, principal='owner', agent='codex', native='native'):
    return store.save_session(AgentSession(id=id, agent_name=agent, external_session_id=native,
        authority_instance_id='other-authority', principal_id=principal, status='closed',
        config_json={'secret': 'PRIVATE_ALIAS_CONFIG'}))


def test_aliases_are_exact_provider_scoped_include_closed_and_hide_foreign_principals(tmp_path):
    mgr, _, _, args = fixture(tmp_path)
    alias(mgr.store, 'owned-alias')
    alias(mgr.store, 'PRIVATE_FOREIGN_ID', principal='PRIVATE_PRINCIPAL')
    alias(mgr.store, 'different-agent', agent='cursor')
    alias(mgr.store, 'different-native', native='other')
    r = read(mgr, args)['native_registry']
    assert r['matching_sessions_lower_bound'] == 3 and not r['unique_in_observed_registry']
    assert {a['id'] for a in r['visible_sessions']} == {'s', 'owned-alias'}
    assert r['hidden_matches_present'] and 'PRIVATE' not in json.dumps(r)
    admin = read(mgr, args | {'principal_id': 'admin', 'is_admin': True})['native_registry']
    assert len(admin['visible_sessions']) == 3 and not admin['hidden_matches_present']


def test_alias_capacity_and_missing_native_never_claim_exclusive_ownership(tmp_path):
    mgr, _, _, args = fixture(tmp_path)
    for i in range(35):
        alias(mgr.store, str(i))
    r = read(mgr, args)['native_registry']
    assert r['truncated'] and r['matching_sessions_lower_bound'] == 33
    assert r['unique_in_observed_registry'] is None and len(r['visible_sessions']) <= 32
    (tmp_path / 'missing').mkdir()
    mgr, s, _, args = fixture(tmp_path / 'missing', native_id=None)
    r = read(mgr, args | {'expected_native_id': ''})
    assert r['native_registry']['state'] == 'native_identity_missing'
    assert r['native_registry']['unique_in_observed_registry'] is None
    assert r['native_exclusive_ownership'] == 'unknown'


@pytest.mark.parametrize('change', [ {'principal_id': 'foreign'}, {'expected_native_id': 'wrong'},
    {'expected_binding_hash': '0' * 64}, {'expected_lease_hash': '0' * 64},
    {'expected_session_version': datetime(2000, 1, 1, tzinfo=UTC)}, {'owner_instance_id': 'wrong'}])
def test_every_existing_precondition_is_checked_before_checkpoint_read(tmp_path, change):
    mgr, _, _, args = fixture(tmp_path)
    with patch.object(mgr.store, 'recovery_custody_metadata', side_effect=AssertionError('unauthorized read')):
        with pytest.raises((ValueError, PermissionError)):
            read(mgr, args | change)


@pytest.mark.parametrize('race', ['checkpoint', 'alias', 'lease', 'binding'])
def test_snapshot_races_reject_without_any_repair(tmp_path, race):
    mgr, s, leases, args = fixture(tmp_path)
    original = mgr.store.recovery_custody_metadata
    calls = 0
    def read_once(*a):
        nonlocal calls
        result = original(*a)
        calls += 1
        if calls == 1:
            if race == 'checkpoint': replace_config(mgr.store, '{}')
            elif race == 'alias': alias(mgr.store, 'new-alias')
            elif race == 'lease': leases[0]['fencing_token'] += 1
            else: mgr.store.save_session(s.model_copy(update={'updated_at': s.updated_at + timedelta(seconds=1)}))
        return result
    with patch.object(mgr.store, 'recovery_custody_metadata', side_effect=read_once):
        with pytest.raises(ValueError): read(mgr, args)


def test_alias_lookup_uses_existing_native_index(tmp_path):
    mgr, _, _, _ = fixture(tmp_path)
    with mgr.store._conn() as conn:
        plan = conn.execute('EXPLAIN QUERY PLAN SELECT id FROM agent_sessions WHERE external_session_id=? AND agent_name=? ORDER BY id LIMIT 33', ('native', 'codex')).fetchall()
    assert 'idx_session_native_history' in str([tuple(r) for r in plan])


def test_encoded_response_budget_is_enforced_for_bounded_unicode_identifiers(tmp_path):
    mgr, _, _, args = fixture(tmp_path)
    for i in range(31):
        alias(mgr.store, '\U0001f600' * 197 + str(i).zfill(2))
    with pytest.raises(ValueError, match='output exceeds capacity'):
        read(mgr, args)


def test_oversized_alias_metadata_is_bounded_in_sql_before_python(tmp_path):
    mgr, _, _, args = fixture(tmp_path)
    alias(mgr.store, 'bad-alias')
    with mgr.store._conn() as conn:
        conn.execute('UPDATE agent_sessions SET principal_id=? WHERE id=?', ('PRIVATE' * 10000, 'bad-alias'))
    metadata = mgr.store.recovery_custody_metadata('s')
    assert 'PRIVATE' not in json.dumps(metadata)
    with pytest.raises(ValueError, match='alias shape'):
        read(mgr, args)


def test_http_privacy_read_only_errors_and_mcp_forwarding(tmp_path):
    from pa.modules.agent_chat import router
    from pa.mcp.tools.agent_chat import register_mcp
    from tests.test_instance_mcp import FakeMcp
    mgr, s, _, args = fixture(tmp_path)
    app = FastAPI(); app.state.ctx = SimpleNamespace(settings=SimpleNamespace(instance_id='instance'))
    app.include_router(router)
    params = dict(expected_authority_instance_id='instance', expected_session_version=s.updated_at.isoformat(),
        expected_native_id='native', expected_binding_hash=args['expected_binding_hash'], expected_lease_hash=args['expected_lease_hash'])
    with patch('pa.modules.agent_chat._manager', return_value=mgr), patch('pa.modules.agent_chat.get_principal_id', return_value='owner'), TestClient(app) as client:
        url = '/agent/observability/v1/sessions/s/recovery-custody'
        assert client.get(url).status_code == 422
        good = client.get(url, params=params)
        assert good.status_code == 200 and 'PRIVATE' not in good.text
        with patch.object(mgr.store, 'recovery_custody_metadata', side_effect=OSError('PRIVATE_ERROR')):
            error = client.get(url, params=params)
        assert error.status_code == 503 and 'unknown' in error.text and 'PRIVATE' not in error.text
        for response in (good, error): assert response.headers['cache-control'] == 'private, no-store'
        with patch('pa.modules.agent_chat.get_principal_id', return_value='foreign'):
            denied = client.get(url, params=params)
        assert denied.status_code == 403 and denied.headers['cache-control'] == 'private, no-store'
    mcp = FakeMcp()
    with patch('pa.mcp.local_api.request_local_pa', return_value={'effect': 'none'}) as call:
        register_mcp(mcp, SimpleNamespace(settings='settings'))
        mcp.functions['get_agent_session_recovery_custody']('s/path', 'owner', 'version', '', 'a' * 64, 'b' * 64)
    assert call.call_args.args[:2] == ('settings', 'GET')
    assert 's%2Fpath/recovery-custody' in call.call_args.args[2]
    assert call.call_args.kwargs['params']['expected_native_id'] == ''
    assert call.call_args.kwargs['max_response_bytes'] == 64 * 1024
