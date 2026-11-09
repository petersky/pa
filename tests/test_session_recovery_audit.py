import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch
import asyncio

import pytest
from fastapi import HTTPException

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.modules.agent_chat import audit_session_recovery, SessionRecoveryAuditBody
from tests.test_restart_handoff_reconcile import dismissed


def setup(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    receipt = dismissed(store)
    session = store.save_session(AgentSession(
        id=receipt.session_id, agent_name='codex', status='closed',
        authority_instance_id='owner', principal_id='user:owner',
        external_session_id='saved-native', model_id='saved-model',
        config_json={'configuration': {'reasoning_effort': 'high'}},
        execution_binding={'lease_id': 'retained', 'fencing_token': 403},
    ))
    args = dict(session_id=session.id, expected_version=receipt.phase_version,
                expected_session_version=session.updated_at,
                expected_binding_hash=hashlib.sha256(json.dumps(
                    session.execution_binding, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                owner_instance_id='owner', principal_id='user:owner', idempotency_key='audit-once')
    return store, receipt, session, args


def test_unknown_effect_never_calls_loader_or_delivers(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    with patch.object(store, 'update_restart_handoff', side_effect=AssertionError('rearm')), patch.object(store, 'save_session', side_effect=AssertionError('reopen')):
        result = store.audit_session_recovery(receipt.id, **args)
    assert result['effect'] == 'unknown'
    assert result['state'] == 'blocked'
    assert not result['restored'] and not result['delivery_allowed']
    assert 'explicit_close_or_dismissal_unresolved' in result['blockers']
    assert result['native_history_id'] == 'saved-native'
    assert store.get_session(session.id) == session
    assert store.get_restart_handoff(receipt.id) == receipt
    assert [e.event_type for e in store.list_transcript_events(session.id)] == ['session_recovery_audited']
    assert 'Continue' not in str(result)
    assert not store.pending_restart_continuation(session_id=session.id)


@pytest.mark.parametrize('override', [dict(expected_version=999), dict(principal_id='foreign'),
    dict(owner_instance_id='foreign'), dict(expected_binding_hash='0'*64), dict(idempotency_key=''),
    dict(session_id='foreign')])
def test_stale_foreign_and_lease_snapshot_preserve_state(tmp_path, override):
    store, receipt, session, args = setup(tmp_path)
    args.update(override)
    with pytest.raises(ValueError):
        store.audit_session_recovery(receipt.id, **args)
    assert store.get_restart_handoff(receipt.id) == receipt
    assert store.get_session(session.id) == session
    assert store.list_transcript_events(session.id) == []


def test_duplicate_audit_is_durable_and_concurrent(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: store.audit_session_recovery(receipt.id, **args), range(2)))
    assert results[0] == results[1]
    reopened = CardProjection(tmp_path / 'pa.db')
    assert reopened.audit_session_recovery(receipt.id, **args) == results[0]
    assert len(store.list_transcript_events(session.id)) == 1
    with pytest.raises(ValueError):
        store.audit_session_recovery(receipt.id, **(args | {'expected_version': 999}))


def test_authoritative_completion_is_observation_not_delivery_permission(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    unknown = store.audit_session_recovery(receipt.id, **args)
    store.append_transcript_events([TranscriptEvent(session_id=session.id,
        seq=store.next_transcript_seq(session.id), event_type='turn_completed',
        payload={'queued_prompt_id': receipt.continuation_prompt_id})])
    assert store.audit_session_recovery(receipt.id, **args) == unknown
    confirmed = store.audit_session_recovery(receipt.id, **(args | {'idempotency_key': 'new-observation'}))
    assert confirmed['effect'] == 'confirmed'
    assert not confirmed['delivery_allowed']
    assert 'explicit_close_or_dismissal_unresolved' in confirmed['blockers']
    assert store.get_restart_handoff(receipt.id) == receipt
    assert store.get_session(session.id) == session


def test_pending_receipt_and_missing_native_history_fail_closed(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    from pa.domain.models import RestartHandoff
    session.status = 'idle'
    session.external_session_id = None
    session = store.save_session(session)
    args['expected_session_version'] = session.updated_at
    pending = store.create_restart_handoff(RestartHandoff(session_id=session.id,
        continuation_prompt_id='pending', continuation_prompt='held', idempotency_key='pending', instance_id='owner'))
    result = store.audit_session_recovery(pending.id, **(args | {'expected_version': pending.phase_version}))
    assert 'pending_continuation' in result['blockers']
    assert 'saved_native_history_missing' in result['blockers']
    assert not result['delivery_allowed']


def test_route_uses_authenticated_identity_and_no_runtime_load(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    request = MagicMock()
    request.app.state.ctx.settings.instance_id = 'owner'
    manager = MagicMock(store=store)
    body = SessionRecoveryAuditBody(**{k:v for k,v in args.items() if k not in {'session_id','owner_instance_id','principal_id'}})
    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), patch('pa.modules.agent_chat.get_principal_id', return_value='foreign'):
        with pytest.raises(HTTPException):
            asyncio.run(audit_session_recovery(request, session.id, receipt.id, body))
    manager.create_session.assert_not_called()
    manager.get.assert_not_called()
    assert store.list_transcript_events(session.id) == []


def test_stale_session_and_audit_write_failure_preserve_original(tmp_path):
    from datetime import timedelta
    store, receipt, session, args = setup(tmp_path)
    with pytest.raises(ValueError):
        store.audit_session_recovery(receipt.id, **(args | {
            'expected_session_version': session.updated_at - timedelta(seconds=1)}))
    with patch.object(store, 'append_transcript_events', side_effect=OSError('audit unavailable')):
        with pytest.raises(OSError):
            store.audit_session_recovery(receipt.id, **args)
    assert store.get_session(session.id) == session
    assert store.get_restart_handoff(receipt.id) == receipt
    assert not store.list_transcript_events(session.id)


def test_audit_receipt_survives_transcript_retention(tmp_path):
    store, receipt, session, args = setup(tmp_path)
    original = store.audit_session_recovery(receipt.id, **args)
    store.transcripts.prune([session.id])
    assert store.audit_session_recovery(receipt.id, **args) == original
    assert store.get_session(session.id) == session
    assert store.get_restart_handoff(receipt.id) == receipt
