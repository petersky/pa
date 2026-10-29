"""Native settlement facts retain exact archived receipt/followup guards."""
import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from functools import lru_cache
import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4
from pathlib import Path

import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore
from tests.test_autonomous_verification import system


@lru_cache
def archived_settlement():
    from pa.execution import verification
    # Exact method from fcabfdc75e2f, retained as data for shallow CI checkouts.
    source = Path('tests/fixtures/verification_origins_settled_611.txt').read_text()
    assert hashlib.sha256(source.encode()).hexdigest() == 'ab8349eb14cf1a3267656861af19c9fccb8c4ae5a6d33ef6a38ce1134dc9671d'
    method = ast.parse(source).body[0]
    namespace = dict(vars(verification))
    exec(compile(ast.Module(body=[method], type_ignores=[]), 'exact611-origins-settled', 'exec'), namespace)
    return namespace['origins_settled']


def retained_origin(target, **changes):
    values = dict(mutation_id=str(uuid4()), state='completed', session_id='origin',
        authority_instance_id='instance-a', authority_url='http://fixture.invalid',
        target_instance_id='instance-a',
        acknowledged_at=datetime.now(UTC), completion_delivery_class='acknowledged',
        reconciliation_state='not_required', recoverable=False,
        materialization_plan={'steps': [{'proof': ['x' * 4096]} for _ in range(10)]})
    values.update(changes)
    return target.ctx.services['dispatch_store'].put(DispatchRecord(**values))


def packet(record):
    return dict(origin_dispatches=[record.dispatch_id], pending_repair_controls=[])


CASES = [
    ({}, True),
    ({'state': 'acknowledged', 'acknowledged_at': None}, False),
    ({'state': 'queued'}, False),
    ({'state': 'running'}, False),
    ({'state': 'completion_pending'}, False),
    ({'acknowledged_at': None}, False),
    ({'completion_delivery_class': 'pending'}, False),
    ({'reconciliation_state': 'pending'}, False),
    ({'reconciliation_state': 'blocked'}, False),
    ({'state': 'failed', 'recoverable': True}, False),
    ({'state': 'failed'}, True),
    ({'state': 'cancelled', 'recoverable': True}, False),
    ({'state': 'cancelled'}, True),
    ({'state': 'failed', 'session_id': None, 'recoverable': True,
      'acknowledged_at': None, 'reconciliation_state': 'not_requested'}, True),
    ({'state': 'cancelled', 'session_id': None, 'recoverable': True,
      'acknowledged_at': None, 'reconciliation_state': 'not_requested'}, True),
    ({'state': 'failed', 'session_id': None, 'recoverable': True,
      'prompt_acknowledged_at': datetime.now(UTC)}, False),
    ({'state': 'failed', 'session_id': None, 'recoverable': True,
      'initial_prompt_operation': {'state': 'pending'}}, False),
    ({'followup_operations': {'k': {'state': 'pending'}}}, False),
    ({'followup_operations': {'k': {'state': 'delivery_ambiguous'}}}, False),
    ({'followup_operations': {'k': {'state': 'delivery_not_admitted'}}}, True),
    ({'followup_operations': {'k': {'state': 'delivery_not_admitted', 'response': {'accepted': True}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 403}}}}, True),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status': 409}}}}, True),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 403, 'status': 403}}}}, True),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 403, 'status': 409}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 403, 'status': '403'}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': True, 'status_code': 403}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 503}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': '403'}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': {'recoverable': False, 'status_code': 403}, 'response': {'accepted': True}}}}, False),
    ({'followup_operations': {'k': {'state': 'failed', 'error': [], 'response': []}}}, False),
    ({'followup_turns': [{'state': 'running', 'delivery_state': 'acknowledged'}]}, False),
    ({'followup_turns': [{'state': 'ended', 'delivery_state': 'pending'}]}, False),
    ({'followup_turns': [{'state': 'ended', 'delivery_state': 'acknowledged'}]}, True),
    ({'followup_turns': [{'state': 'ended', 'delivery_state': 'acknowledged', 'prompt_id': 'p'}],
      'followup_operations': {'k': {'prompt_id': 'p', 'state': 'pending'}}}, True),
    ({'followup_turns': [{'state': 'ended', 'delivery_state': 'acknowledged', 'idempotency_key': 'k'}],
      'followup_operations': {'k': {'state': 'pending'}}}, True),
    ({'followup_turns': [{'state': 'ended', 'delivery_state': 'acknowledged', 'prompt_id': 'p'}],
      'followup_operations': {'k': {'response': {'prompt_id': 'p', 'accepted': True}}}}, True),
    ({'state': 'failed', 'session_id': None, 'followup_operations': {'k': {'state': 'pending'}}}, False),
]


@pytest.mark.parametrize('changes,expected', CASES)
def test_complete_settlement_matches_archived_guard_matrix(system, changes, expected):
    record = retained_origin(system, **changes)
    request = packet(record)
    assert archived_settlement()(system.service, request) is expected
    assert system.service.origins_settled(request) is expected
    with patch.object(system.ctx.services['dispatch_store'], 'verification_settlement', None):
        assert system.service.origins_settled(request) is expected
    assert not system.calls


def test_native_settlement_does_not_copy_retained_models(system):
    record = retained_origin(system)
    expected = archived_settlement()(system.service, packet(record))
    with patch.object(DispatchRecord, 'model_copy', side_effect=AssertionError('No retained history copy')):
        assert system.service.origins_settled(packet(record)) == expected
    assert not system.calls


def test_missing_empty_and_pending_controls_keep_short_circuits(system):
    assert not system.service.origins_settled(dict(origin_dispatches=['missing'], pending_repair_controls=[]))
    assert system.service.origins_settled(dict(origin_dispatches=[], pending_repair_controls=[]))
    ledger = system.ctx.services['dispatch_store']
    with patch.object(ledger, 'verification_settlement', side_effect=AssertionError('Pending repair must stop before reads')):
        assert not system.service.origins_settled(dict(origin_dispatches=['missing'], pending_repair_controls=['retry']))


def test_nonterminal_native_does_not_inspect_followup_payload(system):
    record = retained_origin(system, state='running')
    with patch('pa.execution.followup.dispatch_followup_pending', side_effect=AssertionError('Nonterminal shortcut')):
        assert not system.service.origins_settled(packet(record))


def test_projection_is_immutable_and_fresh_after_durable_followup(system):
    record = retained_origin(system)
    ledger = system.ctx.services['dispatch_store']
    before = ledger.verification_settlement(record.dispatch_id)
    with pytest.raises(FrozenInstanceError):
        before.followup_pending = True
    record.followup_operations['new-key'] = {'state': 'delivery_ambiguous'}
    ledger.put(record)
    after = ledger.verification_settlement(record.dispatch_id)
    assert not before.followup_pending and after.followup_pending
    assert ledger.verification_settlement('missing') is None
    assert all(isinstance(v, (str, bool, type(None))) for v in vars(after).values())


def test_settlement_observes_only_committed_publication(system):
    record = retained_origin(system)
    ledger = system.ctx.services['dispatch_store']
    before = ledger.verification_settlement(record.dispatch_id)
    record.followup_operations['new-key'] = {'state': 'pending'}
    entered, release = threading.Event(), threading.Event()
    def latch(boundary):
        if boundary == 'commit_before':
            entered.set()
            assert release.wait(2)
    ledger._fault_injector = latch  # Disposable private transaction latch only.
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(ledger.put, record)
            try:
                assert entered.wait(2)
                assert ledger.verification_settlement(record.dispatch_id) == before
                assert system.service.origins_settled(packet(record))
            finally:
                release.set()
            future.result(timeout=2)
        assert ledger.verification_settlement(record.dispatch_id).followup_pending
        assert not system.service.origins_settled(packet(record))
    finally:
        release.set()
        ledger._fault_injector = None


@pytest.mark.parametrize('custom', ['unsupported', 'getter', 'snapshot', 'legacy_nonterminal'])
def test_custom_and_legacy_full_read_contract_remains(system, custom):
    record = retained_origin(system)
    ledger = system.ctx.services['dispatch_store']
    if custom == 'unsupported':
        with patch.object(ledger, 'verification_settlement', None), patch.object(ledger, 'get', wraps=ledger.get) as get:
            assert system.service.origins_settled(packet(record))
            get.assert_called_once_with(record.dispatch_id)
    elif custom == 'getter':
        with patch.object(ledger, 'get', return_value=None) as get:
            assert not system.service.origins_settled(packet(record))
            get.assert_called_once_with(record.dispatch_id)
    elif custom == 'snapshot':
        with patch.object(ledger, '_snapshot', return_value=SimpleNamespace(state='running')) as snapshot:
            assert not system.service.origins_settled(packet(record))
            snapshot.assert_called_once()
    else:
        system.ctx.services['dispatch_store'] = SimpleNamespace(get=lambda _: SimpleNamespace(state='running'))
        try:
            assert not system.service.origins_settled(packet(record))
        finally:
            system.ctx.services['dispatch_store'] = ledger


def test_native_settlement_keeps_readable_guard(system):
    ledger = system.ctx.services['dispatch_store']
    with patch.object(ledger, '_require_readable', side_effect=RuntimeError('closed')):
        with pytest.raises(RuntimeError, match='closed'):
            ledger.verification_settlement('missing')
