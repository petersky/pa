"""Real private ledger work is observable without changing its authority contract."""
import json
import threading
import time
from dataclasses import asdict
from unittest.mock import patch

import pytest

from pa.collaboration.models import ModeTransitionRequest, ModeTransitionResult
from pa.collaboration.store import CollaborationStore
from pa.core.async_runtime import AsyncRuntime
from pa.core.operation_budget import OperationBudget, OperationDeadline, current_operation
from pa.core.performance import OperationObservation, current_observation


@pytest.fixture
def ledger(tmp_path):
    store = CollaborationStore(tmp_path)
    request = ModeTransitionRequest(session_id='private-session', requested_mode='default',
        purpose='private-purpose', intended_next_action='private-action', idempotency_key='private-key')
    result = ModeTransitionResult(status='approved_applied', requested_mode='default',
        effective_mode='default', reason='private-result')
    store.save_mode_request(request, result)
    return store, request, result


def test_real_receipt_sql_and_named_lock_without_payloads(ledger):
    store, request, result = ledger
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        receipt = store.get_mode_request(request.session_id, request.idempotency_key)
    finally:
        current_observation.reset(token)
    assert receipt[1] == request and receipt[2] == result
    assert observation.sqlite_calls >= 4 and observation.sqlite_rows == 1
    assert observation.sqlite_statement_ms > 0
    assert observation.sqlite_statement_thread_cpu_ms > 0
    assert observation.python_lock_hold_ms > 0 and observation.outer_lock_hold_ms > 0
    assert observation.lock_events_total == 1 and observation.lock_depth == 0
    assert [event['name'] for event in observation.lock_events] == ['collaboration.store']
    numeric = json.dumps(asdict(observation))
    assert all(value not in numeric for value in ['private-session','private-key','private-purpose','private-result','SELECT','collaboration.db'])


@pytest.mark.parametrize('action', ['read', 'duplicate_write', 'rollback'])
def test_observation_preserves_deadline_phase_and_transaction_results(ledger, action):
    store, request, result = ledger
    deadline = OperationDeadline(OperationBudget(), time.monotonic())
    deadline.execution_started(time.monotonic())
    before = (deadline.phase, deadline.last_progress, deadline.expires_at())
    observation = OperationObservation()
    dt = current_operation.set(deadline); ot = current_observation.set(observation)
    try:
        with patch.object(deadline, 'lock_wait', wraps=deadline.lock_wait) as wait, patch.object(deadline, 'lock_acquired', wraps=deadline.lock_acquired) as acquire:
            if action == 'read':
                assert store.get_mode_request(request.session_id, request.idempotency_key)[2] == result
            elif action == 'duplicate_write':
                assert store.save_mode_request(request, result) == result.model_copy(update={'duplicate':True})
            else:
                with pytest.raises(RuntimeError, match='rollback fixture'):
                    with store._observed_lock(), store._conn() as conn:
                        conn.execute('DELETE FROM collaboration_mode_requests')
                        raise RuntimeError('rollback fixture')
            assert wait.call_count == acquire.call_count == 0
    finally:
        current_observation.reset(ot); current_operation.reset(dt)
    assert (deadline.phase, deadline.last_progress, deadline.expires_at()) == before
    # Includes rollback: the original receipt remains durably readable.
    assert CollaborationStore(store.path.parent).get_mode_request(request.session_id, request.idempotency_key)[2] == result
    assert observation.lock_depth == 0 and observation.lock_events_total == 1


def test_lock_history_is_bounded_and_observers_are_optional(ledger):
    store, request, _ = ledger
    with patch('pa.core.sqlite_metrics.time.thread_time', side_effect=AssertionError('inactive CPU clock')), patch('pa.core.operation_budget.time.monotonic', side_effect=AssertionError('inactive lock clock')):
        assert store.get_mode_request(request.session_id, request.idempotency_key)
    observation = OperationObservation(); token = current_observation.set(observation)
    try:
        for _ in range(100):
            assert store.get_mode_request(request.session_id, request.idempotency_key)
    finally:
        current_observation.reset(token)
    assert observation.lock_events_total == 100 and len(observation.lock_events) == 64
    assert {event['name'] for event in observation.lock_events} == {'collaboration.store'}


def test_existing_rlock_still_serializes_competing_reader(ledger):
    store, request, _ = ledger
    attempted = threading.Event(); finished = threading.Event(); errors = []
    original = store._lock
    class AnnouncedLock:
        def acquire(self):
            attempted.set(); return original.acquire()
        def release(self):
            return original.release()
    store._lock = AnnouncedLock()
    observation = OperationObservation()
    def read():
        token = current_observation.set(observation)
        try:
            assert store.get_mode_request(request.session_id, request.idempotency_key)
        except BaseException as error:
            errors.append(error)
        finally:
            current_observation.reset(token); finished.set()
    with original:
        thread = threading.Thread(target=read); thread.start()
        assert attempted.wait(2) and not finished.is_set()
    thread.join(3)
    assert not thread.is_alive() and not errors
    assert observation.python_lock_wait_ms > 0 and observation.lock_events_total == 1


@pytest.mark.asyncio
async def test_native_worker_retains_real_receipt_phases(ledger):
    store, request, result = ledger
    runtime = AsyncRuntime(max_workers=1)
    try:
        receipt = await runtime.run_blocking('collaboration.mode_receipt_read', store.get_mode_request, request.session_id, request.idempotency_key)
        assert receipt[2] == result
        await runtime.close()
        operation = runtime.snapshot()['operations']['collaboration.mode_receipt_read']
        assert operation['completed'] == 1 and operation['failed'] == operation['timed_out'] == operation['rejected'] == 0
        assert operation['sqlite_calls']['max'] >= 4 and operation['sqlite_rows']['max'] == 1
        assert operation['python_lock_hold_ms']['max'] > 0
        assert operation['sqlite_statement_thread_cpu_ms']['max'] > 0
    finally:
        await runtime.close()
