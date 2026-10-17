"""New ref-lock telemetry must not select a different admission wait budget."""
import threading
import time
from unittest.mock import patch

import pytest

from pa.core.operation_budget import OperationBudget, OperationDeadline, current_operation, measured_lock
from pa.core.performance import OperationObservation, current_observation
from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@pytest.mark.parametrize('operation', ['reload', 'get_head', 'advance', 'append'])
def test_ref_metrics_preserve_previous_deadline_phase(tmp_path, operation):
    objects = ObjectStore(tmp_path / 'objects')
    log = EventLog(objects, tmp_path / 'data', 'fixture')
    head = objects.put_json(SyncCommit(hash='', realm_id='default',
        instance_id='fixture', parent_hashes=[], event_hashes=[]).model_dump(mode='json'))
    deadline = OperationDeadline(OperationBudget(), time.monotonic())
    deadline.execution_started(time.monotonic())
    observation = OperationObservation()
    budget_token = current_operation.set(deadline)
    observation_token = current_observation.set(observation)
    try:
        with patch.object(deadline, 'lock_wait', wraps=deadline.lock_wait) as waiting, \
             patch.object(deadline, 'lock_acquired', wraps=deadline.lock_acquired) as acquired:
            if operation == 'reload':
                log.reload_refs()
            elif operation == 'get_head':
                assert log.get_head('default') is None
            elif operation == 'advance':
                log.advance_ref('default', head, expected_head=None, update_index=False)
                assert log.read_cached_head('default') == head
            else:
                _, commit = log.append_event(CardEvent(type=EventType.CARD_CREATED,
                    realm_id='default', card_id='fixture-card', author_instance='fixture',
                    author_principal='user:local', payload={'title': 'fixture'}))
                assert log.read_cached_head('default') == commit.hash
            # These raw ref locks previously did not change the budget phase.
            # Configured lock/idle budgets can differ, so observing them must not
            # switch an existing idle deadline to a longer lock deadline.
            assert waiting.call_count == acquired.call_count == 0
    finally:
        current_observation.reset(observation_token)
        current_operation.reset(budget_token)
    assert deadline.phase == 'executing'
    assert {'sync.refs', 'sync.refs_file'} <= {e['name'] for e in observation.lock_events}
    assert observation.file_lock_hold_ms > 0 and observation.python_lock_hold_ms > 0


def test_preexisting_budgeted_locks_keep_their_deadline_contract():
    deadline = OperationDeadline(OperationBudget(), time.monotonic())
    deadline.execution_started(time.monotonic())
    token = current_operation.set(deadline)
    try:
        with patch.object(deadline, 'lock_wait', wraps=deadline.lock_wait) as waiting, \
             patch.object(deadline, 'lock_acquired', wraps=deadline.lock_acquired) as acquired:
            with measured_lock(threading.Lock(), name='projection.mutation'):
                assert deadline.phase == 'executing'
            assert waiting.call_count == acquired.call_count == 1
    finally:
        current_operation.reset(token)
