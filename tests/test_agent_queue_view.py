"""Queue presentation follows the real ordering and control-mode gates."""
from unittest.mock import Mock

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt


@pytest.fixture
def runtime(tmp_path):
    store = CardProjection(tmp_path / 'queue.db')
    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_enabled=False), store)
    session = store.save_session(AgentSession(agent_name='codex', purpose='chat'))
    runtime = AgentSessionRuntime(manager, session)
    # No provider is launched: exercise real enqueue/reorder, never drain.
    runtime._start_drain = Mock()
    yield runtime


def test_fifo_priority_insertion_and_explicit_reorder(runtime):
    auto = runtime.enqueue('Automated update', source='pr-supervisor')
    first = runtime.enqueue('First human', source='ui')
    second = runtime.enqueue('Second human', source='ui')
    response = runtime.enqueue('Correlated answer', source='notification-response')
    rows = runtime.queue_view()
    assert [r['id'] for r in rows] == [first.id, second.id, response.id, auto.id]
    assert [r['display']['source'] for r in rows] == ['Human', 'Human', 'Response', 'Automated']
    assert [r['display']['position'] for r in rows] == [1, 2, 3, 4]
    assert [r['id'] for r in rows if r['display']['is_next']] == [first.id]
    runtime.reorder_queue([auto.id, second.id, first.id, response.id])
    assert [r['id'] for r in runtime.queue_view()] == [auto.id, second.id, first.id, response.id]
    assert runtime.queue_view()[0]['display']['is_next']
    # Reading cues changes neither priority nor durable prompt identity/order.
    before = [q.model_dump() for q in runtime._queue]
    runtime.snapshot(include_transcript=False)
    assert before == [q.model_dump() for q in runtime._queue]


def test_human_control_skips_held_item_and_pause_does_not_change_order(runtime):
    auto = runtime.enqueue('Automated', source='pr-supervisor')
    human = runtime.enqueue('Human', source='ui')
    runtime.reorder_queue([auto.id, human.id])
    runtime.session.control_mode = 'human'
    runtime.pause_queue()
    rows = runtime.snapshot(include_transcript=False)['queue']
    assert rows[0]['display']['eligibility'] == 'held'
    assert rows[0]['display']['is_next'] is False
    assert rows[1]['display'] == dict(position=2, source='Human', eligibility='eligible', is_next=True)
    runtime.resume_queue()
    assert runtime.queue_view() == rows
    assert not runtime.queue_paused


@pytest.mark.parametrize('gate', ['admission', 'restart'])
def test_unresolved_gate_does_not_promise_later_item_is_next(runtime, gate):
    first = QueuedPrompt(message='Waiting', source='restart-handoff:fixture' if gate == 'restart' else 'ui',
                         admission_pending=gate == 'admission')
    second = QueuedPrompt(message='Human', source='ui')
    runtime._queue = [first, second]
    rows = runtime.queue_view()
    assert rows[0]['display']['eligibility'] == ('checking' if gate == 'restart' else 'admission_pending')
    assert not any(r['display']['is_next'] for r in rows)
    if gate == 'admission':
        first.admission_pending = False
        assert runtime.queue_view()[0]['display']['is_next']
    else:
        runtime._queue.remove(first)
        assert runtime.queue_view()[0]['id'] == second.id
        assert runtime.queue_view()[0]['display']['is_next']
