from concurrent.futures import ThreadPoolExecutor

import pytest

from pa.domain.models import RestartHandoff, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.restart_lifecycle import RestartTransitionConflict


def receipt(store, **fields):
    return store.create_restart_handoff(RestartHandoff(session_id="exact-session", idempotency_key="once", continuation_prompt="Continue", continuation_prompt_id="exact-prompt", instance_id="owner", **fields))


def advance(store, current, status, **fields):
    return store.update_restart_handoff(current.id, status=status, expected_status=current.status, expected_version=current.phase_version, owner_instance_id="owner", **fields)


def completion(store):
    store.append_transcript_events([TranscriptEvent(session_id="exact-session", seq=1, event_type="turn_completed", payload={"queued_prompt_id": "exact-prompt", "stop_reason": "end_turn"})])


def test_receipt_cas_and_actual_completion_evidence(tmp_path):
    store = CardProjection(tmp_path / "cards.db")
    current = receipt(store)
    with pytest.raises(RestartTransitionConflict, match="Illegal"):
        advance(store, current, "continuation_queued")
    with pytest.raises(RestartTransitionConflict, match="owner"):
        store.update_restart_handoff(current.id, status="waiting_for_turn_end", expected_status="requested", expected_version=0, owner_instance_id="wrong-owner")
    for status in ("waiting_for_turn_end", "quiescing", "restarting", "resuming", "continuation_queued"):
        current = advance(store, current, status)
    assert current.observation["phase"] == "accepted"
    assert current.observation["effect_state"] == "pending"
    with pytest.raises(RestartTransitionConflict, match="completion evidence"):
        advance(store, current, "continuation_delivered")
    completion(store)
    delivered = advance(store, current, "continuation_delivered")
    with pytest.raises(RestartTransitionConflict):
        advance(store, current, "failed", error="delayed callback")
    assert advance(store, current, "continuation_delivered") == delivered
    assert delivered.observation["phase"] == "succeeded"
    assert store.get_restart_handoff(current.id).error is None


def test_competing_callbacks_preserve_failure_and_reconcile_same_identity(tmp_path):
    store = CardProjection(tmp_path / "cards.db")
    original = receipt(store, status="resuming")
    completion(store)  # Turn completed before the original receipt callback.
    def callback(status):
        peer = CardProjection(tmp_path / "cards.db")
        try:
            return advance(peer, original, status, error="lost acknowledgement" if status == "failed" else None)
        except RestartTransitionConflict:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(callback, ["failed", "continuation_delivered"]))
    current = store.get_restart_handoff(original.id)
    if current.status == "failed":
        current = advance(store, current, "continuation_delivered")
    assert current.continuation_prompt_id == original.continuation_prompt_id
    assert current.status == "continuation_delivered"
    failed = next((item for item in results if item and item.status == "failed"), None)
    if failed:
        assert any(item["error"] == "lost acknowledgement" for item in current.transition_history)
    assert len(store.list_restart_handoffs()) == 1
    assert current.phase_version == len(current.transition_history)


def test_retry_preserves_prior_attempt_error_and_legacy_receipt(tmp_path):
    store = CardProjection(tmp_path / "cards.db")
    original = receipt(store, status="failed", error="old durable failure", failure_stage="resuming", attempts=2)
    retried = store.retry_restart_handoff(original.id, session_id=original.session_id)
    assert retried.id == original.id
    assert retried.continuation_prompt_id == original.continuation_prompt_id
    assert retried.attempts == 3
    assert retried.transition_history[0]["error"] == "old durable failure"
    assert retried.transition_history[0]["attempt"] == 2
    reopened = CardProjection(tmp_path / "cards.db").get_restart_handoff(original.id)
    assert reopened.transition_history == retried.transition_history
    assert reopened.observation["phase"] == "reconciling"


def test_observation_uses_current_pause_and_exact_prompt_worker(tmp_path):
    from types import SimpleNamespace
    from pa.domain.models import AgentSession
    from pa.instance.restart_lifecycle import restart_observation_fields
    store = CardProjection(tmp_path / "cards.db")
    current = receipt(store, status="continuation_queued")
    session = AgentSession(id=current.session_id, agent_name="codex", purpose="chat", control_mode="human")
    runtime = SimpleNamespace(_queue_paused=True, _in_flight=None, _draining_prompt=None, _queue=[])
    paused = restart_observation_fields(current, session=session, runtime=runtime)
    assert paused["phase"] == "waiting"
    assert paused["next_action"] == "resume_by_operator"
    runtime._queue_paused = False
    runtime._in_flight = SimpleNamespace(id=current.continuation_prompt_id)
    running = restart_observation_fields(current, session=session, runtime=runtime)
    assert running["phase"] == "running"
    assert running["worker_state"] == "active"
    assert running["effect_state"] == "pending"
