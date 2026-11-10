"""Defaults that must not interrupt a single card running end to end.

Each default here replaced a cap or timer that fired on ordinary long card
work. Every test also asserts that a setting restores the previous behaviour.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.config import Settings
from pa.configuration.registry import REGISTRY
from pa.domain.instance_config import InstanceConfig
from pa.execution.dispatch import (
    CAPACITY_RESERVATION_TTL,
    DispatchEvent,
    DispatchRecord,
    DispatchStore,
)
from pa.execution.post_turn import (
    FollowupActionName,
    PostTurnEvaluator,
    automatic_followup_budget_exhausted,
    automatic_followups_used,
)
from tests.test_post_turn_evaluation import context_for, snapshot


def _registry_default(key: str):
    return REGISTRY[key].default


# --- automatic follow-up budget ------------------------------------------------


def _evaluation_with_automatic_followup():
    evaluation = PostTurnEvaluator().evaluate(
        context_for(snapshot(disposition={"lane": "active"}, disposition_status="accepted"))
    )
    action = next(
        item
        for item in evaluation.recommended_actions
        if item.name == FollowupActionName.PROMPT_SAME_SESSION
    )
    action.audit.append({"event": "authorized", "automatic": True})
    return evaluation


def test_automatic_followup_budget_default_allows_long_running_card() -> None:
    assert Settings().post_turn_max_automatic_followups >= 20
    assert InstanceConfig().post_turn_max_automatic_followups == (
        Settings().post_turn_max_automatic_followups
    )
    assert _registry_default("post_turn_max_automatic_followups") == (
        Settings().post_turn_max_automatic_followups
    )

    budget = Settings().post_turn_max_automatic_followups
    prior = [_evaluation_with_automatic_followup() for _ in range(10)]
    assert automatic_followups_used(prior) == 10
    assert not automatic_followup_budget_exhausted(prior, budget)


def test_automatic_followup_budget_setting_restores_old_limit() -> None:
    old = Settings(post_turn_max_automatic_followups=2).post_turn_max_automatic_followups
    assert old == 2
    prior = [_evaluation_with_automatic_followup() for _ in range(2)]
    assert automatic_followup_budget_exhausted(prior, old)
    assert not automatic_followup_budget_exhausted(prior[:1], old)


# --- capacity reservation expiry ------------------------------------------------


def _queued(store: DispatchStore, key: str, card: str, *, target: str = "a") -> DispatchRecord:
    record, _ = store.admit(
        DispatchRecord(
            mutation_id=f"mutation-{key}",
            idempotency_key=key,
            request_fingerprint=key,
            placement_request_fingerprint=key,
            card_id=card,
            authority_instance_id="authority",
            authority_url="http://authority.test",
            target_instance_id=target,
            placement_policy="best_match",
            placement_decision={"chosen_instance_id": target},
        )
    )
    return record


def _overdue(store: DispatchStore, record: DispatchRecord) -> None:
    record.capacity_reservation_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    store.put(record)


def _enter_stage(
    store: DispatchStore, record: DispatchRecord, state: str, *, age: timedelta
) -> DispatchRecord:
    """Move a reserved dispatch into a pre-start stage ``age`` ago, then make it overdue."""
    record.state = state
    record.events.append(
        DispatchEvent(
            seq=record.events[-1].seq + 1,
            state=state,
            message=f"entered {state}",
            created_at=datetime.now(UTC) - age,
        )
    )
    _overdue(store, record)
    return record


def test_reservation_default_is_not_hard_expiry() -> None:
    assert Settings().dispatch_capacity_reservation_hard_expiry is False
    assert InstanceConfig().dispatch_capacity_reservation_hard_expiry is False
    assert _registry_default("dispatch_capacity_reservation_hard_expiry") is False


def test_queued_reservation_is_renewed_even_when_another_dispatch_waits() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record = _queued(store, "healthy", "card-healthy")
        waiter = _queued(store, "waiter", "card-waiter")
        waiter.state = "waiting_capacity"
        store.put(waiter)
        _overdue(store, record)

        assert store.expire_capacity_reservations() == []

        kept = store.get(record.dispatch_id)
        assert kept.state == "queued"
        assert kept.error_code is None
        assert kept.capacity_released_at is None
        assert kept.capacity_reservation_expires_at > datetime.now(UTC) + (
            CAPACITY_RESERVATION_TTL - timedelta(minutes=1)
        )


def test_progressing_prestart_stage_is_renewed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record = _queued(store, "slow", "card-slow")
        _enter_stage(store, record, "provisioning", age=timedelta(minutes=5))

        assert store.expire_capacity_reservations() == []

        kept = store.get(record.dispatch_id)
        assert kept.state == "provisioning"
        assert kept.capacity_reservation_expires_at > datetime.now(UTC)


def test_dispatch_hung_in_starting_session_is_still_failed() -> None:
    """The live capacity_reservation_timeout: no transition for a full TTL, capacity free."""
    for waiting in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            hung = _queued(store, "hung", "card-hung")
            if waiting:
                waiter = _queued(store, "waiter", "card-waiter")
                waiter.state = "waiting_capacity"
                store.put(waiter)
            _enter_stage(
                store, hung, "starting_session", age=CAPACITY_RESERVATION_TTL + timedelta(minutes=1)
            )

            expired = store.expire_capacity_reservations()

            assert [item.dispatch_id for item in expired] == [hung.dispatch_id]
            failed = store.get(hung.dispatch_id)
            assert failed.state == "failed"
            assert failed.error_code == "capacity_reservation_timeout"
            assert failed.recoverable is True
            assert failed.capacity_release_reason == "timeout"


def test_reservation_hard_expiry_setting_restores_unconditional_timeout() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp), reservation_hard_expiry=True)
        record = _queued(store, "hard", "card-hard")
        _overdue(store, record)

        expired = store.expire_capacity_reservations()

        assert [item.dispatch_id for item in expired] == [record.dispatch_id]
        failed = store.get(record.dispatch_id)
        assert failed.state == "failed"
        assert failed.error_code == "capacity_reservation_timeout"
        assert failed.capacity_release_reason == "timeout"
