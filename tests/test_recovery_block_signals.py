"""A blocked session is announced once and no longer leaves its dispatch running."""
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.agent_session import AgentSessionManager

LEASE = (
    "Card already has a live worktree lease; resume its owner or explicitly "
    "allow concurrent dispatch"
)


class FakeNotifications:
    def __init__(self):
        self.created = []
        self.store = SimpleNamespace(find_notification_by_dedup=self.find)

    def find(self, realm_id, key):
        return next((d for d, _ in self.created if d.deduplication_key == key), None)

    def create(self, data, *, principal_id, instance_id=None):
        self.created.append((data, principal_id))
        return data


@pytest.fixture
def lab(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    card = store.create_card(CardCreate(title="Ship the importer"), via_log=False)
    settings = Settings(data_dir=tmp_path)
    ledger = DispatchStore(tmp_path)
    session = store.save_session(AgentSession(
        id="run", agent_name="codex", status="recoverable_interrupted",
        purpose="automated_run", card_id=card.id, dispatch_id="d1",
        principal_id="user:local",
    ))
    ledger.put(DispatchRecord(
        dispatch_id="d1", mutation_id="m1", card_id=card.id, session_id="run",
        authority_instance_id=settings.instance_id, authority_url="http://pa.test",
        target_instance_id=settings.instance_id, state="running",
        principal_id="user:local", realm_id=session.realm_id,
    ))
    manager = AgentSessionManager(settings, store, dispatch_store=ledger)
    manager.notification_service = FakeNotifications()
    return SimpleNamespace(
        store=store, card=card, ledger=ledger, session=session, manager=manager,
        notices=manager.notification_service.created,
    )


async def block(lab, message=LEASE):
    current = lab.store.get_session("run")
    return await lab.manager._mark_recovery_interrupted(
        lab.manager._snapshot_from_persisted(current), RuntimeError(message)
    )


@pytest.mark.asyncio
async def test_blocking_notifies_once_with_high_priority_and_retry_action(lab):
    await block(lab)
    assert len(lab.notices) == 1
    notice, principal = lab.notices[0]
    assert notice.priority == "high"
    assert notice.session_id == "run" and notice.card_id == lab.card.id
    assert "Ship the importer" in notice.summary
    assert "workspace_lease_conflict" in notice.body
    assert notice.deduplication_key == "session-recovery-blocked:run:workspace_lease_conflict"
    assert [a.id for a in notice.actions] == ["retry"]
    assert notice.actions[0].href == "/agent?session=run"
    assert principal.startswith("instance:")
    # A restart pass failing identically neither re-notifies nor rewrites state.
    for _ in range(3):
        again = lab.store.get_session("run")
        again.status = "provisioning_failed"
        lab.store.save_session(again)
        await block(lab)
    assert len(lab.notices) == 1


@pytest.mark.asyncio
async def test_blocking_marks_the_running_dispatch_failed_and_recoverable(lab):
    await block(lab)
    record = lab.ledger.get("d1")
    assert record.state == "failed"
    assert record.error_code == "session_recovery_blocked"
    assert record.recoverable is True
    assert record.retryable is True
    assert "workspace_lease_conflict" in record.last_error
    assert record.events[-1].state == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "completion_pending", "cancelled", "queued"])
async def test_blocking_leaves_other_dispatch_states_alone(lab, state):
    record = lab.ledger.get("d1")
    record.state = state
    lab.ledger.put(record)
    before = lab.ledger.get("d1").model_dump()
    await block(lab)
    assert lab.ledger.get("d1").model_dump() == before


@pytest.mark.asyncio
async def test_a_transient_failure_neither_notifies_nor_fails_the_dispatch(lab):
    state = await block(lab, "provider hiccup")
    assert state == "recoverable_interrupted"
    assert lab.notices == []
    assert lab.ledger.get("d1").state == "running"


@pytest.mark.asyncio
async def test_exhausted_session_is_announced_as_such(lab):
    session = lab.store.get_session("run")
    session.recovery_json = {"attempts": 7}
    lab.store.save_session(session)
    await block(lab, "provider hiccup")
    assert lab.store.get_session("run").recovery_json["exhausted"] is True
    notice, _ = lab.notices[0]
    assert notice.title == "Agent session gave up recovering"
    assert lab.ledger.get("d1").state == "failed"


@pytest.mark.asyncio
async def test_recovery_restores_the_dispatch_that_blocking_failed(lab):
    await block(lab)
    await lab.manager._settle_dispatch_after_recovery_async("run")
    record = lab.ledger.get("d1")
    assert record.state == "running"
    assert record.error_code is None and record.last_error is None


@pytest.mark.asyncio
async def test_recovery_never_reopens_a_dispatch_that_failed_for_another_reason(lab):
    record = lab.ledger.get("d1")
    lab.ledger.fail(record, "operator cancelled it", code="cancelled_by_operator")
    await lab.manager._settle_dispatch_after_recovery_async("run")
    assert lab.ledger.get("d1").state == "failed"


def test_lease_reclaim_notice_names_card_session_and_rescue_ref(lab):
    lab.manager._on_lease_reclaimed({
        "lease_id": "lease-1", "session_id": "run", "card_id": lab.card.id,
        "reason": "recovery_blocked", "requested_by": "successor",
        "previous_fencing_token": 4, "worktree_path": "/ws/run",
        "rescue": {"ref": "refs/pa/rescue/run/20261109T000000Z-abcd1234",
                   "sha": "deadbeef", "preserved": True},
    })
    notice, _ = lab.notices[0]
    assert notice.priority == "high"
    assert "refs/pa/rescue/run/20261109T000000Z-abcd1234" in notice.body
    assert "Ship the importer" in notice.body and "run" in notice.body
    assert notice.deduplication_key == "workspace-lease-reclaimed:lease-1:4"
    event = lab.ledger.get("d1").events[-1]
    assert event.detail["rescue_ref"] == "refs/pa/rescue/run/20261109T000000Z-abcd1234"
    assert lab.ledger.get("d1").state == "running"


def test_attention_notices_stay_in_the_inbox_until_dismissed():
    from pa.domain.notifications import (
        SESSION_ATTENTION_NOTICE_CAPABILITY, Notification,
    )
    notice = Notification(
        type="service_health", capability=SESSION_ATTENTION_NOTICE_CAPABILITY,
        title="x", realm_id="default",
    )
    assert notice.outstanding is True


@pytest.mark.asyncio
async def test_startup_sweep_announces_sessions_blocked_before_this_boot(lab):
    session = lab.store.get_session("run")
    session.status = "recovery_blocked"
    session.recovery_json = {
        "blocked": True, "attempts": 127, "code": "workspace_lease_conflict",
        "last_error": LEASE, "remedy": "resume or close the owner",
    }
    lab.store.save_session(session)
    lab.manager._startup_complete = True
    await lab.manager._recovery_once()
    await lab.manager._recovery_once()
    assert len(lab.notices) == 1
    assert lab.ledger.get("d1").state == "failed"
    assert lab.store.get_session("run").recovery_json["attempts"] == 127
    # A new process finds the existing notice and does not add another.
    lab.manager._blocked_sessions_signalled = False
    await lab.manager._recovery_once()
    assert len(lab.notices) == 1
