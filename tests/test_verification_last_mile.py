"""Verification last mile: stale revisions, failure feedback, churn, independence and notices."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pa.domain.models import CardUpdate, CompletionEvidence
from tests.test_autonomous_verification import origin, ordinary, system  # noqa: F401  (fixtures)


def settle(system, record, state='completed'):
    record.state = state
    record.acknowledged_at = datetime.now(UTC)
    record.completion_delivery_class = 'acknowledged'
    record.reconciliation_state = 'not_required'
    system.ctx.services['dispatch_store'].put(record)


async def accepted_once(system):
    card, _ = ordinary(system)
    await system.service.process(card)
    job = system.service.store.list()[0]
    verifier = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision='a' * 40, milestones=['verified'], references=['verification-intent:' + job['id']])
    accepted = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at, completion_acceptance=evidence),
        principal_id=verifier.principal_id, actor_session_id=verifier.session_id,
        actor_dispatch_id=verifier.dispatch_id, idempotency_key='accepted-once')
    return accepted, job, verifier


@pytest.mark.asyncio
async def test_an_accepted_verifier_that_was_not_retired_does_not_block_the_next_revision(system, monkeypatch):
    accepted, job, verifier = await accepted_once(system)
    settle(system, verifier)
    await system.service.process(accepted)
    done = system.ctx.store.get_card(accepted.id)
    assert done.lane == 'done'
    changed = done.completion_requirement.model_copy(update={'criteria': 'A new scenario'})
    reopened = system.ctx.store.update_card(done.id, CardUpdate(expected_version=done.updated_at, lane='waiting',
        field_intent=['lane', 'completion_requirement'], completion_requirement=changed), idempotency_key='new-scenario')

    real = system.service.origin_readiness

    async def never_ready(record):
        if record.dispatch_id == verifier.dispatch_id:
            return {'ready': False, 'reason': 'origin_runtime_still_owned'}
        return await real(record)
    monkeypatch.setattr(system.service, 'origin_readiness', never_ready)
    calls = len(system.calls)
    await system.service.process(reopened)
    assert len(system.calls) == calls + 1 and system.calls[-1][1] == '/api/fleet/dispatch'
    assert system.service.store.list()[-1]['dispatch_id']


@pytest.mark.asyncio
async def test_a_covered_requirement_edit_finishes_without_a_new_verifier(system):
    accepted, job, verifier = await accepted_once(system)
    assert accepted.lane != 'done'
    edited = system.ctx.store.update_card(accepted.id, CardUpdate(expected_version=accepted.updated_at,
        field_intent=['completion_requirement'],
        completion_requirement=accepted.completion_requirement.model_copy(update={'milestones': ['verified']})),
        idempotency_key='narrow')
    assert edited.completion_status['accepted'] and edited.completion_requirement.prior_revisions
    calls = len(system.calls)
    await system.service.process(edited)
    assert system.ctx.store.get_card(accepted.id).lane == 'done'
    assert not [c for c in system.calls[calls:] if c[1] == '/api/fleet/dispatch']


# ---------------------------------------------------------------- failure feedback

class Recorder:
    """Stands in for the notification service; collects what notify_stall would create."""
    store = None

    def __init__(self):
        self.created = []

    def create(self, data, principal_id=None):
        self.created.append(data)
        return data


def verdict(system, job, outcome='The login test still fails on a fresh database.', state='completed'):
    record = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    settle(system, record, state)
    record = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    record.card_disposition_payload = {'contract': 'pa.card-disposition/v1', 'lane': 'waiting', 'outcome': outcome, 'evidence': {}}
    system.ctx.services['dispatch_store'].put(record)
    return record


def dispatches(system):
    return [c for c in system.calls if c[1] == '/api/fleet/dispatch']


async def second_attempt(system, card):
    """Let the re-engaged executor finish; the corrected work is verified under a fresh intent."""
    system.ctx.settings.completion_verification_quiet_seconds = 0
    redo = dispatches(system)[-1]
    record = system.ctx.services['dispatch_store'].by_authority_idempotency('instance-a', redo[3])
    settle(system, record)
    card = system.ctx.store.get_card(card.id)
    await system.service.process(card)
    return system.ctx.store.get_card(card.id)


@pytest.mark.asyncio
async def test_a_failed_verdict_is_fed_back_to_the_executor_once_then_the_owner_is_told(system):
    notices = system.ctx.services['notifications'] = Recorder()
    card, repair = ordinary(system)
    await system.service.process(card)
    first = system.service.store.list()[0]
    verdict(system, first)
    for _ in range(3):  # idempotent: repeated cycles change nothing
        await system.service.process(card)
    reengage = [c for c in dispatches(system) if c[3].startswith('verification-reengage:')]
    assert len(reengage) == 1 and reengage[0][3] == 'verification-reengage:' + first['id']
    assert 'login test still fails' in reengage[0][2]['message'] and reengage[0][2]['card_id'] == card.id
    assert [n.severity.value for n in notices.created] == ['warning']
    assert notices.created[0].capability == 'stall:verification_failed' and 'login test' in notices.created[0].body

    # The corrected work is verified afresh, fails again, and this time PA stops and says so.
    card = await second_attempt(system, card)
    second = [j for j in system.service.store.list() if j['id'] != first['id'] and j['dispatch_id']]
    assert second, system.service.store.list()
    verdict(system, second[-1], 'Still failing after the fix.')
    for _ in range(2):
        await system.service.process(system.ctx.store.get_card(card.id))
    assert len([c for c in dispatches(system) if c[3].startswith('verification-reengage:')]) == 1
    assert [n.severity.value for n in notices.created] == ['warning', 'error']
    assert 'Still failing' in notices.created[-1].body and 'stopped retrying' in notices.created[-1].body


@pytest.mark.asyncio
async def test_reengagement_can_be_disabled_and_a_crash_is_not_a_verdict(system):
    notices = system.ctx.services['notifications'] = Recorder()
    system.ctx.settings.completion_verifier_reengage_max = 0
    card, _ = ordinary(system)
    await system.service.process(card)
    verdict(system, system.service.store.list()[0])
    await system.service.process(card)
    assert not [c for c in dispatches(system) if c[3].startswith('verification-reengage:')]
    assert [n.severity.value for n in notices.created] == ['error']

    system.ctx.settings.completion_verifier_reengage_max = 1
    crashed, _ = ordinary(system)
    await system.service.process(crashed)
    job = [j for j in system.service.store.list(crashed.id)][0]
    verdict(system, job, state='failed')
    await system.service.process(crashed)
    assert not [c for c in dispatches(system) if c[3].startswith('verification-reengage:')]
    assert len(notices.created) == 2


# ----------------------------------------------------------------- newer head / debounce

@pytest.mark.asyncio
async def test_a_newer_head_during_verification_is_verified_after_the_run_not_instead_of_it(system):
    notices = system.ctx.services['notifications'] = Recorder()
    card, _ = ordinary(system)
    await system.service.process(card)
    first = system.service.store.list()[0]
    verifier = system.ctx.services['dispatch_store'].get(first['dispatch_id'])
    newer = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
        principal_id='instance:pr-supervisor', integration_evidence=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision, subject_revision='b' * 40, outcome='integrated'),
        idempotency_key='second-merge')
    await system.service.process(newer)
    assert len(dispatches(system)) == 1  # no rival while the first verifier runs
    assert system.service.store.get(first['id'])['state'] == 'dispatched'
    from pa.domain.completion import CompletionConflict
    stale = CompletionEvidence(requirement_revision=card.completion_requirement.revision, subject_revision='a' * 40,
                               milestones=['verified'], references=['verification-intent:' + first['id']])
    with pytest.raises(CompletionConflict, match='verification_newer_head_pending'):
        system.service.validate_acceptance(newer, verifier, stale)
    assert system.service.store.get(first['id'])['diagnostic'].startswith('newer_head:')

    # The old run ends without a verdict that matters; the latest head is then verified exactly once.
    system.settings = system.ctx.settings
    system.ctx.settings.completion_verification_quiet_seconds = 0
    settle(system, verifier)
    for _ in range(2):
        await system.service.process(system.ctx.store.get_card(card.id))
    latest = [j for j in system.service.store.list() if j['dispatch_id'] and j['id'] != first['id']]
    assert len(latest) == 1 and latest[0]['packet']['subject'] == 'b' * 40
    assert not notices.created  # a superseded run is not a failed verification
    assert system.service.store.get(first['id'])['state'] == 'superseded'


@pytest.mark.asyncio
async def test_a_burst_of_merges_waits_for_the_quiet_period_only_after_an_earlier_run(system):
    card, _ = ordinary(system)
    await system.service.process(card)  # first verification starts at once
    first = system.service.store.list()[0]
    assert first['dispatch_id']
    settle(system, system.ctx.services['dispatch_store'].get(first['dispatch_id']))
    newer = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
        principal_id='instance:pr-supervisor', integration_evidence=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision, subject_revision='b' * 40, outcome='integrated'),
        idempotency_key='burst-merge')
    await system.service.process(newer)
    held = system.service.store.list()[-1]
    assert held['diagnostic'] == 'waiting_for_quiet_head' and not held['dispatch_id'] and len(dispatches(system)) == 1
    system.ctx.settings.completion_verification_quiet_seconds = 0  # the setting restores immediate re-verification
    await system.service.process(system.ctx.store.get_card(card.id))
    assert system.service.store.list()[-1]['dispatch_id'] and len(dispatches(system)) == 2
