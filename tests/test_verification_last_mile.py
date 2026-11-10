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
