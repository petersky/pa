"""Read canonical lifecycle acceptance; never create a second completion truth."""
from __future__ import annotations

from pa.health_journal.store import JournalError


def _dict(value):
    return value.model_dump(mode='json') if hasattr(value, 'model_dump') else dict(value or {})


def verify_acceptance(card, group, assessment, *, repair_sessions=(), repair_dispatches=()):
    """Require an exact current lifecycle receipt for this fix/build/scenario.

    References in the canonical receipt bind health-group:<id>, scenario:<name>
    and instance:<id>. The journal only mirrors them; it cannot mint acceptance.
    Legacy Done, an integrated PR, stale declarations and unrelated receipts do
    not satisfy this production-verification consumer.
    """
    requirement = _dict(getattr(card, 'completion_requirement', None))
    status = getattr(card, 'completion_status', {}) or {}
    if not requirement or requirement.get('schema_version') != 1:
        raise JournalError('awaiting_acceptance')
    if not requirement.get('acceptance_principals'):
        raise JournalError('acceptance_owner_unconfigured')
    if not status.get('accepted'):
        raise JournalError('awaiting_acceptance')
    if not group['data'].get('card_id') or group['data']['card_id'] != card.id:
        raise JournalError('acceptance_card_mismatch')
    subject = assessment.accepted_subject_revision
    expected_subject = assessment.commit or group['data'].get('commit')
    if not subject or not expected_subject or subject != expected_subject:
        raise JournalError('acceptance_subject_mismatch')
    if not assessment.acceptance_scenario or not assessment.accepted_instances:
        raise JournalError('acceptance_scope_required')
    receipts = [_dict(item) for item in getattr(card, 'completion_evidence', [])]
    current = [r for r in receipts if r.get('requirement_revision') == requirement.get('revision')]
    integrated = [r for r in current if r.get('outcome') == 'integrated']
    if integrated and integrated[-1].get('subject_revision') != subject:
        raise JournalError('acceptance_subject_superseded')
    required_refs = {f'health-group:{group["id"]}', f'scenario:{assessment.acceptance_scenario}',
                     *(f'instance:{i}' for i in assessment.accepted_instances)}
    origin_sessions = set(repair_sessions) | {requirement.get('originating_session_id')}
    origin_dispatches = set(repair_dispatches) | {requirement.get('originating_dispatch_id'), group['data'].get('dispatch_id')}
    origin_sessions.difference_update({None, ""})
    origin_dispatches.difference_update({None, ""})
    for receipt in current:
        if (receipt.get('idempotency_key') == assessment.acceptance_reference
                and receipt.get('subject_revision') == subject
                and receipt.get('outcome') == 'accepted'
                and receipt.get('actor') and receipt.get('recorded_at')
                and 'verified' in receipt.get('milestones', [])
                and required_refs <= set(receipt.get('references', []))):
            if receipt.get('actor_kind') == 'bound_session':
                session, dispatch = receipt.get('actor_session_id'), receipt.get('actor_dispatch_id')
                if not session or not dispatch or not (origin_sessions or origin_dispatches):
                    raise JournalError('acceptance_independence_unconfirmed')
                if session in origin_sessions or dispatch in origin_dispatches:
                    raise JournalError('repair_self_acceptance_forbidden')
                if receipt['actor'] not in requirement['acceptance_principals']:
                    raise JournalError('acceptance_actor_unauthorized')
            elif receipt.get('actor_kind') != 'human':
                raise JournalError('acceptance_actor_unbound')
            return True
    raise JournalError('current_scoped_acceptance_receipt_required')
