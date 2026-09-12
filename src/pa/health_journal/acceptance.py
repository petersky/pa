"""Read canonical lifecycle acceptance; never create a second completion truth."""
from __future__ import annotations

from pa.health_journal.store import JournalError


def _dict(value):
    return value.model_dump(mode='json') if hasattr(value, 'model_dump') else dict(value or {})


def verify_acceptance(card, group, assessment):
    """Require an exact current lifecycle receipt for this fix/build/scenario.

    References in the canonical receipt bind health-group:<id>, scenario:<name>
    and instance:<id>. The journal only mirrors them; it cannot mint acceptance.
    Legacy Done, an integrated PR, stale declarations and unrelated receipts do
    not satisfy this production-verification consumer.
    """
    requirement = _dict(getattr(card, 'completion_requirement', None))
    status = getattr(card, 'completion_status', {}) or {}
    if not requirement or requirement.get('schema_version') != 1 or not status.get('accepted'):
        raise JournalError('canonical_acceptance_pending')
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
    for receipt in current:
        if (receipt.get('idempotency_key') == assessment.acceptance_reference
                and receipt.get('subject_revision') == subject
                and receipt.get('outcome') == 'accepted'
                and receipt.get('actor') and receipt.get('recorded_at')
                and 'verified' in receipt.get('milestones', [])
                and required_refs <= set(receipt.get('references', []))):
            return True
    raise JournalError('current_scoped_acceptance_receipt_required')
