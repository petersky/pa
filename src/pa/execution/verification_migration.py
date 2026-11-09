"""Evidence-qualified metadata corrections; no lane/evidence mutation."""
from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from pa.domain.completion import CompletionConflict
from pa.domain.models import CompletionRequirement


def comparable(value):
    return CompletionRequirement.model_validate(value).model_dump(exclude={'revision'})


LOCAL_OPERATOR = 'user:local'


def stalled_declared_plan(service, card, *, now=None):
    """Hand a stalled, merged ``declared`` card to the independent verifier.

    Only the narrow case where nothing else can ever record the remaining checkpoints:
    the explicit acceptance principal is the local operator, the work is integrated, and
    every dispatch for the card has been finished for ``completion_auto_verify_stalled_seconds``.
    Criteria, milestones, principals and evidence are preserved; only the policy changes.
    """
    from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
    req = card.completion_requirement
    threshold = service.ctx.settings.completion_auto_verify_stalled_seconds
    if not threshold:
        raise CompletionConflict('completion_auto_verification_disabled')
    if (req.mode != 'explicit_acceptance' or req.provenance != 'explicit' or req.verification_policy != 'declared'
            or LOCAL_OPERATOR not in req.acceptance_principals or 'integrated' not in card.completion_status['satisfied']):
        raise CompletionConflict('explicit_requirement_preserved')
    ledger = service.ctx.services['dispatch_store']
    records = ledger.list(card_id=card.id, realm_id=card.realm_id, limit=101)
    if not records or len(records) > 100:
        raise CompletionConflict('verification_origin_unconfirmed')
    now = now or datetime.now(UTC)
    for record in records:
        if record.state not in TERMINAL_DISPATCH_STATES:
            raise CompletionConflict('completion_work_in_progress')
        if now - record.updated_at < timedelta(seconds=threshold):
            raise CompletionConflict('completion_not_stalled')
        session = service.ctx.store.get_session(record.session_id) if record.session_id else None
        if session and session.status == 'active':
            raise CompletionConflict('completion_work_in_progress')
    adopted = req.model_copy(update={'verification_policy': 'independent_agent_v1', 'verification_principal': LOCAL_OPERATOR,
                                     'verification_authority_instance_id': service.ctx.settings.instance_id})
    packet = service.packet(card.model_copy(update={'completion_requirement': adopted}))
    if not (packet['origin_sessions'] or packet['origin_dispatches']):
        raise CompletionConflict('verification_origin_unconfirmed')
    return adopted.model_copy(update={'originating_session_ids': packet['origin_sessions'],
                                      'originating_dispatch_ids': packet['origin_dispatches']})


def migration_plan(service, card, kind):
    req = card.completion_requirement
    if not req or card.lane == 'done' or card.completion_status['accepted']:
        raise CompletionConflict('completion_migration_not_eligible')
    if kind == 'stalled_declared':
        return stalled_declared_plan(service, card)
    events = service.ctx.store.event_log.entity_history(card.realm_id, 'card', card.id)
    history = [r['event'] for r in events if r.get('projection_effect') != 'ignored_duplicate_create']
    if kind != 'system_health':
        raise CompletionConflict('unknown_completion_migration')
    groups = re.findall(r'health-group:([a-zA-Z0-9-]+)', req.criteria)
    if len(groups) != 1 or req.acceptance_principals or req.provenance != 'explicit' or req.verification_policy != 'declared':
        raise CompletionConflict('explicit_requirement_preserved')
    group = service.health.journal.group(groups[0], realms=[card.realm_id])
    actions = service.health.journal.repair_actions(groups[0], expected_version=group['version'], realms=[card.realm_id])
    proven = []
    for action in actions:
        saved = action['result'].get('effects', {}).get('requirement', {}).get('payload', {}).get('body', {}).get('completion_requirement')
        if saved and comparable(saved) == comparable(req) and action['result'].get('card_id') == card.id:
            proven.append(action)
    if not proven:
        raise CompletionConflict('system_requirement_provenance_unconfirmed')
    action = proven[0]
    if group['data'].get('card_id') != card.id or action['result'].get('card_id') != card.id:
        raise CompletionConflict('verification_health_card_mismatch')
    # Only a first system declaration is adopted automatically. An augmented
    # explicit declaration requires its actual authority to select agent policy.
    declarations = [e for e in history if e.get('payload', {}).get('completion_requirement')]
    if not declarations or any(comparable(e['payload']['completion_requirement']) != comparable(req) for e in declarations):
        raise CompletionConflict('explicit_requirement_preserved')
    expected = f'Verify the declared repair build and scenario for health-group:{groups[0]}; retain exact canonical acceptance receipt and affected instance references.'
    if req.criteria.strip() != expected or req.milestones != ['verified'] or req.mode != 'explicit_acceptance':
        raise CompletionConflict('explicit_requirement_preserved')
    principal = action['payload']['principal_id']
    adopted = req.model_copy(update={'provenance': 'system_health', 'source_reference': 'health-group:' + groups[0],
        'verification_policy': 'independent_agent_v1', 'verification_principal': principal,
        'acceptance_principals': [principal],
        'originating_dispatch_id': req.originating_dispatch_id or action['result'].get('dispatch_id')})
    candidate = card.model_copy(update={'completion_requirement': adopted})
    packet = service.packet(candidate)
    if not (packet['origin_sessions'] or packet['origin_dispatches']):
        raise CompletionConflict('verification_origin_unconfirmed')
    return adopted.model_copy(update={'originating_session_ids': packet['origin_sessions'],
                                      'originating_dispatch_ids': packet['origin_dispatches']})
