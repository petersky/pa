"""Evidence-qualified metadata corrections; no lane/evidence mutation."""
from __future__ import annotations

import re

from pa.domain.completion import CompletionConflict
from pa.domain.conversation_issues import ConversationIssues
from pa.domain.models import CompletionRequirement


def comparable(value):
    return CompletionRequirement.model_validate(value).model_dump(exclude={'revision'})


def migration_plan(service, card, kind):
    req = card.completion_requirement
    if not req or card.lane == 'done' or card.completion_status['accepted']:
        raise CompletionConflict('completion_migration_not_eligible')
    events = service.ctx.store.event_log.entity_history(card.realm_id, 'card', card.id)
    history = [r['event'] for r in events if r.get('projection_effect') != 'ignored_duplicate_create']
    if kind == 'inherited_child':
        # The frozen notebook proves the server copied this parent declaration.
        # A later deliberate edit is never eligible, even if text is identical.
        match = re.search(r'Source conversation: ([a-zA-Z0-9-]+)\s*$', card.body)
        if not match or not card.parent_id or not history or history[0]['type'] != 'card_created':
            raise CompletionConflict('inherited_requirement_provenance_unconfirmed')
        if any('completion_requirement' in (e.get('field_intent') or []) or e['type'] == 'card_upserted' for e in history[1:]):
            raise CompletionConflict('explicit_requirement_preserved')
        state = ConversationIssues(service.ctx.store).read(match[1])
        frozen = [r for r in state['requests'] if r['context'].get('source_card_id') == card.parent_id
                  and any(g.get('card_id') == card.id for g in r['groups'])]
        if len(frozen) != 1 or not frozen[0]['context'].get('completion_requirement'):
            raise CompletionConflict('inherited_requirement_provenance_unconfirmed')
        original = frozen[0]['context']['completion_requirement']
        created = history[0]['payload'].get('completion_requirement')
        if not created or comparable(original) != comparable(created) or comparable(created) != comparable(req):
            raise CompletionConflict('explicit_requirement_preserved')
        if 'health-group:' not in req.criteria:
            raise CompletionConflict('unrelated_health_requirement_required')
        supervisor = service.ctx.services.get('pr_supervisor_store')
        watches = supervisor.list_watches(card_id=card.id, realm_id=card.realm_id, include_retired=True) if supervisor else []
        source_evidence = any(e.outcome == 'integrated' and e.actor_kind == 'integration' for e in card.completion_evidence)
        source_evidence = source_evidence or any(w.provenance_version >= 1 and w.head_sha
            and str(w.status) in {'active','blocked','merged'} for w in watches)
        if not source_evidence:
            # A design/research issue has no source-integration producer. Keep
            # its declaration pending for an evidence-based scope correction.
            raise CompletionConflict('inherited_child_deliverable_unconfirmed')
        return CompletionRequirement(mode='integration_only', milestones=['integrated'],
            provenance='inherited_repair_correction', source_reference='card:' + card.parent_id,
            criteria='Integrate this card’s source changes. Unrelated inherited health acceptance was corrected; the original declaration remains in immutable history.')
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
