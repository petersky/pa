"""Completion evidence and immutable ACK boundaries through existing producers."""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence, CardLane
from pa.domain.completion import CompletionConflict, completion_state
from pa.execution.dispatch import DispatchRecord
from pa.pr_supervisor.models import PRPolicy
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore
from tests.test_completion_requirements import projection
from tests.test_pr_supervisor import _FakeGitHub, _DedupeDispatcher, snapshot, watch


@pytest.mark.asyncio
@pytest.mark.parametrize("realm", ["default", "acceptance-realm"])
async def test_pending_watch_acceptance_cannot_mint_integration_or_certify_next_subject(tmp_path, realm):
    store = projection(tmp_path)
    card = store.create_card(CardCreate(realm_id=realm, title='integrate then verify', lane='waiting', completion_requirement={
        'mode': 'explicit_acceptance', 'milestones': ['integrated', 'verified'], 'acceptance_principals': ['user:verifier']}))
    settings = Settings(data_dir=tmp_path, instance_id='instance-a', instance_url='http://instance-a', fleet_owner_url='http://instance-a', peers=[])
    watches = PRSupervisorStore(tmp_path / 'supervisor.db')
    service = PRSupervisor(settings, store, supervisor_store=watches, github_client=_FakeGitHub([snapshot(), snapshot(state='merged', merge_commit_sha='c' * 40)]), dispatcher=_DedupeDispatcher())
    await service.refresh_capability(force=True)
    item = watch(policy=PRPolicy(stable_head_seconds=0, stable_observations=1))
    item.card_id = card.id
    item.realm_id = realm
    await service.register_watch(item, replicate=False)
    await service.run_once()
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision, subject_revision='b' * 40, milestones=['integrated', 'verified'])
    def accept(current, proof, key):
        return store.update_card(current.id, CardUpdate(expected_version=current.updated_at, completion_acceptance=proof),
            realm_id=realm, principal_id='user:verifier', actor_session_id='verifier-session', actor_dispatch_id='verifier-dispatch', idempotency_key=key)
    with pytest.raises(CompletionConflict, match='completion_integration_producer_required'):
        accept(card, evidence, 'cannot-mint-integration')
    card = accept(card, evidence.model_copy(update={'milestones': ['verified']}), 'accept-A')
    assert card.completion_status['missing'] == ['integrated']
    # A historical acceptance claim cannot satisfy integration on replay either.
    old_claim = card.completion_evidence[-1].model_copy(update={'milestones': ['integrated', 'verified']})
    assert 'integrated' in completion_state(card.completion_requirement, [old_claim])['missing']
    watches.schedule_now(watch_id=item.id)
    await service.run_once()
    card = store.get_card(card.id, realm_id=realm)
    assert card.lane == CardLane.WAITING
    assert 'integrated' in card.completion_status['satisfied']
    assert 'verified' in card.completion_status['missing']
    assert card.completion_evidence[-1].outcome == 'integrated'
    assert card.completion_evidence[-1].subject_revision == 'c' * 40
    card = accept(card, evidence.model_copy(update={'milestones': ['verified'], 'subject_revision': 'c' * 40}), 'accept-B')
    await service._complete_merged_card(watches.get_watch(item.id))
    assert store.get_card(card.id, realm_id=realm).lane == CardLane.DONE


@pytest.mark.parametrize('conflict', ['version', 'completion'])
def test_http_completion_ack_survives_mutable_card_conflict_and_duplicate(tmp_path, conflict):
    from pa.core.kernel import Kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces', agent_enabled=False, peers=[], sync_token='isolated-peer-token')
    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title='worker outcome', lane='active'))
        ledger = app.state.ctx.services['dispatch_store']
        ledger.put(DispatchRecord(dispatch_id='dispatch-1', mutation_id='immutable-turn', card_id=card.id,
            realm_id='default', card_version=card.updated_at.isoformat(), card_snapshot=card.model_dump(mode='json'),
            authority_instance_id=settings.instance_id, authority_url='http://testserver', target_instance_id='target',
            session_id='worker-session', state='running'))
        body = {'mutation_id':'immutable-turn', 'card_id':card.id, 'realm_id':'default', 'card_version':card.updated_at.isoformat(),
            'source_instance_id':'target', 'session_id':'worker-session', 'result':{'summary':'turn ended'},
            'disposition':{'contract':'pa.card-disposition/v1','lane':'done','outcome':'source-only outcome','evidence':{'integration_required':False}}}
        body['result']['card_disposition'] = body['disposition']
        target_record = ledger.get('dispatch-1')
        original = store.update_card
        calls = []
        def concurrent_requirement(card_id, data, **kwargs):
            calls.append(data)
            original(card_id, CardUpdate(expected_version=card.updated_at, completion_requirement={
                'mode':'explicit_acceptance', 'milestones':['verified']}, field_intent=['completion_requirement']))
            if conflict == 'completion':
                raise CompletionConflict('acceptance_pending')
            return original(card_id, data, **kwargs)
        headers = {'Authorization':'Bearer isolated-peer-token', 'Idempotency-Key':'immutable-turn'}
        with patch.object(store, 'update_card', side_effect=concurrent_requirement):
            first = client.post('/api/fleet/dispatch/dispatch-1/complete', json=body, headers=headers)
            assert first.status_code == 200, first.text
            second = client.post('/api/fleet/dispatch/dispatch-1/complete', json=body, headers=headers)
            assert second.status_code == 200, second.text
        one, two = first.json(), second.json()
        assert one['acknowledged'] and not one['duplicate'] and two['duplicate']
        assert one['acknowledged_at'] == two['acknowledged_at']
        assert one['reconciliation'] == two['reconciliation']
        assert one['reconciliation']['state'] == 'conflict_requires_resolution'
        assert one['reconciliation']['condition'] == ('stale_card_version' if conflict == 'version' else 'acceptance_pending')
        assert one['reconciliation']['recoverable']
        assert len(calls) == 1
        current = store.get_card(card.id)
        assert current.lane == CardLane.ACTIVE
        assert current.completion_requirement.mode == 'explicit_acceptance'
        acknowledged = ledger.get('dispatch-1')
        from pa.modules.fleet import DispatchCompletionBody
        assert acknowledged.completion_envelope == DispatchCompletionBody.model_validate(body).model_dump(mode='json')
        assert acknowledged.acknowledged_at is not None
        assert len([e for e in acknowledged.events if e.detail.get('agent_turn_ended')]) == 1
        # The real target outbox consumes the authority's duplicate ACK and
        # retains reconciliation truth independently of transport success.
        import asyncio
        from types import SimpleNamespace
        from pa.execution.dispatch import DispatchStore, CompletionOutbox
        target = DispatchStore(tmp_path / 'target-ledger')
        target_record.state = 'completion_pending'
        target_record.completion_payload = body['result']
        target.put(target_record)
        outbox = CompletionOutbox(target, 'isolated-peer-token')
        async def forward(url, **kwargs):
            return client.post(url, **kwargs)
        with patch.object(outbox, '_http_client', return_value=SimpleNamespace(post=forward)):
            asyncio.run(outbox._send(target.get('dispatch-1')))
        delivered = target.get('dispatch-1')
        assert delivered.completion_delivery_class == 'acknowledged'
        assert delivered.reconciliation_state == 'conflict_requires_resolution'
        assert delivered.reconciliation_condition == one['reconciliation']['condition']
        assert delivered.reconciliation_recoverable
        assert delivered.completion_next_retry_at is None

    reset_store()
    reset_instance_agent()


def test_no_integration_acceptance_does_not_union_different_subjects(tmp_path):
    store = projection(tmp_path)
    card = store.create_card(CardCreate(title='runtime verification', lane='waiting', completion_requirement={
        'mode':'explicit_acceptance', 'milestones':['active','verified'], 'acceptance_principals':['user:verifier']}))
    for subject, milestone in [('A','active'), ('B','verified'), ('B','active')]:
        card = store.update_card(card.id, CardUpdate(expected_version=card.updated_at, completion_acceptance=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision, subject_revision=subject, milestones=[milestone])),
            principal_id='user:verifier', actor_session_id='verifier', actor_dispatch_id='verify', idempotency_key=f'{subject}-{milestone}')
        if subject == 'B' and milestone == 'verified':
            assert card.completion_status['missing'] == ['active']
    assert card.completion_status['accepted']
    assert completion_state({'schema_version':1,'mode':'integration_only','milestones':[]})['missing'] == ['integrated']


@pytest.mark.parametrize('outcome', ['applied', 'not_applicable', 'operator_state_preserved', 'conflict_requires_resolution'])
def test_http_outbox_completion_terminal_vocabulary_reaches_session_lifecycle(tmp_path, outcome):
    import asyncio
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from pa.core.kernel import Kernel
    from pa.domain.models import AgentSession
    from pa.domain.store import reset_store
    from pa.execution.dispatch import DispatchStore, CompletionOutbox
    from pa.instance.agent_session import reset_instance_agent
    from pa.instance.session_lifecycle import SessionLifecyclePolicy

    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces',
                        agent_enabled=False, peers=[], sync_token='isolated-peer-token')
    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app) as client:
        store = app.state.ctx.store
        card = store.create_card(CardCreate(title='acknowledged outcome', lane='active'))
        record = DispatchRecord(dispatch_id='dispatch-1', mutation_id='turn-1', card_id=card.id,
            realm_id='default', card_version=card.updated_at.isoformat(), card_snapshot=card.model_dump(mode='json'),
            authority_instance_id=settings.instance_id, authority_url='http://testserver',
            target_instance_id='target', session_id='worker-session', state='running')
        authority = app.state.ctx.services['dispatch_store']
        authority.put(record)
        if outcome in {'operator_state_preserved', 'conflict_requires_resolution'}:
            store.update_card(card.id, CardUpdate(lane='done' if outcome == 'operator_state_preserved' else 'waiting'))
        payload = {'summary': 'turn ended'}
        if outcome != 'not_applicable':
            payload['card_disposition'] = {'contract': 'pa.card-disposition/v1',
                'lane': 'waiting' if outcome == 'operator_state_preserved' else 'done',
                'outcome': 'verified turn outcome', 'evidence': {'integration_required': False}}
        target = DispatchStore(tmp_path / 'target-ledger')
        record.state = 'completion_pending'
        record.completion_payload = payload
        target.put(record)
        outbox = CompletionOutbox(target, 'isolated-peer-token')
        responses = []
        async def forward(url, **kwargs):
            response = client.post(url, **kwargs)
            responses.append(response)
            return response
        async def scenario():
            with patch.object(outbox, '_http_client', return_value=SimpleNamespace(post=forward)):
                await outbox._send(target.get('dispatch-1'))
            assert responses[0].status_code == 200, responses[0].text
            ack = responses[0].json()
            assert ack['acknowledged']
            assert ack['reconciliation']['state'] == outcome
            delivered = target.get('dispatch-1')
            assert delivered.state == 'completed'
            assert delivered.completion_delivery_class == 'acknowledged'
            assert delivered.reconciliation_state == outcome
            assert delivered.reconciliation_recoverable == (outcome == 'conflict_requires_resolution')
            session = AgentSession(id='worker-session', agent_name='codex', status='idle',
                                   purpose='automated_run', card_id=card.id)
            manager = SimpleNamespace(settings=settings, get=lambda _: None)
            policy = SessionLifecyclePolicy(manager, {})
            try:
                decision = await policy._decision(session, sessions=[session], dispatches=[delivered],
                    watches=[], leases=[], now=datetime.now(UTC))
            finally:
                await policy.close()
            assert decision == (('retained', 'reconciliation_active') if outcome == 'conflict_requires_resolution'
                                else ('close', 'dispatch_completed'))
            assert authority.get('dispatch-1').reconciliation_state == outcome
        asyncio.run(scenario())
    reset_store()
    reset_instance_agent()
