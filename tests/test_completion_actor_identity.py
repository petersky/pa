from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import BackgroundTasks, Response, HTTPException
from starlette.requests import Request

from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence, AgentSession
from pa.domain.projection import CardProjection
from pa.domain.completion import CompletionConflict
from pa.modules.items import _direct_human_card_action, update_card_api
from pa.config import Settings
from pa.acp.environment import assigned_service_session_capability


def actor_request(tmp_path, *, origin=False, bound=True, owners=None, same_card=False):
    from tests.test_completion_requirements import projection
    store = projection(tmp_path)
    settings = Settings(data_dir=tmp_path, instance_id='instance-a')
    card = store.create_card(CardCreate(title='acceptance', lane='waiting', completion_requirement={'mode': 'explicit_acceptance', 'milestones': ['verified'], 'acceptance_principals': ['user:local'] if owners is None else owners, 'originating_session_id': 'repair-session', 'originating_dispatch_id': 'repair-dispatch'}))
    session_id = 'repair-session' if origin else 'verifier-session'
    dispatch_id = 'repair-dispatch' if origin else 'verifier-dispatch'
    session = store.save_session(AgentSession(id=session_id, agent_name='codex', dispatch_id=dispatch_id, authority_instance_id='authority', status='active'))
    from pa.execution.dispatch import DispatchStore, DispatchRecord, GoalDispatchProvenance
    ledger = DispatchStore(tmp_path / 'dispatch')
    record = ledger.put(DispatchRecord(mutation_id='bound-run', dispatch_id=dispatch_id, session_id=session_id, target_instance_id=settings.instance_id, authority_instance_id='authority', authority_url='http://authority', state='running', principal_id='user:local', card_id=card.id if same_card else 'separate-verifier-card', goal_provenance=GoalDispatchProvenance(goal_id='verification', goal_version=1, policy_revision=1, authority_instance_id='authority', fencing_token=1, action_reservation_id='reservation', actor_principal='user:local')))
    services = {'dispatch_store': ledger, 'instance_agent': SimpleNamespace(get=lambda key: SimpleNamespace(_closed=False, connected=True))}
    ctx = SimpleNamespace(store=store, settings=settings, services=services, require_service=services.__getitem__)
    token = assigned_service_session_capability(secret=settings.session_secret, dispatch_id=dispatch_id, session_id=session_id, target_instance_id=settings.instance_id)
    request = Request({'type': 'http', 'method': 'PATCH', 'path': '/api/cards/'+card.id, 'headers': [(b'idempotency-key', b'accept-once'), (b'x-pa-assigned-session-id',session_id.encode()), (b'x-pa-assigned-dispatch-id',dispatch_id.encode())], 'app': SimpleNamespace(state=SimpleNamespace(ctx=ctx))})
    request.state.principal_id = 'user:local'
    request.state.assigned_session_capability = token if bound else None
    request.state.user_authenticated = False
    return store, card, request


def submit(store, card, request):
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision, subject_revision='build-a', milestones=['verified'], actor='forged', actor_session_id='forged', actor_dispatch_id='forged')
    with patch('pa.modules.items.get_store', return_value=store):
        return update_card_api(request, Response(), card.id, CardUpdate(lane='done', expected_version=card.updated_at, completion_acceptance=evidence), BackgroundTasks(), 'accept-once')


@pytest.mark.parametrize('same_card', [False, True])
def test_bound_independent_acceptance_stamps_existing_receipt(tmp_path, same_card):
    store, card, request = actor_request(tmp_path, same_card=same_card)
    result = submit(store, card, request)
    receipt = result['completion_evidence'][-1]
    assert result['lane'] == 'done'
    assert receipt['actor'] == 'user:local'
    assert receipt['actor_kind'] == 'bound_session'
    assert receipt['actor_session_id'] == 'verifier-session'
    assert receipt['actor_dispatch_id'] == 'verifier-dispatch'
    assert submit(store, card, request) == result


@pytest.mark.parametrize('origin,bound,owners,code', [(True, True, None, 'completion_self_acceptance_forbidden'), (False, False, None, 'completion_actor_unbound'), (False, True, [], 'completion_owner_unconfigured')])
def test_repair_unbound_and_unconfigured_actors_cannot_accept(tmp_path, origin, bound, owners, code):
    store, card, request = actor_request(tmp_path, origin=origin, bound=bound, owners=owners)
    with pytest.raises(HTTPException) as error:
        submit(store, card, request)
    assert error.value.detail['code'] == code
    assert store.get_card(card.id).lane.value == 'waiting'
    assert store.get_card(card.id).completion_evidence == []
    if owners == []:
        assert store.get_card(card.id).completion_status['reason_code'] == 'completion_owner_unconfigured'


def test_shared_bearer_or_proxy_header_absence_is_not_human_identity(tmp_path):
    _, _, request = actor_request(tmp_path, bound=False)
    assert not _direct_human_card_action(request)
    request.state.user_authenticated = True
    assert not _direct_human_card_action(request)  # CLI/shared bearer authentication
    request.state.authentication_method = 'browser_session'
    assert _direct_human_card_action(request)


def test_unverified_session_claim_cannot_supply_acceptance_identity(tmp_path):
    store, card, request = actor_request(tmp_path)
    request.state.assigned_session_capability = 'invalid-credential'
    with pytest.raises(HTTPException) as error:
        submit(store, card, request)
    assert error.value.status_code == 403
    assert error.value.detail['code'] == 'invalid_assigned_session_capability'
    assert store.get_card(card.id).completion_evidence == []
