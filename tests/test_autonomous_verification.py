"""Real canonical/dispatch/journal stores with bounded provider admission doubles."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from pa.config import Settings
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.domain.completion import CompletionConflict, completion_capabilities
from pa.domain.models import CardCreate, CardUpdate, CompletionEvidence
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.verification import VerificationService, VerificationStore
from pa.execution.verification_migration import migration_plan
from pa.health_journal.models import Assessment, Observation, Policy, digest
from pa.health_journal.service import HealthService
from pa.health_journal.store import Journal
from pa.pr_supervisor.models import PRWatch, PRWatchStatus
from pa.pr_supervisor.store import PRSupervisorStore
from tests.test_completion_requirements import projection


@pytest.fixture
def system(tmp_path):
    settings = Settings(data_dir=tmp_path/'data', instance_id='instance-a', workspace_root=tmp_path/'workspaces')
    ctx = AppContext(settings, HookBus(), projection(tmp_path))
    ctx.services['dispatch_store'] = DispatchStore(tmp_path/'dispatch')
    ctx.services['pr_supervisor_store'] = PRSupervisorStore(tmp_path/'prs.db')
    health = HealthService(ctx, Journal(tmp_path/'health.db', settings.instance_id))
    service = VerificationService(ctx, health, VerificationStore(tmp_path/'verification.db'))
    ctx.services.update(health_journal=health, verification=service)
    calls, replies = [], {}
    async def api(actor, method, path, *, body=None, key=None):
        calls.append((method, path, body, key))
        if key in replies:
            return replies[key]
        if path == '/api/fleet/dispatch':
            record = DispatchRecord(mutation_id=str(uuid4()), idempotency_key=key,
                card_id=body['card_id'], target_instance_id=settings.instance_id,
                authority_instance_id=settings.instance_id, authority_url='http://owner',
                principal_id=actor['principal_id'], state='running', session_id=str(uuid4()))
            ctx.services['dispatch_store'].put(record)
            replies[key] = {'dispatch_id': record.dispatch_id}
        elif path.startswith('/api/cards/'):
            card_id = path.split('/')[-1].split('?')[0]
            result = ctx.store.update_card(card_id, CardUpdate.model_validate(body),
                principal_id=actor['principal_id'], idempotency_key=key)
            replies[key] = result.model_dump(mode='json')
        else:
            raise AssertionError(path)
        return replies[key]
    health.local_api = api
    yield SimpleNamespace(ctx=ctx, service=service, health=health, calls=calls, replies=replies)
    ctx.services['dispatch_store'].close()
    health.pool.shutdown(wait=True)


def origin(system, card_id, **changes):
    record = DispatchRecord(mutation_id=str(uuid4()), card_id=card_id,
        target_instance_id='instance-a', authority_instance_id='instance-a', authority_url='http://owner',
        state='completed', session_id='implementation-session', acknowledged_at=datetime.now(UTC),
        completion_delivery_class='acknowledged', reconciliation_state='not_required', **changes)
    return system.ctx.services['dispatch_store'].put(record)


def ordinary(system, **changes):
    req = {'mode': 'explicit_acceptance', 'milestones': ['integrated', 'verified'],
           'acceptance_principals': ['user:local'], 'verification_policy': 'independent_agent_v1',
           'verification_principal': 'user:local', 'originating_session_id': 'implementation-session', **changes}
    card = system.ctx.store.create_card(CardCreate(title='Independent check', lane='waiting', completion_requirement=req))
    record = origin(system, card.id)
    card = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
        principal_id='instance:pr-supervisor', integration_evidence=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision, subject_revision='a'*40, outcome='integrated'),
        idempotency_key='integrated')
    return card, record


@pytest.mark.asyncio
async def test_dispatch_recovery_exact_assignment_defaults_and_acceptance(system):
    card, repair = ordinary(system)
    actual = system.health.local_api
    async def lose(*args, **kwargs):
        await actual(*args, **kwargs)
        raise httpx.ReadError('lost receipt after durable dispatch admission')
    system.health.local_api = lose
    await system.service.process(card)
    first = system.service.store.list()[0]
    assert first['state'] == 'blocked' and first['dispatch_id'] is None
    # Restarted scheduler recovers the existing ordinary dispatch by its key.
    system.health.local_api = actual
    service = VerificationService(system.ctx, system.health, VerificationStore(system.service.store.path))
    await service.process(card)
    job = service.store.list()[0]
    assert job['state'] == 'dispatched'
    assert len(system.ctx.services['dispatch_store'].list(card_id=card.id)) == 2
    assert len(system.calls) == 1
    payload = system.calls[0][2]
    assert not ({'provider', 'model_id', 'mode_id', 'allow_concurrent', 'capacity_override'} & payload.keys())
    assert payload['execution_contract']['profile'] == 'repository'
    verifier = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision='a'*40, milestones=['verified'], references=['verification-intent:' + job['id']])
    service.validate_acceptance(card, verifier, evidence)
    with pytest.raises(CompletionConflict, match='assignment_not_current'):
        service.validate_acceptance(card, repair, evidence)
    accepted = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at, completion_acceptance=evidence),
        principal_id=verifier.principal_id, actor_session_id=verifier.session_id,
        actor_dispatch_id=verifier.dispatch_id, idempotency_key='accepted-once')
    await service.process(accepted)
    assert system.ctx.store.get_card(card.id).lane == 'done'
    assert service.store.get(job['id'])['state'] == 'accepted'
    await service.process(system.ctx.store.get_card(card.id))
    assert len(service.store.list()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('changes,diagnostic', [({'acceptance_principals': []}, 'verification_principal_unconfigured'),
                                              ({'verification_principal': None}, 'verification_principal_unconfigured')])
async def test_missing_configuration_is_durable_work_without_operator_question(system, changes, diagnostic):
    card, _ = ordinary(system, **changes)
    for _ in range(3):
        await system.service.process(card)
    assert len(system.service.store.list()) == 1
    assert system.service.store.list()[0]['diagnostic'] == diagnostic
    assert not system.calls


@pytest.mark.asyncio
async def test_explicit_external_authority_and_integration_only_stay_declared(system):
    card, _ = ordinary(system, verification_policy='declared', acceptance_principals=['user:external'])
    await system.service.process(card)
    source = system.ctx.store.create_card(CardCreate(title='Source', completion_requirement={'mode':'integration_only'}))
    await system.service.process(source)
    assert not system.service.store.list() and not system.calls
    assert system.ctx.store.get_card(card.id).completion_requirement.acceptance_principals == ['user:external']


@pytest.mark.asyncio
async def test_requirement_subject_and_child_lineage_changes_invalidate_assignment(system):
    card, _ = ordinary(system)
    await system.service.process(card)
    job = system.service.store.list()[0]
    verifier = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision='a'*40, milestones=['verified'], references=['verification-intent:' + job['id']])
    child = system.ctx.store.create_card(CardCreate(title='Actual implementation', parent_id=card.id))
    child_origin = origin(system, child.id, dispatch_id='child-implementation')
    packet = system.service.packet(card)
    assert child_origin.dispatch_id in packet['origin_dispatches']
    with pytest.raises(CompletionConflict, match='assignment_not_current'):
        system.service.validate_acceptance(card, verifier, evidence)
    await system.service.process(card)
    assert len(system.calls) == 1  # Active old assignment prevents a rival.
    changed = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at,
        field_intent=['completion_requirement'], completion_requirement=card.completion_requirement.model_copy(update={'criteria':'New scenario'})))
    with pytest.raises(CompletionConflict, match='assignment_not_current'):
        system.service.validate_acceptance(changed, verifier, evidence)


@pytest.mark.asyncio
async def test_failed_verifier_dedupes_and_pending_repair_blocks(system):
    card, repair = ordinary(system)
    repair.state = 'running'
    system.ctx.services['dispatch_store'].put(repair)
    await system.service.process(card)
    assert system.service.store.list()[0]['diagnostic'] == 'repair_workflow_pending'
    repair.state = 'completed'
    system.ctx.services['dispatch_store'].put(repair)
    await system.service.process(card)
    job = system.service.store.list()[0]
    verifier = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    verifier.state = 'failed'
    system.ctx.services['dispatch_store'].put(verifier)
    for _ in range(3):
        await system.service.process(card)
    assert len(system.calls) == 1 and system.service.store.get(job['id'])['state'] == 'failed'


def health_repair(system, *, legacy=False):
    system.health.runtime_build['source_revision'] = 'b'*40
    journal = system.health.journal
    journal.configure(Policy(authority_id='instance-a', enabled=True, project_id='project', principal_id='user:local'),
        expected_version=1, actor='user:local')
    observation = Observation(subsystem='sync', error_code='failure', summary='A reproducible issue',
                              occurrence_key='one', reproduction='Run the declared smoke scenario')
    report = journal.append(observation, principal='user:local', realm='default', key='report')
    page = journal.page(realms=['default'])['items'][0]
    lease = journal.lease(system.health.owner)
    journal.ingest(page, source_id='instance-a', realms=['default'], owner=system.health.owner, fence=lease['fence'])
    action = journal.action(owner=system.health.owner, fence=lease['fence'])
    card = system.ctx.store.create_card(CardCreate(title='Canonical repair', lane='waiting'))
    record = origin(system, card.id)
    journal.action_result(action['id'], state='dispatched', result={'card_id':card.id,'dispatch_id':record.dispatch_id}, owner=system.health.owner, fence=lease['fence'])
    criteria = f' Verify the declared repair build and scenario for health-group:{action["group_id"]}; retain exact canonical acceptance receipt and affected instance references.'
    req = {'mode':'explicit_acceptance', 'milestones':['verified'], 'criteria':criteria}
    if not legacy:
        req.update(provenance='system_health', source_reference='health-group:'+action['group_id'],
            verification_policy='independent_agent_v1', acceptance_principals=['user:local'],
            verification_principal='user:local', originating_dispatch_id=record.dispatch_id)
    plan = {'method':'PATCH', 'path':'/api/cards/'+card.id+'?realm=default', 'body':{
        'expected_version':card.updated_at.isoformat(), 'field_intent':['completion_requirement'], 'completion_requirement':req}}
    journal.reserve_effect(action['id'], 'requirement', plan, owner=system.health.owner, fence=lease['fence'])
    card = system.ctx.store.update_card(card.id, CardUpdate.model_validate(plan['body']))
    group = journal.group(action['group_id'], realms=['default'])
    journal.assess(group['id'], Assessment(expected_version=group['version'], disposition='merged',
        reason='Exact source merge', card_id=card.id, commit='b'*40, pr_url='https://github.com/test/pa/pull/1'), actor='user:local', realms=['default'])
    watch = PRWatch(repository='test/pa', pr_number=1, pr_url='https://github.com/test/pa/pull/1',
        card_id=card.id, originating_session_id=record.session_id, dispatch_id=record.dispatch_id,
        status=PRWatchStatus.MERGED, head_sha='c'*40, state={'merge_commit_sha':'b'*40})
    watch.state.update(state='merged', head_sha='c'*40, confirmed_head_sha='c'*40,
                       stable_green_evidence={'green': True, 'head_sha':'c'*40})
    watch.stable_head_observations = watch.policy.stable_observations
    watch.stable_head_since = datetime.now(UTC) - timedelta(seconds=watch.policy.stable_head_seconds + 1)
    system.ctx.services['pr_supervisor_store'].upsert_watch(watch)
    return card, action, lease


@pytest.mark.asyncio
async def test_legacy_onboarding_preserves_history_and_requires_exact_proof(system):
    card, action, lease = health_repair(system, legacy=True)
    before = system.ctx.store.event_log.entity_history('default','card',card.id)
    result = await system.service.migrate(card.id, 'default', 'system_health', card.completion_requirement.revision,
        card.updated_at.isoformat(), key='migration', apply=False)
    assert result['eligible'] and system.ctx.store.get_card(card.id) == card
    await system.service.process(card)
    current = system.ctx.store.get_card(card.id)
    assert current.completion_requirement.criteria == card.completion_requirement.criteria
    assert current.completion_requirement.milestones == ['verified']
    assert current.completion_requirement.revision != card.completion_requirement.revision
    assert current.completion_requirement.originating_dispatch_ids
    assert current.lane == 'waiting' and not current.completion_evidence
    after = system.ctx.store.event_log.entity_history('default','card',card.id)
    assert [r['event_hash'] for r in after[:len(before)]] == [r['event_hash'] for r in before]


@pytest.mark.asyncio
async def test_health_scope_full_coverage_stale_build_and_durable_collector_handoff(system, monkeypatch):
    card, action, lease = health_repair(system)
    # Use the actual lifecycle obligation decision, with no live session obligation.
    system.ctx.services['session_lifecycle'] = SimpleNamespace(_decision=None)
    system.ctx.services['instance_agent'] = SimpleNamespace(workspace_manager=SimpleNamespace(list=lambda **kw: []))
    async def settled(_, **kwargs): return None
    monkeypatch.setattr(system.health, 'repair_obligation', settled)
    await system.health.dispatch_one(lease)
    assert not system.health.journal.status()['active_actions']
    assert system.service.store.list()[0]['packet']['health_group'] == action['group_id']
    await system.service.process(card)
    job = system.service.store.list()[0]
    verifier = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    refs = ['verification-intent:'+job['id'], 'health-group:'+action['group_id'],
            'scenario:'+job['packet']['scope']['scenario'], 'runtime-build:instance-a:'+'b'*40,
            'runtime-startup:instance-a:'+system.health.runtime_build['startup_id']]
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision='b'*40, milestones=['verified'], references=refs)
    with pytest.raises(CompletionConflict, match='scope_incomplete'):
        system.service.validate_acceptance(card, verifier, evidence)
    evidence.references.append('instance:instance-a')
    system.service.validate_acceptance(card, verifier, evidence)
    system.health.runtime_build['startup_id'] = 'different-loaded-process'
    with pytest.raises(CompletionConflict, match='assignment_not_current'):
        system.service.validate_acceptance(card, verifier, evidence)
    assert system.ctx.store.get_card(card.id).lane == 'waiting'
    assert system.health.journal.group(action['group_id'], realms=['default'])['disposition'] == 'merged'


def test_agent_policy_requires_new_owner_capability(system):
    card, _ = ordinary(system)
    assert 'completion-agent-verification:v1' in completion_capabilities(card.completion_requirement)


def test_child_migration_only_corrects_proven_copy_and_preserves_explicit_edits(system):
    from pa.domain.conversation_issues import ConversationIssues, IssueEdit, StartIssues
    from pa.domain.models import AgentSession
    parent = system.ctx.store.create_card(CardCreate(title='Parent', completion_requirement={
        'mode':'explicit_acceptance', 'criteria':'Verify health-group:unrelated', 'milestones':['verified']}))
    system.ctx.store.save_session(AgentSession(id='conversation', agent_name='codex'))
    notes = ConversationIssues(system.ctx.store)
    notes.edit('conversation', IssueEdit(revision=0, issues=[{'id':'child','title':'Unrelated repair'}]))
    req = parent.completion_requirement.model_dump(mode='json')
    context = {'source_card_id':parent.id, 'realm_id':'default', 'completion_requirement':req}
    state = notes.admit('conversation', StartIssues(request_id='selection', revision=1, issue_ids=['child']), context)
    child = system.ctx.store.create_card(CardCreate(title='Child', parent_id=parent.id,
        body='Source conversation: conversation', completion_requirement=req))
    groups = state['requests'][0]['groups']
    groups[0]['card_id'] = child.id
    notes.update_request('conversation','selection',groups=groups)
    corrected = migration_plan(system.service, child, 'inherited_child')
    assert corrected.mode == 'integration_only' and corrected.milestones == ['integrated']
    assert system.ctx.store.get_card(child.id).completion_requirement == child.completion_requirement
    changed = system.ctx.store.update_card(child.id, CardUpdate(expected_version=child.updated_at,
        field_intent=['completion_requirement'], completion_requirement=child.completion_requirement.model_copy(update={'criteria':'Explicit child verification'})))
    with pytest.raises(CompletionConflict, match='explicit_requirement_preserved'):
        migration_plan(system.service, changed, 'inherited_child')


@pytest.mark.asyncio
async def test_migration_lost_reply_replays_exact_saved_patch(system):
    card, _, _ = health_repair(system, legacy=True)
    actual = system.health.local_api
    async def lose(*args, **kwargs):
        await actual(*args, **kwargs)
        raise httpx.ReadError('lost migration receipt')
    system.health.local_api = lose
    args = (card.id, card.realm_id, 'system_health', card.completion_requirement.revision, card.updated_at.isoformat())
    with pytest.raises(httpx.ReadError):
        await system.service.migrate(*args, key='one-migration', apply=True)
    system.health.local_api = actual
    result = await system.service.migrate(*args, key='one-migration', apply=True)
    assert result['completion_requirement']['revision'] != card.completion_requirement.revision
    assert len(system.calls) == 2 and system.calls[0] == system.calls[1]


@pytest.mark.asyncio
async def test_handoff_releases_unrelated_triage_and_routes_novel_evidence_to_verification(system, monkeypatch):
    card, action, lease = health_repair(system)
    async def settled(_, **kwargs): return None
    monkeypatch.setattr(system.health, 'repair_obligation', settled)
    await system.health.dispatch_one(lease)
    await system.service.process(card)
    old = system.service.store.list()[0]
    journal = system.health.journal
    for code, occurrence in [('failure','one'), ('different','two')]:
        journal.append(Observation(subsystem='sync', error_code=code, summary='Changed bounded observation',
            occurrence_key=occurrence), principal='user:local', realm='default', key='new-'+code)
    for entry in journal.page(realms=['default'])['items']:
        journal.ingest(entry, source_id='instance-a', realms=['default'], owner=system.health.owner, fence=lease['fence'])
    next_action = journal.action(owner=system.health.owner, fence=lease['fence'])
    assert next_action['group_id'] != action['group_id']
    await system.service.process(card)
    assert len(system.service.store.list()) == 2
    assert len([c for c in system.calls if c[1] == '/api/fleet/dispatch']) == 1
    assert digest(system.service.packet(card)) != old['id']


@pytest.mark.asyncio
@pytest.mark.parametrize('race', ['active_dispatch', 'changed_group'])
async def test_handoff_does_not_release_active_or_changed_obligation(system, monkeypatch, race):
    card, action, lease = health_repair(system)
    async def obligation(_, **kwargs): return 'dispatch_active' if race == 'active_dispatch' else None
    monkeypatch.setattr(system.health, 'repair_obligation', obligation)
    ensure = system.service.ensure
    async def changed(card):
        result = await ensure(card)
        group = system.health.journal.group(action['group_id'], realms=['default'])
        system.health.journal.assess(group['id'], Assessment(expected_version=group['version'],
            disposition='merged', reason='Concurrent changed evidence'), actor='user:local', realms=['default'])
        return result
    if race == 'changed_group':
        monkeypatch.setattr(system.service, 'ensure', changed)
    await system.health.dispatch_one(lease)
    assert system.health.journal.status()['active_actions'][0]['id'] == action['id']


@pytest.mark.asyncio
@pytest.mark.parametrize('busy', [False, True])
async def test_first_placement_after_reopen_settles_only_safe_finished_run(system, monkeypatch, busy):
    import asyncio
    from unittest.mock import AsyncMock
    from starlette.requests import Request
    from pa.domain.models import AgentSession
    from pa.instance.session_lifecycle import SessionLifecyclePolicy
    from pa.modules.fleet import FleetDispatchBody, _resolve_policy_placement
    card = system.ctx.store.create_card(CardCreate(title='Reopened design', lane='active'))
    run = system.ctx.store.save_session(AgentSession(id='implementation-session', agent_name='codex',
        purpose='automated_run', lifecycle_owner='dispatch', status='idle', card_id=card.id))
    origin(system, card.id)
    runtime = SimpleNamespace(prompting=busy, _queue=[], _pending_permissions={}, _pending_elicitations={},
        _transcript_buffer=[], _transcript_queue=asyncio.Queue(), _closed=False, session=run, connection=None)
    lease = SimpleNamespace(session_id=run.id, state='completed', worktree_path='/isolated-fixture')
    manager = SimpleNamespace(store=system.ctx.store, settings=system.ctx.settings, get=lambda sid:runtime if sid == run.id else None,
        workspace_manager=SimpleNamespace(list=lambda:[lease], _status=lambda p:(False, False)),
        reconcile_closed_sessions=AsyncMock(), _runtimes={run.id:runtime})
    lifecycle = SessionLifecyclePolicy(manager, system.ctx.services)
    system.ctx.services['session_lifecycle'] = lifecycle
    class Inspected(Exception): pass
    def materialization(*args, **kwargs):
        assert (system.ctx.store.get_session(run.id).status == 'closed') is not busy
        assert runtime._closed is not busy
        raise Inspected()
    monkeypatch.setattr('pa.modules.fleet._check_dispatch_preview', lambda *a:None)
    monkeypatch.setattr('pa.modules.fleet._placement_materialization_plan', materialization)
    request = Request({'type':'http', 'headers':[], 'app':SimpleNamespace(state=SimpleNamespace(ctx=system.ctx))})
    try:
        with pytest.raises(Inspected):
            await _resolve_policy_placement(request, FleetDispatchBody(card_id=card.id), card=card, project=None, project_id=None)
    finally:
        await lifecycle.close()


def test_real_dispatch_and_bound_http_acceptance_use_current_assignment(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    from fastapi.testclient import TestClient
    from pa.acp.environment import assigned_service_session_capability
    from pa.auth.users import UserDirectory
    from pa.core.kernel import Kernel
    from pa.domain.models import AgentSession, ProjectCreate, RepositoryCreate
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent
    from pa.instance.session_lifecycle import SessionLifecyclePolicy
    from tests.test_fleet_placement import _candidate
    reset_store(); reset_instance_agent()
    monkeypatch.setattr(SessionLifecyclePolicy, 'start', lambda self:None)
    async def manual_scheduler(self): pass
    monkeypatch.setattr(VerificationService, 'run', manual_scheduler)
    settings = Settings(data_dir=tmp_path/'data', workspace_root=tmp_path/'workspaces', agent_enabled=False,
                        instance_id='authority', auth_required=True, peers=[])
    app = Kernel.boot(settings=settings).build_app()
    try:
        with TestClient(app) as client:
            ctx = app.state.ctx
            client.portal.call(ctx.require_service('dispatch_worker').close)
            project = ctx.store.create_project(ProjectCreate(title='Verification project'))
            repo = ctx.store.create_repository(RepositoryCreate(url='https://example.test/test/pa.git'))
            ctx.store.link_project_repository(project.id, repo.id)
            card = ctx.store.create_card(CardCreate(title='Ready for verification', lane='waiting', project_id=project.id,
                completion_requirement={'mode':'explicit_acceptance', 'milestones':['integrated','verified'],
                    'acceptance_principals':['user:local'], 'verification_policy':'independent_agent_v1',
                    'verification_principal':'user:local', 'originating_dispatch_id':'implementation'}))
            ledger = ctx.require_service('dispatch_store')
            ledger.put(DispatchRecord(dispatch_id='implementation', mutation_id='implementation', card_id=card.id,
                authority_instance_id=settings.instance_id, target_instance_id=settings.instance_id,
                authority_url='http://owner', state='failed', recoverable=False, reconciliation_state='not_required'))
            card = ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
                principal_id='instance:pr-supervisor', idempotency_key='integration', integration_evidence=CompletionEvidence(
                    requirement_revision=card.completion_requirement.revision, subject_revision='a'*40, outcome='integrated'))
            candidate = _candidate(settings.instance_id, local=True, repositories=[repo.id])
            candidate.capabilities.extend(['completion-requirements:v1','completion-agent-verification:v1'])
            monkeypatch.setattr('pa.modules.fleet._placement_candidates', AsyncMock(return_value=[candidate]))
            service = ctx.require_service('verification')
            client.portal.call(service.process, card)
            job = service.store.list(card.id)[0]
            assert job['state'] == 'dispatched', job
            record = ledger.get(job['dispatch_id'])
            assert record.goal_provenance is None
            session = ctx.store.save_session(AgentSession(agent_name='codex', dispatch_id=record.dispatch_id,
                authority_instance_id=settings.instance_id, status='active', card_id=card.id))
            record = ledger.put(record.model_copy(update={'session_id':session.id,'state':'running'}))
            monkeypatch.setattr(ctx.require_service('instance_agent'), 'get', lambda key:SimpleNamespace(_closed=False,connected=True) if key == session.id else None)
            capability = assigned_service_session_capability(secret=settings.session_secret, dispatch_id=record.dispatch_id,
                session_id=session.id, target_instance_id=settings.instance_id, purpose='completion-acceptance')
            headers = {'Authorization':'SessionAcceptance '+capability,
                       'X-PA-Assigned-Session-ID':session.id, 'X-PA-Assigned-Dispatch-ID':record.dispatch_id,
                       'Idempotency-Key':'acceptance'}
            body = {'expected_version':card.updated_at.isoformat(), 'completion_acceptance':{
                'requirement_revision':card.completion_requirement.revision, 'subject_revision':'a'*40,
                'milestones':['verified'], 'references':['verification-intent:'+job['id']]}}
            rejected = client.patch('/api/cards/'+card.id, headers=headers | {'Idempotency-Key':'wrong-subject'},
                json={**body,'completion_acceptance':{**body['completion_acceptance'],'subject_revision':'wrong-build'}})
            assert rejected.status_code == 409 and rejected.json()['detail']['code'] == 'verification_subject_or_actor_mismatch'
            response = client.patch('/api/cards/'+card.id, json=body, headers=headers)
            assert response.status_code == 200, response.text
            assert client.patch('/api/cards/'+card.id, json=body, headers=headers).json() == response.json()
            receipt = response.json()['completion_evidence'][-1]
            assert receipt['actor_session_id'] == session.id and receipt['actor_dispatch_id'] == record.dispatch_id
            accepted = ctx.store.get_card(card.id)
            client.portal.call(service.process, accepted)
            assert ctx.store.get_card(card.id).lane == 'done'
            assert service.store.get(job['id'])['state'] == 'accepted'
            # Agent-managed gates give no automatic operator-password request.
            page = client.get('/partials/cards/'+card.id+'/detail?realm=default')
            assert 'Sign in to review and accept' not in page.text
    finally:
        reset_instance_agent(); reset_store()


@pytest.mark.asyncio
async def test_provider_recovery_retries_same_dispatch_only_after_meaningful_change(system, monkeypatch):
    card, _ = ordinary(system)
    dependency = ['offline', False]
    async def provider(): return tuple(dependency)
    monkeypatch.setattr(system.service, 'provider_dependency', provider)
    await system.service.process(card)
    job = system.service.store.list()[0]
    record = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    record.state, record.session_id, record.recoverable, record.error_code = 'failed', None, True, 'no_compatible_execution'
    system.ctx.services['dispatch_store'].put(record)
    actual = system.health.local_api
    retries = []
    async def retry(actor, method, path, **kwargs):
        if path.endswith('/retry'):
            retries.append((path, kwargs['key']))
            record.state = 'queued'
            system.ctx.services['dispatch_store'].put(record)
            return {'dispatch_id':record.dispatch_id}
        return await actual(actor, method, path, **kwargs)
    system.health.local_api = retry
    for _ in range(3):
        await system.service.process(card)
    assert not retries
    dependency[:] = ['ready-model-catalog', True]
    await system.service.process(card)
    assert len(retries) == 1 and record.dispatch_id in retries[0][0]
    # A repeat failure on unchanged provider state does not schedule a loop.
    record.state = 'failed'
    system.ctx.services['dispatch_store'].put(record)
    await system.service.process(card)
    assert len(retries) == 1 and len(system.service.store.list()) == 1


@pytest.mark.asyncio
async def test_health_pause_and_transfer_keep_verification_fenced(system, monkeypatch):
    from pa.health_journal.store import JournalError
    card, _, _ = health_repair(system)
    await system.service.ensure(card)
    monkeypatch.setattr(system.health, 'peer_url', lambda _: 'http://peer')
    with pytest.raises(JournalError, match='authority_transfer_pending_verification'):
        await system.health.transfer('freeze', target_id='peer', expected_version=2, actor='user:local')
    policy = system.health.journal.status()['policy']
    system.health.journal.configure(Policy.model_validate(policy).model_copy(update={'paused':True}),
        expected_version=policy['version'], actor='user:local')
    await system.service.process(card)
    assert not system.calls


def accepted_receipt(system, card, job):
    record = system.ctx.services['dispatch_store'].get(job['dispatch_id'])
    evidence = CompletionEvidence(requirement_revision=card.completion_requirement.revision,
        subject_revision=job['packet']['subject'], milestones=['verified'], references=['verification-intent:'+job['id']])
    return system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at, completion_acceptance=evidence),
        principal_id=record.principal_id, actor_session_id=record.session_id,
        actor_dispatch_id=record.dispatch_id, idempotency_key='scoped-acceptance')


@pytest.mark.asyncio
async def test_lost_final_lane_receipt_recovers_accepted_intent_without_dispatch(system):
    card, _ = ordinary(system)
    await system.service.process(card)
    job = system.service.store.list()[0]
    accepted = accepted_receipt(system, card, job)
    actual = system.health.local_api
    async def lose(*args, **kwargs):
        await actual(*args, **kwargs)
        raise httpx.ReadError('lost final lane reply')
    system.health.local_api = lose
    with pytest.raises(httpx.ReadError):
        await system.service.process(accepted)
    assert system.ctx.store.get_card(card.id).lane == 'done'
    system.health.local_api = actual
    await system.service.process(system.ctx.store.get_card(card.id))
    assert system.service.store.get(job['id'])['state'] == 'accepted'
    assert len([c for c in system.calls if c[1] == '/api/fleet/dispatch']) == 1


@pytest.mark.asyncio
async def test_new_open_watch_racing_finish_prevents_done(system):
    card, _ = ordinary(system)
    await system.service.process(card)
    job = system.service.store.list()[0]
    accepted = accepted_receipt(system, card, job)
    system.ctx.services['pr_supervisor_store'].upsert_watch(PRWatch(repository='test/pa', pr_number=2,
        pr_url='https://github.com/test/pa/pull/2', card_id=card.id, status=PRWatchStatus.ACTIVE))
    await system.service.finish(accepted, job, accepted.completion_evidence[-1])
    assert system.ctx.store.get_card(card.id).lane == 'waiting'
    assert system.service.store.get(job['id'])['diagnostic'] == 'waiting_for_current_integration'


@pytest.mark.asyncio
async def test_active_durably_assigned_verifier_allows_obsolete_collector_release(system):
    card, action, lease = health_repair(system)
    record = system.ctx.services['dispatch_store'].get(system.health.journal.repair_dispatches(action['group_id'])[0])
    record.state, record.session_id, record.recoverable = 'failed', None, True
    system.ctx.services['dispatch_store'].put(record)
    system.ctx.services['session_lifecycle'] = SimpleNamespace()
    await system.service.process(card)
    job = system.service.store.list()[0]
    assert job['dispatch_id']
    await system.health.dispatch_one(lease)
    assert not system.health.journal.status()['active_actions']
    assert system.service.store.get(job['id'])['state'] == 'dispatched'


@pytest.mark.asyncio
async def test_unknown_repair_retry_keeps_collector_and_verification_fenced(system):
    card, action, lease = health_repair(system)
    journal = system.health.journal
    journal.action_result(action['id'], state='blocked', result={'retry_pending':True},
        owner=system.health.owner, fence=lease['fence'])
    current = journal.repair_action(action['group_id'], expected_version=journal.group(action['group_id'], realms=['default'])['version'], realms=['default'])
    record = system.ctx.services['dispatch_store'].get(current['result']['dispatch_id'])
    record.state, record.session_id, record.recoverable = 'failed', None, True
    system.ctx.services['dispatch_store'].put(record)
    system.ctx.services['session_lifecycle'] = SimpleNamespace()
    assert await system.health.repair_obligation(current) == 'repair_retry_outcome_unknown'
    await system.service.process(card)
    await system.health.dispatch_one(lease)
    assert journal.status()['active_actions'][0]['id'] == action['id'] and not system.calls
    assert system.service.store.list()[0]['diagnostic'] == 'repair_workflow_pending'
    record.control_operations['health-start_retry:'+action['id']] = 'retry'
    system.ctx.services['dispatch_store'].put(record)
    await system.health.dispatch_one(lease)
    assert not journal.status()['active_actions']
