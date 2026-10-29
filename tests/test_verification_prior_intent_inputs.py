"""Prior-intent facts preserve supersession/readiness and custom full readers."""
import ast
import hashlib
from pathlib import Path
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from pa.domain.models import CardUpdate, CompletionEvidence
from pa.execution.dispatch import DispatchRecord, VerificationPriorIntent
from pa.execution import verification
from tests.test_autonomous_verification import system, ordinary, origin


def older(system, **changes):
    bind_dispatch = changes.pop('bind_dispatch', True)
    card, _ = ordinary(system)
    old = system.service.store.reserve(system.service.packet(card))
    record = origin(system, card.id, session_id=changes.pop('session_id', None),
        idempotency_key='verification:' + old['id'], **changes)
    if bind_dispatch:
        system.service.store.update(old['id'], 'dispatched', dispatch_id=record.dispatch_id)
    card = system.ctx.store.update_card(card.id, CardUpdate(expected_version=card.updated_at),
        principal_id='instance:pr-supervisor', integration_evidence=CompletionEvidence(
            requirement_revision=card.completion_requirement.revision,
            subject_revision='b'*40, outcome='integrated'), idempotency_key='new-integration')
    return card, old, record


def archived_ensure():
    text = Path('tests/fixtures/verification_ensure_613.txt').read_text()
    assert hashlib.sha256(text.encode()).hexdigest() == 'b1502791d06cc230536be8714c221441d5da7570867c5032190319c570f59690'
    node = ast.parse(text).body[0]
    ns = dict(vars(verification))
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<exact613-ensure>', 'exec'), ns)
    return ns['ensure']


@pytest.mark.asyncio
@pytest.mark.parametrize('changes', [
    {}, {'state':'running'}, {'state':'failed','recoverable':True},
    {'state':'cancelled'}, {'target_instance_id':'remote'},
    {'followup_operations':{'pending':{'state':'delivery_ambiguous'}}},
    {'initial_prompt_operation':{'state':'requested'}},
])
async def test_complete_supersession_matches_archived_reader(system, changes):
    # origin already supplies state/target; build overrides after valid creation.
    card, old, record = older(system)
    for key, value in changes.items():setattr(record, key, value)
    system.ctx.services['dispatch_store'].put(record)
    results = []
    for call in [archived_ensure(), verification.VerificationService.ensure]:
        system.service.store.update(old['id'], 'dispatched', dispatch_id=record.dispatch_id)
        result = await call(system.service, card)
        results.append((result, system.service.store.get(old['id'])))
    # Intent timestamps update on reserve/update; compare all decision facts.
    for pair in zip(*results):
        left, right = [dict(v) for v in pair]
        for key in ['created_at','updated_at']:left.pop(key,None);right.pop(key,None)
        assert left == right
    assert not system.calls


@pytest.mark.asyncio
@pytest.mark.parametrize('lookup', ['id','authority'])
async def test_pre_session_plan_is_not_copied(system, lookup):
    card, old, record = older(system, bind_dispatch=lookup!='authority', materialization_plan={'steps':[{'retained':'x'*1024}]*1000})
    ledger = system.ctx.services['dispatch_store']
    if lookup == 'authority':
        assert system.service.store.get(old['id'])['dispatch_id'] is None
    original = DispatchRecord.model_copy
    def guarded(value, **kwargs):
        assert value.dispatch_id != record.dispatch_id, 'copied unrelated pre-session retained plan'
        return original(value, **kwargs)
    with patch.object(DispatchRecord, 'model_copy', guarded):
        assert await system.service.ensure(card)
    assert system.service.store.get(old['id'])['state'] == 'superseded'
    assert system.service.store.get(old['id'])['dispatch_id'] == record.dispatch_id
    assert not system.calls


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['get','by_authority_idempotency','_snapshot',
    '_verification_prior_intent','verification_prior_intent','verification_prior_intent_by_authority'])
async def test_custom_reader_keeps_full_fallback(system, name):
    card, old, record = older(system)
    ledger = system.ctx.services['dispatch_store'];snapshots=[];original=DispatchRecord.model_copy
    def counted(value, **kwargs):
        if value.dispatch_id == record.dispatch_id:snapshots.append(value.dispatch_id)
        return original(value, **kwargs)
    with patch.object(ledger, name, wraps=getattr(ledger,name)), patch.object(DispatchRecord,'model_copy',counted):
        await system.service.ensure(card)
    assert snapshots and not system.calls


def test_projection_is_detached_fresh_and_preserves_session_records(system):
    card, old, record = older(system, control_operations={'retained':'retry'})
    ledger=system.ctx.services['dispatch_store'];value=ledger.verification_prior_intent(record.dispatch_id)
    assert isinstance(value,VerificationPriorIntent)
    value.control_operations['retained']='changed'
    assert ledger.get(record.dispatch_id).control_operations=={'retained':'retry'}
    record.state='running';ledger.put(record)
    assert ledger.verification_prior_intent(record.dispatch_id).state=='running'
    record.session_id='later-session';ledger.put(record)
    full=ledger.verification_prior_intent(record.dispatch_id)
    assert isinstance(full,DispatchRecord) and full==ledger.get(record.dispatch_id)
    full.control_operations['retained']='changed'
    assert ledger.get(record.dispatch_id).control_operations=={'retained':'retry'}
    assert ledger.verification_prior_intent('missing') is None
    assert ledger.verification_prior_intent_by_authority('missing','missing') is None


def test_authority_lookup_matches_newest_and_tie_order(system):
    card, old, record=older(system);ledger=system.ctx.services['dispatch_store']
    other=record.model_copy(update={'dispatch_id':'other','mutation_id':'other','state':'running'})
    # put() stamps updated_at itself; freeze only these private row timestamps
    # so the first case genuinely exercises an equal-time insertion-order tie.
    fixed = record.updated_at
    class FixtureDatetime(datetime):
        @classmethod
        def now(cls, tz=None):return fixed
    for offset in [0, 1]:
        fixed = record.updated_at + timedelta(seconds=offset)
        with patch('pa.execution.dispatch.datetime', FixtureDatetime):
            ledger.put(other)
        actual = ledger.verification_prior_intent_by_authority(record.authority_instance_id,record.idempotency_key)
        expected = ledger.by_authority_idempotency(record.authority_instance_id,record.idempotency_key)
        assert actual.dispatch_id == expected.dispatch_id == (record.dispatch_id if offset == 0 else other.dispatch_id)
        assert ledger.get(other.dispatch_id).updated_at == fixed
        if offset == 0:assert ledger.get(record.dispatch_id).updated_at == fixed


@pytest.mark.asyncio
@pytest.mark.parametrize('action', [None,'wrong','retry'])
async def test_pending_recovery_confirmation_keeps_exact_control_receipt(system,action):
    card,old,record=older(system);ledger=system.ctx.services['dispatch_store']
    dependency='original-dependency'
    system.service.store.recovery(old['id'],dependency,'pending')
    if action is not None:
        record.control_operations['verification-retry:'+old['id']+':'+dependency]=action
        ledger.put(record)
    await system.service.ensure(card)
    assert (system.service.store.get(old['id'])['state']=='superseded')==(action=='retry')
    assert system.service.store.recovery(old['id'],dependency)==('confirmed' if action=='retry' else 'pending')
    assert not system.calls


@pytest.mark.parametrize('lookup', ['id','authority'])
def test_prior_intent_sees_only_committed_publication(system,lookup):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    card,old,record=older(system,control_operations={'original':'retry'})
    ledger=system.ctx.services['dispatch_store']
    read=(lambda:ledger.verification_prior_intent(record.dispatch_id)) if lookup=='id' else (lambda:ledger.verification_prior_intent_by_authority(record.authority_instance_id,record.idempotency_key))
    before=read();record.control_operations={'published':'retry'}
    entered,release=threading.Event(),threading.Event()
    def latch(boundary):
        if boundary=='commit_before':entered.set();assert release.wait(2)
    ledger._fault_injector=latch  # Only this disposable private transaction.
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(ledger.put,record)
            try:
                assert entered.wait(2)
                assert read()==before
            finally:release.set()
            future.result(timeout=2)
        assert read().control_operations=={'published':'retry'}
        assert before.control_operations=={'original':'retry'}
    finally:release.set();ledger._fault_injector=None


@pytest.mark.parametrize('lookup',['id','authority'])
def test_readability_and_yield_guards_remain(system,lookup):
    ledger=system.ctx.services['dispatch_store']
    call=(lambda:ledger.verification_prior_intent('missing')) if lookup=='id' else (lambda:ledger.verification_prior_intent_by_authority('missing','missing'))
    with patch.object(ledger,'_require_readable',side_effect=RuntimeError('closed')):
        with pytest.raises(RuntimeError,match='closed'):call()
    with patch.object(ledger,'_yield_to_index_writer',wraps=ledger._yield_to_index_writer) as yielded:
        assert call() is None
        yielded.assert_called_once_with()


def test_projection_runs_outside_index_lock_and_is_frozen(system):
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import FrozenInstanceError
    import threading
    card,old,record=older(system);ledger=system.ctx.services['dispatch_store']
    project=ledger._verification_prior_intent;entered,release=threading.Event(),threading.Event()
    def latch(value):entered.set();assert release.wait(2);return project(value)
    with patch.object(ledger,'_verification_prior_intent',latch),ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(ledger.verification_prior_intent,record.dispatch_id)
        try:
            assert entered.wait(2)
            assert ledger._index_lock.acquire(timeout=1)
            ledger._index_lock.release()
        finally:release.set()
        value=future.result(timeout=2)
    with pytest.raises(FrozenInstanceError):value.state='changed'


@pytest.mark.asyncio
async def test_class_reader_override_uses_full_fallback(system):
    card,old,record=older(system);ledger=system.ctx.services['dispatch_store']
    from pa.execution.dispatch import DispatchStore
    original=DispatchStore.get;calls=[]
    def custom(self,key):calls.append(key);return original(self,key)
    with patch.object(DispatchStore,'get',custom):await system.service.ensure(card)
    assert record.dispatch_id in calls and not system.calls


@pytest.mark.parametrize('lookup',['id','authority'])
def test_session_full_snapshot_keeps_original_index_lock_boundary(system,lookup):
    card,old,record=older(system,session_id='existing-session')
    ledger=system.ctx.services['dispatch_store'];snapshot=ledger._snapshot
    def guarded(value):
        assert ledger._index_lock._is_owned(), 'full session snapshot escaped original index-lock boundary'
        return snapshot(value)
    with patch.object(ledger,'_snapshot',guarded):
        expected=ledger.get(record.dispatch_id)
        actual=ledger.verification_prior_intent(record.dispatch_id) if lookup=='id' else ledger.verification_prior_intent_by_authority(record.authority_instance_id,record.idempotency_key)
    assert actual==expected


@pytest.mark.asyncio
async def test_legacy_direct_reader_does_not_access_unused_authority_reader(system):
    from types import SimpleNamespace
    card,old,record=older(system);ledger=system.ctx.services['dispatch_store']
    record.state='running';ledger.put(record)
    packet=system.service.packet(card)
    system.ctx.services['dispatch_store']=SimpleNamespace(get=ledger.get)
    try:
        with patch.object(system.service,'packet',return_value=packet):
            assert await system.service.ensure(card)
        assert system.service.store.get(old['id'])['state']=='dispatched'
    finally:system.ctx.services['dispatch_store']=ledger


@pytest.mark.asyncio
async def test_receipt_recovery_reselects_current_authority_reader(system):
    from unittest.mock import Mock
    card,old,record=older(system,bind_dispatch=False)
    ledger=system.ctx.services['dispatch_store'];original=ledger.by_authority_idempotency
    system.service.store.update(old['id'],'blocked',diagnostic='HTTPStatusError')
    first=Mock(return_value=None);later=Mock(wraps=original)
    async def recovered(job):
        assert job['id']==old['id']
        ledger.by_authority_idempotency=later
        return 'c'*64
    try:
        ledger.by_authority_idempotency=first
        with patch.object(system.service,'absent_admission_head',recovered):
            await system.service.ensure(card)
        assert first.call_count==1 and later.call_count==1
        assert system.service.store.get(old['id'])['dispatch_id']==record.dispatch_id
        assert system.service.store.get(old['id'])['state']=='superseded'
        assert not system.calls
    finally:del ledger.by_authority_idempotency
