"""Compressed-clock ledger tests; these do not claim real ten-minute evidence."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json

import pytest
from pa.scheduling.store import Ledger, Schedule, Trigger


def spec(**overrides):
    return Schedule.model_validate({'name': 'report', 'trigger': {'kind': 'interval', 'seconds': 600},
        'destination': {'kind': 'agent_session', 'target': 'existing-session'},
        'payload': 'Check current state and report to this conversation', **overrides})


def test_catchup_restart_ack_and_stable_retry(tmp_path):
    path = tmp_path / 's.db'
    ledger = Ledger(path)
    schedule = ledger.create('owner', 'create', spec(), 0)
    ledger.tick(2400)
    pending = ledger.pending(2400)
    assert len(pending) == 1
    oid = pending[0]['id']
    assert json.loads(pending[0]['envelope'])['missed_count'] == 3
    ledger.finish(oid, 2400, error='unavailable_target')
    ledger = Ledger(path)
    assert ledger.pending(2401) == []
    assert ledger.pending(2402)[0]['id'] == oid
    ledger.finish(oid, 2402, accepted=True)
    assert not ledger.pending(2403)
    ledger.tick(3000)
    assert len(ledger.pending(3000)) == 1
    assert ledger.get('owner', schedule['id'])['next_due'] == 3600
    assert ledger.create('owner', 'create', spec(), 9999) == schedule


def test_concurrent_tick_deduplicates(tmp_path):
    ledger = Ledger(tmp_path / 's.db')
    schedule = ledger.create('owner', 'create', spec(), 0)
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda _: ledger.tick(600), range(12)))
    assert len(ledger.history('owner', schedule['id'])) == 1


def test_pause_resume_update_delete_and_version(tmp_path):
    ledger = Ledger(tmp_path / 's.db')
    s = ledger.create('owner', 'create', spec(), 0)
    paused = spec(enabled=False)
    ledger.update('owner', s['id'], 'pause', paused, 1, 10)
    ledger.tick(600)
    assert not ledger.pending(600)
    with pytest.raises(ValueError, match='version'):
        ledger.update('owner', s['id'], 'stale', spec(), 1, 700)
    ledger.update('owner', s['id'], 'resume', spec(), 2, 700)
    ledger.tick(700)
    assert len(ledger.pending(700)) == 1
    ledger.update('owner', s['id'], 'delete', spec(), 3, 700, delete=True)
    assert not ledger.pending(700)
    with pytest.raises(KeyError):
        ledger.get('owner', s['id'])


def test_event_filter_duplicate_and_owner_isolation(tmp_path):
    ledger = Ledger(tmp_path / 's.db')
    s = ledger.create('owner', 'create', spec(trigger={'kind':'event', 'event_type':'card.changed', 'filters':{'state':'done'}}), 0)
    assert ledger.event('e1', 'card.changed', {'state':'active'}, 10) == []
    first = ledger.event('e2', 'card.changed', {'state':'done'}, 10)
    assert ledger.event('e2', 'card.changed', {'state':'done'}, 20) == first
    with pytest.raises(KeyError):
        ledger.history('other', s['id'])
    assert ledger.list('other') == []


def test_exhaustion_manual_retry_reuses_id(tmp_path):
    ledger = Ledger(tmp_path / 's.db')
    s = ledger.create('owner', 'create', spec(), 0)
    oid = ledger.fire('owner', s['id'], 'fire', 1)['occurrence_id']
    for i in range(8):
        ledger.finish(oid, i, error='target_missing')
    assert not ledger.pending(9999)
    ledger.retry('owner', s['id'], oid, 'retry', 100)
    assert ledger.pending(100)[0]['id'] == oid
    ledger.finish(oid, 100, accepted=True)
    with pytest.raises(ValueError):
        ledger.retry('owner', s['id'], oid, 'retry2', 101)


@pytest.mark.parametrize('policy,count,missed', [('catch_up',1,9),('skip',0,0),('replay',3,7)])
def test_missed_policies(tmp_path, policy, count, missed):
    ledger = Ledger(tmp_path / 's.db')
    ledger.create('owner', 'create', spec(missed_policy=policy, replay_limit=3), 0)
    ledger.tick(6000)
    pending = ledger.pending(6000)
    assert len(pending) == count
    assert sum(json.loads(r['envelope'])['missed_count'] for r in pending) == missed


def test_coalesce_and_maximum(tmp_path):
    ledger = Ledger(tmp_path / 's.db')
    ledger.create('owner', 'create', spec(coalesce=True, max_occurrences=2), 0)
    ledger.tick(600)
    ledger.tick(1200)
    pending = ledger.pending(1200)
    assert len(pending) == 1
    assert json.loads(pending[0]['envelope'])['coalesced_count'] == 1
    ledger.tick(1800)
    assert ledger.pending(1800) == pending


def test_cron_preview_timezone_dst():
    t = Trigger(kind='cron', cron='0 9 * * *', timezone='America/New_York')
    before = datetime(2026,3,7,14,tzinfo=UTC).timestamp()
    next_time = t.next(before)
    assert datetime.fromtimestamp(next_time, UTC) == datetime(2026,3,8,13,tzinfo=UTC)
    fall = datetime(2026,10,31,13,tzinfo=UTC).timestamp()
    assert datetime.fromtimestamp(t.next(fall), UTC) == datetime(2026,11,1,14,tzinfo=UTC)


@pytest.mark.asyncio
async def test_lost_ack_retry_same_envelope_and_id(tmp_path):
    from pa.scheduling.service import Scheduler
    ledger = Ledger(tmp_path/'s.db')
    ledger.create('owner','create',spec(),0)
    accepted = {}
    calls = []
    async def deliver(item):
        calls.append(item['envelope'])
        if item['id'] not in accepted:
            accepted[item['id']] = item['envelope']
            raise TimeoutError('lost acknowledgment after destination acceptance')
        assert accepted[item['id']] == item['envelope']
        return {'accepted':True}
    service = Scheduler(None,None,ledger,deliver)
    await service.cycle(600)
    await service.cycle(602)
    assert len(accepted)==1 and len(calls)==2
    assert not ledger.pending(999)


@pytest.mark.asyncio
async def test_runner_process_fence_and_recovery(tmp_path):
    import fcntl
    from pa.scheduling.service import Scheduler
    ledger = Ledger(tmp_path/'s.db')
    ledger.create('owner','create',spec(),0)
    accepted=[]
    async def deliver(item):
        accepted.append(item['id'])
        return {'accepted':True}
    scheduler = Scheduler(None,None,ledger,deliver)
    with ledger.path.with_suffix('.runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        await scheduler.cycle(600)
        assert accepted==[]
    ledger.tick(600)
    oid=ledger.pending(600)[0]['id']
    ledger.claim(oid,600)  # Simulate process death before recording the acceptance.
    await scheduler.cycle(601)
    assert accepted==[oid]


def test_destination_correction_preserves_failed_occurrence(tmp_path):
    ledger=Ledger(tmp_path/'s.db')
    s=ledger.create('owner','create',spec(),0)
    oid=ledger.fire('owner',s['id'],'fire',1)['occurrence_id']
    ledger.finish(oid,1,error='missing')
    corrected=spec(destination={'kind':'agent_session','target':'corrected'})
    ledger.update('owner',s['id'],'correct',corrected,1,2)
    pending=ledger.pending(3)
    assert pending[0]['id']==oid
    assert json.loads(pending[0]['destination'])['target']=='corrected'


@pytest.mark.parametrize('url',['http://example.com','https://user:secret@example.com','https://127.0.0.1/test','https://[::1]/test'])
def test_webhook_rejects_unsafe_destination(url):
    from pa.scheduling.webhook import send_webhook
    from pa.scheduling.errors import DeliveryError
    with pytest.raises(DeliveryError):
        send_webhook(url,{},'stable-id')


def test_dst_nonexistent_and_repeated_local_time():
    spring=Trigger(kind='cron',cron='30 2 * * *',timezone='America/New_York')
    point=spring.next(datetime(2026,3,7,8,tzinfo=UTC).timestamp())
    assert datetime.fromtimestamp(point,UTC)==datetime(2026,3,8,7,tzinfo=UTC)
    fall=Trigger(kind='cron',cron='30 1 * * *',timezone='America/New_York')
    point=fall.next(datetime(2026,10,31,7,tzinfo=UTC).timestamp())
    assert datetime.fromtimestamp(point,UTC)==datetime(2026,11,1,5,30,tzinfo=UTC)
    assert datetime.fromtimestamp(fall.next(point),UTC)==datetime(2026,11,1,6,30,tzinfo=UTC)


def test_long_cron_outage_is_bounded_and_resumable(tmp_path):
    ledger=Ledger(tmp_path/'s.db')
    ledger.create('owner','create',spec(trigger={'kind':'cron','cron':'* * * * *'}),0)
    ledger.tick(180000)
    assert ledger.pending(180000)==[]
    ledger.tick(180000)
    ledger.tick(180000)
    result=ledger.pending(180000)
    assert len(result)==1
    assert json.loads(result[0]['envelope'])['missed_count']==2999


@pytest.mark.parametrize('expression',['0 0 L * *','0 0 ? * MON','0 0 * * MON#2','@daily'])
def test_cron_extensions_are_rejected(expression):
    with pytest.raises(ValueError):
        Trigger(kind='cron',cron=expression)


def test_unknown_timezone_is_validation_error():
    with pytest.raises(ValueError,match='unknown IANA timezone'):
        Trigger(kind='cron',cron='0 0 * * *',timezone='Not/AZone')


def test_coalesce_preserves_unknown_delivery_after_crash(tmp_path):
    ledger=Ledger(tmp_path/'s.db')
    ledger.create('owner','create',spec(coalesce=True),0)
    ledger.tick(600)
    original=ledger.pending(600)[0]['id']
    ledger.claim(original,600)  # Destination may have accepted before process death.
    ledger.recover()
    ledger.tick(1200)
    pending=ledger.pending(1200)
    assert len(pending)==2
    assert original in {row['id'] for row in pending}


@pytest.mark.asyncio
async def test_slow_destination_does_not_block_other_acceptance(tmp_path):
    import asyncio
    from pa.scheduling.service import Scheduler
    ledger=Ledger(tmp_path/'s.db')
    first=ledger.create('owner','slow',spec(name='slow'),0)
    second=ledger.create('owner','healthy',spec(name='healthy'),0)
    ledger.fire('owner',first['id'],'fire-slow',1)
    ledger.fire('owner',second['id'],'fire-healthy',1)
    healthy=asyncio.Event()
    release=asyncio.Event()
    async def deliver(item):
        if item['schedule_id']==first['id']:
            await release.wait()
        else:
            healthy.set()
        return {'accepted':True}
    scheduler=Scheduler(None,None,ledger,deliver)
    cycle=asyncio.create_task(scheduler.cycle(1))
    try:
        await asyncio.wait_for(healthy.wait(),2)
    finally:
        release.set()
        await cycle
    assert all(ledger.history('owner',r['id'])[0]['state']=='delivered' for r in [first,second])


@pytest.mark.parametrize('now,expected',[(600.5,1),(3600,0)])
def test_skip_policy_for_once_has_one_polling_second_grace(tmp_path,now,expected):
    ledger=Ledger(tmp_path/'s.db')
    ledger.create('owner','create',spec(trigger={'kind':'once','at':datetime.fromtimestamp(600,UTC)},missed_policy='skip'),0)
    ledger.tick(now)
    assert len(ledger.pending(now))==expected
