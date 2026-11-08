from datetime import UTC,datetime
from types import SimpleNamespace
from unittest.mock import Mock

from pa.auth.users import UserDirectory
from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.scheduling.events import poll_events
from pa.scheduling.store import Ledger,Schedule


def test_durable_event_source_restart_and_duplicate(tmp_path):
    user=UserDirectory(tmp_path).ensure_default_user()
    ledger=Ledger(tmp_path/'schedule.db')
    ledger.create('user:'+user.id,'create',Schedule(name='events',payload='Check current state',
        trigger={'kind':'event','event_type':EventType.CARD_CREATED.value,'filters':{'card':'target'}},
        destination={'kind':'inbox','target':'user:'+user.id}),0)
    log=Mock()
    log.get_head.return_value='baseline'
    ctx=SimpleNamespace(settings=SimpleNamespace(subscribed_realms=['default'],data_dir=tmp_path),services={'event_log':log})
    poll_events(ctx,ledger)
    event=CardEvent(id='stable-event',type=EventType.CARD_CREATED,realm_id='default',card_id='target',author_principal='user:local',author_instance='fixture')
    log.get_head.return_value='new'
    log.get_commit.return_value=SyncCommit(hash='new',realm_id='default',instance_id='fixture',parent_hashes=['baseline'],event_hashes=['event'])
    log.get_event.return_value=event
    poll_events(ctx,ledger)
    assert len(ledger.pending(datetime.now(UTC).timestamp()))==1
    poll_events(ctx,Ledger(tmp_path/'schedule.db'))
    assert len(ledger.pending(datetime.now(UTC).timestamp()))==1
    assert log.get_commit.call_count==1


def test_filters_use_actual_pa_card_lane_and_repository_payload_shapes():
    from pa.scheduling.events import event_fields
    card=CardEvent(type=EventType.CARD_UPDATED,realm_id='default',card_id='card',
        author_principal='user:local',author_instance='fixture',payload={'lane':'done'})
    assert event_fields(card)['state']=='done'
    repository=CardEvent(type=EventType.REPOSITORY_UPDATED,realm_id='default',
        author_principal='user:local',author_instance='fixture',payload={'id':'repo','status':'archived'})
    assert event_fields(repository)=={'card':None,'session':None,'state':'archived','repo':'repo'}
