"""Fleet's active hints preserve the indexed choice and detached public values."""
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from pa.execution.dispatch import DispatchRecord, DispatchStore, TERMINAL_DISPATCH_STATES


def record(index, **updates):
    values = dict(dispatch_id=f'd-{index}', mutation_id=f'm-{index}',
        card_id=f'c-{index}', realm_id='default', authority_instance_id='local',
        authority_instance_name='Old local', target_instance_id='remote',
        target_instance_name='Old remote', authority_url='http://fixture',
        state='running', placement_decision={'candidates': [{'proof': ['retained']}]},
        request_payload={'execution_selection': {'models': ['fixture']},
                         'collaboration_decision': {'mode': ['default']}})
    values.update(updates)
    return DispatchRecord(**values)


def legacy(store, ids, realm_id=None):
    return {key: row.public_dict() for key, row in store.latest_by_card(ids, realm_id=realm_id).items()
            if row.state not in TERMINAL_DISPATCH_STATES}


def test_active_card_public_projection_matches_legacy_and_never_snapshots_terminal_rows(tmp_path):
    store = DispatchStore(tmp_path)
    for i, state in enumerate(['running', 'blocked', 'completed', 'failed', 'cancelled', 'completion_pending']):
        store.put(record(i, state=state))
    ids = {f'c-{i}' for i in range(6)} | {'missing'}
    expected = legacy(store, ids)
    calls = []
    original = DispatchRecord.public_dict

    def project(row, **kwargs):
        calls.append(row.state)
        assert row.state not in TERMINAL_DISPATCH_STATES
        return original(row, **kwargs)

    with patch.object(store, '_snapshot', side_effect=AssertionError('full model copy')), \
         patch.object(DispatchRecord, 'public_dict', project):
        actual = store.active_public_by_card(ids)
    assert actual == expected
    assert sorted(calls) == ['blocked', 'completion_pending', 'running']
    assert store.active_public_by_card(set()) == {}
    store.close()


def test_active_card_public_projection_detaches_every_mutable_public_field(tmp_path):
    store = DispatchStore(tmp_path)
    store.put(record(0))
    expected = legacy(store, {'c-0'})
    value = store.active_public_by_card({'c-0'})['c-0']
    value['placement_decision']['candidates'][0]['proof'].append('changed')
    value['execution_selection']['models'].append('changed')
    value['collaboration']['decision']['mode'].append('changed')
    assert store.active_public_by_card({'c-0'}) == expected
    assert legacy(store, {'c-0'}) == expected
    store.close()


def test_active_card_public_projection_keeps_active_preference_and_realm_fence(tmp_path):
    store = DispatchStore(tmp_path)
    now = datetime.now(UTC)
    store.put(record(0, updated_at=now-timedelta(seconds=2)))
    store.put(record(1, card_id='c-0', state='completed', updated_at=now))
    store.put(record(2, realm_id='other'))
    ids = {'c-0', 'c-2'}
    for realm in (None, 'default', 'other', 'missing'):
        assert store.active_public_by_card(ids, realm_id=realm) == legacy(store, ids, realm)
    assert store.active_public_by_card(ids, realm_id='default')['c-0']['dispatch_id'] == 'd-0'
    current = store.get('d-0')
    current.state = 'completed'
    store.put(current)
    assert store.active_public_by_card({'c-0'}) == {}
    store.close()
