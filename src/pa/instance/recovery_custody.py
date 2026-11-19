"""Read checkpoint identity and native registry facts without recovery effects."""
import json
from pa.instance.binding_inspection import inspect_binding, fingerprint, BindingInspectionConflict


def _identifier(value):
    return isinstance(value, str) and 0 < len(value) <= 200 and not any(ord(c) < 32 for c in value)


def _checkpoint(row):
    unavailable = dict(state='unavailable', in_flight={'state': 'unavailable'},
                       queue={'state': 'unavailable'})
    if row['root_type'] != 'object' or row['duplicate_keys']:
        return unavailable | {'state': 'oversized' if (row['config_bytes'] or 0) > 262144 else 'unavailable'}
    kind = row['checkpoint_type']
    if kind != 'object':
        return unavailable | {'state': 'absent' if kind is None else 'null' if kind == 'null' else 'invalid'}
    kind = row['in_flight_type']
    flight = {'state': 'absent' if kind is None else 'null' if kind == 'null' else 'invalid'}
    if kind == 'object' and row['in_flight_id_type'] == 'text' and _identifier(row['in_flight_id']):
        flight = {'state': 'identified', 'prompt_id': row['in_flight_id']}
    kind = row['queue_type']
    queue = {'state': 'absent' if kind is None else 'null' if kind == 'null' else 'invalid'}
    if kind == 'array':
        count = row['queue_count']
        if count > 32:
            queue = {'state': 'capacity_exceeded', 'count': count}
        elif len(row['queue']) == count and all(r['item_type'] == 'object' and r['id_type'] == 'text'
                and _identifier(r['id']) for r in row['queue']):
            ids = [r['id'] for r in row['queue']]
            queue = {'state': 'observed', 'count': count, 'prompt_ids': ids,
                     'duplicate_ids': len(set(ids)) != len(ids)}
    return {'state': 'object', 'in_flight': flight, 'queue': queue}


def inspect_recovery_custody(manager, session_id, **preconditions):
    # Authorize and enforce ALL established binding/native/session/lease guards
    # before reading checkpoint or alias metadata. No runtime connect/get/load.
    before = inspect_binding(manager, session_id, history_limit=1, history_offset=0, **preconditions)
    row = manager.store.recovery_custody_metadata(session_id)
    if row is None or not _identifier(row['agent_name']):
        raise ValueError('Custody identity unavailable')
    checkpoint = _checkpoint(row)
    aliases = row['aliases']
    native = row['external_session_id']
    visible = []
    for alias in aliases[:32]:
        if not all(_identifier(alias[k]) for k in ('id', 'principal_id', 'authority_instance_id', 'updated_at')):
            raise ValueError('Custody alias shape unavailable')
        if alias['principal_id'] != preconditions['principal_id'] and not preconditions['is_admin']:
            continue
        visible.append({k: alias[k] for k in ('id', 'authority_instance_id', 'updated_at')})
    registry = {'state': 'observed' if native else 'native_identity_missing',
        'scope': 'exact_saved_agent_name_and_native_id_current_registry',
        'matching_sessions_lower_bound': len(aliases), 'truncated': len(aliases) > 32,
        'visible_sessions': visible,
        'hidden_matches_present': len(visible) < min(len(aliases), 32),
        'unique_in_observed_registry': len(aliases) == 1 if native and len(aliases) <= 32 else None}
    if (manager.store.recovery_custody_metadata(session_id) != row
            or inspect_binding(manager, session_id, history_limit=1, history_offset=0, **preconditions) != before):
        raise BindingInspectionConflict('snapshot_changed_during_read')
    observation = {'checkpoint': checkpoint, 'native_registry': registry}
    result = {'schema_version': 1, 'session_id': session_id,
        'session_version': before['session_version'], 'native_id': before['native_id'],
        'binding_hash': before['binding_hash'], 'lease_snapshot_hash': before['lease_snapshot_hash'],
        **observation, 'snapshot_hash': fingerprint(observation),
        'native_exclusive_ownership': 'unknown', 'runtime_inactivity': 'not_observed',
        'historical_effect': 'unknown', 'effect': 'none', 'restored': False, 'delivery_allowed': False,
        'limits': ['Repeated metadata snapshots are not an admission or provider ownership fence.',
                   'Registry uniqueness is not proof of exclusive native ownership or inactivity.',
                   'Missing/null checkpoints and queued identities do not prove historical non-delivery.',
                   'No filesystem, Git HEAD, cold body, transcript or provider process is inspected.']}
    if len(json.dumps(result).encode()) > 64 * 1024:
        raise ValueError('Custody output exceeds capacity')
    return result
