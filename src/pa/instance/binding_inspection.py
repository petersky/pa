"""Bounded provenance observations. Never restore, provision, or admit a provider."""
import hashlib
import json
from datetime import datetime

MAX_BINDING_BYTES = 16 * 1024
BINDING_FIELDS = ('version', 'execution_card_id', 'execution_project_id',
    'origin_instance_id', 'dispatch_id', 'realm_id', 'repository_ids',
    'worktree_paths', 'lease_ids', 'branch', 'base_sha', 'cwd')
LEASE_FIELDS = ('id', 'session_id', 'repository_id', 'card_id', 'project_id',
    'worktree_path', 'branch', 'base_sha', 'fencing_token', 'state', 'updated_at')
REASONS = {'workspace_binding_initialized', 'workspace_materialized',
    'legacy_workspace_recovered', 'workspace_pending_base_resolved',
    'stale_data_dir_cwd_removed', 'missing_cwd_removed'}


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()


def decode_binding(raw):
    if raw is None:
        return {'state': 'oversized', 'metadata': None}
    depth = 0
    quoted = escaped = False
    for c in raw:
        if quoted:
            if escaped:
                escaped = False
            elif c == '\\':
                escaped = True
            elif c == '"':
                quoted = False
        elif c == '"':
            quoted = True
        elif c in '[{':
            depth += 1
            if depth > 32:
                return {'state': 'depth_limit', 'metadata': None}
        elif c in ']}':
            depth -= 1
    try:
        return binding_metadata(json.loads(raw))
    except (ValueError, RecursionError):
        return {'state': 'malformed', 'metadata': None}


def binding_metadata(value):
    if not isinstance(value, dict) or len(json.dumps(value).encode()) > MAX_BINDING_BYTES:
        return {'state': 'unsupported_shape', 'metadata': None}
    result = {}
    for key in BINDING_FIELDS:
        if key not in value:
            continue
        item = value[key]
        if key == 'version' and type(item) is int:
            result[key] = item
        elif item is None:
            result[key] = None
        elif isinstance(item, str) and len(item) <= 1024 and not any(ord(c) < 32 for c in item):
            result[key] = item
        elif key in {'repository_ids', 'worktree_paths', 'lease_ids'} and isinstance(item, list) and len(item) <= 32 and all(isinstance(s, str) and len(s) <= 1024 and not any(ord(c) < 32 for c in s) for s in item):
            result[key] = item
        else:
            return {'state': 'unsupported_shape', 'metadata': None}
    return {'state': 'observed', 'metadata': result,
            'unrecognized_fields_present': bool(set(value) - set(BINDING_FIELDS))}


def inspect_binding(manager, session_id, *, owner_instance_id, principal_id,
                    is_admin, expected_session_version, expected_native_id,
                    expected_binding_hash, expected_lease_hash, history_limit=16,
                    history_offset=0):
    if not 1 <= history_limit <= 32 or not 0 <= history_offset <= 4096:
        raise ValueError('Inspection bounds invalid')
    session = manager.store.binding_inspection_session(session_id)
    if session is None:
        raise LookupError('Session not found')
    if not principal_id or (session['principal_id'] != principal_id and not is_admin):
        raise PermissionError('Session is owned by another principal')
    if session['authority_instance_id'] != owner_instance_id:
        raise ValueError('Saved authority changed')
    if (datetime.fromisoformat(session['updated_at']) != expected_session_version
            or (session['external_session_id'] or '') != expected_native_id
            or fingerprint(session['execution_binding'] or {}) != expected_binding_hash):
        raise ValueError('Saved session/native/binding version changed')
    leases = manager.workspace_manager.binding_inspection_leases(session_id)
    if fingerprint(leases) != expected_lease_hash:
        raise ValueError('Lease identity/fence/version snapshot changed')
    history = manager.store.binding_inspection_history(session_id,
        limit=history_limit, offset=history_offset)
    observed = binding_metadata(session['execution_binding'] or {})
    metadata = observed['metadata'] or {}
    bound = [l for l in leases if l['id'] in metadata.get('lease_ids', [])]
    current = [l for l in leases if l['state'] != 'cleaned']
    candidates = current if current else bound
    differences = []
    if len(candidates) == 1:
        lease = candidates[0]
        comparison = {'repository_ids': [lease['repository_id']],
            'worktree_paths': [lease['worktree_path']], 'lease_ids': [lease['id']],
            'branch': lease['branch'], 'base_sha': lease['base_sha']}
        differences = [k for k, v in comparison.items() if metadata.get(k) != v]
    else:
        differences = ['comparison_lease_not_unique_or_missing']
    gaps = ['historical_correctness_not_proven', 'filesystem_and_git_proof_not_inspected',
            'provider_outcome_unknown', 'restoration_capability_not_available']
    if not bound or {l['id'] for l in bound} != set(metadata.get('lease_ids') or []):
        gaps.append('original_lease_identity_incomplete')
    if len(candidates) > 1:
        gaps.append('comparison_lease_ambiguous')
    if not session['external_session_id']:
        gaps.append('native_identity_missing')
    if session['origin_instance_id'] != owner_instance_id:
        gaps.append('original_execution_authority_not_established')
    if any(l['state'] == 'cleaned' for l in bound):
        gaps.append('original_lease_cleaned')
    if metadata.get('base_sha') in (None, '', 'pending'):
        gaps.append('original_base_unresolved')
    # Read preconditions are checked again after all bounded work. No provider
    # access, admission, provisioning, lease renewal, or journal write occurs.
    if (manager.store.binding_inspection_session(session_id) != session
            or manager.workspace_manager.binding_inspection_leases(session_id) != leases
            or manager.store.binding_inspection_history(session_id,
                limit=history_limit, offset=history_offset) != history):
        raise ValueError('Inspection snapshot changed during read')
    result = {'schema_version': 1, 'session_id': session['id'],
        'authority_instance_id': owner_instance_id,
        'session_state': session['status'], 'origin_instance_id': session['origin_instance_id'],
        'session_version': session['updated_at'],
        'native_id': session['external_session_id'], 'binding_hash': expected_binding_hash,
        'lease_snapshot_hash': expected_lease_hash, 'binding': observed,
        'leases': leases, 'original_lease_ids_observed': [l['id'] for l in bound],
        'comparison_lease_id': candidates[0]['id'] if len(candidates) == 1 else None,
        'history': history, 'different_fields': differences,
        'restore_plan': {'state': 'unsupported', 'gaps': gaps,
            'required_next_actor': 'workspace_recovery_maintainer',
            'required_proof': ['original_binding_history_provenance',
                'exact_original_repository_branch_base_and_head',
                'exclusive_native_and_lease_ownership', 'dirty_and_queued_work_custody',
                'reviewed_admission_serialized_CAS_prepare_apply_status_contract']},
        'effect': 'none', 'historical_effect': 'unknown', 'restored': False,
        'mutation_authorized': False, 'delivery_allowed': False}
    if len(json.dumps(result).encode()) > 256 * 1024:
        raise ValueError('Inspection output exceeds fixed capacity')
    return result
