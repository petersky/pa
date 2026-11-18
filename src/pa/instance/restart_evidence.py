"""Read canonical continuation evidence without auditing or recovering a receipt."""
from datetime import datetime
import json


def inspect_restart_evidence(store, session_id, handoff_id, *, owner_instance_id,
                            principal_id, is_admin, expected_version,
                            expected_attempt, expected_session_version,
                            expected_native_id, after_seq=-1, limit=64,
                            expected_snapshot=None):
    if not 1 <= limit <= 128 or not -1 <= after_seq <= 2**63-1 or (after_seq >= 0 and not expected_snapshot):
        raise ValueError('Invalid evidence bounds')
    identity = store.restart_evidence_identity(session_id, handoff_id)
    if identity is None:
        raise LookupError('Exact session/receipt not found')
    if not principal_id or (identity['principal_id'] != principal_id and not is_admin):
        raise PermissionError('Session belongs to another principal')
    if (identity['authority_instance_id'] != owner_instance_id
            or identity['instance_id'] != owner_instance_id
            or identity['phase_version'] != expected_version
            or identity['attempts'] != expected_attempt
            or datetime.fromisoformat(identity['session_version']) != expected_session_version
            or (identity['native_id'] or '') != expected_native_id):
        raise ValueError('Evidence identity/version changed')
    for key in ('prompt_id', 'native_id'):
        if identity[key] is not None and (not isinstance(identity[key], str) or len(identity[key]) > 512):
            raise ValueError('Unsupported evidence identity')
    page = store.transcripts.prompt_evidence_metadata(
        session_id, identity['prompt_id'], handoff_id, expected_attempt,
        after_seq=after_seq, limit=limit, expected_snapshot=expected_snapshot)
    if store.restart_evidence_identity(session_id, handoff_id) != identity:
        raise ValueError('Evidence owner snapshot changed during read')
    result = {
        'schema_version': 1, 'session_id': session_id, 'handoff_id': handoff_id,
        'continuation_prompt_id': identity['prompt_id'],
        'receipt_version': expected_version, 'receipt_attempt': expected_attempt,
        'session_version': identity['session_version'], 'native_id': identity['native_id'],
        'receipt_status': identity['status'], 'session_status': identity['session_status'],
        'authority_instance_id': owner_instance_id, 'evidence': page,
        'historical_effect': 'unknown', 'retention_complete': 'unknown',
        'restored': False, 'delivery_allowed': False, 'receipt_changed': False,
        'audit_written': False, 'replayed': False,
        'limits': ['Completion metadata is not proof of task effects.',
                   'Prompt identity alone does not attribute a completion to a receipt attempt.',
                   'Missing, cold, pruned or unscanned records never prove non-submission.',
                   'This read does not release a dismissal, close disposition or recovery fence.'],
    }
    if len(json.dumps(result).encode()) > 128 * 1024:
        raise ValueError('Evidence response exceeds capacity')
    return result
