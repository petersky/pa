"""Allowlisted repair rejection metadata; never expose arbitrary Git/path output."""

_REASONS = {
    'Exact owner is being admitted': 'owner_admitting',
    'Exact owner has an active or pending prompt': 'owner_busy',
    'Durable exact-owner idle evidence is unavailable': 'owner_idle_unverified',
    'Owner card binding changed': 'owner_card_changed',
    'Workspace belongs to another principal': 'owner_principal_mismatch',
    'Owner is not bound to this exact lease': 'owner_lease_mismatch',
    'Exact owner state changed': 'owner_state_changed',
    'Exact lease or fencing token changed': 'lease_fence_changed',
    'Lease is not an unfinished materialization target': 'lease_terminal',
    'Worktree HEAD changed': 'head_changed',
    'Only an empty index can be explicitly materialized': 'index_not_empty',
    'Index has an existing writer or unsafe path': 'index_writer_or_unsafe',
    'Existing tracked bytes differ from HEAD; retained for manual review': 'tracked_bytes_differ',
    'Existing tracked mode differs from HEAD; retained for manual review': 'tracked_mode_differs',
    'Existing tracked path type differs from HEAD': 'tracked_type_differs',
    'Tracked path has a retained symlink parent': 'tracked_symlink_parent',
    'Tracked path collides with retained data': 'tracked_collision',
    'Unsupported tracked path; manual preservation required': 'tracked_path_unsupported',
    'Special worktree files require manual preservation': 'special_file',
    'Worktree exceeds bounded preservation capacity': 'preservation_byte_limit',
    'Worktree exceeds bounded preservation entry capacity': 'preservation_entry_limit',
    'Insufficient preservation capacity; no worktree changes made': 'preservation_disk_capacity',
    'Idempotency key belongs to a different exact repair': 'operation_identity_mismatch',
    'Preservation or repair outcome unknown; inspect this same operation': 'operation_outcome_unknown',
    'No prepared preservation receipt for this exact operation': 'preservation_missing',
    'Exact preservation revision is required': 'preservation_revision_required',
    'Preservation revision changed': 'preservation_revision_changed',
    'Index, worktree or HEAD changed after preservation': 'preserved_state_changed',
    'Preserved bytes changed; refusing repair': 'preserved_bytes_changed',
    'Missing ignore definitions require explicit quarantine review': 'ignore_review_required',
}


def rejection_detail(reason: str) -> dict[str, str]:
    """Identify known guards while keeping unknown exception contents private.

    This describes a rejection, not whether an earlier operation committed.
    Callers must inspect the same workspace receipt before dependent mutations.
    """
    code = _REASONS.get(reason, 'precondition_failed')
    result = {'code': 'workspace_materialization_' + code,
              'recovery_action': 'workspace_materialization_repair(action=status)'}
    if reason in _REASONS:
        result['message'] = reason
    return result
