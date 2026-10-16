"""Preserve observed green; never infer historical approval from a merge."""
from __future__ import annotations

from pa.pr_supervisor.gating import evaluate_gate


def check_identity(snapshot):
    return sorted([{'name':c.name, 'required':c.required, 'status':c.status,
        'conclusion':c.conclusion, 'details_url':c.details_url} for c in snapshot.checks], key=lambda c:c['name'])


def green_fingerprint(snapshot, policy):
    # GitHub no longer computes mergeability after merge. Only those transport
    # fields are normalized; checks, reviews, head and policy remain exact.
    if snapshot.stale or snapshot.confirmed_head_sha != snapshot.head_sha:
        return None
    allowed = {'success', *policy.allowed_neutral_conclusions}
    if any(not c.terminal or c.conclusion not in allowed for c in snapshot.checks):
        return None
    gate = evaluate_gate(snapshot.model_copy(update={
        'state':'open', 'mergeable':True, 'mergeable_state':'clean'}), policy, stable_head=True)
    return gate.fingerprint if gate.green else None


def preserved_green(watch, snapshot):
    proof = watch.state.get('stable_green_evidence') or {}
    if (proof.get('head_sha') == snapshot.head_sha == watch.head_sha
            and proof.get('policy_revision') == watch.policy_revision
            and proof.get('checks') == check_identity(snapshot)
            and proof.get('fingerprint') == green_fingerprint(snapshot, watch.policy)):
        return proof
    return None


def recover_green(watch, snapshot, events):
    """Recover only a complete, compatible sequence from immutable observations."""
    fingerprint = green_fingerprint(snapshot, watch.policy)
    if (not snapshot.merged or not fingerprint or not watch.provenance_version or not watch.policy_revision
            or snapshot.repository != watch.repository or snapshot.number != watch.pr_number
            or snapshot.url != watch.pr_url or snapshot.base_branch != watch.base_branch
            or snapshot.head_sha != watch.head_sha
            or snapshot.merge_commit_sha != watch.state.get('merge_commit_sha')
            or not snapshot.merge_commit_sha or not watch.stable_head_since):
        raise ValueError('terminal_green_identity_or_github_unconfirmed')
    ordered = sorted(events, key=lambda e:(e.created_at, e.id))
    greens = [e for e in ordered if e.event_type == 'observation'
        and e.head_sha == watch.head_sha and e.payload.get('green') is True
        and e.payload.get('stable_head') is True and e.condition_fingerprint == fingerprint]
    if len(greens) < watch.policy.stable_observations:
        raise ValueError('terminal_green_observations_missing')
    first, last = greens[0], greens[-1]
    if (watch.policy_snapshot_at > first.created_at
            or (first.created_at-watch.stable_head_since).total_seconds() < watch.policy.stable_head_seconds):
        raise ValueError('terminal_green_policy_or_stability_changed')
    # Later uncertainty may concern only GitHub's transient mergeability field.
    unknown_fingerprints = {
        evaluate_gate(snapshot.model_copy(update={'state':'open', 'mergeable':mergeable,
            'mergeable_state':state}), watch.policy, stable_head=True).fingerprint
        for mergeable in (None, True) for state in (None, 'unknown')}
    observations = []
    for event in ordered:
        if event.watch_id != watch.id:
            raise ValueError('terminal_green_watch_mismatch')
        if event.created_at < first.created_at:
            continue
        if event.event_type in {'watch_policy_reauthorized','provenance_repaired'}:
            raise ValueError('terminal_green_authority_changed')
        if event.event_type != 'observation':
            continue
        observations.append(event)
        if event.head_sha != watch.head_sha or event.payload.get('stable_head') is not True:
            raise ValueError('terminal_green_head_changed')
        if sorted(event.payload.get('checks', []), key=lambda c:c['name']) != check_identity(snapshot):
            raise ValueError('terminal_green_checks_changed')
        compatible_green = event.payload.get('green') is True and event.condition_fingerprint == fingerprint
        transient_unknown = (event.payload.get('actionable') is False
            and event.payload.get('reasons') == ['merge state is unknown']
            and event.condition_fingerprint in unknown_fingerprints)
        if not (compatible_green or transient_unknown):
            raise ValueError('terminal_green_later_evidence_changed')
    return {'green':True, 'head_sha':watch.head_sha, 'fingerprint':fingerprint,
        'policy_revision':watch.policy_revision, 'checks':check_identity(snapshot), 'observed_at':last.created_at.isoformat(),
        'observations':len(greens), 'stable_head_since':watch.stable_head_since.isoformat(),
        'source':'immutable_supervisor_observations', 'observation_ids':[e.id for e in observations],
        'github_revalidated_at':snapshot.observed_at.isoformat()}
