from datetime import timedelta

import pytest

from pa.pr_supervisor.green_evidence import check_identity, green_fingerprint, recover_green
from pa.pr_supervisor.gating import evaluate_gate
from pa.pr_supervisor.models import PRPolicy, PRWatchEvent, utcnow
from tests.test_pr_supervisor import snapshot, watch


def fixture():
    now = utcnow()
    merged = snapshot(state='merged', merge_commit_sha='c'*40, mergeable=None, mergeable_state='unknown')
    target = watch(policy=PRPolicy(stable_observations=2, stable_head_seconds=15))
    target.head_sha = merged.head_sha
    target.base_branch = merged.base_branch
    target.policy_revision = 'original-policy'
    target.policy_snapshot_at = now-timedelta(seconds=100)
    target.stable_head_since = now-timedelta(seconds=80)
    target.state = merged.model_dump(mode='json')
    events = [PRWatchEvent(watch_id=target.id, event_key=str(i), event_type='observation',
        created_at=now-timedelta(seconds=40-20*i), head_sha=target.head_sha,
        condition_fingerprint=green_fingerprint(merged, target.policy),
        payload={'green':True,'stable_head':True,'checks':check_identity(merged)}) for i in range(2)]
    unknown = evaluate_gate(merged.model_copy(update={'state':'open'}), target.policy, stable_head=True)
    events.append(PRWatchEvent(watch_id=target.id, event_key='unknown', event_type='observation',
        head_sha=target.head_sha, condition_fingerprint=unknown.fingerprint,
        payload={'green':False,'actionable':False,'stable_head':True,'reasons':unknown.reasons,
                 'checks':check_identity(merged)}))
    return target, merged, events


def test_recovery_uses_original_green_observations_and_fresh_exact_github():
    target, merged, events = fixture()
    proof = recover_green(target, merged, events)
    assert proof['observed_at'] == events[1].created_at.isoformat()
    assert proof['observation_ids'] == [e.id for e in events]
    assert proof['observations'] == 2 and proof['head_sha'] == target.head_sha


@pytest.mark.parametrize('change', ['head','confirmed_head','merge','failure','pending','check_run',
    'review','threads_unknown','policy','missing','late_failure','late_review','late_head','late_policy','provenance',
    'repository','number','base','foreign_event'])
def test_recovery_rejects_missing_or_changed_proof(change):
    target, merged, events = fixture()
    if change == 'head': merged.head_sha = 'd'*40
    elif change == 'confirmed_head': merged.confirmed_head_sha = 'd'*40
    elif change == 'merge': merged.merge_commit_sha = 'd'*40
    elif change == 'failure': merged.checks[0].conclusion = 'failure'
    elif change == 'pending': merged.checks[0].status = 'in_progress'
    elif change == 'check_run': merged.checks[0].details_url = 'https://example.test/different-run'
    elif change == 'review': merged.review_decision = 'CHANGES_REQUESTED'
    elif change == 'threads_unknown': merged.review_threads_known = False
    elif change == 'policy': target.policy_snapshot_at = utcnow()
    elif change == 'missing': events.pop(0)
    elif change == 'provenance': target.provenance_version = 0
    elif change == 'repository': merged.repository = 'other/repo'
    elif change == 'number': merged.number += 1
    elif change == 'base': merged.base_branch = 'other'
    elif change == 'foreign_event': events[0].watch_id = 'other-watch'
    elif change == 'late_failure': events[-1].payload['actionable'] = True
    elif change == 'late_review': events[-1].condition_fingerprint = 'new-review-fingerprint'
    elif change == 'late_head': events[-1].head_sha = 'd'*40
    elif change == 'late_policy': events[-1].event_type = 'watch_policy_reauthorized'
    with pytest.raises(ValueError, match='terminal_green_'):
        recover_green(target, merged, events)
