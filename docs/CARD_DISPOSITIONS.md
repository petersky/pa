# Dispatch completion and card disposition

Remote agent transport and card workflow are separate state machines. An ACP
`end_turn` means that one model turn ended. It does not mean that work was
accepted, integrated, merged, or cleaned up.

PA therefore acknowledges a remote dispatch completion without changing its
card by default. The completion API accepts an optional `disposition` object.
Only the following versioned contract can request a lane change:

```json
{
  "contract": "pa.card-disposition/v1",
  "lane": "waiting",
  "outcome": "Implementation is ready, but CI is still running.",
  "evidence": {
    "integration_required": true,
    "pr_watch_id": "watch-id",
    "watched_head_sha": "40-character-head-sha",
    "merge_commit_sha": null,
    "references": ["https://github.com/owner/repo/pull/123"]
  }
}
```

`lane` is one of `active`, `waiting`, or `done`. `outcome` and `evidence` are
required. Unknown versions, extra fields, missing fields, and invalid values are
malformed. An absent or malformed disposition is audited on the dispatch and
preserves the current card lane.

## Done guard

A `done` request is a business assertion and is checked server-side. If the card
has linked PR watches, PA requires all linked watches to be merged and requires
the disposition to name a linked watch. The named watch must contain:

- an exact match among the requested head, watched head, observed head, and
  independently confirmed head;
- a matching, non-empty merge commit SHA;
- stable-green evidence for that exact head, including the configured stability
  window and observation count;
- terminal merged state when merge-on-green applies.

An open, pending, behind, failing, conflicted, closed, retired, or unknown watch,
unknown required-check/review state, unresolved review, unstable head, or absent
merge evidence causes Done to be downgraded to Waiting. The reason is stored in
the dispatch or PR-watch audit history. A card without linked integration may be
marked Done only when `integration_required` is explicitly `false`.

The PR supervisor uses this same public v1 contract when reconciling a merged
watch; it has no private bypass around the Done guard.

## Diagnostics and compatibility

Dispatch diagnostics report three independent facts: agent turn completion,
authority acknowledgement of dispatch completion, and the card-disposition
decision/lane. Completed legacy dispatch records are tagged
`legacy_unrecorded`; migration does not mutate their cards. In particular, PA
does not silently reopen cards that were already Done before this contract.

Completion remains idempotent. Replaying the same acknowledged mutation returns
the recorded disposition result, while a stale authority card version or a
mismatched mutation, target, card, realm, or session is still rejected.

## Autonomous verification

An explicitly declared `independent_agent_v1` verification policy uses the same
completion planner as system-generated health gates. Integration-only work keeps
its ordinary integration route. Independent verification remains bound to the
exact card, realm, requirement revision, source subject, eligible principal,
assigned dispatch/session, and complete excluded repair lineage. Its receipt must
reference the current `verification-intent:<id>` and required scenario/instance
scope. A changed declaration, build or membership invalidates a pending assignment.
Explicit external/user authority stays declared and is never replaced by an agent.

PA displays these routine gates as verification pending and schedules the work;
it does not ask the user to arrange an approver. Only current canonical successful
evidence permits completion. See [the journal workflow](HEALTH_JOURNAL.md) for
onboarding and migration APIs. Selected conversation issues receive their own
completion scope; parent repair requirements are not copied. Before placement of
a reopened card, PA runs the ordinary guarded lifecycle reconciliation for that
card so a finished idle automated run can close and expire its old lease on the
first attempt. Live prompts, interactions, dirty workspaces and unsettled outcomes
continue to retain their owners.
