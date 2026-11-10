# Cleanup when the original branch reference is missing

The existing exact-target APIs can preserve and collect a terminal worktree whose
original leased branch was renamed for review. This is cleanup-only proof: it
does not repair a provider's execution binding, reopen a session, load history,
or change its branch, files, model, permissions, or defaults.

Add these fields to the existing `target` object for
`POST /api/workspaces/preserve-selected-untracked`, or each target for
`POST /api/workspaces/collect-selected`:

```json
{
  "lease_id": "<exact lease ID>",
  "expected_fence": 129,
  "expected_head": "<exact full HEAD>",
  "expected_updated_at": "<current lease timestamp>",
  "expected_actual_branch": "<observed branch>",
  "expected_native_id": "<saved native history identity>",
  "expected_owner_updated_at": "<current owner timestamp>"
}
```

`expected_owner_updated_at` is mandatory for a legacy owner with an empty
execution binding. That case requires exactly one non-cleaned lease for the
owner, matching saved CWD, card, project and execution instance. This follows
the existing lease-first legacy workspace resolution, without writing a binding.
A partial or conflicting binding is rejected. Missing identity/version evidence
is a blocker; it must not be guessed.

The original leased reference must be absent. The observed branch must share
the leased session's branch prefix, have the exact requested HEAD and belong to
no other lease. The owner must be closed with explicit durable idle state and an
empty queue, have exclusive native-history ownership, and no live runtime or
concurrent admission. These checks repeat after waits and before the effect.
Ordinary admission and automatic cleanup continue to reject the branch mismatch.
No branch or lease is rebound. Successful selected cleanup records the branch
proof on the original lease, and deletes only the observed reference using the
expected HEAD comparison. An intervening reference change cannot be force-deleted.

All existing completion/acceptance, expiry, restart, dirty/index, integration,
fresh-origin, attachment checksum, source metadata and open-file guards remain.
Preservation still requires the existing durable ZIP and retains the original
file or flat directory intact. Inventory and preserve valuable ignored artifacts
before collection; a clean Git status does not establish their custody.
This capability does not permit Git network access under an explicit network hold.

Keep the complete exact preservation request. After an uncertain response, call
its existing `action: "status"` with the same target and attachment identity;
do not repeat `preserve`. Existing requests with no new fields retain their old
operation IDs. Collection uncertainty is recovered with `workspace_leases`, not
by re-collecting an absent path. A cleaned lease confirms lifecycle state, while
preservation acceptance, caller attribution and measured bytes remain separate.
Missing historical receipt or allocation evidence cannot be reconstructed from
path absence or a concurrent volume free-space delta.
