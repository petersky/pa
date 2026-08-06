# Post-turn evaluation

PA treats four facts as separate:

1. an ACP turn ended;
2. its result was durably delivered;
3. the requested card outcome was achieved;
4. PA selected and executed a follow-up action.

`pa.turn-end-snapshot/v1` is the immutable neutral boundary. It records the
stop reason, provider/session and dispatch state, card lanes, disposition and
parse errors, sanitized deliverables and validations, blockers, follow-up
state, freshness, and provenance. `end_turn` and a terminal progress checkpoint
never imply card success.

After persisting a snapshot, PA builds a bounded
`pa.post-turn-context/v1` with a SHA-256 digest and runs the read-only
evaluator. Its instructions prohibit all writes, prompts, dispatches, card
moves, PR operations, service operations, and deletion. The evaluator returns
only `pa.post-turn-evaluation/v1`. PA rejects malformed results, unknown or
inadmissible actions, executable command payloads, stale context digests, and
stale authority versions.

The versioned action catalog is available from:

```text
GET /api/fleet/post-turn/action-catalog
```

Turn evidence and evaluations are available from:

```text
GET /api/fleet/dispatch-jobs/{dispatch_id}/turn-end
```

PA is the sole action executor. Record-only actions are idempotently recorded;
mutating actions require explicit approval and are fenced by authority version
and action-specific preconditions. Each action carries idempotency inputs,
target scope, safety classification, status, and audit history. Evaluator and
automatic-follow-up budgets are configurable through `PA_POST_TURN_*`
settings.

Acknowledged dispatch completion is immutable. Follow-up turns use separate
turn records and delivery, and cannot transition the dispatch back to
`running`. For legacy inconsistent records, the audited repair operation is:

```text
POST /api/fleet/dispatch-jobs/{dispatch_id}/repair-terminal
```

Fleet activity uses acknowledged completion as the effective terminal state
while retaining any conflicting stored state as a lifecycle diagnostic.

The default repair mode still requires durable completion acknowledgement and
normalizes to `completed`. A separate
`abandoned_without_acknowledgement` mode exists for legacy `running` rows
only. It requires an exact expected state, an operator reason and explicit
no-outcome-inference confirmation, a canonical Done card, no preserved
completion envelope, a nonrecoverable dispatch, and an exactly linked `closed`
session with no retained or live ACP runtime. Missing, quiesced, disconnected,
or otherwise recovery-retained sessions are not terminal repair evidence.
Governed records must be repaired on their recorded authority so PA can release
the reservation through its existing fence. Authority ownership is required
even when the recorded reservation was already released.

When the authority and target differ, authority-local session replicas and
runtime indexes are never accepted as evidence. The authority obtains a fresh,
peer-authenticated proof from the exact target instance. That proof is bound to
the dispatch mutation, state, session, authority, target, idempotency key, and
target-local timestamps, and attests that no completion evidence exists. A
live, stale, malformed, unreachable, identity-mismatched, or mixed-version
target without the proof route fails closed.

Both repair modes commit through one ledger compare-and-mutate operation. PA
re-reads and revalidates the complete durable record under the ledger writer
lock immediately before mutation. Any concurrent lifecycle, completion,
progress, provenance, or event change rejects the repair without overwriting
the newer evidence. Abandoned repair normalizes to `cancelled`, leaves
completion unacknowledged, preserves all prior evidence, and appends the
qualifying evidence to lifecycle diagnostics. The operation can be peer-routed
by supplying `authority_instance_id` to the MCP tool.

Target proof capture uses the same compare-and-mutate primitive as a read-only
fence: it re-reads the target dispatch and checks the exact terminal session,
runtime, and completion state under the target ledger lock without committing a
mutation.

Legacy PR discovery accepts only an explicit line such as:

```text
Integration PR: https://github.com/owner/repository/pull/123
```

Ordinary prose, research citations, upstream references, and acceptance-criteria
links do not create watches. Every migrated watch stores its creation reason and
the exact qualifying line.
