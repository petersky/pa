# Operation status and recovery admission

Operation status is an observation of the owner's receipt. It is not an instruction
to repeat an effect. HTTP `GET /api/operations/{key}` and MCP
`get_operation_outcome` use bounded, passive SQLite and dispatch-index adapters.
They never invoke the legacy canonical history lookup on the request worker.

## Identity and legacy replay

The additive `identity` object is version 1 and contains `owner`, `realm_id`, the
unchanged `idempotency_key`, operation name, and the owner's opaque `request_fingerprint` when one
is recorded. Owners are `canonical`, `restart`, and `dispatch`; an absent untyped
legacy key has no proven owner yet. Key text is never
parsed as a namespace or rewritten. In particular follow-up and initial prompt IDs
and their existing replay fingerprints remain unchanged.

Optional owner, operation, and request-fingerprint selectors must agree with the
receipt. A fingerprint from one owner's request schema must not be recomputed as
another owner's fingerprint. Historical restart/control receipts that do not
record a fingerprint cannot certify a supplied fingerprint. Such a request fails
closed. Realm access is checked before receipt lookup. Conflicting owner claims,
multiple legacy dispatch/restart claims, or a mismatched realm/selector return 409;
lookup never chooses the newest claim. An untyped local receipt remains
`lookup_pending` with bounded `observed_receipts` evidence until the canonical
owner has ruled out a hidden historical claim. Negative lookup proof is tied to
the exact published head and invalidated by a later head; typed reads remain
available during that resolution. This uses the sole writer's passive cached
publication head, without reloading refs or acquiring the history lock. The
authoritative producer's existing
payload/idempotency checks still govern mutation replay.

A missing canonical receipt is `lookup_pending`, with unknown durability. It is
not proof of not-found: immutable history might contain a committed event whose
acknowledgement/projection was lost. Likewise a stale pending receipt cannot
certify non-commit or safe creation under a new key. `accepted` means an intent
receipt exists. `committed` refers to the owner's durability boundary (immutable
canonical event or durable local receipt). `projected` is unknown unless the
canonical receipt proves completion; it is not applicable to local ledgers.
`effect` remains unknown without effect evidence. Dispatch admission is accepted,
not fabricated effect success. Existing restart and follow-up state details remain
available in the result, pending the separate lifecycle observation adapter.

## Bounded reconciliation ownership

Kernel owns a status service with two read workers, eight queued reads, and a
500 ms caller deadline. SQLite receipt queries are read-only, have a 50 ms busy limit and a 250 ms work
limit, and dispatch lookup uses an incrementally maintained key index with a
50 ms lock limit. It retains at most two claims to detect ambiguity and copies
only selected receipt evidence, without an unbounded list, whole-dispatch copy,
or record-history scan.

Missing/nonterminal canonical receipts and follow-up acceptance gaps may admit a
separate durable reconciliation request. Its stable ID hashes owner, realm, and
unchanged operation key. Admission is persisted in `operation_reconciliation.db`;
this database owns repair requests, never canonical/dispatch/restart outcomes.
At most 32 requests are owned at once and one worker executes blocking canonical
repair. Concurrent polls coalesce before worker admission. Client cancellation
and timeout do not cancel the repair or return its capacity early. Pending/error
retries have a cooldown; a changed receipt revision can resume the same job ID.
A process restart marks unfinished receipts interrupted and a later authenticated
poll resumes them. Status returns current owner evidence even when repair capacity
is unavailable. No cached repair result is promoted into an operation success or
not-found assertion.

The existing canonical repair method and its global SQLite mutation lock are
retained for repair/replay callers. The existing object-recovery service separately
owns one bounded worker and eight queued calls, retaining its realm/head/key
receipts, immutable-object verification, stale-head checks, and shielded owners.
No SQLite single-writer invariant is bypassed by splitting projection locks.

## Route dependencies

Authentication executes before the recovery gate. Passive requests do not consult
the recovery mutex; mutation admission uses a nonblocking health snapshot and
fails conservatively when that state is busy. Classification grants no user,
fleet, or realm permissions. GET/HEAD/OPTIONS retain their read policy except the
legacy `GET /sync/check`, which actually converges refs and therefore requires an
explicit healthy query realm.
Unknown mutations remain globally history-dependent and are blocked while any
realm is degraded. Explicit card create (body realm) and update (query realm)
operations depend on that subscribed realm. Sync push, convergence, and conflict
resolution require an explicit healthy body realm and retain their normal
membership, immutable-object, and ref-CAS validation. Missing, duplicate, malformed, or
unknown realm selectors remain conservative. The gate never lets a query realm
override a body-owned realm.

The five existing sync object/recovery admission paths remain narrow exemptions.
`sync/reconcile` first passively replays a completed legacy receipt when available;
otherwise it admits the existing durable object-recovery owner before entering
ordinary canonical receipt admission. Pending/failed dependency repair is reported
as such, not as a consistent projection. It preserves normal same-key replay after
repair. Other ref mutation, auth/permissions, workspace creation, and unknown
routes remain conservatively gated; sync push is realm-gated, never exempt.

`pa.core.operation_dependencies.local_operational` is an explicit endpoint
decorator for independently authenticated local operational reporting/control.
It is the extension seam for the separately owned health journal. Apply it below
the route decorator only after auditing the endpoint's actual dependencies and
authentication. It does not add a fleet-authentication allowlist entry. Existing
quiesce, authenticated target progress ingestion, and plain local progress checkpoints
are explicitly classified. Checkpoints carrying operator input remain gated because
they create canonical notifications;
canonical card mutations in a degraded realm remain gated.
