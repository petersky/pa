# Durable dispatch progress

PA dispatch progress is a versioned side channel attached to the existing
durable dispatch identity. It lets the authority, Card UI, and Fleet UI show
useful work status without opening the ACP transcript.

## Protocol and negotiation

The current schema is `pa.dispatch-progress` version `1`.

The authority includes `progress_versions` while materializing a remote
dispatch. The target selects the highest mutually supported version and returns
`progress_protocol_version`. If the field is absent or there is no common
version, the dispatch remains compatible and renders as `lifecycle_only`.
Historical dispatches are not rewritten.

Every checkpoint contains:

- `card_id`, `dispatch_id`, and exact `acp_session_id`
- originating and authority instance IDs plus the authoritative card version
- UTC occurrence and last-activity timestamps
- a monotonically allocated per-dispatch sequence and an idempotency key
- a normalized phase and concise current-action summary
- optional branch, commit, pull request, changed-file count, validation,
  blocker/retry, and operator-input metadata

Normalized phases are `investigating`, `planning`, `implementing`, `testing`,
`opening_pr`, `waiting_ci`, `addressing_review`, `merging`, `blocked`,
`retrying`, and `completed`.

Heartbeats use the same provenance and sequence space but are stored separately
as a replaceable freshness signal. They never append to card activity history.

## Ordering, idempotency, and delivery

The target first writes a checkpoint to its dispatch ledger. A background
outbox retries delivery to the authority. Authority ingestion verifies the
authenticated origin, target, authority, dispatch, card, session, and card
version before accepting the payload.

- Repeated idempotency keys are acknowledged without another entry.
- The first payload for a sequence wins; conflicting payloads are counted and
  rejected.
- Late unique checkpoints are retained and sorted by sequence.
- Sequence gaps are reported rather than silently renumbered.
- Identical phase/summary updates within five seconds are coalesced.
- Checkpoint history is bounded to 200 entries and the idempotency window to
  512 keys per dispatch.
- Heartbeats replace the previous heartbeat, so freshness does not flood the
  activity timeline.
- Target records keep undelivered payloads across restart and retry transient
  network failures until the authority acknowledges them.

Authority transfer does not rewrite provenance. The immutable originating and
authority IDs remain on every checkpoint, while a newly authoritative dispatch
record continues the same ordered stream. Concurrent sessions are isolated by
the tuple of dispatch, ACP session, and originating instance.

## ACP derivation and explicit checkpoints

PA derives visibility from two sources:

1. deliberate, user-visible `agent_message_chunk` commentary; and
2. allowlisted ACP tool lifecycle fields (`title`, `kind`, and `status`).

`agent_thought_chunk`, raw tool input, raw tool output, and arbitrary tool
content are never used. Commentary chunks are buffered and rate-coalesced
before becoming checkpoints. Tool titles are mapped to normalized phases such
as testing, opening a PR, or addressing review.

Agents and operators may emit richer allowlisted metadata with the
`report_dispatch_progress` MCP tool or:

```text
POST /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint
```

Explicit checkpoint fields are schema-validated, bounded, redacted, and linked
to the dispatch's already-established session and provenance.

## Redaction and payload bounds

Before persistence and again before transport, PA:

- removes bearer credentials, common GitHub/OpenAI/Slack token forms, URL
  credentials, private keys, and secret-like assignments;
- collapses control/whitespace-heavy content;
- caps summaries at 500 characters, tool/detail fields at 240 characters, and
  stored validation command labels at 2,000 characters;
- accepts at most 20 validation records and 10 tool-detail records per
  checkpoint; and
- rejects checkpoint, heartbeat, and explicit-event payloads over 64,000
  encoded UTF-8 bytes; completion reports intentionally retain their separate
  64,000-byte ceiling; and
- stores no unrestricted command output.

Validation records contain only a sanitized command label, normalized result,
optional concise summary, and duration. Operators must open the exact ACP
session for full transcript context.

The compact UI truncates long command labels to 240 characters independently
of durable storage and exposes the complete sanitized label in an expandable
detail. Storage limits must not be reused as presentation limits.

## UI semantics and retention

Cards show the latest phase, summary, freshness, and seconds since activity.
Card Activity includes meaningful checkpoints, validation results, blockers,
PR/supervisor events, and the final report. Each progress entry links to the
exact ACP session; sanitized details are collapsed by default.
The Work board and an open card's progress banner refresh every 15 seconds;
card lifecycle SSE invalidations continue to refresh the board immediately.

Fleet Overview uses the same dispatch snapshot. Reporting states are:

- `live`: activity within 45 seconds
- `delayed`: activity between 45 and 120 seconds, or negotiated reporting that
  has not emitted its first checkpoint
- `stale`: more than 120 seconds without activity
- `disconnected`: lifecycle-only mixed-version reporting or a delivery error
- `completed` and `failed`: terminal dispatch reporting states

The dispatch ledger retains at most 200 meaningful checkpoints per dispatch.
Heartbeats retain only the newest value. Dispatch cleanup follows the existing
workspace/session and dispatch retention policy; progress has no independent
raw-output archive.

## Completion reconciliation and extensions

Turn completion produces `CompletionReportV1` with outcome, source-control and
PR metadata, validations, CI/review evidence, merge commit, remaining blockers,
and card disposition. The authority enriches the report from the linked PR
watch before persisting it. Card disposition remains a separate guarded
business decision, so transport completion, PR supervision, and lane changes
cannot contradict one another.

Future incompatible changes should use a new negotiated schema version. Readers
must tolerate historical records independently: an invalid versioned progress
event may be discarded without preventing its dispatch or other dispatches from
loading. Schema-bound migrations should preserve the dispatch lifecycle record,
and startup/MCP registration must not depend on every historical side-channel
payload validating under the current schema.
Provider-specific derivation may extend the normalizer only with deliberate
user-visible content or explicitly allowlisted lifecycle metadata.

## Dispatch leases and fencing (stage 2)

A running remote dispatch carries an explicit **lease** on its authority record.
The lease is the liveness signal the authority's stale-running sweeper uses; it
replaces inferring liveness from "time since the last progress event".

Fields on `DispatchRecord` (JSON payload of the `dispatches` row; no indexed
column, so no migration): `lease_protocol`, `lease_id` (`<dispatch_id>:<fence>`),
`lease_fence`, `lease_holder_instance_id`, `lease_granted_at`,
`lease_renewed_at`, `lease_expires_at`, `lease_ttl_seconds`, and, on the target
mirror only, `lease_superseded_at` / `lease_superseded_by_fence`.

**Negotiation.** The authority's materialize request offers
`lease_protocols: ["dispatch-lease/v1"]` plus its `lease_fence` and `lease_id`.
A target that supports it records its fence on its mirror and answers with
`lease_protocol` and the echoed `lease_fence`; the authority sets
`lease_protocol` only on an exact match. An older target ignores the fields and
answers without them, so that dispatch is never leased and keeps the status-probe
behaviour (`dispatch_stale_running_seconds` / `dispatch_lost_remote_seconds`).
A local dispatch (authority == target) never negotiates a lease: it is covered by
session liveness and `DispatchRecord.lease_is_fresh()` is always true for it.

**Grant and renew.** The lease is granted when the dispatch commits `running`
(the target acknowledged the prompt) with TTL `dispatch_lease_ttl_seconds`.
Every accepted progress event, every heartbeat (both ingested by
`POST /api/fleet/dispatch/{id}/progress`) and every status probe that confirms
the dispatch is still running extends `lease_expires_at` in the same commit as
the evidence; the expiry only moves forward and a settled dispatch is never
revived. Heartbeats are already gated on real provider activity, so a hung
provider stops renewing; a quiet but healthy turn is renewed by the probe below.

**Expiry.** The sweeper does nothing while the lease is fresh. After expiry it
probes the target (`GET /api/fleet/dispatch/{id}/status`) exactly as in stage 1:
a held completion is adopted, a confirmation renews the lease, and a target that
answers "unknown" or "dead" fails the dispatch immediately. A target that cannot
answer is given `dispatch_lease_grace_seconds` after expiry, then the dispatch
fails recoverably (`remote_dispatch_lease_expired`), the owner is notified and
it is re-offered (`dispatch_reoffer_lost_remote`). Records without a lease use
the stage-1 time thresholds unchanged.

**Fence.** `lease_fence` starts at 1 and is incremented by every
`retry_with_capacity` (operator retry or re-offer), which also clears the lease
and the negotiated protocol so the new holder renegotiates. A target sends its
fence in the `X-PA-Lease-Fence` header on every progress, heartbeat and
`/complete` request. A fence lower than the record's current one is rejected
with `409 {"code": "stale_lease_fence", "current_fence": N, "recoverable":
false}` and changes nothing. The current holder's completion still wins, an
acknowledged completion still replays idempotently, and a request without the
header (older target) is checked only by the existing provenance rules. On that
409 the target stops reporting: the progress service and the completion outbox
mark the mirror superseded (`lease_superseded_at`, completion class
`superseded`), no longer retry, and the session transcript gets a
`dispatch_superseded` note. Failing a dispatch alone does not bump the fence, so
the holder's late completion for a failed-but-not-reassigned dispatch is still
adopted.

### How stage 3 (target-pulled claims) reuses this

Stage 3 negotiates `dispatch-claim/v1` in the same handshake (a sibling of
`lease_protocols`, same fallback to push when absent). The authority keeps the
queue; a target claims an admitted dispatch with
`POST /api/fleet/dispatch-claims` and the authority answers by performing the
existing grant: `lease_fence` is bumped (or the current value is assigned),
`lease_id`/`lease_holder_instance_id` name the claimer and `lease_expires_at` is
set to now + TTL, atomically with the `queued -> claimed` transition. Nothing in
the lease model changes: the claim response carries the fence, the claimer sends
`X-PA-Lease-Fence` on every progress/heartbeat/complete (the same renewal and
fencing paths), a lease that is not renewed lapses and the same sweeper
re-offers it by returning the dispatch to the queue with a bumped fence, and a
superseded claimer gets `stale_lease_fence`. What stage 3 adds is only the
claim endpoint and an explicit renewal call for targets that have nothing to
report; the push worker and status probe can then be retired.
