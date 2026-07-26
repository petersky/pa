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
- caps summaries at 500 characters and tool/detail fields at 240 characters;
- accepts at most 20 validation records and 10 tool-detail records per
  checkpoint; and
- rejects checkpoint/heartbeat payloads over 32 KB and completion reports over
  64 KB; and
- stores no unrestricted command output.

Validation records contain only a sanitized command label, normalized result,
optional concise summary, and duration. Operators must open the exact ACP
session for full transcript context.

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

Future versions should add fields through a new negotiated schema version.
Provider-specific derivation may extend the normalizer only with deliberate
user-visible content or explicitly allowlisted lifecycle metadata.
