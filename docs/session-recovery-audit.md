# Exact-session recovery audit

`POST /api/agent/sessions/{session_id}/restart-handoffs/{handoff_id}/recovery-audit`
records a fail-closed recovery assessment. It does not load a provider, reopen a
session, reconcile receipt status, restart PA, enqueue or deliver any prompt.
There is no production application merely because this endpoint exists.

The authenticated principal must own the saved session, and the server instance
must match both session authority and receipt owner. The body contains
`expected_version` (receipt phase version), `expected_session_version` (saved
session updated_at), `expected_binding_hash` (SHA256 of execution_binding encoded
as sorted compact JSON), and a stable `idempotency_key`. New observations reject
stale versions/bindings. An exact duplicate returns the original observation,
not a fresh health assertion. Reuse of the same key for different inputs fails.
Operation receipts survive transcript retention. A failed transcript mirror can
be repaired by retrying the same key; session and handoff are never changed.

The result records exact native history identity, a hash of saved provider/model/
mode/configuration, receipt/session/binding versions, completion event identity
when present, explicit blockers and an evidence query. It never records prompt
content or raw settings. Missing local completion remains UNKNOWN, including
when queue admission, launcher failure or provider load evidence exists.
Completion evidence does not settle intentional close/dismissal. Every result
has `state=blocked`, `restored=false` and `delivery_allowed=false`. Live lease and
worker revalidation remains required; matching the stored binding is not proof
of a live lease. This audit does not call a loader, so provider load failures and
concurrent workers cannot cause an attempted recovery or prompt delivery here.

## Why loading is deferred

At this revision, ACPConnection.connect in src/pa/acp/client.py sets a restored
saved session to idle. AgentSessionRuntime.start in
src/pa/instance/agent_session.py restores queued_prompts, checkpoints ready and
starts the drain unless _defer_drain is set. AgentSessionManager._resume_from_snapshot
passes the recovered queue to that method and publishes the runtime. Runtime
prompt and resume_queue are independent entry points; queue_paused and
_defer_drain do not persistently prohibit all user, automatic and continuation
admission. The manager deliberately skips durably closed snapshots.

Consequently, removing that closed-session check or using a paused queue would
not establish the requested all-source delivery prohibition. A later restore
implementation needs a durable admission fence shared by all prompt sources,
owner/lease/worker fencing, exact native restore and saved settings verification,
plus an explicit disposition for deliberate cancellation before admission. This
endpoint makes no assertion that those requirements have been implemented.

## Original recovery remains open

No live original audit/recovery operation was invoked by this source change.
For session 1eb4393e-26a0-471b-9b01-0b96763d2c01 and receipt
be010d09-11db-54e2-8415-262746e3cc84, the outcome auditor must query the exact
saved continuation_prompt_id associated with key
cleanup14-pr655-exact9c3542b9-ordinary-managed-activation-20261108-v1.
Obtain authoritative prompt-specific completion or non-delivery evidence from
the preserved native history, correlating provider request/turn identity and
terminal outcome with the receipt's single attempt. Audit the recorded close
and dismissal separately. Neither absence of local turn evidence nor a healthy
ACP launcher proves non-delivery. Preserve UNKNOWN if those links cannot be
established. Do not rearm the receipt or replace its prompt identity.

## Fixed wire metadata reader

GET `/api/agent/observability/v1/sessions/{session_id}/restart-handoffs/{handoff_id}/wire-metadata`
(and MCP `get_agent_session_wire_metadata`) requires the expected saved native ID,
receipt version and attempt. The server checks the owning principal or admin,
local session authority, receipt ownership and exact saved identities before
reading. It rechecks saved bindings after inspection. It never opens a provider
connection, loads history, changes receipt/session state or authorizes delivery.

Only `sessions/<saved UUID>/wire.jsonl` and fixed suffixes `.1` through `.3`
under the configured data root are readable. Descriptor-relative no-follow opens
reject symlinks and nonregular artifacts. At most 1 MiB from each file tail,
4 MiB total, 20,000 JSON records and 200 returned records are inspected/returned.
The response describes byte ranges, inspected-range hashes, changed files,
partial lines, malformed records, missing artifacts and output truncation.
`observed_fixed_files_complete` means only that these observed file snapshots
were fully inspected. Other rotations, prior logging loss, retention and native
provider stores remain outside that claim: historical coverage stays UNKNOWN.

Output contains fixed method/direction/status enums, validated opaque IDs,
normalized timestamps and numeric error codes. Prompts, answers, raw errors,
settings, tool results, headers and raw protocol envelopes are never returned.
An outgoing request log is not a submission acknowledgment. Exact-attempt
classification requires explicit prompt + native + attempt linkage, with a
matching message ID for a separate response. No mapping is invented from time
proximity, ordering or a shared native history ID. Missing or uncorrelated
records preserve UNKNOWN, including the legacy logger's independent UUIDs and
unidentified terminal responses. A terminal result describes protocol outcome
only; it does not prove task effects or reverse an intentional cancellation.

## Snapshot pagination

The same GET and MCP tool accept optional `paged=true`, `cursor`, and
`selection=all|outcomes`. The legacy tail response remains the default. A new
paged walk starts at byte zero and traverses every fixed artifact. Resume by
passing the opaque `next_cursor` with the same identity preconditions and
selection. Persist that cursor and the returned range receipts after each page;
replaying it is deterministic while the snapshot remains unchanged.

Cursors are HMAC authenticated with a domain-separated existing server secret.
They bind the principal, authority, receipt/version, session/native history,
attempt and selection to a fixed artifact manifest and directory identities.
Device/inode, size, mtime and ctime fence every fixed file (including missing
rotation entries) before and after each call. Growth, replacement, truncation,
rotation, symlinks or changed directory bindings reject continuation with HTTP 409.
A rejected snapshot must not be combined with a fresh generation. This metadata
fence is not a full-file digest or a historical retention manifest.

Each call reads at most 1 MiB, examines at most 20,000 complete records and returns
at most 200 selected metadata records. It stops at the output limit instead of
dropping the remaining records. Scanned byte ranges carry individual SHA256
hashes and an ordered hash chain. Short boundary fragments are reread on the
next page; lookahead is explicitly excluded from committed scanned coverage.
Oversized records are counted and skipped across bounded pages without putting
raw fragments in cursors. Malformed and unterminated records remain explicit
metadata gaps. `snapshot_exhausted` attests only that all bytes of the fixed
observed snapshot were consumed; historical retention remains UNKNOWN.

`selection=all` pages every eligible allowlisted metadata record.
`selection=outcomes` omits only identity-free session updates and counts them;
requests, terminal responses, errors and correlation-bearing updates remain.
Both selections report exact scanned ranges and cumulative counters. Neither
selection permits negative inference from a partial range. Explicit message
links are carried across pages with a bounded table; conflicting prompt/attempt
identities or link overflow force UNKNOWN. Delivery remains prohibited.
