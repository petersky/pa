# Exact-session recovery audit

## Reviewable sealed preparation (source only)

This revision adds a durable **deny** fence, not a working passive provider
restore. Nothing is applied to the original production session by integrating
this source. There is no requested production install, restart or restoration.

`POST /api/agent/sessions/{session_id}/restart-handoffs/{handoff_id}/recovery-fence`
seals only an inactive, durably closed session with a dismissed handoff. It
requires the owning authenticated principal, local authority/receipt owner,
the exact receipt phase version, session updated_at, saved native ID,
execution-binding hash, saved-settings hash and the exact saved lease IDs with
their expected fencing tokens. The settings hash is the existing audit's
`saved_settings_hash` (provider, model, mode and full saved config hashed as
sorted JSON); the binding hash uses sorted compact JSON. This preserves saved
requested defaults; it does not attest to effective provider settings.

The request extends `SessionRecoveryAuditBody` with `expected_native_id`,
`expected_settings_hash`, and `lease_expectations` containing
`{"lease_id": "<saved lease ID>", "fencing_token": 403}` for every saved
repository lease. These are placeholders, not approved values. Obtain fresh
versions/hashes from the existing recovery audit and authority's lease inventory.
All supplied values must describe the original snapshot; the server does not
repair, renew, fetch, substitute or rematerialize a workspace. Unknown fields
(including invented load/approval choices) are rejected. Normal REST CSRF and
authentication requirements apply; `idempotency_key` is part of the body.

The existing inactive-session admission reservation excludes local concurrent
startup and rejects a live runtime. Lease reads require ready, unexpired exact
session ownership, fence, path, branch, base and saved cwd. They are snapshot
observations, not a distributed lease grant or proof of native process absence.
Receipt/session/binding/settings are rechecked at the serialized durable commit.
A lease change after its observation cannot authorize anything: this operation
can only add a deny fence and never starts a provider. Later activation would
need fresh independent lease and external-process ownership validation.

The receipt is persisted in `session_recovery_fences` under the existing recovery
owner, independent of session config and transcript retention. It reports
`state=sealed_provider_load_unsupported`, `effect=unknown`, `restored=false`,
`delivery_allowed=false`, and `close_disposition=required_not_granted`.
Original session, handoff, native history, queues, model settings and leases are
unchanged. A same-key, same-input retry returns the same receipt and repairs only
its audit mirror after a lost acknowledgement. A different key or fingerprint
conflicts. GET on that exact route reads the owned receipt; 404 or a failed read
is not evidence of non-submission or permission to load. Never use a new key to
retry an ambiguous mutation. There is deliberately no release/unseal API.

The shared deny checks cover manager admission/recovery/publication, snapshot
startup, runtime enqueue/prompt/drain/resume, final provider send, and low-level
ACP connect/prompt. Manual, queued, dispatch, supervisor, reconciliation and
restart sources all traverse these boundaries. They do not depend on source
labels. Linked context replacement and native/copy branches also revalidate the
PA source. Native-ID guards reject aliases of sealed histories and fail closed
on an ambiguous ownership set (more than 64 PA identities).
They do not depend on mutable queue/control modes. Reads fail closed, and any
existing seal blocks even if its version is stale. Independently, a closed session with a
dismissed restart is denied even when the preparation receipt is missing. Thus
a failed/unknown prepare followed by restart cannot make receipt absence a load
authorization. Restart retry still rejects dismissed receipts; reconciliation
cannot release a seal or dispose a deliberate close.

### Exact unsupported provider boundary

`AgentConnection.connect` starts the ACP subprocess and then calls
`resume_session` / `load_session` with native ID, cwd, MCP servers and optional
additional directories. The installed SDK's resume/load request models and the
provider capability checks used here expose no passive-load flag, durable
no-execution token, or acknowledgement that pending provider-native work is
inhibited. `PAClient` accepts session updates and permission requests during
connection. Disabling host queue draining or denying new permissions cannot
guarantee that the provider itself will not run saved work, write files, use its
own tools, or resume a previously accepted request. This is a source capability
limit, not a claim that the historical provider actually did those things.

Accordingly a sealed session is rejected **before** provider resolution/spawn,
initialization or native history loading. The implementation proves no new PA
admission/load/send through its supported paths; it cannot certify an already
running external/native process or private direct access to an SDK transport.
It never claims that a fence reconciles historical UNKNOWN effects.

### Concrete lifecycle choices for the coordinator

1. **Keep closed.** Preserve dismissed receipt, UNKNOWN outcome and all evidence.
   No new mutation is required. Runtime recovery acceptance remains open.
2. **Approve sealed preparation only.** After source review and a separately
   authorized deployment, an operator may authorize the exact POST above using
   fresh versions, settings/binding hashes and lease fences. The result is a
   durable admission prohibition, not restoration. This is restrictive and has
   no release action in this revision; do not invoke it as an experiment.
3. **Require passive restore capability before approval.** A future reviewed
   adapter must provide a verified no-execution native load contract, bind it to
   the original native identity and configuration, and independently validate
   exclusive process/lease ownership. It must supply an audited deliberate-close
   disposition and keep the historical continuation prohibited independently of
   any future delivery decision. No current API grants that approval. A definitive
   historical outcome additionally needs retained authoritative prompt/attempt
   evidence; a passive restore cannot manufacture it.

The source coordinator for this card has not approved production close reversal,
sealing or provider loading. Do not reopen, rearm, replay, replace a prompt, load
history or fabricate a capacity/permission answer to exercise these choices.
The original runtime and cleanup acceptance gates remain open.

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

## Focused unparsed-record assessment

Set `assess_unparsed=true` with an **existing** signed page cursor to revisit
only that page's unparsed segments. The original snapshot, principal, authority,
native history, receipt/version/attempt and selection fences still apply. No
arbitrary path or byte offset is accepted. The ordinary records, counters,
positions, next cursor and range hash chain remain identical to a normal replay.
Use a preserved journal to select pages whose unparsed count increased; follow
continuation segments only when needed to locate a skipped record's end. A
fresh snapshot or a full rescan is not required or silently substituted.

`unparsed_assessment.gaps` reports segment ranges, known record boundaries,
oversized versus incomplete-EOF reasons, and recovery status. A continuation
segment has no invented record start. Segment bytes lie inside the original
page's committed range; a bounded lookahead may establish a later record end,
but does not extend that committed range. Complete recovered records have a
separate SHA256 over the JSON line excluding its newline.

Only complete newline-terminated JSON records up to 2 MiB are interpreted.
Recovery work (additional reads plus decoded input bytes) is capped at 6 MiB
per call, nesting at 64, and gap descriptors at 32. Duplicate object keys are
rejected. Larger records, incomplete EOF, ambiguous/malformed JSON, depth/work
limits and filtered envelopes remain explicit gaps; prefixes are never treated
as complete evidence. Existing envelope allowlisting is reused. Private content
and raw fragments are neither returned nor placed in cursors.

Recovered metadata reports an exact correlation only for an explicit matching
native + prompt + attempt in that same complete envelope. It does not join
independent IDs, reinterpret the original page's effect, reconcile a receipt,
override dismissal/close, or permit delivery. Missing mappings still leave the
historical effect UNKNOWN. Native provider stores and historical retention
remain outside the reader's coverage.

The fixed limits cover the fifteen remaining known oversized records (16,997,194
bytes total, largest 1,221,317 bytes including newline). Assess only their fifteen
original signed pages, verifying the exact known record boundaries and original
snapshot/page receipts. This finite pass has at most fifteen calls, 90 MiB of
recovery work plus 15 MiB ordinary page reads, and does not repeat the full scan
or the 533-record assessment. Limits cannot be supplied or disabled by a caller.
