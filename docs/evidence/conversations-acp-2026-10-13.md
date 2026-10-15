# Conversation entry, provider titles, Terminal and independent work

Execution: macbook `0c7d8ecb-7e45-4579-8fa0-35159492d3f1`; card
`d2b9d8ef-4797-4764-b398-4302319ea2b6`; session
`e758e94e-894c-4eae-b8c4-9293ce02b526`. Repository
`https://github.com/petersky/pa`, base `c4bf508bf14735fab0b183b79314b0497bb6196f`.
PA workspace lease `db04a027044a657da96ccc015955f94bbbdd51311c26175bb9cf58f9ef457bc1`,
fence 257, ready/verified; dedicated `pa/d2b9d8ef-4797-47-495f01ae-e758e94e-894c-4e-66642996-266ade2c` branch.
Initial tree clean, cached origin/main 0/0; subsequent fetch found three upstream
commits, including the unrelated acceptance and browser fixes. No other worktree,
production configuration, restart, or release activation was used.

## Exact adapter evidence

Inspected local `@agentclientprotocol/codex-acp` **2.2.2**, package.json and
`/Users/petersky/.npm/_npx/4877722a062902ce/node_modules/@agentclientprotocol/codex-acp/dist/index.js`.
Bundle SHA-256: `6b1bb4e7e9caaf4aeca1042ae4aec2dc2433b0b0e4b1614b38a4d41699902aaa`.
PA lockfile installs Python `agent-client-protocol` **0.10.1**.

- Bundle 31355–31366 maps `thread/name/updated` to `session_info_update.title`.
- 39960 onward: TitleGenerator starts after the first completed turn; it uses a
  separate title-generation operation. Human/existing titles suppress generation.
- 44364 onward: history load publishes explicit thread names or fallback titles.
- 28102–28240: standard tool renderer emits `tool_call`/`tool_call_update`,
  `rawInput`, `rawOutput`, content, `terminal_output_delta`, and terminal exit data.
  39811 chooses the delta channel for ordinary non-AIR clients.
- 42435 onward: initialize advertises load, resume, list, close, delete, fork;
  `_meta.steering.supported` is true. AIR asyncTasks metadata is AIR-specific.
- 43789 onward: steering requests serialize within each session; separate
  sessions have independent queues. Steering joins an active turn or starts a
  new turn and acknowledges start. This is different from prompt completion.

The current [ACP prompt-turn contract](https://agentclientprotocol.com/protocol/v1/prompt-turn)
uses streaming notifications while the prompt request remains outstanding.
[Session setup](https://agentclientprotocol.com/protocol/v1/session-setup) defines
independent session contexts, capability-gated load and resume, with load replaying
history and resume omitting replay. These documents are supporting evidence;
installed capabilities determine what PA may call.

Recommended architecture: retain PA's independent session runtimes, durable prompt
IDs/queues, multiplexed browser event routing, and fenced worker workspaces.
Keep discussion in its own session while selected work uses a separately routed
execution. Do not issue concurrent generic prompts on one session. PA currently
queues append prompts and waits for the provider prompt result; provider turn end
remains separate from task completion. Codex steering should be a future explicit,
capability-gated integration with durable injection receipts and turn correlation,
not a silent protocol switch. Fork capability alone does not authorize shared
working trees, parallel agents, or treating a fork as identical durable identity.
No AIR client identity or asyncTasks support is claimed by this change.

## First-slice behavior

- Home and Chats have a plainly labeled New chat link. The composer receives
  focus without a setup modal; established `chat.default` selection/permission
  defaults remain authoritative. Options remain separately available.
- A stable URL request key/label survives startup reload; draft storage remains
  scoped to that pending conversation. Provider readiness is still awaited by
  the existing creation endpoint; cold-start acknowledgement optimization is
  remaining scope, not a subsecond claim.
- Provider titles, including exact-session list/load metadata, persist. Explicit
  renames stay manual until Use automatic title. Timestamp, session and
  connection-generation checks reject invalid/stale updates. PA supplies a
  first-message fallback when no provider title exists.
- Terminal is a read-only button in the left rail, between tools and plans.
  Rows reconstruct from loaded transcript evidence by provider session/turn/call,
  with ordered updates and event deduplication. Earlier evidence loads through
  existing history paging. Non-text content is identified without pretending to
  render it. Output uses textContent, never HTML or a shell.
- `terminal_output_char_limit` defaults to 1000, validated 100–100000 in runtime
  and persisted configuration. The viewer links to Settings → Configuration.
  Display truncation does not modify source events. Canonical input/output and
  delta fields pass through existing transcript redaction, hot/cold storage,
  and retention; raw provider envelopes remain excluded.

## Verification and remaining acceptance

Focused backend/client tests exercise provider metadata, manual/automatic naming,
invalid and cross-session events, redaction and full retained output, chronological
stream assembly, out-of-order delivery, duplicate replay, turn isolation,
non-text content and Unicode truncation. Existing ACP, history, draft, stream,
configuration and session action regressions run alongside these tests.

The browser fixture uses the real PA templates, JavaScript, transcript history
handler and multiplex SSE with synthetic provider events. No live provider was
started. Browser operation evidence includes `conversation-home-new-chat-v1`,
`conversations-terminal-snapshot-v1`, `conversation-manual-title-preserved-v2`.
A reload check exposed omitted raw transcript fields; canonical redacted evidence
was added and is covered by a persistence regression. A fixture-only missing
Jinja helper was corrected. The first browser attach returned ambiguous 503
(correlation `c6886f74-e24d-4504-b1b8-f846be0e36cd`), reported as PA problem
`c64f9e68-ed52-4ca7-86d5-5fadcf959c69`; a same-key retry attached successfully
after outcome/state checks. These are not production acceptance claims.

A–D checklist at the first slice (superseded by the current checklist below):

- A: chat entry, titles and Terminal implemented; outcome/next-actor copy and
  initial submission latency still need acceptance.
- B: running issue list and durable start-selected orchestration remain.
- C: bounded summary discovery and submit/accept/select/workspace/provider/first
  useful event measurements remain. Existing startup and history diagnostics
  should be extended, not duplicated. No p95 performance guarantee is claimed.
- D: existing recovery/completion/worktree requirements preserved; shared outcome
  presentation and full ten-issue/retry/reload/permission acceptance remain.

Integration and activation are separate. No activation is part of this worker.

## Issue-list slice (2026-10-14)

The conversation now has a bounded list (100 notes, up to 20 selected per request)
with multiline addition, editing, grouping, defer/restore and Start selected.
Discussion creates no cards and starts no workers. MCP exposes the same list;
the start tool requires an explicit user fix/start request in its contract.

Start selected freezes the exact issue revisions, grouping, source identity,
permission mode, explicit selection preferences and conjunctive constraints in an
instance-local receipt. Missing permission mode blocks admission rather than
falling through to fleet's broader default. Each group uses stable child keys in
the existing canonical card and fleet dispatch ledgers. Cards retain source
completion requirements. Secondary session links preserve the discussion's own
binding; dedicated work continues through existing selection and workspace
provisioning. No cwd, capacity override or concurrent-writer permission is added.

An owned mutation continues after HTTP disconnect, with a 250ms waiter budget.
Reload exposes the saved request; retry uses the same identity and frozen inputs.
Receipt states describe admission, never task completion. A blocked request keeps
its evidence and card links for recovery. Automatic restart resumption is not
claimed: after a server restart, Retry same request resumes admission through the
existing ledgers. Work outcomes remain on their cards.

Regression evidence includes inert ten-note discussions, stale-version and owner
rejection, deferred/missing-mode rejection, preservation of explicit false options
and hard constraints, concurrent duplicate submission, lost dispatch acknowledgement,
and crash after canonical card-event persistence but before the receipt. Client
coverage retains retry IDs and rejects late responses from another selected thread.

Real-template browser fixture with 301 historical cards:
`conversations-issues-ten-snapshot-v1`, `issues-defer-snapshot-v1`,
`issues-start-evidence-v1`, `issues-reload-evidence-v1`, and
`issues-other-evidence-v2`. Ten notes created zero cards/dispatches; two grouped
selected notes created one card and one synthetic dispatch. Reload retained notes,
receipt and composer draft. Switching threads showed the independent empty list.
The fixture uses synthetic dispatches; it does not prove live provider generation
latency. Screenshot review caught a stale fixture stylesheet; cache-busting fixed
the view and the panel is bounded while the composer remains visible.

Updated remaining scope: C timing visibility and compact discovery; A cold-start
submission; D unified outcomes and full permission/recovery acceptance; negotiated
steering and New thread/Branch from here. PR #457 is green and ready; automatic
approval review blocked its shared-main merge, and correlated PA request
`conversations-pr457-merge-approval-v1` is pending. No merge or activation claimed.

## Discovery and latency evidence

`GET /api/cards/summaries` and `list_card_summaries` use the existing body-free
Work projection, returning 30 summaries by default and at most100. Summaries are
display-bounded to1000 characters with a truncation flag; detail/history links
load full evidence only when requested. No transcript fan-out occurs.

Timing details show browser navigation→composer focus, submit→durable receipt,
selection, workspace preparation, provider launch/initialize/session creation,
queue+PA preparation, and provider call→first useful event. The latter begins at
the prompt call and ends at the first nonempty assistant text/tool call/plan;
thought-only events do not count. Timing events contain IDs/durations, no prompt
text. They persist alongside existing transcript evidence and remain scoped to
the provider session. Provider turn completion is unchanged.

Repeatable command: `uv run python -m tests.conversation_performance_fixture`.
On this macbook,30 samples against301 historical cards measured warm summary-read
p95 **185.31ms** and Start-selected receipt p95 **257.25ms**. Transport is in-process
ASGI; dispatch is synthetic with a400ms delay. These measurements meet the proposed
500ms local-read and1s acknowledgement targets for this fixture only. Browser
composer and live-provider generation are explicitly **not measured** by this
benchmark. Startup/turn diagnostics support those subsequent measurements.

## Independent threads and next actors (2026-10-14)

New thread uses the existing independent conversation admission. Branch from here
offers a negotiated native fork of the latest available provider history and an
explicit Copy recent context alternative (last 100 saved events, at most 8000
text characters). Source conversation/card/instance/provider identity and the
copy's sequence boundary persist on the new session. Neither action sends a
prompt or authorizes executing new work.

The new target uses normal PA selection and workspace admission with the source's
permission mode and explicit preferences. No source cwd, card execution binding,
dispatch identity or replacement-context operation is reused. Native fork runs
on the target connection with its own cwd and MCP configuration, after verifying
the fork capability and source connection identity. `request_sent` is durable
before the RPC; the returned provider identity is saved before later startup
steps. A lost response remains `outcome_unknown`; retry never blindly forks again
or silently copies context. Recovery of a known provider ID uses the existing
required-restore path. An ambiguous fork requires inspection; the user may choose
a separate context copy explicitly. Native fork was validated with protocol fakes,
not a live provider invocation in this worker.

Installed-source evidence: `forkSession` at bundle 43113 delegates to the fork
creation path; app-server `threadFork` at 34654 uses the target cwd/config and
`excludeTurns: true`. Generic session/fork does not expose a message-specific
checkpoint; the AIR fork-point metadata is client-specific and is not used.

The installed steering extension is advertised, but not safe to enable under PA's
durable turn contract. Bundle 43837–44000 shows that an injection racing with turn
completion calls `startNewTurnFromSteering`, which acknowledges start before the
prompt completes. `parseSessionSteerParams` accepts sessionId/prompt, with no
idempotent delivery ID or expected-turn fence. The reply has only `injected` or
`startedNewTurn`; generic clients get no completion receipt for the latter.
PA therefore exposes the advertised capability separately from availability and
uses the clearly labeled Queue for current work fallback. Native Send to current
work remains blocked, not implemented or claimed as accepted/injected.

A supported extension needs an idempotent client delivery ID, an expected-turn
ID plus an inject-only/no-new-turn option, and correlated durable result/status
recovery. If it may start another turn, it also needs generic turn completion and
error notifications keyed to that turn. PA can then record submitted → injected /
queued / started-new-turn, reconcile uncertain receipts, and retain permission
routing without competing session/prompt calls. This is a proposed provider
contract change, not a protocol migration. PA problem
`cbdacf92-1e66-416c-85d9-b2f85c47aa4f` records this limitation.

Home/Work/card progress and conversation snapshots now expose the same next-actor
vocabulary: You, Agent, PA, Reviewer, No action required, or Not confirmed. Existing
completion evidence determines status; an ended provider turn remains unfinished
work unless the existing completion requirements are satisfied. Queue/status
refreshes coalesce over 250ms and reject obsolete thread/generation/revision
responses. Composer text remains usable during work and branch admission.

Browser operations `threads-receipt-v1`, `threads-new-target-evidence-v1`, and
`threads-source-draft-restore-evidence-v1` verified explicit copy, independent
target routing, an empty target draft, and the restored source draft. Backend and
Node tests cover duplicate branch requests, owner isolation, bounded copied text,
capability rejection, target fork parameters, uncertain native response recovery,
late receipts, and stale queue-refresh isolation.

## Browser timings and current checklist

The real-template Chromium fixture (`tests.conversation_browser_fixture`) stores
301 historical cards and uses synthetic provider responses. Thirty completed
browser samples, excluding one warm-up, measured navigation → focused composer
p95 **127.70ms** and synthetic prompt acknowledgement p95 **28ms**, with no missing
samples. Raw samples are in `conversation-browser-timings-2026-10-14.json`;
operation IDs are `browser-perf-fixed-0-v2` through `browser-perf-fixed-30-v2`.
The synthetic prompt endpoint does not establish production durable prompt or
provider-generation latency. The separate ASGI benchmark above exercises actual
Start-selected admission and compact reads. Cold provider startup is still
awaited by session creation and has no subsecond claim.

The first browser measurement exposed missing timing when streamed acceptance
beat the HTTP reply. Timing now records the first confirmed receipt through either
path, keyed by session/generation/prompt, without changing durable acceptance.
The initial incomplete run is excluded; the retained run waits for every sample's
completion receipt. Minor status-refresh code changes caused asset revalidation
during the retained run; no slow sample was discarded.

Current A–D scope:

- A implemented: direct focused New chat, provider/manual/automatic titles,
  retained safe Terminal evidence and configurable 1000-character display limit.
  Cold startup still requires provider readiness before the first send.
- B implemented: inert running issue list, edit/group/defer/restore, stable
  start-selected receipt and create/link/select/provision/dispatch delegation.
  Recovery after a server restart uses explicit same-request retry.
- C implemented: bounded summary discovery and phase/timing diagnostics; measured
  301-card synthetic fixtures above. Live-provider latency remains unmeasured.
- D implemented: shared next actors, coalesced thread-scoped status refreshes,
  independent New thread and native/copy branch admission, retaining completion,
  permission, prompt recovery and workspace fencing. Live native-fork acceptance
  and native steering remain outstanding; steering needs the provider contract
  change described above.

Changes remain review-gated. The earlier automatic approval rejection of merging
PR #457 is not bypassed by publishing more scoped review commits. Any approval
for its old head is stale after a push. Integration and activation remain separate;
this worker performs no production restart, release or activation.

## Priority history reentry correction (2026-10-14)

The user reported returning to root conversation `786444b2-5cfb-46b4-92c4-a87df6d2a865`
and seeing its beginning from the previous day. Coordinator and worker browser
checks independently observed history-unavailable/live-recovery warnings. The
original transient HTTP error is **not established**: current bounded API reads
returned 200 for root/worker in 502–941ms. Root `after_seq=0` returned the oldest
page (1–289), whereas its recent read returned 3398–3931. No transcript loss was
observed or inferred from the warnings.

The client had two demonstrable recovery defects. A failed initial tail left its
cursor at zero, so live gap repair could start painting the oldest pages. A
successful cache restore also skipped checking the recent tail. Thread switching
cleared the previous window without first caching its reading position.

Recent-history bootstrap now retries a bounded **recent** request; it never uses
an initial zero cursor to recover the tail. While unavailable, an explicitly
partial live preview shows incoming text without pretending the old window is
current. Full evidence stays durable. Current permission controls, submission
receipts and turn status remain independent of missing history; the live snapshot
cursor prevents old replay events from overriding current state. Recovery merges
the recent page with retained incoming events in sequence and deduplicates them.

Every cached reentry checks the tail. A deliberate older window keeps its
message-relative anchor and exposes Jump to latest; normal reentry/reload shows
recent messages. Requests and retry timers are fenced by session, owner API and
generation. Drafts retain their existing per-session persistence.

`tests/agent_chat_reentry_fixture.py` uses 3,442 synthetic retained events and PA's
real multiplex SSE endpoint, in temporary data only. Backend/Node regressions and
`test_agent_chat_reentry_browser.py` cover initial/tail failures, partial live
preview, recovery, incoming messages while away, cached thread switches, draft
isolation, older reading anchors, actual Back/Forward, reload, delayed responses,
and unique ordered rows. PA browser operations `reentry-fixture-live-preview-evidence-v1`,
`reentry-fixture-recovered-tail-evidence-v1`, `reentry-fixture-latest-reload-v2`, and
`reentry-fixture-latest-scroll-v2` retain manual browser evidence. No production
failure was injected; no transcript reset or service restart was used.

Reports `409aa01e-416c-4376-b760-597c2bc5fa7a` and
`8a1f4954-7e5d-4a93-9aec-fe55ed77aeda` remain linked to the investigation. The client
correction addresses the reproduced recovery behavior; it does not establish or
claim to repair an unobserved server-side cause of the initial request failure.

The mobile Home New chat link moves into the page heading at widths up to 700px,
preserving a single action to the composer and keeping sign-in/navigation clear.
The actual auth/navigation journey passes in light/dark themes across 320–1920px.
Approval-review ownership and the read-only exact-head evidence correction are
recorded separately in `pr457-approval-evidence-2026-10-14.md`.
