# Conversation entry, provider titles and Terminal — first slice

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

A–D checklist:

- A: chat entry, titles and Terminal implemented; outcome/next-actor copy and
  initial submission latency still need acceptance.
- B: running issue list and durable start-selected orchestration remain.
- C: bounded summary discovery and submit/accept/select/workspace/provider/first
  useful event measurements remain. Existing startup and history diagnostics
  should be extended, not duplicated. No p95 performance guarantee is claimed.
- D: existing recovery/completion/worktree requirements preserved; shared outcome
  presentation and full ten-issue/retry/reload/permission acceptance remain.

Integration and activation are separate. No activation is part of this worker.
