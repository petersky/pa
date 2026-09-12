# Registration-only MCP repair

Card `b4b6a07a-99ad-4901-9c21-8909b0066cfb`; worker session
`86e8f629-6851-42e8-9c70-052a30e789e1`; execution/authority instance macbook
`0c7d8ecb-7e45-4579-8fa0-35159492d3f1`. Repository `petersky/pa`, base and
verified remote main `cb2588d3df5e346c148cebc0919764a1796a8988`. PA verified the
sole card worktree lease `eaab432d0661df3152f730977cfd6c5ae0a0e3b74eef80dfcefa325d7d438cd1`,
fence 218, ready/verified, clean before edits, no upstream, divergence 0/0.
The branch and full PA-supplied worktree path are recorded in dispatch checkpoint
`1403c98b-683b-4a16-a5ee-ca48abaeacf4`.

## Behavior and scope

Both console and module MCP entrypoints register the canonical built-in proxy
functions without importing the service CLI or loading service module lifecycles.
The service modules delegate to those same functions. The stdio context reads the
small instance configuration, provides a bounded process-local async executor,
and rejects Store access. It never calls get_settings/ensure_dirs, Kernel.boot,
service on_load, shared logging setup, SQLite/schema migrations, fleet or sync
initialization. Existing plugin registration skips on_load as well.

Provider operations and restart preview use authenticated owner HTTP endpoints.
Assigned preview has a read endpoint protected by the existing exact live
session/dispatch capability validation. Ordinary credentials are read, never
created by the MCP child; malformed assigned bindings fail closed. The 205
ordinary input schemas match fingerprints captured from the pre-repair base;
assigned discovery remains the exact nine-tool allowlist.

Codex's two-second observation deadline leaves MCP health pending. Explicit
startup success, when an adapter supports it, confirms connectivity. Codex ACP
1.11.0 emits only startup failures/cancellations, so a successful PA tool result
from the exact current provider/session confirms connectivity instead. Late
failure changes published health and live UI status; a subsequent bound PA tool
success recovers it. Silence stays pending. Evidence is tied to the
PAClient object and exact native session ID, invalidated on new connection,
disconnect, or rejected initial startup. Cancelled-only notifications remain
nonfatal. A timeout mixed with cancellation is still a failure. Error details
use existing redaction. No MCP startup timeout was increased. Local provider
install/update forwarding preserves their existing 900-second service action
budget; other HTTP requests retain the 120-second cap.

The confirmed 1.4.6 production incident does not identify the exact child phase
that stalled. Isolated import profiling motivated extracting proxy definitions;
it is not evidence of the production timeout's precise cause.

## Validation

- Relevant isolated regression run: 370 passed, 39 subtests passed; one unchanged
  restart-receipt responsiveness check exceeded its 200 ms threshold (302 ms).
  That check passed on its isolated rerun without changing the threshold.
- Final entrypoint/health/assigned-auth run: 94 passed, 2 subtests passed.
- Actual `python -m pa mcp` and `pa mcp`, ordinary and assigned: initialize and
  tools/list with an exclusive SQLite lock and a 128 MiB sparse history file.
  Child audit hooks reject SQLite initialization and service-directory writes;
  file size/mtime inventories remain identical. Final handshakes: 11.09, 11.07,
  10.64, and 7.87 seconds, each within the 25-second acceptance bound.
- Actual isolated ACP JSON-RPC fixture: session creation, two seconds of silence,
  failed startup at 30 seconds, then positive recovery. No external provider
  or production data is used. Unit cases also cover stale client generations,
  stale native IDs, tool_call_update, cancelled-only races, and live UI payloads.
- Final owner-forwarding/credential budget checks: 19 passed.
- Node UI check covers pending, error detail, and recovered status.
- Isolated service boot smoke: 511 routes. Wheel/sdist build succeeded; wheel
  initialize/tools-list returned 205 tools in 11.890 seconds without creating
  its configured data directory. Wheel console entrypoint metadata verified.

Final test runs clear inherited PA_* variables and set temporary data/workspace
roots. Earlier selected tests used explicit temporary data directories but
inherited PA_WORKSPACE_ROOT; the broad run was interrupted when discovered.
The original broad run is invalidated as acceptance evidence. Retained evidence
does not establish the exact earlier subprocess environment or production
central database writes; the persisted worker workspace root is its leased tree. No direct shared-data inspection or corrective write was attempted. This
qualification was reported durably in checkpoint
`3e1795ac-7dff-4345-ba17-d978bcb92608`; the owner API subsequently still reported
this session's sole lease ready at fence 218.

## Integration ownership

The sync-recovery worker's service/recovery implementation is unchanged. The
register_mcp method moves, including modules/sync.py, were reported on this
card for integration coordination. Root session
`32e91c3d-c139-453b-8031-42158c39d59b` owns independent exact-head review,
merge, official release, installation, and production activation/acceptance.
This worker does not merge, publish, restart PA, or edit production receipts or
queues. Keep the card Waiting through source integration until root completes
fresh toolful New chat and dispatch acceptance.

## CI compatibility follow-up

The first CI run exposed transport tests that relied on the MCP child creating
credentials, a static source-path assertion for moved tools, and a stream-order
assertion. Fixtures now provision their test-owned credentials explicitly, the
source assertion follows the canonical proxy module, and stream finalization
still precedes health display. The registry also reserves all original built-in
module names before plugin discovery, preserving duplicate-module rejection.

The follow-up transport/configuration/stream tests passed (64 tests and 306
subtests before the entrypoint cases). The focused registration/forwarding/UI
rerun passed 32 tests. The handshake test uses the existing bootstrap probe's
AsyncExitStack pattern: its unchanged 25-second deadline covers spawn,
initialize, and tools/list; SDK process teardown uses its separate bounded waits.
The four latest full runs, including teardown, took 13.32–21.27 seconds.

## Coordinator adapter review follow-up

Revalidated installed Codex ACP 1.11.0 `createMcpStartupUpdates`,
`completeItemEvent` MCP branch, and raw input/output helpers. The checked-in
contract fixture extracts those functions verbatim and executes them with Node;
ready yields no event, failure yields the forwarded error, and a completed PA
call includes `rawInput.server/tool` and `rawOutput.result/error`. This is a
contract fixture, not a production provider launch. The isolated ACP wire test
now uses those actual event shapes for late failure and bound-tool recovery.
Tests reject other-server, stale-client/session, failed-result, title-only,
in-progress and missing-result evidence. Live health includes the bound-tool
recovery. Provider/job route syntax is rejected before authenticated forwarding.
Local install/update retain the tested 910-second HTTP/900-second service budget;
fleet forwarding retains its pre-existing 120-second service transport bound.

The two preserved stderr logging edits were reviewed and included: stdio uses
only a stderr handler with the existing secret/exception redactor, no FileHandler.
All runs below clear inherited PA_* and set temporary data and workspace roots:

- 49 focused registration, forwarding, redaction and adapter regressions passed.
- Four actual locked-data stdio cases and the 30-second ACP wire case passed.
- 91 related ACP client, session live-event and stream-join tests passed.
- Wheel and sdist build passed.
- Plain source initialize/tools-list: 4.379 s (5.240 s including teardown).
- Plain wheel initialize/tools-list: 5.116 s (5.717 s including teardown).
  Both returned 205 tools without creating data/workspace directories. These
  are plain protocol timings without test audit hooks, not production stall
  attribution. The wheel was loaded directly without installing or activating it.

Review progress correlation: `mcp-repair-pr436-adapter-review-v1` (HTTP 200).
Root still owns final exact-head approval, merge and release/production acceptance.

## Composer integration and evidence ordering

At `aa097cf2edcbe42b119c5b89e103c8acb1f59a80`, MCP JavaScript regions are
`renderMcpHealth` line 1613, snapshot health line 1759, and tool call/update
health lines 2484/2488. Composer submission methods occupy the separate
3687–4101 region; this follow-up does not edit JavaScript or the composer tree.
Root chooses merge order. The second PR must rebase on the first actual merge
and run the combined JS regressions and build before independent acceptance.

The ordering follow-up requires a recovery call/startup attempt to start after
the latest hard failure before its completion can clear that failure. Every
new hard failure invalidates in-flight recovery candidates for that session;
tracking is bounded to 256 candidates. The exact client/native-session fence
still applies. Regressions cover an old in-flight completion and delayed
startup-ready event after a newer failure, followed by a fresh successful PA
call. Healthy initial tool success still confirms usability without a ready
notification. The isolated wire fixture now sends the real ACP start shape
(including required title) before recovery completion.

Ordering validation: 95 focused registration/ACP tests passed before the wire
fixture correction; its isolated 30-second wire rerun then passed. Four actual
stdio cases (ordinary/assigned, console/module) plus the separate 30-second
silence/failure/recovery test passed. The stdio children use test-only owner
credentials/session context and isolated workspace roots. Five focused
ordering/adapter/live/UI checks and the wheel/sdist build passed. No production
provider was launched and no production credentials were given to test children.

Root merged composer PR #434 as `9e82b41c0cdfbac8cfb17909d5b5744074bdd843`.
This second PR was rebased onto that actual main commit without conflicts.
Comparison against merged main confirms draft-widget is unchanged and
agent-chat contains only MCP health additions at 1613, 1759, 2485 and 2489.
Combined isolated draft/receipt, prompt-recovery, stream-join and MCP checks
passed (78 tests); wheel/sdist build passed. Progress correlation
`mcp-repair-pr436-composer-rebase-v1` returned HTTP 200. Root still owns
independent exact-head review and integration/release/production acceptance.

## Session/load history fence

Completed MCP history from Codex ACP `createHistoryUpdates` is not connectivity
proof. Positive PA tool evidence now requires an invocation start observed only
after session/new or session/load has returned for the current native session.
Opening this boundary clears pre-load success/start state; hard failures remain
authoritative and invalidate all earlier invocation starts. Completions without
a matching live start cannot confirm initial health or recover a failure.
History is still delivered through the existing transcript path; this change
only restricts health evidence and never synthesizes live tool work.

97 focused registration/ACP tests, including the actual 30-second failure wire
case, passed. Six focused history/order/generation cases passed, including a
real isolated ACP session/load protocol test that emits completed history both
before and after its response, then confirms health with a fresh live call.
All tests clear inherited PA_* and use temporary data/workspace roots. Build
passed. No timing threshold, tool proxy, authorization, redaction or composer
code changed in this follow-up. Root retains exact-head approval and integration.
