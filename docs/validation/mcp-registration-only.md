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
startup success confirms connectivity; late failure changes published health and
live UI status, and later explicit success recovers it. Evidence is tied to the
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
Shared workspace-manager schema initialization may have occurred in those
runs. No direct shared-data inspection or corrective write was attempted. This
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
