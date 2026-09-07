# Live delivery and operation recovery — 2026-09-07

## Execution provenance

- Repository: `petersky/pa`; base: `4ce6fb2c2cc09ce4186e3b5c5d51ff13710fa37c`.
- Instance: macbook `0c7d8ecb-7e45-4579-8fa0-35159492d3f1`.
- Card: `434f6c90-d0fe-462d-89ea-d8a129b15937`.
- Session: `93157ec9-780d-44dc-bc3e-257f48e9042a`; dispatch: `b09a701b-36c3-40ad-a6f6-25ece7a67ea8`.
- New isolated worktree, clean at entry, ready verified lease fence 201. Its exact
  branch, worktree path, base, and running provider evidence were recorded through
  `report_dispatch_progress` with key `live-delivery-93157ec9-verified-workspace-v1`.
- Actual provider PID 52232, with this command executor in its descendant tree.
  The previous card session had no connected runtime or active turn.

## Live production baseline (before this change)

Read-only loopback HTTP probes, without restarting, modifying service data, or
sending prompts to a production conversation:

| Request | Elapsed seconds |
| --- | ---: |
| `/api/status` | 1.578 |
| `/agent`, first observed request | 4.432 |
| `/agent`, repeated request | 3.586 |
| `/api/agent/sessions?view=chats` | 0.353 |
| Selected active session snapshot | 0.039 |
| Selected history, message boundaries, limit 100 | 0.106 |

Later samples: `/agent` 5.089s (`page_context` 2712.9ms), activity view 6.399s
(`page_context` 5409.7ms). These are observed warm/first-request measurements,
not a claim that a production cold cache was forcibly cleared.

The live status endpoint reported process 51729, version 1.4.3, asset version
`e64d855dfe5c`, installed version 1.4.3, and equal durable/projection sync heads.
`runtime_revision` was null: an exact live Git revision was not inferred.

## Browser evidence on isolated source fixture

Run `uv run python -m tests.agent_chat_live_fixture`, then open
`http://127.0.0.1:18081/` through PA's isolated browser. The fixture renders PA's
actual chat template and scripts and serves its actual shared SSE handler. Its
synthetic replies need no provider or tools; all storage is temporary.

Observed through PA browser actions:

1. Full initial mount enables the real composer.
2. Sending a fixture prompt displays the exact progress text
   `Fixture progress: the live connection delivered this message.` and final text
   `Fixture final: all assistant text arrived without refreshing.` without reload.
3. Interrupting the stream then writing a final during the disconnect displays
   `Fixture reconnect: retained final replayed exactly once.` once, without reload.
   The fixture recorded reconnect cursors changing from `{"live-fixture":0}` to
   `{"live-fixture":4}`.
4. Switching to another session and back restores the exact unsent draft
   `Preserve this unsent fixture draft.` and all delivered message bodies.

Evidence operation IDs: `latency-fixture-live-proof-v1`,
`latency-fixture-reconnect-proof-v1`, and `latency-fixture-draft-proof-v1`.
The successful browser attachment belongs to this execution session. An early
fixture navigation hit `ERR_CONNECTION_REFUSED` during fixture startup; its
operation outcome was inspected rather than reported as success.

These are fixture results, not post-deployment production results. Release
coordination and production activation must independently verify the new build,
repeat bounded page/history/status probes, and check live transport after release.

## Budgets and durable ownership

`blocking_operation_budgets` (also `PA_BLOCKING_OPERATION_BUDGETS`) maps exact
operation names to queue, execution-idle, lock-wait, and absolute seconds. The
initial policies for web intake, restart receipt creation, transcript append,
and quiesce snapshot persistence are 120/120/300/600 seconds respectively.
Other operations retain their explicit/default deadlines.

Only internally observed work, SQLite execution progress, and actual lock waits
affect these budgets. Transport heartbeats do not renew operation deadlines.
Executor telemetry separates queue, lock, actual thread runtime, and caller wait;
active operations expose phase and elapsed time. A caller timing out cannot free
a worker slot while its native call still runs.

Owned prompt/follow-up mutations retain the intake-to-admission chain after
caller cancellation. The client wait is bounded; the underlying native write
cannot safely be killed. Its eventual result and post-commit work remain owned.
Restart creation attaches scheduling to actual commit completion, with the
existing recovery coordinator also sweeping unscheduled requested receipts.
Transcript retries await the real write and preserve immutable batches even if
cancelled during retry backoff. Restart refuses to proceed after an unsuccessful
transcript drain and reports the blocking session/batch state.

Follow-up outcome lookup uses the exact follow-up record's state, error, and
prompt ID. The existence of its parent dispatch does not imply success.

## Validation

Focused suites cover real prompt-route cancellation and exact-key recovery,
late receipt commit after timeout/cancellation, the pending-receipt watchdog,
transcript retry-backoff cancellation, independent stream replay, missing-final
recovery signals, JavaScript gap repair and owner changes, restart/quiesce,
configuration, fleet, dispatch consistency, and transcript storage. Virtual
monotonic deadlines exercise 83s queue and 130s lock waits, idle expiry,
heartbeat exclusion, and the absolute cap without slow wall-clock sleeps.

The isolated boot smoke test and artifact build pass. The existing mixed-load
responsiveness test exceeded its 20ms probe bound once while another test process
was running (69.6ms); its complete 19-test module passed when rerun alone. Full CI
must independently validate the committed head. No timing threshold was loosened.
