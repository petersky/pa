# Restart continuation recovery validation

Card: `f38067cc-3963-4f99-8447-29b817e8e077`.
Base: `4d54f7ed38c3f337a223e3f311390b6e0d006622`.
Execution: macbook / `519a0eff-a731-4633-87c0-ef924463526b`, lease fence 213.

## Failure and recovery contract

Provider startup probed `/api/ready`, which depended on completing that same
provider startup and restart receipt replay. The owner probe now uses
`/api/owner-ready`: required API services, warmed routes, and completed local sync
repair remain mandatory; provider lifecycle and previous probe health are not
circular prerequisites. The public operator readiness gate remains unchanged in
scope. Authentication and instance fencing still apply.

Human control holds unrelated automatic prompts. Taken-over automated runs
remain paused; the exception applies only to chats. A restart continuation is
eligible only when its durable receipt matches the exact session, prompt ID,
content, principal, workspace, environment, card/project, instance and execution
binding, and the receipt is queued and undelivered. Operator queue pauses and
cancellation remain effective, including while a user turn is running. Admission
rechecks authority before starting the provider turn.

The recovery watchdog replays pending continuations and the known transient
`api_not_ready` failure at the resuming stage. It uses the existing session retry
budget (eight attempts), backoff, and blocked state. Authentication, instance
mismatch, and incompatible API failures are not automatically rearmed. Recovery
and receipt replay retain their exact-session locks. Completed prompt evidence
repairs receipts without starting a provider or delivering another prompt.

The watchdog also recognizes a dequeued prompt during asynchronous admission,
before `_in_flight` is set. An isolated lifecycle run exposed this interval:
without explicit ownership, a watchdog sweep could incorrectly terminalize a
receipt even though its continuation subsequently completed.

Session observability selects the current live in-flight turn before queued
work. Queue presentation distinguishes an operator pause, human-control hold,
current response, and provider recovery/capacity.

## Regression evidence

Five selected regressions were run against the unchanged base source, with the
new test files retained. All five failed: human continuation eligibility,
transient readiness replay, current running-turn selection, the owner dependency
readiness route, and the owner probe's route selection. The modified source was
then restored and the tests rerun.

Additional coverage includes forged receipt labels/content/context, pause and
cancel, user-turn serialization, bounded exhaustion, nontransient failure holds,
concurrent replay coalescing, the dequeue/admission race, durable completion
repair, and existing receipt/queue recovery tests.

Validation passed: 301 focused and related tests, followed by 78 control/receipt
tests after the final takeover guard; isolated boot smoke with separate data and
workspace paths; `uv build` (source distribution and wheel).

## Isolated browser lifecycle, 2026-09-11 UTC

`tests/restart_browser_app.py` runs a real PA HTTP server and real ACP wire
process using the existing tool-free provider fixture. It retains the actual
owner readiness probe. Only its own restart hook is substituted: it signals the
isolated server process, which is started again on the same isolated data.
It neither uses provider accounts nor calls a production service manager.

- Instance: `831156d3-f921-4b8b-a6ec-7a47bf9279b8`, loopback port 8097.
- PA session: `875739c4-6919-4e3d-a531-2842e323f1fa`.
- Provider thread: `dd9eb884-22f4-4730-a66a-414377ec75bd` throughout.
- Initial human prompt submitted with PA-managed browser type/click operations.
- First receipt: `4988cf86-e201-5d5b-a0e0-614776bdff58`; exposed the admission
  race above. Its exact completion later repaired the failed receipt.
- Repeated receipt: `db954779-1ec8-5c3b-9ae6-18584853a51f`; created at
  `23:31:45Z`, delivered at `23:32:37Z` after the updated server resumed.
- Provider starts at transcript sequences 1, 8, 15 all retained the same provider
  thread. There was one start per server generation.
- First continuation: enqueue 9, dequeue 10, user_message 12, completion 14.
- Repeated continuation: enqueue 16, dequeue 17, user_message 19, completion 21.
- Each prompt retained `restart-handoff:<receipt-id>` and completed exactly once.
  Both receipts ended `continuation_delivered`, queue empty, control mode `human`.
- No Retry live session or Resume session operation was used to deliver either
  continuation. The browser was reattached for final visual inspection only.

Raw HTTP snapshots, server logs, regression output, and `browser-final.png` are
retained under `.dev/restart-validation/` in the fenced worktree. The final
browser showed all three turns. It also displayed a separate history retry
banner despite successful history HTTP responses; that presentation issue is
not claimed fixed here. The initial fixture shutdown needed a second signal
because raw uvicorn waited on browser SSE connections; subsequent launches used
`--timeout-graceful-shutdown 3`. An orphaned fixture-only Chrome was closed before
reattachment. These fixture limitations do not constitute production acceptance.

## Release boundary

No production receipt, queue, provider, installation, or restart was modified.
Root owns exact-head review, release activation, and production acceptance.
The card must remain Waiting after source merge until that acceptance succeeds.

## Repeatable root-audit coverage

Run `uv run python -m tests.restart_browser_validation` with port 8097 free.
The harness submits a human prompt using PA browser type/click, requests an
isolated restart receipt, relaunches the fixture, and checks automatic delivery
before reattaching the browser for inspection. It asserts the same provider
thread, two provider starts across two generations, exactly one continuation
message/completion, human control, and an empty queue. It records its HTTP calls
and browser screenshot under `.dev/restart-validation/automated-browser-*`.

Retry live state refreshes live discovery; Resume session calls `/recover`;
`/queue/resume` is a separate queue control. The harness invokes none of these
manual recovery controls to deliver the continuation.

`test_admission_in_progress_keeps_same_receipt_retryable` verifies an admission
race leaves the exact receipt `resuming`, then the watchdog delivers the same
prompt once. The existing dispatch follow-up fixture uses the harmless phrase
“Bearer credentials”: its acceptance test explicitly requires `[REDACTED_AUTH]`
in stored text while concurrent retries retain one accepted prompt identity.
The restart and follow-up suites pass together (89 tests).

## Root review: off-loop receipt authorization

Receipt lookup no longer runs in `_prompt_eligible`, `_start_drain`, or
presentation. Those paths inspect derived in-memory receipt evidence only.
A source label can schedule asynchronous verification but cannot authorize
provider delivery. Drain loads receipts through `_offload`, then checks the
current queue/pause state; execution reloads the exact receipt after admission
configuration and immediately before establishing the in-flight turn.

Cached evidence is invalidated before refresh, when starting a drain, on
pause/cancel/control changes, and when delivery finishes. Every eligibility
check still compares exact content, source/receipt identity, session and
execution context. A revocation-during-admission regression proves a previously
cached grant cannot start a provider turn.

The delayed-store regression holds receipt lookup in a worker thread while
checking four event-loop heartbeat ticks and a live metadata snapshot. The
previous head fails: `_start_drain` blocks about 1.1 seconds. Updated source
passes the responsiveness and revocation tests, 174 related tests, and the
repeatable browser lifecycle (session `7624da6c-799d-4bb8-9876-7d9325388d3e`,
receipt `35644ed6-7199-5302-a6fd-70ead13b5543`).

## Root review: preserve queue priority during receipt validation

Drain now scans queue order and loads only the restart candidate that would
otherwise be selected next. An ordinary eligible prompt ahead of that candidate
runs before any receipt lookup. A failed lookup holds only that candidate and
allows other eligible prompts to proceed. After each read the scan restarts,
rechecking queue membership, priority, pause, connection and current scope;
execution still independently reloads receipt authorization.

Seven regressions cover slow/failed lookup behind a user prompt, failure ahead
of a user prompt, and pause, removal, scope change or a new higher-priority user
prompt during validation. The three priority/failure regressions fail against
`e4777c5929e3eba8ef754a5da8729dd9f9928cd6` and pass with this correction.
All 147 related tests and `uv build` pass. The isolated browser lifecycle passed
again: session `fe7150e0-a370-4352-8532-798240c15817`, receipt
`aa9f8711-16b9-535b-96ae-1d2672af17cc`, with exact provider identity and one
automatically delivered continuation. Production acceptance remains root-owned.
