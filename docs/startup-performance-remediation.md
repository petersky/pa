# Startup replay, session entry, and background read amplification

Implementation evidence for card `f4a668e4-7ef1-4475-841a-986d308182f7`,
2026-09-06. Production recovery is not claimed by these repository tests.

## Startup safety

Incremental catch-up must apply `ancestors(target) - ancestors(projected)` in
parent-first order. Stopping only at the projected head allows a merge's other
parent to revisit projected ancestors. The macmini investigation found 11,699
candidates, of which 11,647 were already projected and only 52 were new. An old
incomplete conflict-resolution event then attempted to overwrite its completed
operation receipt. The receipt validator correctly rejected the reverse transition.

The fix excludes the complete projected ancestry. Immutable history, every merge
parent, receipt validators, and the atomic SQLite projection/head transaction are
preserved. Tests construct a completed conflict-resolution receipt, reproduce the
old failure, verify that only new branch/merge effects apply, reopen the projection,
and verify repeat catch-up is a no-op. A missing new event rolls back earlier new
effects and retains the old projection head. Ancestry collection still walks the
bounded immutable history once; it does not decode or apply its old events.

After this fix is released, macmini requires a separately authorized managed update
and start, followed by server-owned reconciliation and sustained startup/head checks.
Do not delete receipts, edit SQLite or refs, force a head, or weaken validation.
Macmini was left stopped during this implementation.

## Initial session entry

`mountAll` starts the owner lookup before the sidebar selects its shared event
transport. Previously, `closeSSE` incremented the same generation used to cancel
owner/history requests, leaving the first owner lookup permanently pending.

The generation now tracks selection lifetime. Transport closure leaves valid
requests alive; session selection, clearing, API-base changes, ending, and widget
destruction invalidate or reject obsolete work. Dedicated streams also reject
callbacks from obsolete stream objects/selections. Delayed fallback callbacks
revalidate their selection before changing controls.

The Node owner harness fails against the original release source and passes against
the fixed source. It covers delayed success/failure, transport replacement, switching,
destruction and a delayed history fallback. PR #410's history harness now explicitly
checks both coalescing valid history across transport replacement and rejecting
history from an obsolete selection.

PA's isolated browser ran `tests/agent_chat_owner_browser_harness.html` with the actual
widget and sidebar mount code. All six checks passed: sidebar startup during pending
owner resolution; writable first entry without reselection; reconnect preserving
selection; rapid switching; back navigation; forward navigation. Owner responses are
delayed 250 ms and the fixture uses synthetic HTTP/SSE responses, not production data.

## Performance changes

- Recovery filters closed and archived sessions in SQL before hydrating records.
- Reconciliation checks indexed completion events before loading a pending turn's
  messages. Canonical payload matching still handles compressed/cold evidence.
- Merged-card bookkeeping queries at most 100 due rows, excluding completed cards
  through a partial index. Missing cards, update failures and blocked dispositions
  retain audit/state and use durable exponential backoff (60 seconds to one hour).
  Missing cards are never marked done; later retries can recognize a restored card.
- Runtime and maintenance status return the last completed transcript diagnostic
  snapshot. Cold status explicitly says not measured/not checked. Integrity checks,
  counts, and object existence scans remain in explicit diagnostics and scheduled
  maintenance. `measured_at` exposes snapshot freshness; polling never starts scans.

Run `uv run python scripts/benchmark_startup_performance.py` for a disposable synthetic
benchmark. One run during concurrent local testing measured seven samples per path:

| Path | Previous read pattern, median ms | New read pattern, median ms |
|---|---:|---:|
| Recovery: 300 sessions, 299 closed | 228.505 | 7.342 |
| PR bookkeeping: 250 completed merged watches | 119.532 | 10.840 |
| Pending prompt: 5,000 transcript events | 53.906 | 1.690 |
| Transcript diagnostics vs status snapshot | 55.911 | 0.001 |

These are local fixture measurements, not production endpoint percentiles. The
original live baseline was health 206 ms, owner 71–76 ms, snapshot 242 ms, history
504 at 3.116 s, status 6.329 s, runtime 504 around 5.3 s, and page/list over 12 s.
Production request queueing, peer retry load, and endpoint latency after deployment
remain to be measured. No claim is made that every slow-operation log shares one
cause. Existing async runtime telemetry distinguishes queue wait from execution.

## Remote update diagnosis

Read-only SSH verified the macmini alias resolved to `Kyles-Mac-mini.localdomain`.
The launchd GUI job was absent, consistent with the coordinator stopping it. The
plist's executable and `~/.local/bin/pa` resolve to the same uv tool; both that
executable and `uv tool list` report PA 1.3.0. Its package `direct_url.json` points
to the v1.3.0 release wheel. This is not a new CLI paired with an old launchd path.

The installation receipt records v1.3.0 at `2026-09-06T15:52:09.041951Z`.
GitHub reports v1.4.0 published at `2026-09-06T19:10:53Z`: the last recorded update
predates that release. No later successful installation was found. The noninteractive
SSH PATH also lacks `pa` and `uv`; remote commands relying on bare names cannot find
them. Without a transcript of a later update attempt, its exact failure cannot be
established. No installed package, service configuration, or production data was edited.

## Validation record

Boot smoke and `uv build` passed. The pre-interruption broad suite completed with
2,423 passed and five failures: two UI fixture assumptions subsequently corrected
and passing; three timing/scale failures. The resumed scoped run passed 240 of 241
cases including the startup/receipt, UI/history, completion, PR retry, storage,
recovery, maintenance, scale and fleet checks. Its remaining mixed-load maximum
loop-lag threshold also failed against the unchanged base source (44.95 ms against
40 ms). Thresholds were not relaxed. CI on the final integrated PR is authoritative
for the required clean integration gate; local timing failures are retained as evidence.
