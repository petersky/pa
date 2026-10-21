# Progress ingestion copy and serialization — 2026-10-21

All **638 findings and 116 concerns remain open**. All 636 prior source rows and historical failures are unchanged. The [complete numeric evidence](benchmarks/systemic-progress-ingest-copy-20261021.json) adds separate findings for redundant provisional copying and retained event JSON accounting; the latter is **not repaired** in this patch.

Progress ingestion only mutates the event and seen-key containers in place. It now copies those two lists for the provisional record; retained payloads stay read-only and range helpers replace their lists. The full deep copy after commit remains. Every other application AST node equals pinned baseline de7ab4dc. SQL durability, exact receipts, provenance, rejection/coalescing/late behavior, sorting and age/count/byte/protected retention are unchanged.

**74 guarded disposable contracts pass: 20 new and 54 existing.** New cases cover all four branches at before/after-commit boundaries, exact exceptions, unchanged prior publication, replay and changed-payload rejection after reload, detached nested state/plan/ranges, three retention boundaries, provenance/duplicate no-op and 20 concurrent ordered checkpoints. The initial run had 69 passes and three fixture failures: lifecycle `put` correctly preserved existing normalized history. Those fixtures now seed retained history through legacy migration; no production behavior or limits changed.

Ten alternating pairs for each 1/128/1,000 retained-event and plan-step shape across accepted/late/conflict/coalesced ingestion preserve complete models/results with identical timestamps, logical writes, SQLite changes, core bytes and reload. All **240 calls** and overlapping copy/event-JSON/native-SQL measurements are retained. Full snapshots fall **2→1** per operation. This shared-host comparison does not establish statistical bounds or deployed performance acceptance.

| Retained events / branch | Baseline wall p50 / p95 / max ms | Source wall p50 / p95 / max ms |
|---|---|---|
|1:accepted|1.803 / 2.762 / 2.762|1.336 / 3.011 / 3.011|
|1:late|1.421 / 3.437 / 3.437|1.588 / 2.022 / 2.022|
|1:conflict|1.421 / 2.267 / 2.267|0.877 / 1.609 / 1.609|
|1:coalesced|1.360 / 2.904 / 2.904|0.803 / 1.696 / 1.696|
|128:accepted|14.397 / 16.729 / 16.729|7.928 / 11.750 / 11.750|
|128:late|12.351 / 18.387 / 18.387|7.079 / 11.807 / 11.807|
|128:conflict|8.402 / 12.041 / 12.041|5.470 / 7.753 / 7.753|
|128:coalesced|8.639 / 13.977 / 13.977|4.936 / 6.367 / 6.367|
|1000:accepted|89.866 / 170.241 / 170.241|53.120 / 115.180 / 115.180|
|1000:late|92.461 / 148.051 / 148.051|54.244 / 65.317 / 65.317|
|1000:conflict|84.135 / 165.856 / 165.856|36.544 / 49.622 / 49.622|
|1000:coalesced|75.059 / 94.206 / 94.206|38.312 / 118.067 / 118.067|

**The 1,000-event coalesced p95/max regresses 94.206→118.067 ms and CPUmax 89.980→116.981 ms.** The one-event accepted max regresses 2.762→3.011 ms and one-event late median 1.421→1.588 ms. These outliers, previous acknowledgment/heartbeat tail regressions and historical CI failures remain; no rerun selected a better tail. At ten samples p95/p99 equal max; shapes are not pooled.

The source accepted and late 1,000-event scenarios still call event `model_dump_json` **1,002→1,011 times per ingestion** as protected history grows. Ten accepted calls materialize **6,371,570 JSON bytes**; ten late calls materialize **6,371,440 bytes**, identical to baseline. This partial serialization observer overlaps operation time and excludes other core/receipt serialization; zero event JSON calls on conflict/coalescing does not mean no serialization. Byte accounting remains unmodified, with a separate open repair requiring exact delivery/removal/reload/encoding/retention parity. One deep publication and core serialization/index costs remain.

PR561 is integrated as de7ab4dc from exact 8795de28 with equal trees and five first-attempt successful head and main checks. Its original admission 372.370 ms / 200 ms failure and 381 ms callback remain unclassified despite corrected CI green. Latest separately observed c02e/package5917/PID78321 runtime predates this source; no activation or deployed retest occurred. All original representative mixed-load, cold-start/drain, API/background/branch/provider/MCP/placement/workspace, native busy/CPU/disk/write amplification, sync/peer/log/retention, backup, GC/saved-plan/publication, ambiguous receipts and independent acceptance requirements remain. DB89957 accepted original scope stays closed; backup0331/b527 and ordinary independent17d retain their ownership. No competing worker or verifier, threshold/debug/default/model/permission/queue change, unsafe service fault, root backup/GC action, restart or ambiguous resend occurred.

## Preserved prior inventory and evidence

# Progress acknowledgment copy repair — 2026-10-21

All **636 findings and 114 concerns remain open**; all635prior source rows are unchanged. The acknowledgment finding previously preserved in the durable card report is now folded into the [complete source evidence](benchmarks/systemic-progress-ack-copy-20261021.json), including its original three-call probe and full prior coverage delta. Ten original card attachments remain; the earlier extra attachment was rejected at the ten-attachment boundary and was not resent or claimed attached.

Delivery success/failure used to deep-copy every retained event and plan before changing one payload, then deep-copy again for committed publication. The helper now locates the first matching event or heartbeat, shallow-copies only its scalar-mutated payload and containing event list/provisional record, and returns without copying for absent keys. Existing scalar updates/error sanitization, writer fence, SQL writes, NORMAL transaction, exact before/after-commit fault handling and full deep committed publication remain unchanged. Every other application AST node equals pinned baseline45347da4.

**52 guarded disposable contracts pass:17new and35existing overlapping prior suites.** They cover event/heartbeat success/failure across both commit boundaries, exact exceptions, unchanged earlier publication, reload, deep publication/index/list order, stale nested metadata/plan/receipt isolation, absent-key no-op, readonly fencing and20concurrent delivery/attempt updates. Full snapshots fall2→1for updates and1→0for absent keys; no durable payload is dropped.

Ten alternating pairs per0/128/1,000retained-event+plan shape and delivered/failed/absent action preserve complete models with identical pinned timestamps, core bytes, logical writes and SQLite totalchanges. All180calls and numeric phases are retained. **The1,000event delivered p95/max regresses114.026→138.401ms and CPUmax108.734→136.784ms despite median104.084→57.806ms improvement.** Shared-host outliers remain unclassified; no timing rerun selected a better tail. At10samples,p95/p99 equalmax; shapes are not pooled.

| Retained events / action | Baseline wall p50 / p95 / max ms | Source wall p50 / p95 / max ms |
|---|---|---|
|0:delivered|1.170 / 1.691 / 1.691|0.916 / 1.375 / 1.375|
|0:failed|1.066 / 1.739 / 1.739|0.919 / 1.256 / 1.256|
|0:absent|0.170 / 0.309 / 0.309|0.023 / 0.032 / 0.032|
|128:delivered|14.578 / 15.892 / 15.892|8.484 / 9.799 / 9.799|
|128:failed|14.729 / 15.921 / 15.921|8.384 / 9.264 / 9.264|
|128:absent|6.434 / 7.779 / 7.779|0.186 / 2.005 / 2.005|
|1000:delivered|104.084 / 114.026 / 114.026|57.806 / 138.401 / 138.401|
|1000:failed|109.139 / 191.571 / 191.571|57.272 / 78.328 / 78.328|
|1000:absent|55.762 / 148.176 / 148.176|1.274 / 2.462 / 2.462|

Zero-history update cases target the heartbeat. Counts/core bytes are logical evidence, not physical disk/fsync/serialization attribution. One complete post-commit copy, core JSON and index work remain. Original bootstrap/native wait/CPU/GC scheduling, sync amplification, valid branch/provider/MCP/placement/workspace/startup/drain, peers/log gaps, backup/GC/saved-plan/publication and rollout obligations remain open.

PR560 is integrated45347da4/equal trees with five successful latest head and main checks. Its original263.571ms failure against the unchanged100ms mock responsiveness budget,256ms callback and single same-head retry remain preserved under the existing open ci542 concern. This delayed PR557 notice refers to superseded072chead; fresh GitHub/current native watch confirm correctedb8cd was already merged e109/retiredmerged. No repeat repair/merge/rerun occurred. Latest separately observed installed/loadedc02e/package5917/PID78321/startupa50c predates these repairs. No activation, deployed acceptance, service workload, threshold/default/model/permission change, backup/GC action, queue policy/resend/cancellation or competing worker/verifier occurred.

## PR561 CI admission recurrence and bounded diagnostics

Initialheadca9341b3 CI39527806951attempt1 failed the existing durable-admission200ms assertion at**372.370ms**, with a381ms callback warning;998other cases and182subtests passed in124.60s and the otherfour checks succeeded. The original raw log SHA14620f0b is preserved locally; only [sanitized failure metadata and numeric diagnostic evidence](benchmarks/systemic-pr561-ci-admission-20261021.json) are published because the raw log maycontain fixture secrets. This recurs under existing open ci533; rowcount stays636/114, allrows unchanged. No same-head CI retry occurred.

One guarded original diagnostic passes with both acknowledgment methods and Workspace construction denied, confirming the new ackpaths are unused. Helperprofiles do not reproduce the372ms delay. IsolatedprocessGC185/188ms durations lack originalCI/exacttimedwindowcorrelation and do not establish cause. The existing test now includes bounded calling-threadCPU and first8+longest GCintervals only in its failure message, with callbackcleanup in finally. Both cold admission/body constructions, every original assertion, defaultdebug and200ms threshold are preserved by ASTchecks; no warmup orGCpolicy change. The correctedexistingcase passes withobservercleanup verified. Together with52prior guarded contracts,53distinctselectedcases pass; repeateddiagnostics are not additionalunique tests. Ackapplication source/timing samples are unchanged; correctednewheadCI and originalfullacceptance remain required.

## Preserved prior inventory and evidence

# Dispatch SQLite observation coverage — 2026-10-21

All **635 findings and 113 concerns remain open**, with all634prior rows unchanged. [Complete evidence](benchmarks/systemic-dispatch-sql-observation-20261021.json) confirms DispatchStore used raw SQLite connections: a captured zero SQL count was missing coverage. Both writer and read-only opens now use the existing numeric observer. Every other application AST node is unchanged. Execute/lazy-fetch durations and row counts, actual commit durations and BUSY/LOCKED errors become visible without retaining SQL, parameters or results. Writer30s/readonly5s timeouts, WAL, NORMAL/temporaryFULL durability, admission and receipt fences remain unchanged.

**35 guarded disposable contracts pass (six new,29existing overlapping prior suites)**: lazy rows, commits, exact before/after-commit errors/publication/reload, receipts, contention rollback, read-only SELECT-only initialization and unchanged database/WAL/metadata bytes+mtimes, deferred opening and promotion. The first collection failed on an invalid test-helper import. The next run34pass/one fail incorrectly included SQLite's ephemeral shared-memory reader marks in durable file equality. Both failures are retained; fixtures were corrected, application semantics/deadlines unchanged.

Ten alternating paired commits per0/128retained-event+plan shape preserve complete visible state excludingupdated_at, history, core bytes and logical writes. Raw→observed wall median **1.188→1.317ms** and **8.794→8.862ms** retain instrumentation cost; p95/max2.241→2.046ms and11.266→9.608ms are small shared-host samples, not a speedup or statistical overhead bound. Every observed heartbeat captures six execute/fetch calls plus commit time; calls are not distinct statements/incidents. p95/p99 equal max at10samples. Bootstrap `executescript` and rollback duration are not timed by the shared adapter and remain explicit gaps. Successful nativebusy time still cannot be separated from CPU/I/O/scheduling by statementelapsed, and physicalbytes/fsync costs remain unmeasured. No deployed/mixed/fleet/startup/drain acceptance follows.

PR559 is integrated597724c8 with equal trees and five successful head/main checks; its1,000history tail regression remains. The delayed PR556 notice was verified alreadymerged0f05 at exact8e11; originalmainCI39519841572 failure is preserved without a repeat merge/rerun. Latest separately observed installed/loadedc02e is older source; no root activation, service workload, queuechange, backup/GC action, warning/deadline increase or competing worker/verifier occurred. Full original inventory, remaining CPU/publication/sync/startup/peer/log/backup/rollout obligations and ordinary independent acceptance remain open.

## Preserved prior inventory and evidence

# Heartbeat provisional copy repair — 2026-10-21

All **634 findings and 112 concerns remain open**; all 633 prior rows remain unchanged. [Complete evidence](benchmarks/systemic-heartbeat-copy-20261021.json) confirms heartbeat updates deep-copied retained dispatch history before a second deep committed publication. Only the seen-key list mutates in place; range helpers and the incoming heartbeat replace detached values. The source now shallow-copies the provisional record and independently copies seen keys. Receipt/provenance checks, late/current branches, full serialization, durable commit and deep post-commit publication remain unchanged.

**50 guarded disposable contracts pass: six new and44 existing.** They cover late/current before/after-commit failures, prior publication and caller isolation, retained plans/ranges/keys, indexes, delivered state,16concurrent durable receipts and exact replay/conflict. Only one full snapshot is constructed for successful committed publication. The first run had49pass/one fixture failure: a replay regenerated its timestamp under an existing key and was correctly rejected. The fixture now reuses the original payload; the receipt hash guard is unchanged. Both logs remain linked.

Ten alternating real disposable commits per0/128/1,000retained-event/plan-step scenario measured the exact-equivalent compiled candidate before source editing. Native method AST equals that candidate; every other module AST node matches pinned baseline34b599b7. At1,000events, wall median75.918→39.623ms and CPUmedian70.218→29.820ms improve, **but wallp95/max regresses121.401→124.414ms and CPUmax116.941→120.284ms**. At128events, wallp9516.747→9.345ms; zero-history2.096→1.977ms. All raw samples, complete model parity except volatileupdated_at, retained counts, core bytes and logical write counts remain. At10samples,p95/p99 equal max. No timing rerun selected a better tail. The archived/native reproducer is included; no physical disk, fsync or serialization phase measurement, controlled mixed scenario or deployed acceptance is claimed.

PR558 is integrated34b599b7 with equal source/merge trees and independently green five-check head and mainCI; its watch is retired merged. The delayed PR555 notice was independently checked as already mergedcb81b166 at exact3d8b2b81; no repeat merge or unchanged CI rerun occurred. Latest installed/loaded metadata remains separately observedc02e, not proof this newer source is active. Remaining heartbeat/checkpoint/ack CPU, native SQL instrumentation gaps, sync object-read amplification and all original634ledger actions/ordinary independent criteria remain open. No root installation/restart, service workload, backup/GC action, data drop, warning/deadline increase, new worker/verifier or default change occurred.

## Preserved prior inventory and evidence

# Current installed/loaded metadata inventory — 2026-10-21 00:40 UTC

All **633 findings and 111 concerns remain open**; all 633 ledger rows are unchanged. The [complete supported numeric snapshot](benchmarks/systemic-live-runtime-20261021T0040.json), [source/limits summary](benchmarks/systemic-live-runtime-20261021T0040-summary.json), [all121 operations and phases](benchmarks/systemic-live-runtime-20261021T0040-operations.csv) and [all47 route groups](benchmarks/systemic-live-runtime-20261021T0040-routes.csv) retain every returned metric. No new names were needed; each observed family updates its existing evidence overlay.

Source: macbook0c7d8ecb, status/runtime/status reads **2026-10-21T00:40:16.961973Z–00:40:17.776792Z**. Before/after build metadata matches sourcec02e6d84/package5917d4f0/PID78321/startupa50c19a2; installed revision is separatelyc02e. This replaces the earlier228 metadata observation. Source ancestry includes PR555, but full module composition, specific path activation and paired deployed performance acceptance are not established. Root performed no installation/restart. PR558 is newer source; the existing source-review proposal does not authorize a floating activation.

There are **49 slow-or-failed operation names**, with404 slow calls, four failures and zero timeouts. These are overlapping cumulative process counters, not distinct incidents or log warning counts.11,014 non-SSE requests total85,613,067 response bytes; four SSE requests are excluded. Fifteen route windows have p95≥500ms, some with small samples. Event-loop current600-sample p95 is68ms; process maximum2,411ms. No shared-cause attribution follows solely from these maxima. Individual read times437.459/237.577/137.374ms are not acceptance distributions. Ordinary traffic, background activity and recent disposable tests share an uncontrolled host; no fresh log files, peer files or retention audit were collected.

The source outbox repair and every other finding remain open. Transcript append reaches11,478.902ms, session save12,553.332ms, startup state3,471.226ms, schema warm-up1,074.573ms, maintenance89,448.049ms and workspace GC8,318.475ms in cumulative process maxima. Progress sequence allocation still reaches954.441ms despite inclusion of the earlier source repair; warning cause and live contribution remain unverified. These are not rolling-window quantiles. All remaining families and routes are summarized below, with every queue/service/lock/CPU/I/O phase preserved in raw JSON and CSV. No threshold increase, data drop, backup/GC operation or acceptance claim occurred. PR557 head and mainCI are both independently five-success; their original failure stages remain retained.

| Operation family | Names | Submitted | Slow | Failed | Timed out | Highest process max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp` | 4 | 4794 | 8 | 0 | 0 | 944.185 |
| `agent` | 7 | 965 | 26 | 0 | 0 | 3471.226 |
| `browser` | 3 | 3 | 0 | 0 | 0 | 3.657 |
| `conversation` | 2 | 2394 | 23 | 0 | 0 | 1260.14 |
| `dispatch` | 7 | 7944 | 51 | 0 | 0 | 2409.797 |
| `filesystem` | 1 | 555 | 0 | 0 | 0 | 849.705 |
| `fleet` | 13 | 8426 | 43 | 0 | 0 | 10845.37 |
| `goal` | 1 | 60 | 0 | 0 | 0 | 133.251 |
| `health` | 1 | 398 | 8 | 0 | 0 | 2647.507 |
| `http` | 5 | 7542 | 0 | 0 | 0 | 7042.999 |
| `maintenance` | 1 | 1 | 1 | 0 | 0 | 89448.049 |
| `notifications` | 2 | 294 | 0 | 0 | 0 | 303.207 |
| `pr_supervisor` | 5 | 6469 | 0 | 0 | 0 | 391.58 |
| `preferences` | 1 | 40 | 0 | 0 | 0 | 252.724 |
| `progress` | 7 | 1905 | 61 | 0 | 0 | 1531.849 |
| `provider` | 3 | 6 | 6 | 0 | 0 | 3220.149 |
| `reconciliation` | 6 | 3827 | 7 | 0 | 0 | 718.356 |
| `selection` | 4 | 15 | 0 | 0 | 0 | 306.633 |
| `sqlite` | 31 | 38695 | 119 | 3 | 0 | 12553.332 |
| `startup` | 3 | 3 | 1 | 0 | 0 | 1074.573 |
| `sync` | 10 | 182622 | 44 | 0 | 0 | 6114.915 |
| `workspace` | 4 | 16 | 6 | 1 | 0 | 8318.475 |

| Slow or failed operation | Submitted | Slow | Failed / timed out | Window n | p50 / p95 / p99 / max ms | Process max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp.wire_append` | 4788 | 8 | 0 / 0 | 256 | 2.373 / 122.354 / 187.518 / 291.534 | 944.185 |
| `agent.project_recovery_availability` | 636 | 1 | 0 / 0 | 256 | 4.23 / 134.185 / 371.066 / 578.158 | 578.158 |
| `agent.recovery_sessions` | 317 | 24 | 0 / 0 | 256 | 204.505 / 554.159 / 1177.645 / 1640.626 | 1640.626 |
| `agent.startup_state_read` | 1 | 1 | 0 / 0 | 1 | 3471.226 / 3471.226 / 3471.226 / 3471.226 | 3471.226 |
| `conversation.issues` | 1197 | 6 | 0 / 0 | 256 | 26.002 / 173.37 / 262.182 / 695.424 | 1260.14 |
| `conversation.session` | 1197 | 17 | 0 / 0 | 256 | 24.958 / 137.892 / 366.852 / 1064.836 | 1064.836 |
| `dispatch.completion_pending_read` | 356 | 1 | 0 / 0 | 256 | 3.089 / 60.794 / 133.333 / 190.757 | 1280.48 |
| `dispatch.record_read` | 7221 | 50 | 0 / 0 | 256 | 31.057 / 96.883 / 166.508 / 663.013 | 2409.797 |
| `fleet.overview.mcp_bootstrap` | 2 | 2 | 0 / 0 | 2 | 9494.262 / 10845.37 / 10845.37 / 10845.37 | 10845.37 |
| `fleet.overview.sync` | 291 | 6 | 0 / 0 | 256 | 55.249 / 290.632 / 532.471 / 747.967 | 747.967 |
| `fleet.overview_cache_read` | 5199 | 14 | 0 / 0 | 256 | 4.648 / 29.816 / 45.592 / 261.257 | 1513.164 |
| `fleet.overview_cache_write` | 1613 | 18 | 0 / 0 | 256 | 20.979 / 212.719 / 347.697 / 793.643 | 2180.926 |
| `fleet.overview_response_json` | 1080 | 3 | 0 / 0 | 256 | 9.849 / 44.907 / 209.821 / 288.046 | 2173.505 |
| `health.canonical_obligation` | 398 | 8 | 0 / 0 | 256 | 5.618 / 108.958 / 369.734 / 2303.791 | 2647.507 |
| `maintenance.run` | 1 | 1 | 0 / 0 | 1 | 89448.049 / 89448.049 / 89448.049 / 89448.049 | 89448.049 |
| `progress.checkpoint_write` | 75 | 29 | 0 / 0 | 75 | 226.905 / 1022.41 / 1531.849 / 1531.849 | 1531.849 |
| `progress.dispatch_read` | 867 | 18 | 0 / 0 | 256 | 103.912 / 215.302 / 812.602 / 1210.843 | 1210.843 |
| `progress.heartbeat_write` | 135 | 9 | 0 / 0 | 135 | 250.008 / 749.589 / 1064.682 / 1174.302 | 1174.302 |
| `progress.pending_read` | 598 | 2 | 0 / 0 | 256 | 18.629 / 79.016 / 232.984 / 577.168 | 800.661 |
| `progress.sequence_allocate` | 210 | 3 | 0 / 0 | 210 | 132.165 / 276.414 / 507.571 / 954.441 | 954.441 |
| `provider.status.codex` | 2 | 2 | 0 / 0 | 2 | 2988.762 / 3101.687 / 3101.687 / 3101.687 | 3101.687 |
| `provider.status.cursor` | 2 | 2 | 0 / 0 | 2 | 3100.485 / 3220.149 / 3220.149 / 3220.149 | 3220.149 |
| `provider.status.openinterpreter` | 2 | 2 | 0 / 0 | 2 | 642.06 / 994.116 / 994.116 / 994.116 | 994.116 |
| `reconciliation.completion_evidence_read` | 1034 | 4 | 0 / 0 | 256 | 8.11 / 147.544 / 276.778 / 555.781 | 626.323 |
| `reconciliation.dispatch_read` | 1751 | 2 | 0 / 0 | 256 | 3.948 / 50.053 / 153.78 / 230.476 | 651.908 |
| `reconciliation.session_read` | 1033 | 1 | 0 / 0 | 256 | 5.075 / 76.172 / 183.586 / 310.226 | 718.356 |
| `sqlite.agent_runtime_checkpoint` | 18 | 1 | 0 / 0 | 18 | 54.306 / 701.012 / 701.012 / 701.012 | 701.012 |
| `sqlite.agent_session_save` | 194 | 8 | 0 / 0 | 194 | 41.454 / 371.555 / 11034.357 / 12553.332 | 12553.332 |
| `sqlite.card_read` | 10446 | 7 | 0 / 0 | 256 | 3.264 / 27.175 / 105.857 / 735.952 | 951.864 |
| `sqlite.pr_supervisor_completion_due_read` | 553 | 8 | 0 / 0 | 256 | 78.456 / 333.453 / 619.055 / 857.42 | 926.674 |
| `sqlite.pr_supervisor_effect_prepare` | 10 | 0 | 3 / 0 | 10 | 8.934 / 118.098 / 118.098 / 118.098 | 118.098 |
| `sqlite.pr_supervisor_event_write` | 3226 | 6 | 0 / 0 | 256 | 5.774 / 88.445 / 339.161 / 766.108 | 966.164 |
| `sqlite.pr_supervisor_watch_read` | 3553 | 13 | 0 / 0 | 256 | 8.668 / 116.671 / 268.029 / 546.764 | 1611.914 |
| `sqlite.pr_supervisor_watch_write` | 3228 | 11 | 0 / 0 | 256 | 14.608 / 126.728 / 248.644 / 471.435 | 1078.714 |
| `sqlite.project_read` | 7240 | 5 | 0 / 0 | 256 | 2.424 / 42.308 / 116.163 / 168.592 | 775.515 |
| `sqlite.prompt_acceptance_read` | 2 | 2 | 0 / 0 | 2 | 560.691 / 1895.057 / 1895.057 / 1895.057 | 1895.057 |
| `sqlite.repository_read` | 7222 | 7 | 0 / 0 | 256 | 2.571 / 42.4 / 117.642 / 308.912 | 2180.3 |
| `sqlite.restart_handoff_completion` | 719 | 4 | 0 / 0 | 256 | 6.704 / 112.609 / 230.535 / 314.933 | 1223.389 |
| `sqlite.restart_handoffs_pending` | 318 | 1 | 0 / 0 | 256 | 3.708 / 59.537 / 130.838 / 972.334 | 972.334 |
| `sqlite.restart_handoffs_watchdog` | 317 | 4 | 0 / 0 | 256 | 7.658 / 113.984 / 591.849 / 2180.771 | 2180.771 |
| `sqlite.transcript_append` | 428 | 42 | 0 / 0 | 256 | 53.369 / 651.811 / 1191.846 / 1627.509 | 11478.902 |
| `startup.openapi` | 1 | 1 | 0 / 0 | 1 | 1074.573 / 1074.573 / 1074.573 / 1074.573 | 1074.573 |
| `sync.commit_read` | 60270 | 7 | 0 / 0 | 256 | 0.956 / 2.23 / 5.718 / 8.935 | 948.763 |
| `sync.object_has` | 121636 | 8 | 0 / 0 | 256 | 0.271 / 0.586 / 0.761 / 6.344 | 734.372 |
| `sync.reconcile_head` | 30 | 27 | 0 / 0 | 30 | 1232.382 / 5187.671 / 6114.915 / 6114.915 | 6114.915 |
| `sync.ref_read` | 592 | 2 | 0 / 0 | 256 | 1.283 / 113.865 / 221.562 / 233.937 | 828.124 |
| `workspace.collect_garbage` | 1 | 1 | 0 / 0 | 1 | 8318.475 / 8318.475 / 8318.475 / 8318.475 | 8318.475 |
| `workspace.lease_renew` | 6 | 1 | 0 / 0 | 6 | 189.845 / 566.876 / 566.876 / 566.876 | 566.876 |
| `workspace.project_provision` | 8 | 4 | 1 / 0 | 8 | 4739.283 / 6049.257 / 6049.257 / 6049.257 | 6049.257 |

| All route groups | Count | Response bytes | Window n | p50 / p95 / p99 / max ms |
|---|---:|---:|---:|---:|---:|
| `/api/agent/observability/v1/sessions/{session_id}` | 2 | 8992 | 2 | 98.571 / 179.102 / 179.102 / 179.102 |
| `/api/agent/sessions/{session_id}` | 2 | 468958 | 2 | 91.958 / 103.939 / 103.939 / 103.939 |
| `/api/agent/sessions/{session_id}/issues` | 599 | 19691570 | 256 | 41.185 / 374.961 / 879.287 / 1146.316 |
| `/api/agent/sessions/{session_id}/prompts/{client_prompt_id}` | 2 | 590 | 2 | 571.818 / 2021.526 / 2021.526 / 2021.526 |
| `/api/agent/sessions/{session_id}/restart-handoffs` | 1 | 10199 | 1 | 187.584 / 187.584 / 187.584 / 187.584 |
| `/api/agent/sessions/{session_id}/threads` | 598 | 867618 | 256 | 96.007 / 424.468 / 826.189 / 1280.609 |
| `/api/browser/{operation}` | 22 | 548331 | 22 | 383.906 / 2585.946 / 6580.593 / 6580.593 |
| `/api/cards/{card_id}` | 13 | 795048 | 13 | 35.819 / 515.99 / 515.99 / 515.99 |
| `/api/fleet/dispatch-jobs/{dispatch_id}` | 28 | 5025860 | 28 | 21.251 / 48.989 / 166.832 / 166.832 |
| `/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint` | 10 | 2016 | 10 | 881.049 / 1504.379 / 1504.379 / 1504.379 |
| `/api/fleet/dispatch/{dispatch_id}/complete` | 1 | 3819 | 1 | 356.01 / 356.01 / 356.01 / 356.01 |
| `/api/fleet/instances` | 75 | 205425 | 75 | 17.148 / 198.806 / 344.123 / 344.123 |
| `/api/fleet/overview` | 3 | 3485478 | 3 | 761.179 / 931.925 / 931.925 / 931.925 |
| `/api/fleet/overview/dimension` | 78 | 1712369 | 78 | 459.049 / 6023.499 / 11074.652 / 11074.652 |
| `/api/fleet/overview/local` | 347 | 3218428 | 256 | 77.362 / 557.272 / 989.158 / 1093.584 |
| `/api/health-journal/receipts/{receipt_id}` | 450 | 272539 | 256 | 8.23 / 98.435 / 365.268 / 807.718 |
| `/api/health-journal/reports` | 9 | 2891 | 9 | 59.316 / 222.943 / 222.943 / 222.943 |
| `/api/health-journal/reports/{report_id}` | 1 | 153810 | 1 | 92.173 / 92.173 / 92.173 / 92.173 |
| `/api/instance` | 1 | 560 | 1 | 5.979 / 5.979 / 5.979 / 5.979 |
| `/api/notifications` | 147 | 11025 | 147 | 34.762 / 350.062 / 539.939 / 543.289 |
| `/api/notifications/{notification_id}` | 1 | 5569 | 1 | 1250.733 / 1250.733 / 1250.733 / 1250.733 |
| `/api/operation-recovery/{idempotency_key:path}` | 2 | 2298 | 2 | 50.183 / 62.784 / 62.784 / 62.784 |
| `/api/operations/{idempotency_key:path}` | 4 | 4481 | 4 | 54.603 / 128.578 / 128.578 / 128.578 |
| `/api/owner-ready` | 2 | 142 | 2 | 3.275 / 8.527 / 8.527 / 8.527 |
| `/api/pr-supervisor/capabilities` | 58 | 159036 | 58 | 16.902 / 152.829 / 230.91 / 230.91 |
| `/api/pr-supervisor/effects/dispatch` | 10 | 549 | 10 | 231.25 / 1482.157 / 1482.157 / 1482.157 |
| `/api/pr-supervisor/instances/heartbeat` | 61 | 1037 | 61 | 9.954 / 210.092 / 325.937 / 325.937 |
| `/api/pr-supervisor/pull-requests` | 1 | 2211 | 1 | 2376.129 / 2376.129 / 2376.129 / 2376.129 |
| `/api/pr-supervisor/replicas` | 7178 | 40273947 | 256 | 62.928 / 220.743 / 792.25 / 870.181 |
| `/api/pr-supervisor/retirements` | 2 | 10645 | 2 | 60.969 / 195.166 / 195.166 / 195.166 |
| `/api/pr-supervisor/watches` | 1 | 1996 | 1 | 286.204 / 286.204 / 286.204 / 286.204 |
| `/api/pr-supervisor/watches/{watch_id}` | 5 | 57271 | 5 | 52.69 / 189.082 / 189.082 / 189.082 |
| `/api/pr-supervisor/watches/{watch_id}/lease` | 43 | 10324 | 43 | 176.41 / 734.523 / 1861.563 / 1861.563 |
| `/api/pr-supervisor/watches/{watch_id}/merge-evidence` | 9 | 29290 | 9 | 38.685 / 414.389 / 414.389 / 414.389 |
| `/api/pr-supervisor/watches/{watch_id}/refresh` | 2 | 136 | 2 | 78.143 / 329.11 / 329.11 / 329.11 |
| `/api/ready` | 347 | 46301 | 256 | 3.19 / 18.998 / 43.271 / 114.465 |
| `/api/status` | 351 | 989439 | 256 | 101.734 / 440.765 / 774.41 / 1057.007 |
| `/api/sync/audit` | 5 | 179150 | 5 | 41520.767 / 64253.391 / 64253.391 / 64253.391 |
| `/api/sync/convergence` | 6 | 6942 | 6 | 11.515 / 483.705 / 483.705 / 483.705 |
| `/api/sync/get` | 31 | 246211 | 31 | 5.044 / 139.512 / 319.258 / 319.258 |
| `/api/sync/refs` | 17 | 4403 | 17 | 5.598 / 589.459 / 589.459 / 589.459 |
| `/api/telemetry/live` | 376 | 1859942 | 256 | 3.561 / 8.304 / 93.014 / 135.4 |
| `/api/ui/assets` | 70 | 5530 | 70 | 11.391 / 114.906 / 301.791 / 301.791 |
| `/api/workspaces` | 8 | 18054 | 8 | 25.675 / 88.713 / 88.713 / 88.713 |
| `/fleet` | 3 | 4010377 | 3 | 704.913 / 1134.704 / 1134.704 / 1134.704 |
| `/pull-requests` | 2 | 75626 | 2 | 507.714 / 558.953 / 558.953 / 558.953 |
| `__unmatched__` | 30 | 1126634 | 30 | 42.15 / 165.567 / 166.401 / 166.401 |

## Preserved prior inventory and evidence

# Progress outbox copy repair — 2026-10-21

All **633 findings and 111 concerns remain open**; all 632 prior rows remain unchanged. [Full source evidence](benchmarks/systemic-progress-outbox-projection-20261021.json) confirms the native delivery loop copied an entire retained dispatch for each pending event or heartbeat, although delivery consumes only three routing fields and the payload. With128 retained events,128 plan steps and32 pending payloads, that copied32 full snapshots and4,096 retained event models per read.

The native worker now reads an immutable routing target and one detached deep copy per pending payload from the last committed index publication. Ordering, complete payload fields, origin filtering, heartbeat inclusion, retry headers and durable acknowledgment paths remain. The full-record API still returns independent snapshots, and custom readers keep their original path. A read during an in-flight acknowledgement can return the preceding committed payload and permit an additional idempotent attempt; it does not expose uncommitted mutation state.

**44 guarded disposable contracts pass:11 new and33 existing.** They cover nested payload isolation, immutable targets, legacy aliases, ordering/filter/reload, native/custom reader selection, transient transport failure and retry, local acknowledgement, deferred-reader fencing, and concurrent before/after-commit publication. The initial synthetic fixture incorrectly reused the first dispatch's payload identity and was correctly rejected by migration reconciliation; that failure is retained and the guard unchanged.

Ten alternating pairs per scenario on the shared macbook host show32-pending wall p50/p95/max247.665/291.175/291.175ms →2.209/11.505/11.505ms; one-pending p9513.997→0.763ms. Zero-pending median regresses0.030208→0.031000ms; all raw samples and prior prototype tails remain. At10 samples,p95/p99 equal max. SQLite change and writer counters stay unchanged during reads, but physical disk/WAL bytes and allocation bytes were not measured. These are warm source-call measurements, **not endpoint, mixed-load or deployed acceptance**. Remaining scans, ingestion/acknowledgment writes, CPU/disk tails, startup/drain and every original inventory obligation remain open.

PR557 is integrated as e1091451 from exact b8cd5055 with equal trees, stable-green supervisor and independently validated five green checks, protection, zero required approvals/reviews/threads and clean mergeability. Its mainCI39522197504 still requires a final result. Initial snapshot and history failures remain retained. This newer application repair is not installed or loaded; existing source-review and backup gates do not authorize a floating rollout. No service workload, install/restart, backup/GC action, warning/deadline increase, durable-data drop or competing worker occurred.

## Preserved prior inventory and evidence

# Managed history CI follow-up — 2026-10-21

All **632 findings and 110 concerns remain open**; all 631 prior rows remain unchanged. [Retained CI evidence](benchmarks/systemic-history-ci-followup-20261021.json) records initial PR557 run39520959163 at072c784b: four successful jobs, including the corrected snapshot test, and one failed shard. The managed history test expected Fleet but still observed Workshop after two fixed100ms sleeps. The failed shard reports988 passed and168 passed subtests; this is not a clean suite. No same-head retry was requested.

The test now observes `htmx:afterSettle`, matching URL and live root before sending the next click. A disposable response gate demonstrates that receiving the request is insufficient to finish the first navigation. The2.5s completion bound matches existing history restoration checks; this test does not assert a100ms navigation performance contract. **Eight history contracts pass**, including managed Chromium. Existing privacy, cache, controller, draft, back/forward and quota assertions remain. No application code, production deadline or slow-operation threshold changed. The original CI scheduling cause remains unverified and exact corrected-head CI is required.

This validation repair does not establish deployed performance acceptance. All original latency, startup/drain, durability, peer/log coverage, receipt, rollout and linked backup obligations remain open. Source candidate activation is still a separate scoped decision; no install/restart, service workload, backup or GC action occurred.

## Preserved prior inventory and evidence

# Main CI snapshot fixture follow-up — 2026-10-21

All **631 findings and 109 concerns remain open**; all 631 prior ledger rows remain unchanged. [Exact CI/fixture evidence](benchmarks/systemic-snapshot-ci-followup-20261021.json) retains main run39519841572 at integrated0f05: **four successful jobs and failed test0**, where the existing live-metadata snapshot test exceeded its unchanged200ms deadline. LinuxPython3.14.8 recorded a262ms test-task callback and a stack waiting in `asyncio.to_thread`. The failed shard also reports987 passed, one skipped and533 passed subtests; those are not a clean suite. Passing headCI39519314450 does not erase this failure.

The fixture used an open-ended `MagicMock` request, which invented a manager, dispatch service and card store even though the intended metadata case has no optional dispatch service or linked cards. It now supplies an explicit empty service map, real scalar manager flags and a bounded empty card lookup. It retains actual snapshot/configuration/presentation/cards construction, a cold default executor for each unittest,4096 pending transcript entries, no transcript read/flush, and **both200ms wait and latency assertions**. Timeout notes now retain setup, request wait and loop CPU. No application code or production deadline changed.

Ten alternating pairs on the shared macbook host pass in both versions: request p50/p95/max9.441/12.134/12.134ms baseline versus2.091/3.062/3.062ms explicit context; worker CPU p506.290→0.594ms. Separately instrumented invocations count31→0 child-mock constructions. One initial instrumented baseline callback118ms warning is retained, outside paired summaries and including cold first-use work. At10 samples,p95/p99 both equal the maximum. This confirms avoidable fixture work, **not the sole cause of the CI262ms tail or deployed responsiveness**. Scheduling/CPU/GC attribution remains open.

**61 guarded disposable contracts pass**: the corrected existing case plus60 related session-read regressions. Twenty timed and two instrumented benchmark invocations repeat one case and are not additional unique tests. Workspace construction was denied and stores guarded to disposable roots. No same-head CI retry, warning suppression, timeout increase, install/restart, service workload, backup or GC action occurred. Existing a5dd source review remains preparation evidence only; all original criteria, failed stages, receipt recovery and ordinary independent acceptance remain open.

## Preserved prior inventory and evidence

# Loaded228 numeric runtime inventory — 2026-10-21 00:01 UTC

All **631 findings and 109 concerns remain open**. All 613 prior rows remain unchanged; 12 operations and six route groups are newly represented. The [complete numeric API snapshot](benchmarks/systemic-live-runtime-20261021T0001.json), [source and interpretation summary](benchmarks/systemic-live-runtime-20261021T0001-summary.json), [all 170 operations and phase metrics](benchmarks/systemic-live-runtime-20261021T0001-operations.csv) and [all 75 route groups](benchmarks/systemic-live-runtime-20261021T0001-routes.csv) preserve every returned family and distribution.

Source: macbook0c7d8ecb, bounded supported status/runtime/status reads **2026-10-21T00:01:28.671265Z–00:01:29.357245Z**. Before/after metadata matches loaded2283d907/package41eed1ca/PID26578/startup6c743433. This fences process identity, not complete module composition or repair activation. Individual read times377.980/168.984/137.954ms are not acceptance distributions. No fresh log files, peer files or retention inventory were read. Ordinary foreground/background/test traffic is uncontrolled.

The snapshot contains **87 slow-or-failed operation names**, with cumulative **2,597 slow calls, 932 failed and 883 timed out**; these counters overlap and must not be added as distinct incidents. The 313,695 non-SSE requests total2,093,001,293 response bytes; four SSE requests are excluded. Global request process maximum152,558.416ms is separate from route rolling-window percentiles. Event-loop current600-sample p95 is123ms, while process maximum is1,066,452ms; suspension, scheduling, CPU and synchronous-callback contributions remain unverified. No pooled family quantiles are fabricated. Complete per-name p50/p95/p99/window-max/count/process-max and phase observations remain in the linked raw/CSV evidence, including successful paths and small samples.

PR555 source repair mergedcb81b166 from exact3d8b2b81 after stable-green7 observations and independent five-success checks, zero required approvals/reviews/threads, full pagination, attributed commit, protection and CLEAN state. Source/merge trees match16342b30. HeadCI39518479247 and mainCI39518909020 both have five SUCCESS jobs on attempt1; activation remains separate. Loaded228 excludes555 and the earlier547–553 repairs. Integration alone does not close any finding.

A [static GC publication-path audit](benchmarks/systemic-gc-publication-audit-20261020.json) extends existing reportf320ad17 revision3: append/ref publication, merge/import object preparation and epoch updates have different fences; recovery repair **does** hold the ref locks and recheck its head. Saved execute/resume does not reread current inputs. Request-realm authorization is not passed into the saved-plan executor. These are source observations, not proof of production deletion, data loss, cross-realm exploitation or a complete pin/publication audit. The prior delete-spy admitted a refreshed candidate with zero object bytes deleted. Metadata batching remains unimplemented pending a current-generation/realm/epoch/ack/reachability/pin contract and bounded contention validation.

All original startup/drain/order/admission/lease/receipt/sync/acceptance requirements, log-backed slow names, unexercised branches, historical failed/outlier stages, peer/log-loss/retention gaps and linked backup scope remain open. Existing release authority does not float to later source; no install/restart, service workload, live GC, backup capture/restore/ref change or new worker/verifier occurred. The original ten attachments and cap receipts remain preserved. The supervisor changed the card to Waiting at00:03:21.996509Z; a supported lane-only version-fenced update restored Active at00:03:54.802385Z. Independent requirement17d remains unchanged and unaccepted; no continuous-Active claim.

| Operation family | Names | Submitted | Slow | Failed | Timed out | Highest operation process max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp` | 4 | 15838 | 171 | 0 | 0 | 3045.391 |
| `agent` | 11 | 43916 | 178 | 1 | 1 | 9686.965 |
| `browser` | 3 | 3 | 0 | 0 | 0 | 8.45 |
| `conversation` | 2 | 39147 | 132 | 0 | 1 | 8599.475 |
| `dispatch` | 17 | 277093 | 109 | 0 | 0 | 4011.257 |
| `filesystem` | 2 | 21710 | 0 | 0 | 0 | 1271.886 |
| `fleet` | 14 | 255462 | 232 | 0 | 7 | 10554.292 |
| `goal` | 7 | 1950 | 3 | 0 | 0 | 1731.437 |
| `health` | 1 | 6637 | 105 | 0 | 2 | 3178.927 |
| `http` | 9 | 283791 | 3 | 919 | 871 | 21960.568 |
| `intake` | 1 | 10 | 1 | 0 | 0 | 505.667 |
| `maintenance` | 1 | 2 | 2 | 0 | 0 | 49648.995 |
| `notifications` | 2 | 1986 | 10 | 0 | 0 | 21804.632 |
| `pr_supervisor` | 5 | 250279 | 12 | 0 | 0 | 1256.474 |
| `preferences` | 2 | 78 | 1 | 0 | 0 | 902.26 |
| `progress` | 7 | 21854 | 323 | 0 | 0 | 7000.846 |
| `provider` | 3 | 12 | 10 | 0 | 1 | 4346.483 |
| `reconciliation` | 4 | 122205 | 90 | 0 | 0 | 3226.464 |
| `selection` | 16 | 66 | 11 | 0 | 0 | 1963.241 |
| `sqlite` | 42 | 1377111 | 793 | 1 | 0 | 10604.014 |
| `startup` | 3 | 3 | 1 | 0 | 0 | 1035.269 |
| `sync` | 7 | 24482 | 395 | 10 | 0 | 26670.562 |
| `workspace` | 7 | 44 | 15 | 1 | 0 | 38075.661 |

| Slow or failed operation | Submitted | Slow | Failed / timed out | Window n | p50 / p95 / p99 / max ms | Process max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp.pa_mcp_owner_probe` | 6 | 1 | 0 / 0 | 6 | 24.84 / 1023.609 / 1023.609 / 1023.609 | 1023.609 |
| `acp.wire_append` | 15820 | 170 | 0 / 0 | 256 | 1.14 / 51.11 / 143.384 / 148.736 | 3045.391 |
| `agent.history_payload` | 10 | 3 | 0 / 0 | 10 | 170.91 / 944.413 / 944.413 / 944.413 | 944.413 |
| `agent.project_recovery_availability` | 32842 | 10 | 0 / 0 | 256 | 10.781 / 456.096 / 769.527 / 3241.656 | 3241.656 |
| `agent.prompt_compose` | 22 | 2 | 0 / 0 | 22 | 39.632 / 669.034 / 1395.859 / 1395.859 | 1395.859 |
| `agent.recovery_sessions` | 11018 | 161 | 0 / 1 | 256 | 232.333 / 1212.898 / 3963.203 / 9686.965 | 9686.965 |
| `agent.session_snapshot` | 3 | 1 | 0 / 0 | 3 | 323.33 / 1154.485 / 1154.485 / 1154.485 | 1154.485 |
| `agent.snapshot_payload` | 2 | 0 | 1 / 0 | 2 | 7.043 / 86.536 / 86.536 / 86.536 | 86.536 |
| `agent.startup_state_read` | 1 | 1 | 0 / 0 | 1 | 1417.344 / 1417.344 / 1417.344 / 1417.344 | 1417.344 |
| `conversation.issues` | 19573 | 79 | 0 / 0 | 256 | 32.005 / 437.881 / 3654.739 / 8599.475 | 8599.475 |
| `conversation.session` | 19574 | 53 | 0 / 1 | 256 | 31.582 / 450.738 / 757.32 / 880.427 | 3260.964 |
| `dispatch.completion_pending_read` | 11230 | 9 | 0 / 0 | 256 | 11.177 / 341.362 / 1134.226 / 4011.257 | 4011.257 |
| `dispatch.followup_ack` | 16 | 1 | 0 / 0 | 16 | 91.663 / 1231.031 / 1231.031 / 1231.031 | 1231.031 |
| `dispatch.followup_bind` | 8 | 1 | 0 / 0 | 8 | 230.46 / 828.088 / 828.088 / 828.088 | 828.088 |
| `dispatch.followup_pending_read` | 11230 | 8 | 0 / 0 | 256 | 8.57 / 348.601 / 792.06 / 1446.152 | 1446.152 |
| `dispatch.followup_record_write` | 24 | 7 | 0 / 0 | 24 | 375.045 / 2181.007 / 2956.119 / 2956.119 | 2956.119 |
| `dispatch.followup_started` | 8 | 1 | 0 / 0 | 8 | 94.224 / 1142.681 / 1142.681 / 1142.681 | 1142.681 |
| `dispatch.record_read` | 254534 | 79 | 0 / 0 | 256 | 29.757 / 109.919 / 688.823 / 1033.468 | 2818.841 |
| `dispatch.record_write` | 12 | 1 | 0 / 0 | 12 | 51.954 / 1407.287 / 1407.287 / 1407.287 | 1407.287 |
| `dispatch.recovery_source_read` | 1 | 1 | 0 / 0 | 1 | 758.315 / 758.315 / 758.315 / 758.315 | 758.315 |
| `dispatch.session_link` | 1 | 1 | 0 / 0 | 1 | 575.455 / 575.455 / 575.455 / 575.455 | 575.455 |
| `fleet.overview.mcp_bootstrap` | 3 | 3 | 0 / 0 | 3 | 6907.177 / 10554.292 / 10554.292 / 10554.292 | 10554.292 |
| `fleet.overview.reachability` | 3710 | 7 | 0 / 2 | 256 | 10.981 / 208.616 / 588.816 / 1209.76 | 1209.76 |
| `fleet.overview.status` | 3707 | 9 | 0 / 0 | 256 | 10.941 / 237.296 / 647.514 / 1187.899 | 1187.899 |
| `fleet.overview.sync` | 7844 | 44 | 0 / 1 | 256 | 83.639 / 1006.68 / 1885.085 / 5276.08 | 5276.08 |
| `fleet.overview.update` | 1 | 1 | 0 / 0 | 1 | 1621.897 / 1621.897 / 1621.897 / 1621.897 | 1621.897 |
| `fleet.overview_cache_read` | 158981 | 77 | 0 / 0 | 256 | 9.425 / 214.008 / 2152.219 / 2235.554 | 2695.608 |
| `fleet.overview_cache_write` | 48661 | 71 | 0 / 0 | 256 | 31.133 / 200.001 / 341.094 / 810.09 | 7708.191 |
| `fleet.overview_response_json` | 32519 | 19 | 0 / 3 | 256 | 15.32 / 148.445 / 324.617 / 1005.506 | 2305.075 |
| `fleet.response_json` | 23 | 1 | 0 / 1 | 22 | 1.64 / 61.672 / 781.51 / 781.51 | 781.51 |
| `goal.dispatch_lifecycle_index` | 1933 | 3 | 0 / 0 | 256 | 0.544 / 86.859 / 750.984 / 1731.437 | 1731.437 |
| `health.canonical_obligation` | 6637 | 105 | 0 / 2 | 256 | 11.41 / 280.933 / 846.198 / 3178.927 | 3178.927 |
| `http.dispatch_list` | 2 | 1 | 0 / 0 | 2 | 411.303 / 660.897 / 660.897 / 660.897 | 660.897 |
| `http.fleet_overview` | 33383 | 0 | 864 / 864 | 256 | 224.609 / 1156.092 / 1926.217 / 2342.946 | 6175.471 |
| `http.pr_supervisor_peer` | 250228 | 0 | 55 / 7 | 256 | 43.56 / 217.002 / 665.428 / 859.515 | 15002.515 |
| `http.session_list` | 8 | 2 | 0 / 0 | 8 | 67.962 / 507.011 / 507.011 / 507.011 | 507.011 |
| `intake.web_prompt` | 10 | 1 | 0 / 0 | 10 | 172.337 / 505.667 / 505.667 / 505.667 | 505.667 |
| `maintenance.run` | 2 | 2 | 0 / 0 | 2 | 37306.812 / 49648.995 / 49648.995 / 49648.995 | 49648.995 |
| `notifications.list_page` | 993 | 2 | 0 / 0 | 256 | 4.347 / 162.317 / 434.251 / 1748.161 | 1748.161 |
| `notifications.present_page` | 993 | 8 | 0 / 0 | 256 | 22.9 / 176.278 / 1499.601 / 21804.632 | 21804.632 |
| `pr_supervisor.response_json` | 250133 | 12 | 0 / 0 | 256 | 0.864 / 19.6 / 42.143 / 115.827 | 1256.474 |
| `preferences.agent_auto_approve_read` | 77 | 1 | 0 / 0 | 77 | 18.66 / 279.74 / 902.26 / 902.26 | 902.26 |
| `progress.checkpoint_write` | 234 | 81 | 0 / 0 | 234 | 362.358 / 1752.288 / 3531.182 / 4551.745 | 4551.745 |
| `progress.dispatch_read` | 1739 | 57 | 0 / 0 | 256 | 88.776 / 340.543 / 718.801 / 1307.134 | 3320.469 |
| `progress.final_report_build` | 38 | 1 | 0 / 0 | 38 | 17.842 / 196.013 / 742.171 / 742.171 | 742.171 |
| `progress.final_report_write` | 38 | 9 | 0 / 0 | 38 | 356.757 / 1484.738 / 3263.063 / 3263.063 | 3263.063 |
| `progress.heartbeat_write` | 276 | 55 | 0 / 0 | 256 | 225.137 / 1478.977 / 4100.944 / 4686.961 | 4686.961 |
| `progress.pending_read` | 19019 | 29 | 0 / 0 | 256 | 18.884 / 330.993 / 1065.947 / 2187.331 | 2980.594 |
| `progress.sequence_allocate` | 510 | 91 | 0 / 0 | 256 | 227.115 / 1022.878 / 1438.985 / 2314.145 | 7000.846 |
| `provider.status.codex` | 4 | 4 | 0 / 0 | 4 | 1561.243 / 2312.829 / 2312.829 / 2312.829 | 2312.829 |
| `provider.status.cursor` | 4 | 4 | 0 / 1 | 4 | 3074.534 / 4346.483 / 4346.483 / 4346.483 | 4346.483 |
| `provider.status.openinterpreter` | 4 | 2 | 0 / 0 | 4 | 476.815 / 649.279 / 649.279 / 649.279 | 649.279 |
| `reconciliation.completion_evidence_read` | 33318 | 28 | 0 / 0 | 256 | 10.382 / 260.896 / 850.851 / 2386.406 | 3226.464 |
| `reconciliation.completion_queue` | 19 | 6 | 0 / 0 | 19 | 151.3 / 2190.973 / 2190.973 / 2190.973 | 2190.973 |
| `reconciliation.dispatch_read` | 55550 | 39 | 0 / 0 | 256 | 4.257 / 134.827 / 565.264 / 2327.512 | 2327.512 |
| `reconciliation.session_read` | 33318 | 17 | 0 / 0 | 256 | 5.721 / 181.718 / 494.84 / 1291.418 | 1680.507 |
| `selection.boundary_transcript` | 1 | 1 | 0 / 0 | 1 | 502.854 / 502.854 / 502.854 / 502.854 | 502.854 |
| `selection.catalog_enrichment` | 4 | 1 | 0 / 0 | 4 | 10.392 / 1090.049 / 1090.049 / 1090.049 | 1090.049 |
| `selection.prompt_identity` | 22 | 6 | 0 / 0 | 22 | 152.736 / 1168.222 / 1963.241 / 1963.241 | 1963.241 |
| `selection.prompt_outcome` | 20 | 3 | 0 / 0 | 20 | 86.801 / 570.581 / 895.93 / 895.93 | 895.93 |
| `sqlite.agent_runtime_checkpoint` | 78 | 15 | 0 / 0 | 78 | 80.597 / 1185.022 / 3210.373 / 3210.373 | 3210.373 |
| `sqlite.agent_session_save` | 483 | 45 | 0 / 0 | 256 | 117.076 / 1004.561 / 1719.616 / 3009.9 | 3009.9 |
| `sqlite.card_read` | 379681 | 62 | 0 / 0 | 256 | 1.832 / 17.943 / 73.876 / 114.388 | 3457.09 |
| `sqlite.pr_supervisor_capability_write` | 969 | 2 | 0 / 0 | 256 | 3.984 / 97.631 / 313.26 / 533.296 | 533.296 |
| `sqlite.pr_supervisor_completion_due_read` | 21664 | 67 | 0 / 0 | 256 | 109.372 / 757.579 / 3722.495 / 7107.101 | 8467.544 |
| `sqlite.pr_supervisor_due_read` | 21642 | 3 | 0 / 0 | 256 | 5.033 / 108.1 / 372.484 / 2152.407 | 2152.407 |
| `sqlite.pr_supervisor_effect_prepare` | 4 | 0 | 1 / 0 | 4 | 18.713 / 81.047 / 81.047 / 81.047 | 81.047 |
| `sqlite.pr_supervisor_event_write` | 125113 | 32 | 0 / 0 | 256 | 4.472 / 79.365 / 532.395 / 643.11 | 3246.279 |
| `sqlite.pr_supervisor_metric` | 100 | 1 | 0 / 0 | 100 | 3.899 / 118.962 / 498.378 / 1078.228 | 1078.228 |
| `sqlite.pr_supervisor_watch_read` | 125214 | 52 | 0 / 0 | 256 | 7.298 / 180.43 / 445.294 / 1197.502 | 6678.958 |
| `sqlite.pr_supervisor_watch_write` | 125113 | 61 | 0 / 0 | 256 | 11.135 / 150.821 / 476.97 / 2384.644 | 4671.162 |
| `sqlite.project_read` | 254558 | 40 | 0 / 0 | 256 | 1.041 / 49.371 / 244.396 / 376.741 | 2257.589 |
| `sqlite.prompt_acceptance_read` | 39 | 10 | 0 / 0 | 39 | 144.622 / 2539.368 / 5428.536 / 5428.536 | 5428.536 |
| `sqlite.repository_read` | 254524 | 50 | 0 / 0 | 256 | 1.662 / 65.961 / 205.445 / 488.608 | 2793.138 |
| `sqlite.restart_handoff_completion` | 22198 | 28 | 0 / 0 | 256 | 23.15 / 359.891 / 738.124 / 744.538 | 2974.555 |
| `sqlite.restart_handoff_session` | 21887 | 12 | 0 / 0 | 256 | 6.919 / 265.169 / 916.041 / 3293.149 | 3293.149 |
| `sqlite.restart_handoffs_pending` | 11019 | 10 | 0 / 0 | 256 | 6.775 / 311.281 / 906.467 / 1184.502 | 1214.0 |
| `sqlite.restart_handoffs_watchdog` | 11018 | 23 | 0 / 0 | 256 | 16.116 / 634.001 / 1541.961 / 2807.831 | 2807.831 |
| `sqlite.transcript_append` | 1632 | 279 | 0 / 0 | 256 | 66.669 / 1106.301 / 5126.402 / 5533.249 | 10604.014 |
| `sqlite.transcript_read` | 14 | 1 | 0 / 0 | 14 | 52.298 / 872.364 / 872.364 / 872.364 | 872.364 |
| `startup.openapi` | 1 | 1 | 0 / 0 | 1 | 1035.269 / 1035.269 / 1035.269 / 1035.269 | 1035.269 |
| `sync.peer_http` | 579 | 0 | 10 / 0 | 256 | 26.582 / 1028.895 / 3024.395 / 3032.785 | 3032.785 |
| `sync.reconcile_head` | 569 | 379 | 0 / 0 | 256 | 553.111 / 1189.076 / 9504.989 / 26670.562 | 26670.562 |
| `sync.ref_read` | 22194 | 16 | 0 / 0 | 256 | 2.746 / 280.961 / 667.776 / 1113.393 | 3334.371 |
| `workspace.collect_garbage` | 3 | 3 | 0 / 0 | 3 | 10792.93 / 38075.661 / 38075.661 / 38075.661 | 38075.661 |
| `workspace.lease_renew` | 22 | 10 | 0 / 0 | 22 | 485.26 / 4047.011 / 6500.912 / 6500.912 | 6500.912 |
| `workspace.project_provision` | 10 | 2 | 1 / 0 | 10 | 1856.624 / 6709.812 / 6709.812 / 6709.812 | 6709.812 |

| All route groups | Count | Response bytes | Window n | p50 / p95 / p99 / max ms |
|---|---:|---:|---:|---:|
| `/` | 1 | 2076 | 1 | 21.08 / 21.08 / 21.08 / 21.08 |
| `/api/agent/history/{session_id}` | 10 | 2614572 | 10 | 315.246 / 1176.822 / 1176.822 / 1176.822 |
| `/api/agent/observability/v1/sessions` | 2 | 2107314 | 2 | 13766.826 / 22771.776 / 22771.776 / 22771.776 |
| `/api/agent/observability/v1/sessions/{session_id}` | 42 | 207651 | 42 | 103.816 / 559.819 / 1345.638 / 1345.638 |
| `/api/agent/prompt-capabilities` | 10 | 390 | 10 | 36.485 / 1304.306 / 1304.306 / 1304.306 |
| `/api/agent/sessions` | 9 | 361565 | 9 | 91.953 / 21199.107 / 21199.107 / 21199.107 |
| `/api/agent/sessions/{session_id}` | 2 | 195221 | 2 | 10.559 / 118.287 / 118.287 / 118.287 |
| `/api/agent/sessions/{session_id}/close` | 1 | 70 | 1 | 4951.379 / 4951.379 / 4951.379 / 4951.379 |
| `/api/agent/sessions/{session_id}/commands` | 1 | 69015 | 1 | 59.313 / 59.313 / 59.313 / 59.313 |
| `/api/agent/sessions/{session_id}/dispatch-prompts` | 9 | 221785 | 9 | 1747.291 / 13381.535 / 13381.535 / 13381.535 |
| `/api/agent/sessions/{session_id}/execution-catalog` | 2 | 1211 | 2 | 17.086 / 408.908 / 408.908 / 408.908 |
| `/api/agent/sessions/{session_id}/issues` | 9794 | 321965930 | 256 | 75.923 / 1339.063 / 2414.93 / 4220.515 |
| `/api/agent/sessions/{session_id}/prompt` | 1 | 392 | 1 | 5892.206 / 5892.206 / 5892.206 / 5892.206 |
| `/api/agent/sessions/{session_id}/prompts/{client_prompt_id}` | 1 | 295 | 1 | 181.139 / 181.139 / 181.139 / 181.139 |
| `/api/agent/sessions/{session_id}/queue/resume` | 1 | 22 | 1 | 111.817 / 111.817 / 111.817 / 111.817 |
| `/api/agent/sessions/{session_id}/recover` | 1 | 1595153 | 1 | 7593.856 / 7593.856 / 7593.856 / 7593.856 |
| `/api/agent/sessions/{session_id}/restart-handoffs` | 13 | 1970530 | 13 | 245.152 / 1788.634 / 1788.634 / 1788.634 |
| `/api/agent/sessions/{session_id}/threads` | 9780 | 14164470 | 256 | 174.991 / 1583.086 / 3756.271 / 9542.183 |
| `/api/backups/status` | 2 | 152742 | 2 | 64.578 / 76.436 / 76.436 / 76.436 |
| `/api/browser/{operation}` | 28 | 478027 | 28 | 355.789 / 5964.291 / 16002.304 / 16002.304 |
| `/api/cards/summaries` | 1 | 8519 | 1 | 31.287 / 31.287 / 31.287 / 31.287 |
| `/api/cards/{card_id}` | 21 | 1173558 | 21 | 61.576 / 1796.177 / 2976.12 / 2976.12 |
| `/api/configuration` | 2 | 339898 | 2 | 49.748 / 67.48 / 67.48 / 67.48 |
| `/api/execution/catalog` | 1 | 51947 | 1 | 44.138 / 44.138 / 44.138 / 44.138 |
| `/api/execution/defaults` | 1 | 6308 | 1 | 17.25 / 17.25 / 17.25 / 17.25 |
| `/api/fleet/dispatch-jobs` | 2 | 3970505 | 2 | 415.629 / 673.276 / 673.276 / 673.276 |
| `/api/fleet/dispatch-jobs/{dispatch_id}` | 971 | 172514376 | 256 | 14.206 / 69.348 / 306.679 / 1523.454 |
| `/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint` | 34 | 6935 | 34 | 1337.165 / 4622.153 / 27976.29 / 27976.29 |
| `/api/fleet/dispatch-jobs/{dispatch_id}/prompt` | 5 | 1496 | 5 | 9178.407 / 15713.879 / 15713.879 / 15713.879 |
| `/api/fleet/dispatch/materialize` | 1 | 1458 | 1 | 78.362 / 78.362 / 78.362 / 78.362 |
| `/api/fleet/dispatch/{dispatch_id}/turn-end` | 12 | 40986 | 12 | 2350.072 / 5674.995 / 5674.995 / 5674.995 |
| `/api/fleet/instances` | 25 | 68475 | 25 | 12.638 / 1823.333 / 2548.175 / 2548.175 |
| `/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}` | 1 | 1726856 | 1 | 143.222 / 143.222 / 143.222 / 143.222 |
| `/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/{operation}` | 4 | 1364 | 4 | 2106.243 / 3712.524 / 3712.524 / 3712.524 |
| `/api/fleet/instances/{instance_id}/agent/start` | 1 | 151465 | 1 | 8328.297 / 8328.297 / 8328.297 / 8328.297 |
| `/api/fleet/overview` | 1 | 1152888 | 1 | 605.643 / 605.643 / 605.643 / 605.643 |
| `/api/fleet/overview/dimension` | 36 | 985788 | 36 | 482.888 / 7846.268 / 10613.039 / 10613.039 |
| `/api/fleet/overview/local` | 10628 | 95056221 | 256 | 327.171 / 5742.434 / 11443.606 / 15708.019 |
| `/api/fleet/placement/preview` | 2 | 148674 | 2 | 3910.613 / 8424.379 / 8424.379 / 8424.379 |
| `/api/fleet/session-route/{session_id}` | 1 | 1578 | 1 | 28.998 / 28.998 / 28.998 / 28.998 |
| `/api/health` | 11 | 165 | 11 | 3.558 / 24.026 / 24.026 / 24.026 |
| `/api/health-journal/receipts/{receipt_id}` | 266 | 160929 | 256 | 8.745 / 403.969 / 909.758 / 1998.257 |
| `/api/health-journal/reports` | 14 | 4269 | 14 | 70.518 / 3071.905 / 3071.905 / 3071.905 |
| `/api/health-journal/reports/{report_id}` | 1 | 153943 | 1 | 68.313 / 68.313 / 68.313 / 68.313 |
| `/api/instance` | 3 | 1680 | 3 | 48.991 / 1757.805 / 1757.805 / 1757.805 |
| `/api/items/{item_id}` | 8 | 361472 | 8 | 68.23 / 176.585 / 176.585 / 176.585 |
| `/api/notifications` | 994 | 8967054 | 256 | 53.04 / 1504.441 / 8525.744 / 23784.549 |
| `/api/notifications/{notification_id}` | 7 | 40875 | 7 | 985.319 / 9832.463 / 9832.463 / 9832.463 |
| `/api/notifications/{notification_id}/respond` | 2 | 7732 | 2 | 1503.577 / 11516.637 / 11516.637 / 11516.637 |
| `/api/operation-recovery/{idempotency_key:path}` | 5 | 2805 | 5 | 37.307 / 915.765 / 915.765 / 915.765 |
| `/api/operations/{idempotency_key:path}` | 19 | 12414 | 19 | 121.512 / 1815.568 / 1815.568 / 1815.568 |
| `/api/owner-ready` | 7 | 497 | 7 | 2.792 / 4.606 / 4.606 / 4.606 |
| `/api/pr-supervisor/capabilities` | 401 | 1099542 | 256 | 5.906 / 105.389 / 463.716 / 873.378 |
| `/api/pr-supervisor/effects/dispatch` | 4 | 201 | 4 | 71.569 / 831.443 / 831.443 / 831.443 |
| `/api/pr-supervisor/instances/heartbeat` | 1920 | 32640 | 256 | 7.703 / 410.939 / 1639.216 / 3008.208 |
| `/api/pr-supervisor/pull-requests` | 1 | 2147 | 1 | 2508.787 / 2508.787 / 2508.787 / 2508.787 |
| `/api/pr-supervisor/replicas` | 254528 | 1423908615 | 256 | 57.437 / 604.877 / 2578.844 / 3800.661 |
| `/api/pr-supervisor/watches` | 1 | 1996 | 1 | 1834.289 / 1834.289 / 1834.289 / 1834.289 |
| `/api/pr-supervisor/watches/{watch_id}` | 8 | 198404 | 8 | 102.638 / 323.671 / 323.671 / 323.671 |
| `/api/pr-supervisor/watches/{watch_id}/lease` | 28 | 6891 | 28 | 273.469 / 1035.877 / 1099.755 / 1099.755 |
| `/api/pr-supervisor/watches/{watch_id}/merge-evidence` | 6 | 19385 | 6 | 16.711 / 487.19 / 487.19 / 487.19 |
| `/api/ready` | 10628 | 1434220 | 256 | 6.034 / 70.473 / 244.722 / 376.894 |
| `/api/status` | 10643 | 27419643 | 256 | 322.48 / 2664.571 / 5185.137 / 7027.793 |
| `/api/sync/audit` | 2 | 71660 | 2 | 138233.502 / 152558.416 / 152558.416 / 152558.416 |
| `/api/sync/convergence` | 2 | 2312 | 2 | 3.897 / 73.773 / 73.773 / 73.773 |
| `/api/sync/get` | 89 | 1776329 | 89 | 7.652 / 582.159 / 2927.612 / 2927.612 |
| `/api/sync/refs` | 570 | 147630 | 256 | 3.326 / 34.02 / 281.54 / 699.138 |
| `/api/telemetry/live` | 1047 | 978101 | 256 | 3.446 / 18.915 / 55.109 / 76.809 |
| `/api/ui/assets` | 963 | 76077 | 256 | 6.187 / 81.199 / 541.666 / 1365.776 |
| `/api/workspaces` | 18 | 34018 | 18 | 75.722 / 914.595 / 914.595 / 914.595 |
| `/favicon.ico` | 1 | 593 | 1 | 85.764 / 85.764 / 85.764 / 85.764 |
| `/fleet` | 1 | 1329596 | 1 | 674.093 / 674.093 / 674.093 / 674.093 |
| `/partials/home/sections` | 1 | 17022 | 1 | 4761.282 / 4761.282 / 4761.282 / 4761.282 |
| `/{page_path:path}` | 1 | 92162 | 1 | 142.734 / 142.734 / 142.734 / 142.734 |
| `__unmatched__` | 35 | 1120548 | 35 | 25.352 / 44.862 / 48.655 / 48.655 |

## Preserved prior inventory and evidence

# Reduce progress sequence allocation copying — 2026-10-20

All **613 findings and 109 concerns remain open**; all 612 prior rows remain unchanged. [Numeric evidence](benchmarks/systemic-progress-sequence-copy-20261020.json) identifies a source cost in `progress.sequence_allocate`: changing the next sequence and update timestamp copied the entire dispatch, followed by another deep copy when publishing the committed record. The provisional record now uses a shallow copy for those two scalar assignments. The existing transaction, acknowledgement, failure handling and detached post-commit publication remain intact.

**17 guarded disposable contracts pass**: five new commit-boundary/reload, nested isolation, index, concurrent uniqueness and missing-record cases, plus 12 existing progress and rollback contracts. The initial broad run was interrupted after about 60 seconds to bound work. It included two new tests with inconsistent synthetic compacted coverage; the fixture was corrected to provide its accepted ranges. That failed/incomplete stage is retained and is not a passing suite. The final 17 cases overlap it.

Twenty alternating warm pairs per case exercised real disposable SQLite commits. Percentiles use nearest rank; raw timings are retained. Core encoded payload sizes and retained events match. No physical write or HTTP byte reduction was measured.

| Retained progress events | Wall p50 ms, baseline → source | Wall p95 ms | Wall p99/max ms | Thread CPU p50 ms |
|---:|---:|---:|---:|---:|
| 0 | 2.769 → 1.539 | 5.355 → 3.975 | 5.602 → 6.447 | 1.366 → 1.132 |
| 1,000 | 260.344 → 128.845 | 617.785 → 275.200 | 659.707 → 333.318 | 122.582 → 62.436 |

The empty-history maximum regressed in this small uncontrolled sample. Payload serialization, the commit, the remaining full-history publication copy and index refresh still scale with retained state. This repair does not establish the cause of historical live stalls or whole-path acceptance. The current loaded build is 228; this source change is neither installed nor loaded. The existing release owner retains activation coordination and its separate immutable-target authority. Backup integrity and ordinary independent acceptance remain separate gates. No service workload, install/restart, backup action, warning change or timeout change occurred.

## Preserved prior inventory and evidence

# Bound unselected session JSON transfer — 2026-10-19

All **612 findings and108 concerns remain open**; all611 prior rows are preserved unchanged. [Complete numeric evidence](benchmarks/systemic-session-selected-transfer-20261019.json) confirms a remaining layer beneath prior selected-model hydration: the existing list snapshot still fetched and retained every matching row’s opaque configuration/metrics JSON. A native metadata selector now fetches complete rows only for selected persisted sessions, under the **same SQLite read snapshot** as their selection metadata. The original all-row helper, activity/custom-adapter paths and complete selected payloads remain available.

Sixty guarded disposable contracts pass, including23 new contracts for filter/decoder parity, tied/pinned order, selected-ID replacement, live overrides, exact native/custom snapshot/read/decoder/list behavior, concurrent writer consistency, outer transaction rollback and error cleanup. Earlier46/48/57-test stages overlap these60 cases. No production Store/EventLog/workspace or service workload was used.

Twenty alternating exact-baseline/source pairs per scenario use64 retained sessions with8KiB configuration and8KiB metrics each. All metadata and complete selected models match. Returned-text accounting occurs outside query timing and is **not HTTP bytes or physical I/O**:

| Persisted rows selected | Returned text bytes, baseline → source | Query p95 ms, baseline → source |
|---:|---:|---:|
|0|1,059,520 → 3,264|2.874 → 0.689|
|2|1,059,520 → 36,374|3.750 → 1.309|
|32|1,059,520 → 533,024|4.347 → 3.870|
|64|1,059,520 → 1,062,784|4.133 → 4.637|

The **all-selected regression is retained**, not hidden: nonempty native selection adds a second SELECT plus SAVEPOINT/RELEASE. Retained complete rows change from64 to the selected count, while metadata still covers all64. Warm host timings are uncontrolled; realistic WAL/checkpoint contention while holding the metadata read snapshot, larger counts, selected-heavy payloads and full endpoint/mixed workloads remain unverified. Selected HTTP response bytes are unchanged. No historical sole-cause, threshold adjustment, install/restart or deployed acceptance is claimed. Existing backup stable-build/release/receipt gates and ordinary independent17d acceptance remain in force; DB89957’s accepted original scope stays closed.

## Preserved prior inventory and evidence

# Corrected health diagnostic test observation — 2026-10-19

All **611 findings and107 concerns remain open**; all610 prior rows and the initial numeric artifact remain unchanged. [CI follow-up evidence](benchmarks/systemic-health-canonical-ci-20261019.json) retains failed run39339742396 at initial1538cea9: two failed shards, two successful shards and successful smoke. One new test propagated the correct exception and recorded leaf error1, then incorrectly assumed the aggregate completion callback had already run. The **test now waits for actual completion** before inspecting aggregate counters; product instrumentation/runtime behavior is unchanged.

The other failure repeats the unchanged200ms snapshot responsiveness deadline, with a238ms task callback recorded on LinuxPython3.14.8. CPU/scheduling attribution remains unknown; earlier PR550 evidence stays separate. No deadline or warning threshold changed. **19 guarded disposable tests pass** after the correction, including the18 earlier cases and the unchanged snapshot case; these overlap, not37 unique tests. Local isolated success does not erase the CI failure or prove CI stability. New exact-head CI and fresh stable supervisor/independent integration gates remain required. No install/restart/backup action or acceptance is claimed.

## Preserved prior inventory and evidence

# Health canonical attribution — 2026-10-19

All **610 findings and106 concerns remain open**; all609 prior rows are preserved unchanged. [Numeric source evidence](benchmarks/systemic-health-canonical-diagnostics-20261019.json) records a bounded instrumentation change for `health.canonical_obligation`. The prior22:27 runtime snapshot retained2493 submissions,41 slow calls and2 caller timeouts, with process maximum2692.116ms; its256-sample window p50/p95/p99/max was8.737/165.221/1381.544/2243.066ms. These are overlapping process counters and a bounded window, not leaf-specific causes or fresh logs.

The aggregate operation, warning policy, five-second caller deadline and worker occupancy remain intact. Nine fixed source categories add callable wall/thread CPU, callable failure and SQLite call/row/aggregate-byte measurements under existing `work.metrics`: at most54 names with256 numeric samples each. No callable names, SQL, IDs, exception text, arguments or results become metric labels. Worker completion is recorded even after a caller cancels or times out. Callable spans exclude queue/completion delivery, cleanup after return and diagnostic recording; the existing aggregate retains broader timing. Observed SQL bytes are not physical disk I/O. Verification-adapter calls remain explicitly unclassified; runtime-free fallback has no leaf metrics.

**18 guarded disposable contracts pass**, including7 new contracts for result/error/SQL parity, finite labels/sample bounds, fallback, cancellation and late deadline completion, plus existing health lifecycle/acceptance/verification ownership paths. The deadline test shortens only its test runtime to10ms while asserting the source still requests5s; production thresholds are unchanged. No production Store/EventLog/workspace or backup action occurred.

Thirty alternating exact-baseline/source pairs of20 no-op calls measure **diagnostic overhead**, not a repair: batch p50/p95/max4.391/5.421/7.787ms baseline versus3.854/6.448/6.604ms source on an uncontrolled host. All1240 aggregate calls completed with zero slow/failure/timeout observations in this small no-store fixture. This does not establish live health responsiveness, cold start, transcript drain or mixed-load acceptance. After separate exact-build activation authorization and backup stability gates, collect leaf windows and investigate actual expensive branches. No install/restart or acceptance is claimed; the existing backup/release owners and independent17d requirement remain unchanged.

## Preserved prior inventory and evidence

# Existing saved GC plan freshness risk — 2026-10-19

All **609 findings and105 concerns remain open**; all608 prior rows are preserved unchanged. A [separate safe observation](benchmarks/systemic-gc-saved-plan-observation-20261019.json) records an existing execution risk discovered while assessing metadata batching. Saved-plan execution invokes delete callbacks from its persisted candidate list without rereading current catalog age/realm, reachability or pins. The route passes the approved plan directly to that existing executor.

In a disposable1-row catalog/journal fixture with a fixed acknowledged epoch, a30-day-old candidate was planned, then its metadata was refreshed into the14-day safety window. Execution still admitted the callback. The callback was **only a list append: zero object bytes deleted**, no object files/live Store/EventLog/refs/backup were touched. This confirms stale-plan callback admission in the fixture, not production data loss or actual active GC usage. Reportf320ad17rev1/deliverypending preserves exact source hash and observation. The existing systemic session owns a separate audit/repair of execute/resume freshness and canonical publication fences before any metadata optimization; no new worker or production GC is authorized by the report.

The bounded catalog reader change below leaves GC selection/execution untouched. Its18tests and small paired timings remain source evidence only. The metadata N+1 prototype remains **unimplemented**; a faster selection loop alone does not establish deletion safety. Existing backup owner review/stable-build window, release ownership and independent acceptance stay unchanged.

## Preserved prior inventory and evidence

# Bounded catalog hash reads and remaining GC metadata work — 2026-10-19

All **608 findings and104 concerns remain open**. Every606 prior row is preserved unchanged. The [complete source/prototype/test/failure evidence](benchmarks/systemic-catalog-hash-reads-20261019.json) separates a small implemented reader repair from an unimplemented GC metadata prototype.

`ObjectCatalog.iter_hashes` now fetches128rows at a time using the **same ordered SELECT, LIMIT semantics and complete returned hash list**. Its sole current source caller is `GcPlanner.plan`; this is not a repair to `sync.object_collect`, transcript GC or backup algorithms. No selection, deletion, journal, acknowledgment, pin, safety-window or durability behavior changes.

Twenty alternating exact-baseline/current-source pairs over a disposable1000-row derived index preserve all1000 ordered hashes. Observed SQLite execute/fetch calls fall1004→12, with the same one SELECT; these are not distinct SQL statements or physical disk operations. Wall p50/p95/p99/max **5.031/5.853/6.199/6.199→3.587/4.756/5.074/5.074ms**; threadCPU p95 **5.468→3.882ms**. The earlier prototype p956.267→4.846ms remains retained. Small warm-index/uncontrolled-host samples are not representative endpoint/background or deployed acceptance. The [public reproducer](../scripts/benchmark_catalog_hash_reads.py) records all raw pairs and exact source hashes.

18isolated contracts pass:13new ordered-limit/empty/bounded-fetch/read-snapshot/fresh-read/error cases and5existing catalog/GC acknowledgment, peer pin, crash-resume and corrupt-object cases. Catalog/object-store constructors were guarded to disposable roots; workspace construction denied. No production GC, deletion, backup capture/verify, heavy mixed fixture, install or restart was invoked.

The separate dry-run GC metadata loop still performs a lookup per eligible hash. Three fixed-epoch1000-row prototype pairs preserve the entire468candidate/168pin plan and1726returned rows, with2456→1016 observed execute/fetch calls; wall baseline18.908–20.432ms versus prototype9.645–11.603ms. **This prototype is not implemented.** Journal persistence was bypassed in both methods; concurrency/mtime/realm/error and actual route safety/latency remain unverified. No historic warning is attributed to this loop by assumption.

PR549 and550 are integrated at5b60ca8b/1a4d01c4 with equal tested/merge trees and latest head/main CIgreen. PR550 mainCI39336082802 originally failed an unchanged200ms metadata responsiveness contract with a223ms parent callback. An isolated unchanged case passed, then ONE requested failed-shard retry passed. Both attempts and unresolved attribution remain in reportd2781497rev2; no deadline was raised. The existing release-owner handoff0f33 failed quiescing while root and backup turns were active, remains undelivered, and was not retried/replaced by root. Last actual22:37status stillloaded53b/package6cc/PID69337/startup69233661 versus installed228; current source is not deployed. A late Waiting update after the first ActiveCAS was corrected by supported lane-onlyCAS and independently readActive22:44:12; no actualDone transition or acceptance is claimed.

The backup owner’s current stable-build review window is preserved. Existing partial2842-byte historical gap, all8original archives, narrow receipt/API ownership and ordinary b527 acceptance remain separate. All139 live runtime names/86routes, original181baseline operations/115routes, every slow/failure stage, nativeCPU/disk/busy/scheduling, provider/peer/logloss/retention and representative startup/drain/concurrency obligations remain open under unchanged17d independent acceptance. DB89957 originalaccepted scope staysclosed.

## Preserved prior inventory and evidence

# Fresh current-process inventory — 2026-10-19 22:27 UTC

All **606 findings and102concerns remain open**. All598 previous rows remain unchanged; four newly observed runtime names and four route groups are added. The [unchanged numeric API snapshot](benchmarks/systemic-live-runtime-20261019T2227.json), [interpretation/count summary](benchmarks/systemic-live-runtime-20261019T2227-summary.json), [all139 operations CSV](benchmarks/systemic-live-runtime-20261019T2227-operations.csv) and [all86 route groups CSV](benchmarks/systemic-live-runtime-20261019T2227-routes.csv) retain every returned distribution and count.

Source: macbook0c7d8ecb, supported status/runtime/status reads22:27:04.426554–22:27:05.158541UTC. All three reads fence the same53b42/package6cc/PID69337/startup69233661 process; installed revision is separately228. Individual reads440.149/145.506/145.708ms are not acceptance distributions. No fresh log files or peer logs were scanned. This is a process snapshot, not a new24h log audit or distinct incident count.

139 operation names contain70 with a slow or failed/timed outcome. Counters total1012slow calls,1012failed and958timed_out, which overlap and must not be added as distinct incidents. Fleet overview accounts for941failed/timed-out calls, PR peers64failed/11timed-out, sync peers3failed; other exact outcomes remain below. Two attachment-publication failures from the same full-cap concern and the prior workspace failure remain preserved.95,441 non-SSE requests account for694,690,854 response bytes;31SSE excluded. The retained slow-request tail has50 records. Per-operation/per-route p50/p95/p99/window max are bounded windows; process maxima remain separate. Family quantiles are not fabricated by combining quantiles.

Event-loop p95 is45ms in its current600sample window; cumulative maximum is1,071,133ms. The timer does not prove whether this was host/process suspension, scheduling, CPU contention or a synchronous callback. Request process maximum576,320.232ms is distinct from page/rolling percentiles and the known long-running backup timeout contract remains with its original owner. These observations strengthen shared-cause investigation, without assigning every warning to a separate slow query or asserting a single cause.

PR549 additive lock-release diagnostics merged5b60ca8b from97312dbb, exacthead/merge trees equal20fdb276, stable9observations plus independent five-success checks/0requiredapprovals/0reviews/0threads/protection/fullpagination. MainCI39335222796 is tracked separately; no installation or activation occurred. The current source diagnostics therefore cannot retrospectively explain these old-process tails. Existing exact228 release ownership, backup receipt/API ownership and ordinary independent acceptance remain unchanged. All prior mixed warnings/failures/stages, historical missing bytes, peer/log/retention gaps and representative startup/drain/concurrency obligations remain open.

| Operation family | Names | Submitted | Slow calls | Failed | Timed out | Process max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp` | 4 | 13729 | 25 | 0 | 1 | 1679.759 |
| `agent` | 11 | 14429 | 119 | 0 | 0 | 1976.395 |
| `browser` | 3 | 9 | 0 | 0 | 0 | 37.683 |
| `cards` | 2 | 4 | 0 | 2 | 0 | 105.992 |
| `conversation` | 2 | 13900 | 20 | 0 | 0 | 1027.632 |
| `dispatch` | 9 | 74842 | 68 | 0 | 0 | 1338.984 |
| `filesystem` | 2 | 6894 | 0 | 0 | 0 | 1389.027 |
| `fleet` | 8 | 83622 | 116 | 0 | 0 | 1848.711 |
| `github` | 1 | 3 | 0 | 0 | 0 | 53.936 |
| `goal` | 2 | 702 | 1 | 0 | 0 | 524.278 |
| `health` | 1 | 2493 | 41 | 0 | 2 | 2692.116 |
| `http` | 7 | 76610 | 8 | 1005 | 952 | 11166.853 |
| `intake` | 1 | 23 | 2 | 0 | 0 | 644.066 |
| `maintenance` | 1 | 1 | 0 | 0 | 0 | 25586.839 |
| `notifications` | 2 | 1934 | 1 | 0 | 0 | 515.291 |
| `pr_supervisor` | 5 | 63848 | 4 | 0 | 0 | 844.577 |
| `preferences` | 1 | 88 | 0 | 0 | 0 | 302.176 |
| `progress` | 7 | 14751 | 156 | 0 | 3 | 1453.939 |
| `reconciliation` | 4 | 39000 | 26 | 0 | 0 | 1319.939 |
| `selection` | 4 | 46 | 2 | 0 | 0 | 613.374 |
| `sqlite` | 43 | 365597 | 272 | 1 | 0 | 5268.794 |
| `startup` | 3 | 3 | 1 | 0 | 0 | 797.669 |
| `sync` | 10 | 99157 | 142 | 3 | 0 | 6316.062 |
| `workspace` | 6 | 36 | 8 | 1 | 0 | 8675.767 |

| Slow/failed runtime operation | Submitted | Slow | Failed / timed out | Window n | p50 / p95 / p99 ms | Window max ms | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| `acp.wire_append` | 13708 | 25 | 0 / 1 | 256 | 0.699 / 47.497 / 149.899 | 517.98 | 1679.759 |
| `agent.project_recovery_availability` | 10761 | 7 | 0 / 0 | 256 | 1.255 / 89.069 / 211.091 | 444.607 | 828.675 |
| `agent.recovery_sessions` | 3586 | 109 | 0 / 0 | 256 | 128.82 / 649.634 / 1401.207 | 1976.395 | 1976.395 |
| `agent.session_snapshot` | 3 | 2 | 0 / 0 | 3 | 532.577 / 931.426 / 931.426 | 931.426 | 931.426 |
| `agent.startup_state_read` | 1 | 1 | 0 / 0 | 1 | 1237.478 / 1237.478 / 1237.478 | 1237.478 | 1237.478 |
| `cards.attachments.publish` | 2 | 0 | 2 / 0 | 2 | 11.957 / 105.992 / 105.992 | 105.992 | 105.992 |
| `conversation.issues` | 6771 | 10 | 0 / 0 | 256 | 13.85 / 115.565 / 454.904 | 617.974 | 1027.632 |
| `conversation.session` | 7129 | 10 | 0 / 0 | 256 | 19.673 / 145.384 / 524.205 | 584.87 | 825.536 |
| `dispatch.completion_pending_read` | 3579 | 2 | 0 / 0 | 256 | 1.962 / 78.164 / 243.329 | 634.262 | 634.262 |
| `dispatch.followup_ack` | 38 | 2 | 0 / 0 | 38 | 199.74 / 623.926 / 926.716 | 926.716 | 926.716 |
| `dispatch.followup_pending_read` | 3579 | 1 | 0 / 0 | 256 | 2.292 / 93.544 / 212.231 | 773.103 | 773.103 |
| `dispatch.followup_record_write` | 22 | 2 | 0 / 0 | 22 | 323.81 / 555.058 / 678.243 | 678.243 | 678.243 |
| `dispatch.followup_started` | 19 | 1 | 0 / 0 | 19 | 193.895 / 506.996 / 506.996 | 506.996 | 506.996 |
| `dispatch.record_read` | 67557 | 60 | 0 / 0 | 256 | 23.812 / 71.162 / 195.602 | 527.924 | 1338.984 |
| `fleet.overview.reachability` | 1238 | 4 | 0 / 0 | 256 | 9.221 / 294.478 / 603.981 | 1024.403 | 1024.403 |
| `fleet.overview.status` | 1238 | 4 | 0 / 0 | 256 | 9.048 / 311.626 / 605.124 | 963.076 | 963.076 |
| `fleet.overview.sync` | 2641 | 42 | 0 / 0 | 256 | 45.072 / 620.335 / 1301.993 | 1848.711 | 1848.711 |
| `fleet.overview_cache_read` | 52024 | 20 | 0 / 0 | 256 | 1.277 / 23.452 / 56.983 | 78.746 | 887.348 |
| `fleet.overview_cache_write` | 16250 | 43 | 0 / 0 | 256 | 7.214 / 61.837 / 139.566 | 208.129 | 1117.098 |
| `fleet.overview_response_json` | 10192 | 3 | 0 / 0 | 256 | 3.604 / 44.236 / 91.552 | 127.421 | 1498.787 |
| `goal.dispatch_lifecycle_index` | 683 | 1 | 0 / 0 | 256 | 1.095 / 136.003 / 246.31 | 524.278 | 524.278 |
| `health.canonical_obligation` | 2493 | 41 | 0 / 2 | 256 | 8.737 / 165.221 / 1381.544 | 2243.066 | 2692.116 |
| `http.fleet_overview` | 11133 | 0 | 941 / 941 | 256 | 63.374 / 416.766 / 614.559 | 993.358 | 5703.767 |
| `http.pr_supervisor_peer` | 63772 | 0 | 64 / 11 | 256 | 37.801 / 153.205 / 606.291 | 626.901 | 9495.36 |
| `http.session_list` | 1459 | 8 | 0 / 0 | 256 | 26.518 / 175.171 / 466.27 | 1505.743 | 1505.743 |
| `intake.web_prompt` | 23 | 2 | 0 / 0 | 23 | 44.03 / 557.552 / 644.066 | 644.066 | 644.066 |
| `notifications.present_page` | 967 | 1 | 0 / 0 | 256 | 0.572 / 37.891 / 111.887 | 515.291 | 515.291 |
| `pr_supervisor.response_json` | 63651 | 4 | 0 / 0 | 256 | 0.663 / 13.155 / 34.979 | 77.55 | 844.577 |
| `progress.checkpoint_write` | 182 | 15 | 0 / 0 | 182 | 221.806 / 584.893 / 1084.293 | 1106.832 | 1106.832 |
| `progress.dispatch_read` | 5880 | 50 | 0 / 2 | 256 | 30.265 / 100.564 / 546.695 | 669.099 | 1321.394 |
| `progress.final_report_write` | 13 | 2 | 0 / 0 | 13 | 250.384 / 1286.895 / 1286.895 | 1286.895 | 1286.895 |
| `progress.heartbeat_write` | 1153 | 41 | 0 / 0 | 256 | 100.584 / 495.486 / 713.604 | 1045.938 | 1045.938 |
| `progress.sequence_allocate` | 1336 | 48 | 0 / 1 | 256 | 104.765 / 399.433 / 910.843 | 1184.516 | 1453.939 |
| `reconciliation.completion_evidence_read` | 10629 | 10 | 0 / 0 | 256 | 3.251 / 88.678 / 313.011 | 1319.939 | 1319.939 |
| `reconciliation.completion_queue` | 12 | 2 | 0 / 0 | 12 | 331.706 / 611.905 / 611.905 | 611.905 | 611.905 |
| `reconciliation.dispatch_read` | 17730 | 6 | 0 / 0 | 256 | 1.599 / 25.323 / 95.336 | 142.051 | 853.221 |
| `reconciliation.session_read` | 10629 | 8 | 0 / 0 | 256 | 2.042 / 74.426 / 416.705 | 652.148 | 799.549 |
| `selection.prompt_identity` | 18 | 1 | 0 / 0 | 18 | 44.419 / 613.374 / 613.374 | 613.374 | 613.374 |
| `selection.prompt_outcome` | 16 | 1 | 0 / 0 | 16 | 112.659 / 510.15 / 510.15 | 510.15 | 510.15 |
| `sqlite.agent_runtime_checkpoint` | 79 | 3 | 0 / 0 | 79 | 31.948 / 410.609 / 740.511 | 740.511 | 740.511 |
| `sqlite.agent_session_save` | 609 | 10 | 0 / 0 | 256 | 24.149 / 276.768 / 445.266 | 937.439 | 5268.794 |
| `sqlite.card_read` | 99462 | 13 | 0 / 0 | 256 | 1.472 / 26.955 / 84.204 | 162.171 | 1490.678 |
| `sqlite.card_write` | 3 | 2 | 0 / 0 | 3 | 626.703 / 762.961 / 762.961 | 762.961 | 762.961 |
| `sqlite.pr_supervisor_capability_write` | 341 | 2 | 0 / 0 | 256 | 4.697 / 120.499 / 311.546 | 834.475 | 834.475 |
| `sqlite.pr_supervisor_completion_due_read` | 6832 | 16 | 0 / 0 | 256 | 17.135 / 263.407 / 715.833 | 838.163 | 3387.376 |
| `sqlite.pr_supervisor_due_read` | 6802 | 3 | 0 / 0 | 256 | 3.095 / 36.319 / 90.629 | 145.726 | 1203.48 |
| `sqlite.pr_supervisor_effect_prepare` | 6 | 0 | 1 / 0 | 6 | 3.412 / 9.016 / 9.016 | 9.016 | 9.016 |
| `sqlite.pr_supervisor_event_write` | 31879 | 9 | 0 / 0 | 256 | 2.647 / 57.212 / 185.1 | 387.615 | 1252.902 |
| `sqlite.pr_supervisor_lease_write` | 25 | 1 | 0 / 0 | 25 | 7.875 / 242.181 / 537.517 | 537.517 | 537.517 |
| `sqlite.pr_supervisor_watch_read` | 32282 | 21 | 0 / 0 | 256 | 3.611 / 68.081 / 175.545 | 197.262 | 1983.193 |
| `sqlite.pr_supervisor_watch_write` | 31884 | 26 | 0 / 0 | 256 | 7.229 / 90.291 / 272.588 | 652.421 | 1673.877 |
| `sqlite.project_read` | 67590 | 12 | 0 / 0 | 256 | 0.715 / 20.915 / 76.716 | 140.479 | 1066.438 |
| `sqlite.prompt_acceptance_read` | 84 | 17 | 0 / 0 | 84 | 232.649 / 1542.151 / 3258.642 | 3258.642 | 3258.642 |
| `sqlite.repository_read` | 67538 | 16 | 0 / 0 | 256 | 0.932 / 49.509 / 110.936 | 186.041 | 1062.054 |
| `sqlite.restart_handoff_completion` | 7276 | 17 | 0 / 0 | 256 | 3.244 / 176.638 / 394.803 | 653.083 | 1276.51 |
| `sqlite.restart_handoff_session` | 3638 | 3 | 0 / 0 | 256 | 1.708 / 125.782 / 370.698 | 714.0 | 812.988 |
| `sqlite.restart_handoffs_pending` | 3587 | 2 | 0 / 0 | 256 | 2.241 / 172.708 / 389.279 | 817.08 | 817.08 |
| `sqlite.restart_handoffs_watchdog` | 3586 | 7 | 0 / 0 | 256 | 2.46 / 220.283 / 324.72 | 437.555 | 818.214 |
| `sqlite.transcript_append` | 1539 | 85 | 0 / 0 | 256 | 17.134 / 510.534 / 1013.968 | 1425.727 | 2029.88 |
| `sqlite.transcript_message_page` | 18 | 1 | 0 / 0 | 18 | 172.612 / 737.462 / 737.462 | 737.462 | 737.462 |
| `sqlite.transcript_read` | 233 | 6 | 0 / 0 | 233 | 94.863 / 314.226 / 807.422 | 1018.141 | 1018.141 |
| `startup.openapi` | 1 | 1 | 0 / 0 | 1 | 797.669 / 797.669 / 797.669 | 797.669 | 797.669 |
| `sync.commit_read` | 30301 | 5 | 0 / 0 | 256 | 0.964 / 2.631 / 5.619 | 13.508 | 703.709 |
| `sync.object_has` | 60790 | 3 | 0 / 0 | 256 | 0.305 / 0.561 / 0.641 | 7.969 | 818.748 |
| `sync.peer_http` | 202 | 0 | 3 / 0 | 202 | 29.502 / 257.826 / 3007.094 | 3018.13 | 3018.13 |
| `sync.reconcile_head` | 195 | 128 | 0 / 0 | 195 | 599.619 / 1226.56 / 2470.228 | 6316.062 | 6316.062 |
| `sync.ref_read` | 7460 | 6 | 0 / 0 | 256 | 0.734 / 86.477 / 250.629 | 1070.257 | 1070.257 |
| `workspace.collect_garbage` | 2 | 2 | 0 / 0 | 2 | 5804.455 / 8675.767 / 8675.767 | 8675.767 | 8675.767 |
| `workspace.lease_renew` | 18 | 4 | 0 / 0 | 18 | 98.285 / 2850.627 / 2850.627 | 2850.627 | 2850.627 |
| `workspace.project_provision` | 10 | 2 | 1 / 0 | 10 | 1646.02 / 6330.114 / 6330.114 | 6330.114 | 6330.114 |

| All observed route groups | Count | Window n | p50 / p95 / p99 / max ms | Process response bytes |
|---|---:|---:|---:|---:|
| `/` | 7 | 7 | 13.012 / 49.985 / 49.985 / 49.985 | 131472 |
| `/api/agent/collaboration/policies` | 3 | 3 | 22.512 / 89.004 / 89.004 / 89.004 | 2160 |
| `/api/agent/history/{session_id}` | 18 | 18 | 431.738 / 1167.607 / 1167.607 / 1167.607 | 13299054 |
| `/api/agent/observability/v1/sessions/{session_id}` | 78 | 78 | 87.395 / 253.689 / 709.071 / 709.071 | 351008 |
| `/api/agent/observability/v1/sessions/{session_id}/diagnostics` | 1 | 1 | 171.329 / 171.329 / 171.329 / 171.329 | 2933 |
| `/api/agent/observability/v1/sessions/{session_id}/turns` | 1 | 1 | 75.458 / 75.458 / 75.458 / 75.458 | 2199 |
| `/api/agent/prompt-capabilities` | 19 | 19 | 3.14 / 168.526 / 168.526 / 168.526 | 741 |
| `/api/agent/provider-options/{provider_id}` | 6 | 6 | 17.663 / 75.767 / 75.767 / 75.767 | 66438 |
| `/api/agent/sessions` | 1459 | 256 | 30.345 / 194.092 / 471.061 / 1516.463 | 29727279 |
| `/api/agent/sessions/{session_id}` | 22 | 22 | 50.954 / 95.413 / 214.399 / 214.399 | 9602830 |
| `/api/agent/sessions/{session_id}/browser` | 10 | 10 | 3.039 / 11.626 / 11.626 / 11.626 | 180 |
| `/api/agent/sessions/{session_id}/cards` | 1 | 1 | 16.964 / 16.964 / 16.964 / 16.964 | 559 |
| `/api/agent/sessions/{session_id}/commands` | 14 | 14 | 42.429 / 214.992 / 214.992 / 214.992 | 966071 |
| `/api/agent/sessions/{session_id}/conversation-origin` | 4 | 4 | 44.229 / 88.382 / 88.382 / 88.382 | 1368 |
| `/api/agent/sessions/{session_id}/dispatch-prompts` | 19 | 19 | 3001.515 / 11090.955 / 11090.955 / 11090.955 | 234006 |
| `/api/agent/sessions/{session_id}/execution-catalog` | 29 | 29 | 4.837 / 33.252 / 42.16 / 42.16 | 161403 |
| `/api/agent/sessions/{session_id}/issues` | 3747 | 256 | 29.265 / 352.855 / 838.63 / 1670.023 | 83952582 |
| `/api/agent/sessions/{session_id}/prompt` | 4 | 4 | 382.623 / 1128.393 / 1128.393 / 1128.393 | 1568 |
| `/api/agent/sessions/{session_id}/recover` | 3 | 3 | 7091.888 / 7190.933 / 7190.933 / 7190.933 | 13600951 |
| `/api/agent/sessions/{session_id}/restart-handoffs` | 26 | 26 | 19.632 / 32.105 / 137.443 / 137.443 | 5721601 |
| `/api/agent/sessions/{session_id}/threads` | 3378 | 256 | 49.864 / 470.112 / 666.181 / 1342.487 | 5699594 |
| `/api/backups` | 19 | 19 | 6.334 / 576320.232 / 576320.232 / 576320.232 | 66660 |
| `/api/backups/status` | 39 | 39 | 13.012 / 401.485 / 1036.131 / 1036.131 | 2883337 |
| `/api/browser/{operation}` | 11 | 11 | 541.234 / 5469.828 / 5469.828 / 5469.828 | 98229 |
| `/api/cards/summaries` | 2 | 2 | 18.306 / 24.942 / 24.942 / 24.942 | 25623 |
| `/api/cards/{card_id}` | 30 | 30 | 10.875 / 326.543 / 2778.188 / 2778.188 | 1348246 |
| `/api/cards/{card_id}/attachments` | 2 | 2 | 138.895 / 259.398 / 259.398 / 259.398 | 102 |
| `/api/configuration` | 6 | 6 | 13.732 / 18.56 / 18.56 / 18.56 | 1019694 |
| `/api/execution/catalog` | 8 | 8 | 163.009 / 8251.378 / 8251.378 / 8251.378 | 259832 |
| `/api/execution/defaults` | 5 | 5 | 16.017 / 99.747 / 99.747 / 99.747 | 30300 |
| `/api/fleet/dispatch-jobs` | 1 | 1 | 86.207 / 86.207 / 86.207 / 86.207 | 530087 |
| `/api/fleet/dispatch-jobs/{dispatch_id}` | 349 | 256 | 15.991 / 68.462 / 139.444 / 428.106 | 63001970 |
| `/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint` | 25 | 25 | 585.61 / 1088.626 / 1664.701 / 1664.701 | 5077 |
| `/api/fleet/dispatch-jobs/{dispatch_id}/prompt` | 7 | 7 | 3828.116 / 11314.112 / 11314.112 / 11314.112 | 2387 |
| `/api/fleet/dispatch/{dispatch_id}/turn-end` | 11 | 11 | 1580.523 / 2443.48 / 2443.48 / 2443.48 | 38066 |
| `/api/fleet/instances` | 1871 | 256 | 5.871 / 160.962 / 578.874 / 622.299 | 5124669 |
| `/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}` | 9 | 9 | 4.177 / 51.061 / 51.061 / 51.061 | 58281 |
| `/api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/{operation}` | 12 | 12 | 1838.756 / 8889.712 / 8889.712 / 8889.712 | 4092 |
| `/api/fleet/overview` | 6 | 6 | 114.11 / 423.025 / 423.025 / 423.025 | 5884147 |
| `/api/fleet/overview/local` | 3040 | 256 | 76.324 / 514.67 / 1555.312 / 2494.987 | 25467747 |
| `/api/fleet/session-route/{session_id}` | 13 | 13 | 14.788 / 99.694 / 99.694 / 99.694 | 19824 |
| `/api/github/supervision-scope` | 3 | 3 | 62.197 / 75.96 / 75.96 / 75.96 | 1278 |
| `/api/github/supervision-scope/comparison` | 3 | 3 | 39.055 / 56.517 / 56.517 / 56.517 | 6058 |
| `/api/health` | 27 | 27 | 2.019 / 3.895 / 29.695 / 29.695 | 405 |
| `/api/health-journal/receipts/{receipt_id}` | 73 | 73 | 7.073 / 63.688 / 142.728 / 142.728 | 44139 |
| `/api/health-journal/reports` | 19 | 19 | 25.373 / 415.83 / 415.83 / 415.83 | 6112 |
| `/api/health-journal/reports/{report_id}` | 3 | 3 | 45.577 / 82.425 / 82.425 / 82.425 | 312834 |
| `/api/health-journal/verification-intents` | 6 | 6 | 52.869 / 150.393 / 150.393 / 150.393 | 1614356 |
| `/api/health-journal/verification-readiness/{dispatch_id}` | 4 | 4 | 146.896 / 1223.889 / 1223.889 / 1223.889 | 1080 |
| `/api/instance` | 5 | 5 | 4.439 / 21.102 / 21.102 / 21.102 | 2800 |
| `/api/instance/maintenance` | 1 | 1 | 5.666 / 5.666 / 5.666 / 5.666 | 7442 |
| `/api/items/{item_id}` | 6 | 6 | 51.471 / 611.105 / 611.105 / 611.105 | 172276 |
| `/api/mcp-servers` | 3 | 3 | 8.961 / 59.154 / 59.154 / 59.154 | 711 |
| `/api/notifications` | 967 | 256 | 20.359 / 280.251 / 741.847 / 805.749 | 145427 |
| `/api/notifications/{notification_id}` | 2 | 2 | 18.466 / 23.745 / 23.745 / 23.745 | 10188 |
| `/api/operation-recovery/{idempotency_key:path}` | 1 | 1 | 19.355 / 19.355 / 19.355 / 19.355 | 660 |
| `/api/operations/{idempotency_key:path}` | 7 | 7 | 35.907 / 207.383 / 207.383 / 207.383 | 6891 |
| `/api/owner-ready` | 7 | 7 | 3.796 / 11.631 / 11.631 / 11.631 | 497 |
| `/api/pr-supervisor/capabilities` | 170 | 170 | 7.06 / 82.773 / 216.123 / 281.536 | 466140 |
| `/api/pr-supervisor/effects/dispatch` | 3 | 3 | 182.892 / 187.535 / 187.535 / 187.535 | 177 |
| `/api/pr-supervisor/instances/heartbeat` | 630 | 256 | 8.057 / 170.88 / 413.407 / 549.029 | 10710 |
| `/api/pr-supervisor/replicas` | 67509 | 256 | 41.594 / 249.076 / 546.551 / 618.356 | 376083504 |
| `/api/pr-supervisor/retirements` | 1 | 1 | 103.816 / 103.816 / 103.816 / 103.816 | 5312 |
| `/api/pr-supervisor/watches` | 4 | 4 | 163.962 / 1000.551 / 1000.551 / 1000.551 | 7920 |
| `/api/pr-supervisor/watches/{watch_id}` | 11 | 11 | 37.839 / 1065.673 / 1065.673 / 1065.673 | 133882 |
| `/api/pr-supervisor/watches/{watch_id}/lease` | 40 | 40 | 115.093 / 567.345 / 1186.585 / 1186.585 | 9560 |
| `/api/pr-supervisor/watches/{watch_id}/merge-evidence` | 6 | 6 | 11.938 / 145.185 / 145.185 / 145.185 | 19359 |
| `/api/pr-supervisor/watches/{watch_id}/refresh` | 3 | 3 | 55.563 / 290.882 / 290.882 / 290.882 | 204 |
| `/api/ready` | 3040 | 256 | 2.921 / 32.235 / 89.974 / 98.841 | 412873 |
| `/api/runtime` | 21 | 21 | 30.709 / 61.977 / 91.169 / 91.169 | 6536603 |
| `/api/status` | 3096 | 256 | 92.682 / 629.041 / 1096.807 / 1778.002 | 8741195 |
| `/api/sync/get` | 139 | 139 | 5.238 / 166.073 / 225.481 / 239.56 | 1342339 |
| `/api/sync/refs` | 185 | 185 | 3.73 / 53.31 / 197.216 / 200.232 | 47915 |
| `/api/telemetry/live` | 4528 | 256 | 3.349 / 6.897 / 39.709 / 97.676 | 18036973 |
| `/api/ui/assets` | 441 | 256 | 8.916 / 57.855 / 146.709 / 247.476 | 34839 |
| `/api/workspaces` | 23 | 23 | 11.11 / 39.357 / 74.565 / 74.565 | 51354 |
| `/favicon.ico` | 2 | 2 | 39.755 / 312.978 / 312.978 / 312.978 | 1186 |
| `/fleet` | 5 | 5 | 323.638 / 521.388 / 521.388 / 521.388 | 5716765 |
| `/partials/cards` | 24 | 24 | 548.733 / 1597.981 / 2425.62 / 2425.62 | 164572 |
| `/partials/cards/{card_id}/detail` | 2 | 2 | 59.729 / 334.22 / 334.22 / 334.22 | 46263 |
| `/partials/cards/{card_id}/progress` | 2 | 2 | 12.607 / 186.596 / 186.596 / 186.596 | 4183 |
| `/partials/home/sections` | 2 | 2 | 5221.514 / 6502.463 / 6502.463 / 6502.463 | 38962 |
| `/projects` | 8 | 8 | 200.02 / 3781.806 / 3781.806 / 3781.806 | 341124 |
| `/pull-requests` | 5 | 5 | 37.412 / 442.772 / 442.772 / 442.772 | 125605 |
| `/{page_path:path}` | 24 | 24 | 49.538 / 277.175 / 772.483 / 772.483 | 2016820 |
| `__unmatched__` | 65 | 65 | 25.404 / 279.505 / 475.902 / 475.902 | 2566994 |

## Preserved prior inventory and evidence

# Lock release attribution and current activation boundary — 2026-10-19

All **598 findings and102 concerns remain open**. The prior597 rows are unchanged; [full new numeric evidence and retained failures](benchmarks/systemic-lock-release-attribution-20261019.json) link to the [complete ledger](performance-coverage.json).

The existing lock timer ends after `release()` returns. A disposable test with a real native lock proves that another caller can acquire it during a controlled releasing-caller interval. Both Python and filesystem diagnostics report an inclusive5000ms for a2000ms body plus3000ms around release. The change **retains that5000ms total and all warnings** and separately records the2000ms pre-release body and3000ms release/scheduling span. This is better attribution, not a confirmed cause of historical PA latency. The release span includes unlock and scheduling around it; it is not pure unlock CPU or proof the lock stayed held.

Numeric event history stays bounded to64sections per invocation; aggregates include evicted sections. New distributions retain the existing256sample window.27 isolated runtime/filesystem/deadline/nesting tests pass, followed by11 overlapping final contracts after strengthening bounds and unchanged-warning checks. Baseline produced two expected missing-attribution failures and one pass.40 alternating pure pairs of100observed lock sections measured p95 overhead1.318ms before and1.801ms after; every sample is retained. This diagnostic cost is not a speedup or whole-PA acceptance result. No heavy mixed fixture was rerun during release-owner coordination.

PR547 is already integrated as52f042a, exacthead76e7/tree11e1a0bb matching, with all five head and main checks successful. The current22:19:46UTC supported status still captures **loaded53b42/PID69337/startup69233661/package6cc**, while `install_revision` reports2283d907. The latter reads current on-disk distribution metadata. Independent installed-only checks confirmed438exact228files, unchanged Python3.14.2/all52versions and38cachedtemplates/package41eed. PR544-546 are installed but not yet proved loaded; PR547 and this new change are outside that activation scope. Existing release owner retains exact scoped activation and fresh receipt/backup gates. No new installation/restart was issued by systemic; the existing native question remains unanswered, with no fabricated response.

Backup run5659 completed04:30:53UTC with integrity verified but data incomplete by one historical object/2842decodedbytes. Later ordinary scheduled runs completed with the same disclosure and normal retention; source/archives/refs were not mutated by systemic. Fresh metadata-only inventory has nine points, eight historical metadata-verified points, one limited point and no verification in progress; lastscrub06:04:23UTC. These are observations, not a fresh archive audit, blanket future hold release or restore acceptance. The original owner retains the narrow receipt/API timeout repair and independent acceptance. Prior worker warnings3/threshold observations12 remain unchanged, with partial backup overlap disclosed and no sole-cause claim. Peer/log/retention coverage, CPU/disk/busy attribution, representative startup/drain and ordinary independent acceptance remain required.

## Preserved prior inventory and evidence

# Exact deployed inventory and dispatch summary repair — 2026-10-19

All **597 findings and 101 concerns remain open**. The [unchanged full deployed retest artifact](benchmarks/systemic-exact53b42-loaded-retest-20261019.json) publishes the evidence that could not be added to the full ten-attachment card. Its SHA256 remains `5ec8c78ec0da88b0c2b487d54a2442ae2c055f6121d9813da9b917c714e4f191`. All ten existing attachments remain preserved. Both JSON and gzip submissions returned definite413; a fresh read confirmed ten attachments and the source enforces a ten-file cap. The early byte-size hypothesis was withdrawn; no attachment was claimed uploaded or replaced.

macbook `0c7d8ecb` runs exact authorized PR542 merge `53b42d5077971b192e824c62acae71a59db95e80`, package `6ccadfa13713…`, PID69337/startup `69233661-c671-4998-b0c9-a5fda8684a89`. All438 source files, all52 distribution versions under Python3.14.2,38 compiled templates and two servedJS assets were checked. Configuration `86968d12…` and lease fence290 stayed unchanged. At04:14:37Z the saved handoff `e877ac9c-1ea6-524d-bc60-190a7840dcad` was independently terminal `continuation_delivered`/effectcomplete since00:35:55.909550Z, noerror. Originalfailed527 remains failed/undelivered and was not retried. Current checkout includes544/545; this authorization installed53b only, so later source is not claimed deployed.

The completed00:32–00:33 local window had95 ordinary GETs, allHTTP200 on one stable build, **five >=500ms observations**. This is not whole-PA acceptance. Separate runtime totals retain all87 operations and29 route groups,23slow calls across11 names and one workspace-provisioning failure; percentiles are bounded returned windows and maxima are process maxima. Exact per-call phase records, bytes/write totals, every family and opaque-observation omissions are retained. No new log download or exhaustive fleet audit was performed; prior log-backed inventory remains below.

| GET path | n | p50 ms | p95 ms | p99 ms | max ms | >=500ms |
|---|---:|---:|---:|---:|---:|---:|
| `/` | 5 | 26.41 | 38.556 | 38.556 | 38.556 | 0 |
| `/agent` | 5 | 79.882 | 158.949 | 158.949 | 158.949 | 0 |
| `/api/fleet/overview` | 5 | 222.78 | 527.677 | 527.677 | 527.677 | 1 |
| `/api/health` | 20 | 14.701 | 31.443 | 35.209 | 35.209 | 0 |
| `/api/runtime` | 20 | 50.383 | 134.573 | 514.157 | 514.157 | 1 |
| `/api/status` | 20 | 81.869 | 138.908 | 510.678 | 510.678 | 1 |
| `/fleet` | 5 | 357.123 | 564.742 | 564.742 | 564.742 | 1 |
| `/pull-requests` | 5 | 49.294 | 504.39 | 504.39 | 504.39 | 1 |
| `/settings?section=agent` | 5 | 24.658 | 48.259 | 48.259 | 48.259 | 0 |
| `/work` | 5 | 34.215 | 39.772 | 39.772 | 39.772 | 0 |

| Runtime slow/failed name | submitted | slow | failed | p50 ms | p95 ms | p99 ms | process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| `agent.recovery_sessions` | 18 | 1 | 0 | 81.437 | 529.547 | 529.547 | 529.547 |
| `agent.startup_state_read` | 1 | 1 | 0 | 1237.478 | 1237.478 | 1237.478 | 1237.478 |
| `conversation.session` | 45 | 2 | 0 | 15.01 | 139.097 | 825.361 | 825.361 |
| `fleet.overview_response_json` | 81 | 1 | 0 | 2.988 | 42.752 | 683.07 | 683.07 |
| `health.canonical_obligation` | 57 | 1 | 0 | 27.743 | 252.692 | 1048.504 | 1048.504 |
| `sqlite.agent_session_save` | 45 | 2 | 0 | 15.501 | 206.826 | 5268.794 | 5268.794 |
| `sqlite.restart_handoff_completion` | 76 | 1 | 0 | 6.734 | 100.559 | 925.615 | 925.615 |
| `startup.openapi` | 1 | 1 | 0 | 797.669 | 797.669 | 797.669 | 797.669 |
| `sync.reconcile_head` | 12 | 9 | 0 | 591.631 | 6316.062 | 6316.062 | 6316.062 |
| `workspace.collect_garbage` | 2 | 2 | 0 | 5804.455 | 8675.767 | 8675.767 | 8675.767 |
| `workspace.project_provision` | 8 | 2 | 1 | 1725.789 | 6330.114 | 6330.114 | 6330.114 |

The [complete new source artifact](benchmarks/systemic-dispatch-summary-20261019.json) retains a confirmed small repair: summary/status reads were materializing the latest durable turn snapshot and discarding it. Only that unused serialization is skipped. Full detail, durable snapshots, queue/admission/receipt semantics and complete numeric protocol gaps/compaction remain unchanged. Thirty alternating pairs retain identical summary and full detail, with30→0 snapshot serializations; actual-source p50/p95/p99/max are6.552/18.748/51.983/51.983ms before and0.225/0.531/1.713/1.713ms after in this2.576MB synthetic retained-proof fixture. This is measured discarded work, not proof of the sole cause of historic Fleet/health tails.

Five regressions fail against archived main as expected; candidate118tests+4subtests pass. A separate exact-wheel100iteration mixed source scenario adds normal durable retained snapshots to101 dispatches, retains9600 ordered events and12511 verified cold objects, and records **3 worker warnings and 12 threshold observations**. All failure/timeouts/rejections and startup/phase/byte/journal measurements remain in the artifact. This source is not deployed and is not a performance acceptance pass. A separate SQLite JSON rewrite experiment was slower in both variants and was rejected; its complete results remain recorded.

The one-shot Fleet shape audit retained field sizes only: fournodes781307bytes and eightedges206472bytes, including187479/other node sizes and a198516-byte dispatch edge. No response values, prompts or credentials were stored. It does not establish a single payload cause or authorize omission of public detail.

Backup owner18fc/c5a owns held available-data capture, all eightarchives and independentb527 acceptance. The terminal restart gate has been communicated with no new restart or capture authorization. Missing historical bytes remain disclosed without blocking feasible work. The systemic independent17d034a6 requirement, peer failures/log-loss/retention gaps, nativeCPU/disk/busy attribution, representative startup/drain and every older finding remain required. Source integration alone cannot complete this card.

## Preserved prior inventory and evidence

# Fresh integration-base verification — 2026-10-18

All **596 findings and 100 concerns remain open**. [Complete fresh-base workload, prior CI result, source attestation and residuals](benchmarks/systemic-gc-fresh-main-20261018.json). Original PR545 head `2c9c6ac3` passed all five CI checks, but main changed when the existing fork owner integrated PR544 as `ee21b429`. Root merged that main normally into its own branch as `08c95ccd`, without conflicts, resets or force pushes. Since session/chat integration inputs changed, a separate 100-iteration replay tests the resulting source. Earlier measurements and CI results remain immutable and do not substitute for final-head gates.

The fresh replay verifies 438 packaged source files, 38 compiled templates, all recorded module hashes, 9,600 ordered active events and 12,511 retained cold object contents. It records **1 worker warnings and 6 threshold observations**. All failure/timeouts/rejection counters, percentiles, maxima, startup observations, bytes/write measures and hash-chained journal evidence remain in the artifact. This is source fixture evidence; it is not a controlled speedup, deployed acceptance or a passing whole-PA result. Final updated-head CI, stable supervisor evidence and independent GitHub revalidation are still required.

The fork recovery source repair is integrated under its original owner; its running activation and ordinary independent acceptance remain unverified. The earlier GC fixture's two warnings/eight observations, both probe harness failures, 39 existing semantic tests and every older stage remain unchanged. The separate immutable-53b42 operator question retains its original target and receipt identity. No new install/restart, production backup/capture, reference mutation, policy/default change or verifier was performed. Shared host activity is uncontrolled; overlap of the historical backup fixture with this new replay is not established.

## Preserved prior inventory and evidence

# Bounded GC cursor fetch and coordinated remaining work — 2026-10-18

All **596 findings and 100 concerns remain open**; every prior 594-row entry is unchanged. [Complete numeric probe, failures, 39 existing regressions, exact mixed scenario and reproduction](benchmarks/systemic-gc-bounded-fetch-20261018.json). Source changes only two result fetches: each query already has at most 128 digest parameters, and now fetches those rows together instead of a Python/instrumentation cursor call per row. Both SQL queries, parameters, batch bounds, speculative-positive behavior, publication lock, fresh deletion recheck, reader pins and unlink handling remain unchanged. Instrumented call counts include fetch and iteration; distinct SQL statement counts do not decrease. Data, durability, admission, deadlines and warning thresholds retain their existing semantics.

On 4,096 retained cold objects published with real fsync, 30 alternating pairs preserve complete event models and compressed file hashes. Baseline/candidate instrumented SQLite calls: 4,162→66; rows: 4,096→4,096. Wall p50: 128.832→114.734 ms; p95: 180.680→167.881 ms; p99/max: 341.231→212.130 ms. Shared host activity remains uncontrolled; these samples do not establish a universal speedup, the earlier 511 ms tail's cause, deployment or whole-PA acceptance. Two initial harness failures are retained: an exact-string indentation guard before seeding, then incorrect use of default 500-row pagination during final parity. The corrected attempt journals all 60 samples and verifies all 4,096 models/files. **39 existing GC/storage/pruning/retention regressions pass** with guarded disposable data/workspace roots.

The single 100-iteration expanded mixed replay verifies 438 packaged source files, 38 compiled templates and all recorded runtime module hashes, preserving 9,600 ordered active events and 12,511 cold object contents. It retains **2 worker warnings and 8 threshold observations**, plus separate premeasurement records. All numeric samples/percentiles/max, failure counters, response/write measures and hash-chained journal evidence are in the artifact. There is no controlled mixed baseline or deployment acceptance. Earlier source/diagnostic warnings and outliers remain unchanged in their prior artifacts.

The prior fixed-role diagnostic's nested projection maintenance phase measures VACUUM at 390.219 ms wall/337.483 ms thread CPU. This fills that phase label from retained evidence; it does not explain the separate 511 ms execute or prove native busy, disk, OS or GIL cause. Instrumented SQLite call counts include fetch/iteration and cannot be called query counts.

Independent fork verifier bd9 reports that recovery hides source attribution and the new-message boundary despite 39 original checks passing. Original source owner 8524/dispatch 463c has resumed and is running; the new concern is consolidated here. PR536's queued 20:08 backup notice is historical: its final 6e8d head merged at 20:21 as 9e0173f0. Owner checkpoint 1018 confirms that source is included in 53b42, with the prospective plan held and all eight archives preserved. The reported real-fsync fixture I/O window remains possible overlap, so these measurements cannot establish an uncontended causal pair. The missing historical 2,842 bytes do not block feasible work; historical recovery remains unverified.

PR543 merged as exact 0009 with five terminal green head checks and five main checks. The last supported runtime read at 22:56:23Z remains 0d6638cc/PID45958; PR534's same 7d66 handoff was delivered at 21:58, with no pending restart from its owner. The **separate exact-53b42 activation question remains outstanding** at that read; this source repair does not change its target or authorize installation/restart. Backup prospective capture and ordinary independent acceptance remain separate. The historical PR534 report update was rejected with 409 occurrence mismatch/correlation 856791e5; it was not resent. Accepted checkpoints 8930/8932 retain the reconciliation. All original slow-name/runtime/route inventories, peer failures, retention/log-loss gaps and independent acceptance requirements remain open.

## Preserved prior inventory and evidence

# Lock-fixture synchronization and bounded role attribution — 2026-10-18

All **594 findings and98concerns remain open**; every prior593row is unchanged. [Complete intermediate CI failure, deterministic reproduction,22existing tests and fixed-role diagnostic](benchmarks/systemic-ci543-lock-fixture-followup-20261018.json). Application source, dependencies, warning/deadline/admission semantics and immutable53b42 activation proposal are unchanged. The PR includes two test corrections: exact decimal comparison and the lock-entry synchronization below.

Intermediatehead2bbd1d74 CI39172747593 fails a lock-wait test at27.238ms versus its unchanged30ms minimum; other4jobs pass. The test starts a50ms hold timer at taskcreation, before the worker necessarily enters measured_lock. ReportingLock now signals only after measured_lock's wall/deadline clocks start, then the test begins the **same50ms hold and>=30ms assertion**, retaining all idle/lock/queue/absolute budgets and queue/clientwait separation. A new bounded0.5s fixture-entry wait guards synchronization; it changes no production or measured-hold timeout. Actual originaltest/runtime/lock reproduction with controlled dispatch boundary fails baseline and passes correction; real host scheduling gives0.429ms versus50.693ms actual lock waits. This is a private boundary model, not a measurement of CI queue delay. Initial private harness importfailed before either case; original retained, separatev2 adds only currentrepo tosys.path. All **22existing delivery/discovery cases pass in11.08s**. RawCIlogSHAa035c85a060b58087a6f1aaebf7068e343d0e7781a788a00ec9a4023f38fe28c is retainedlocal, no same-head retry. Previous8e197b01 headCI39172904402 was5success but unmerged; only the final correctedhead may pass integration gates.

The separate30iteration fixed-role source diagnostic preserves2880orderedactive+12511cold content hashes,0operation failures/timeouts/rejections,438sourcefiles/38templates/29runtime modules and2220hash-chained journal records. Its32.031s measurement has **0workerwarnings but4universal500ms observations**, plus3premeasurement slow records. Objectcollect3samples p50/p95/p99/max696.239/1613.773/1613.773/1613.773ms; the one inclusive maintenance sweep11415.209ms. This smaller differently instrumented run is not a paired improvement or acceptance; the earlier100iteration2warnings/11observations and uninstrumented1/8 source stages remain retained. The earlier511ms nativeexecute did not recur, so no exact role/cause is assigned retroactively.

Only fixed SQLverbs and trusted packaged internal callsite labels are recorded, never SQLtext/parameters/rowbodies. All observed role buckets/counts/totals/max and each operation's worstworker are published; per-call percentile coverage is unavailable because individual calls/workers are bounded samples, not a complete call log. Unknown/other roles remain explicit gaps. Successfulbusy, disk, GIL and scheduling causes remainunverified. No new runtimeinstall/restart, backup file/archive/default/reference mutation, worker or independent acceptance; existing18fc/c5a prospective work and historical-gap disclosure continue separately.

## Preserved prior inventory, failed and intermediate stages

# Completed native-call attribution; residuals retained — 2026-10-18

All **593 findings and 97 concerns remain open**, with every row unchanged. The separate [complete100iteration per-SQLite-call numeric diagnostic](benchmarks/systemic-sqlite-call-attribution-20261018.json) finished:438sourcefiles/38templates/29runtime modulehashes match the existing source;9600orderedactive and12511cold content hashes are preserved;0operation failures/timeouts/rejections. The113.445s measurement has **2workerwarnings/11universal500ms observations+3premeasurement slow records**. Its numeric wrappers add overhead, so it is not a paired speedup, rollout or performance acceptance result. The earlier uninstrumented source542stage1warning/8observations and every prior stage remain separate and retained. Earlier artifact's running status is historical, not overwritten.

The527.415ms transcript-message-page warning has525.489ms worker service/174.913ms threadCPU. Its5fetchall calls take433.740ms wall/96.219ms CPU;7execute calls take25.747ms wall/14.407ms CPU. The708.558ms reconcile warning has703.860ms service/61.012ms CPU: oneexecute takes511.184ms wall/4.014ms CPU inside an outermost projection.mutation hold525.182ms. Nested holds overlap and cannot be summed. No busyfailure is recorded; successful busy/native/scheduler/GIL/disk cause and exact statement role remainunverified. This is narrower attribution, not proof that each warning is a different slow query. None of the selected worstworker topcalls overlaps the26retainedGCevents>=10ms (all fit the64event window); shortcollections outside thelast256window remainunknown, so this is not blanketGC exclusion.

Startup remains separately slow: OpenAPI685.061ms wall/663.547ms CPU; concurrent refread service0.534ms with754.146ms completiondelivery and followupread service0.093ms with753.136ms delivery. They are not slowSQL evidence. Full call buckets for24named operations, worstworker timelines, GC/lock observations, all raw latency samples and the7274-record verified incremental-journal hash/observer overhead are retained in JSON. No SQL, parameters or row bodies are captured.

| Residual timer | n | p50 ms | p95 ms | p99 ms | max ms | observations>=500ms |
|---|---:|---:|---:|---:|---:|---:|
| `sync.reconcile_head` | 100 | 55.762 | 184.196 | 321.726 | 711.898 | 1 |
| `/api/agent/history/session-0?limit=64&message_boundaries=true` | 100 | 192.645 | 456.659 | 526.296 | 636.632 | 4 |
| `sync.object_collect` | 10 | 412.402 | 1605.046 | 1605.046 | 1605.046 | 4 |
| `/fleet` | 100 | 118.444 | 278.853 | 463.806 | 753.615 | 1 |
| `maintenance.run` | 1 | 18731.252 | 18731.252 | 18731.252 | 18731.252 | 1 |

The first four residuals are individual source fixture timers; maintenance.run is one inclusive background sweep, not an individual query or HTTP request. All remain disclosed. Exact53b42 activation approval is still outstanding; this PR changes documentation and one reproduced decimal-comparison test, leaving application source/dependencies and the proposed runtime target unchanged. No install/restart, backup mutation, duplicate worker, timeout increase or acceptance claim. Existing backup owner retains prospective work and explicit historical-gap disclosure; original archives/history and independent requirements remain intact.

## Preserved earlier inventory and failures

# Exact decimal timing comparison and continued attribution — 2026-10-18

All **593 findings and 97 concerns remain open**; every prior592row is unchanged. The PR now includes one test-only numerical correction alongside the complete64-operation/17-family inventory below. **Application source, dependencies, warning/deadline thresholds and the immutable53b42 runtime proposal are unchanged.** [Full original failure, deterministic reproduction, existing tests and continuing attribution](benchmarks/systemic-ci543-decimal-followup-20261018.json).

InitialPR543CI39172308776 attempt1 has4passingjobs and1failed shard (967passed,172warnings,72subtests). Its session-timing test observes valid rounded decimal milliseconds **54.99 and64.99**, then compares64.99 against binary addition54.99+10, which yields **64.99000000000001**. Actual runtime code rounds both durable fields to2decimal places. Deterministic execution of the original test and real method reproduces baselinefailure and correctedpass by controlling only those two known clock sites; all18event-loop/other monotonic calls stay real. The test now compares decimal values exactly, with **no tolerance or timing-budget increase**. Its>=50ms check, wrong-session rejection and single-emission checks remain. Both existing discovery/timing tests pass in5.39s under isolated roots. OriginalCIlogSHA256256d02c10e9e23fc30f237014568b19e765a749c3261e9cfaf1d1cff4a8dfa56 remainslocal; no same-head retry was used. This is separate from originalPR542's unverified197.9ms responsiveness failure and all real latency tails.

Additional private transcript prototype verification passes72cursor/limitcases,2empty sessions,3invalid-limit exceptions and40stable-cursor reads during100normalappends, retaining2100events with hot/cold parity. It is unshipped; the earlier worse maximum and all source/concurrency/performance obligations remain. Correlated314slow-record phase classification finds260largestworker-service,42completion-delivery and12executor-dispatch records. Counts withphase>=500ms overlap:218service,34delivery,5dispatch,2queue,46workerCPU,60SQL,11Pythonlockwait,0filelockwait. Recorded busy failures are0; successful native busy, GIL reacquisition, scheduling and physical disk causes remainunverified. Queue wait is separate from the warning execution elapsed timer. These are retained slow-tail observations, not all-call percentiles or exact-cause labels.

A separate100iteration disposable source diagnostic with bounded per-SQLite-call execute/fetch/commit wall/threadCPU recording is running as this checkpoint is prepared. It preserves source/warning/deadline semantics and captures no SQL, parameters or rows. Instrumentation adds overhead; no result, speedup, deployment or acceptance is assumed. Inspect its final or partial evidence before further measurement. Existing backup owner remains responsible for its bounded prospective work; no backup file, runtime configuration, archive or independent verification policy is changed here.

## Preserved earlier inventory and failures

# Fresh slow-tail inventory and exact integration — 2026-10-18

All **592 findings and 96 concerns remain open**. Every prior588row is preserved unchanged; additive overrides link fresh observations to existing row IDs. This documentation-only checkpoint publishes the [complete64-operation numeric tail](benchmarks/systemic-live-log-window-20261018T2153-2215.json) and [all17family phases, exact integration/CI/receipt evidence and unshipped query probe](benchmarks/systemic-live-inventory-followup-20261018.json). The immutable proposed runtime target remains53b42d50; there is no new install, restart or acceptance claim.

**Exact source and coverage:** macbook0c7d8ecb; supported installed CLI `pa logs --all --since 2026-10-18T21:53:00Z --lines 100000 --json`;6385records,0malformed,cap not reached; timestamp range **2026-10-18T21:53:01Z–22:15:09Z**. Streams: stdout2698,structured1549,stderr2138; requested supervisor returned0, a coverage gap. **628rawslowwarning emissions include314structured+314stderr copies**, while all numeric phases occur in314structuredrecords. They are not distinct incidents. All64names already map to rows in the prior inventory. Raw logs remainlocal with SHA256b1825d4a1a4a2c0c9940f1455cf1ced20d08c986f53f92bc813f4846e1b66080; numeric artifact SHA256301e3470c68732ba449106891bd79a3b2b1e147c15e92c0d185d43f7d49ffd0e. The retained JSON source-command display omitted spaces after two options; this sentence records the actual argument separation without changing that artifact or its numeric evidence.

The window spans another owner's restart from2e285481 to0d6638, with private fixtures and ordinary host work overlapping. Per-record process build, newPID descriptor/rotation and older retention are unverified; no peer logs were inspected. **These are percentiles of retained slow records only**, not all-call/rolling HTTP percentiles or process maxima. All numeric queue, dispatch, service, completion delivery, CPU, locks, SQL, bytes and filesystem fields remain in JSON. Family percentiles are recomputed from underlying structured records; operation percentiles are never averaged. No endpoint distributions are inferred: the original181runtime operations/115routes and every prior coverage gap remain required.

Confirmed status: PR542 head6fb08417 merged **53b42d5077971b192e824c62acae71a59db95e80**, parents[f0d163c0,6fb08417], source/merge tree4d62c299 equal. Supervisor stable19observations; independent five-success checks/0requiredapprovals/0threads/fullpagination/protection-known. MainCI39171774684 attempt1 is five terminalsuccess. Original headCI39170870092 attempt1 failed unchanged100ms responsiveness budget at197.911ms with191ms own-task callback; four isolated base/patch processes pass and guard shows0schemafilter calls, but do not reproduce/explain/erase the failure. One bounded same-head failed-shard retry passed; no threshold, timer or source changed for that retry. Its CPU/GC/scheduling cause remains unverified.

Full installed438sourcefiles/52dependencyversions/38templatecache and loaded process match **0d6638/PID45958/startup52e06824**; this is separate from later53b42 source integration. Owner784 reports its terminal healthy handoff and no current restart/stability hold. Backupowner1018 confirms unchanged integratedPR536 is in53b42, no new stability hold, and a held prospective capture plan targeting that source. Existing18fc/c5a ownership, all eight archives/history and explicit possibly irrecoverable historical data remain preserved. Missing historical bytes do not block feasible repairs/prospective validation. Fork/backup/systemic independent acceptance stays with the normal scheduler; DB89957 accepted originalscope remains closed. The native exact53b42 activation decision is outstanding; no response is inferred and original527 failed receipt is not retried or relabeled.

Latest exact-source mixed evidence remains **9600orderedactive+12511cold events preserved,0operation failures/timeouts/rejections,1workerwarning/8universal500ms observations+3premeasurement**. Worker transcript-message-page549.510ms includes SQL438.276ms and threadCPU131.417ms; its underlying native cause is unknown. Earlier stages and all residual distributions are retained in the previous artifact, not erased by lower warning counts. The new disposable30pair sort hypothesis has complete selected-event parity and removes a query-plan temporary B-tree: baselinewallp50/p95/max25.498/50.696/54.058ms versuscandidate23.679/29.945/58.601ms. The candidate maximum regressed; **no source repair is shipped**, allcursor/limit/cold/concurrency contracts and representative evidence are still needed. It does not explain the historical549ms tail.

Observability concerns: inventory attachment413/correlationd45a9c95 has failed/ValueError receipt with unknown effect; precise rejection cause is unverified and no existing attachment was removed or resent. This repository publication preserves the full inventory. Release-owner followup transporttimeout8f600791 reconciled under the same key to acceptedqueued prompt1567eca1; later owner784 is separate lifecycle evidence, no duplicate request/approval inference. These concerns and the CI/sort findings are the four added open rows.

## Every retained prefix family — slow records only

| Prefix family | Names | Numeric slow records | p50 ms | p95 ms | p99 ms | max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp` | 1 | 11 | 680.803 | 1677.74 | 1677.74 | 1677.74 |
| `agent` | 5 | 18 | 628.93 | 1911.286 | 1911.286 | 1911.286 |
| `conversation` | 2 | 5 | 668.121 | 1703.222 | 1703.222 | 1703.222 |
| `dispatch` | 7 | 16 | 653.532 | 1892.56 | 1892.56 | 1892.56 |
| `fleet` | 6 | 20 | 663.996 | 6547.797 | 7603.315 | 7603.315 |
| `health` | 1 | 3 | 1550.513 | 2452.894 | 2452.894 | 2452.894 |
| `maintenance` | 1 | 1 | 58136.853 | 58136.853 | 58136.853 | 58136.853 |
| `pr_supervisor` | 1 | 3 | 777.243 | 795.202 | 795.202 | 795.202 |
| `preferences` | 1 | 2 | 534.025 | 919.244 | 919.244 | 919.244 |
| `progress` | 5 | 43 | 586.544 | 1631.672 | 2181.492 | 2181.492 |
| `provider` | 3 | 13 | 1541.384 | 2949.993 | 2949.993 | 2949.993 |
| `reconciliation` | 4 | 9 | 851.475 | 1709.982 | 1709.982 | 1709.982 |
| `selection` | 3 | 3 | 842.737 | 1107.428 | 1107.428 | 1107.428 |
| `sqlite` | 17 | 128 | 679.012 | 2483.386 | 5260.421 | 7069.361 |
| `startup` | 1 | 1 | 936.707 | 936.707 | 936.707 | 936.707 |
| `sync` | 2 | 26 | 961.367 | 4764.322 | 6011.065 | 6011.065 |
| `workspace` | 4 | 12 | 6014.413 | 12012.978 | 12012.978 | 12012.978 |

## Every retained operation — slow records only

| Operation | Raw warning copies | Numeric slow records | p50 ms | p95 ms | p99 ms | max ms |
|---|---:|---:|---:|---:|---:|---:|
| `acp.wire_append` | 22 | 11 | 680.803 | 1677.74 | 1677.74 | 1677.74 |
| `agent.history_payload` | 2 | 1 | 885.957 | 885.957 | 885.957 | 885.957 |
| `agent.project_recovery_availability` | 2 | 1 | 848.524 | 848.524 | 848.524 | 848.524 |
| `agent.recovery_sessions` | 28 | 14 | 607.416 | 972.424 | 972.424 | 972.424 |
| `agent.session_snapshot` | 2 | 1 | 1034.482 | 1034.482 | 1034.482 | 1034.482 |
| `agent.startup_state_read` | 2 | 1 | 1911.286 | 1911.286 | 1911.286 | 1911.286 |
| `conversation.issues` | 4 | 2 | 554.125 | 1242.454 | 1242.454 | 1242.454 |
| `conversation.session` | 6 | 3 | 668.121 | 1703.222 | 1703.222 | 1703.222 |
| `dispatch.completion_pending_read` | 2 | 1 | 1696.415 | 1696.415 | 1696.415 | 1696.415 |
| `dispatch.expire_capacity_reservations` | 2 | 1 | 1718.166 | 1718.166 | 1718.166 | 1718.166 |
| `dispatch.followup_ack` | 2 | 1 | 735.636 | 735.636 | 735.636 | 735.636 |
| `dispatch.followup_record_write` | 2 | 1 | 855.591 | 855.591 | 855.591 | 855.591 |
| `dispatch.followup_started` | 2 | 1 | 521.52 | 521.52 | 521.52 | 521.52 |
| `dispatch.reconcile_interrupted` | 2 | 1 | 1892.56 | 1892.56 | 1892.56 | 1892.56 |
| `dispatch.record_read` | 20 | 10 | 591.055 | 800.121 | 800.121 | 800.121 |
| `fleet.overview.activity` | 2 | 1 | 547.426 | 547.426 | 547.426 | 547.426 |
| `fleet.overview.mcp_bootstrap` | 8 | 4 | 6313.281 | 7603.315 | 7603.315 | 7603.315 |
| `fleet.overview.repositories` | 4 | 2 | 826.314 | 1052.515 | 1052.515 | 1052.515 |
| `fleet.overview.sync` | 10 | 5 | 663.996 | 1134.958 | 1134.958 | 1134.958 |
| `fleet.overview_cache_read` | 4 | 2 | 723.56 | 724.903 | 724.903 | 724.903 |
| `fleet.overview_cache_write` | 12 | 6 | 580.55 | 617.648 | 617.648 | 617.648 |
| `health.canonical_obligation` | 6 | 3 | 1550.513 | 2452.894 | 2452.894 | 2452.894 |
| `maintenance.run` | 2 | 1 | 58136.853 | 58136.853 | 58136.853 | 58136.853 |
| `pr_supervisor.response_json` | 6 | 3 | 777.243 | 795.202 | 795.202 | 795.202 |
| `preferences.agent_auto_approve_read` | 4 | 2 | 534.025 | 919.244 | 919.244 | 919.244 |
| `progress.checkpoint_write` | 14 | 7 | 557.246 | 881.891 | 881.891 | 881.891 |
| `progress.dispatch_read` | 24 | 12 | 537.16 | 874.484 | 874.484 | 874.484 |
| `progress.heartbeat_write` | 26 | 13 | 598.013 | 2181.492 | 2181.492 | 2181.492 |
| `progress.pending_read` | 6 | 3 | 739.112 | 2168.179 | 2168.179 | 2168.179 |
| `progress.sequence_allocate` | 16 | 8 | 525.944 | 1631.672 | 1631.672 | 1631.672 |
| `provider.status.codex` | 10 | 5 | 1465.365 | 1777.044 | 1777.044 | 1777.044 |
| `provider.status.cursor` | 10 | 5 | 2753.777 | 2949.993 | 2949.993 | 2949.993 |
| `provider.status.openinterpreter` | 6 | 3 | 963.491 | 1538.691 | 1538.691 | 1538.691 |
| `reconciliation.completion_evidence_read` | 4 | 2 | 639.628 | 744.18 | 744.18 | 744.18 |
| `reconciliation.completion_queue` | 6 | 3 | 594.59 | 851.475 | 851.475 | 851.475 |
| `reconciliation.dispatch_read` | 6 | 3 | 1155.136 | 1709.982 | 1709.982 | 1709.982 |
| `reconciliation.session_read` | 2 | 1 | 1050.517 | 1050.517 | 1050.517 | 1050.517 |
| `selection.configuration_receipt` | 2 | 1 | 1107.428 | 1107.428 | 1107.428 | 1107.428 |
| `selection.prompt_identity` | 2 | 1 | 514.5 | 514.5 | 514.5 | 514.5 |
| `selection.prompt_outcome` | 2 | 1 | 842.737 | 842.737 | 842.737 | 842.737 |
| `sqlite.agent_runtime_checkpoint` | 2 | 1 | 520.054 | 520.054 | 520.054 | 520.054 |
| `sqlite.agent_session_save` | 18 | 9 | 841.386 | 7069.361 | 7069.361 | 7069.361 |
| `sqlite.card_read` | 18 | 9 | 580.509 | 1677.771 | 1677.771 | 1677.771 |
| `sqlite.pr_supervisor_capability_write` | 2 | 1 | 521.589 | 521.589 | 521.589 | 521.589 |
| `sqlite.pr_supervisor_completion_due_read` | 24 | 12 | 695.891 | 2297.785 | 2297.785 | 2297.785 |
| `sqlite.pr_supervisor_event_write` | 2 | 1 | 500.492 | 500.492 | 500.492 | 500.492 |
| `sqlite.pr_supervisor_watch_read` | 14 | 7 | 719.402 | 826.65 | 826.65 | 826.65 |
| `sqlite.pr_supervisor_watch_write` | 22 | 11 | 665.142 | 2159.708 | 2159.708 | 2159.708 |
| `sqlite.project_read` | 14 | 7 | 589.559 | 1035.323 | 1035.323 | 1035.323 |
| `sqlite.prompt_acceptance` | 2 | 1 | 632.179 | 632.179 | 632.179 | 632.179 |
| `sqlite.prompt_acceptance_read` | 6 | 3 | 1183.824 | 1437.119 | 1437.119 | 1437.119 |
| `sqlite.repository_read` | 10 | 5 | 615.898 | 1083.295 | 1083.295 | 1083.295 |
| `sqlite.restart_handoff_completion` | 4 | 2 | 521.867 | 903.058 | 903.058 | 903.058 |
| `sqlite.restart_handoffs_pending` | 2 | 1 | 632.356 | 632.356 | 632.356 | 632.356 |
| `sqlite.transcript_append` | 100 | 50 | 703.002 | 2102.6 | 3201.649 | 3201.649 |
| `sqlite.transcript_message_page` | 4 | 2 | 702.936 | 2626.369 | 2626.369 | 2626.369 |
| `sqlite.transcript_read` | 12 | 6 | 832.269 | 2877.772 | 2877.772 | 2877.772 |
| `startup.openapi` | 2 | 1 | 936.707 | 936.707 | 936.707 | 936.707 |
| `sync.reconcile_head` | 50 | 25 | 1040.655 | 4764.322 | 6011.065 | 6011.065 |
| `sync.ref_read` | 2 | 1 | 923.537 | 923.537 | 923.537 | 923.537 |
| `workspace.collect_garbage` | 10 | 5 | 7958.728 | 12012.978 | 12012.978 | 12012.978 |
| `workspace.lease_renew` | 6 | 3 | 1216.492 | 1511.412 | 1511.412 | 1511.412 |
| `workspace.project_provision` | 6 | 3 | 8346.696 | 9420.835 | 9420.835 | 9420.835 |
| `workspace.reconcile_terminal_state` | 2 | 1 | 2283.654 | 2283.654 | 2283.654 | 2283.654 |

## Preserved prior coverage and failures

# Owned schema filtering and coordinated dependency evidence — 2026-10-18

All **588 findings and 92 concerns remain open**. Every prior586row, owner and next action is preserved. [Full source/test/prototype/mixed/dependency evidence](benchmarks/systemic-owned-schema-filter-20261018.json) retains all measurements, rejected variants and gaps. PR541 exact head/merge trees and five-success head/main CI are verified; integration is separate from activation.

The schema encoder filters only its freshly owned JSON-mode model dump without copying the container graph again. Full OpenAPI validation/serialization, null/private-key filtering, array nulls and general/version fallback semantics remain. Live model input stays unchanged and concurrent encodings are independent. **58 tests and 9 subtests pass**, with five existing warnings retained. No readiness, deadline, dependency, permission, durability or admission policy changes.

Eight alternating complete-schema pairs preserve all482paths and identical canonical hashes. Wall p50/p95/p99/max baseline **721.296/1007.022/1007.022/1007.022ms**, source **701.311/961.695/961.695/961.695ms**. Adapter counts remain200/200. Warm-process/uncontrolled-host timing is modest and not complete startup acceptance; the earlier recursive hash-memo prototype regressed and remains rejected.

The exact-wheel100iteration mixed fixture retains9600ordered active events and verifies12511cold content hashes. It records **1 worker warning and 8 universal500ms observations**; every ordinary/diagnostic failure, timeout and rejection counter, raw timer, route, response byte, SQL/lock/GC/native phase, startup record and observer cost remains in the full artifact. The113.094s workload completes3063ordinary operations and100diagnostics with zero failures/timeouts/rejections. The difference from the preceding13warning/19observation stage is not a paired comparison and cannot establish a global source speedup. This is not whole-PA performance acceptance. No unchanged replay forgreen.

Three premeasurement slow records remain separate: startup.openapi778.388ms (worker770.947ms/threadCPU687.382ms), sync.ref_read869.453ms (worker1.399ms/delivery864.910ms), and dispatch.followup_pending_read867.204ms (worker0.101ms/delivery866.708ms). Full cold-start acceptance still fails the unchanged500ms observation target. The fast-service completion delays are not evidence of slow SQL; GIL/OS scheduling attribution remains unverified.

| Current residual | n | p50 ms | p95 ms | p99 ms | max ms | ≥500 ms |
|---|---:|---:|---:|---:|---:|---:|
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 191.042 | 408.538 | 604.457 | 637.034 | 3 |
| sync.object_collect | 10 | 414.172 | 1465.766 | 1465.766 | 1465.766 | 4 |
| maintenance.run | 1 | 18389.186 | 18389.186 | 18389.186 | 18389.186 | 1 |

The prior installed0d/loaded2e mismatch is preserved. Fresh later status reports0d/PID45958, and the two served fork JavaScript files hash-match0d under one process fence. Existing PR534 owner reports activation healthy; this session did not install or restart. Fork requirement36140832 remains unaccepted; its current scheduler intent was blocked with genericHTTPStatusError. Exact-key receipt reconciliation confirmed no dispatch admitted at the observed head; no new verifier or replay was started.

User steering removes the missing historical backup payload as a blanket blocker to feasible repairs, prospective reliability/performance validation and independent verification. Its exact existing-owner prompt receipt now reportscompleted; this is not proof that all owner work is done. Preserve the historical recovery limitation, all eight archives/history and unchanged independent acceptance requirements. Do not fabricate bytes, silently omit data or destructively repair references. Existing backup0331/18fc retains its bounded scope; database89957accepted original scope stays closed.

The same prompt is absent from the native four-entry turn inventory and latest backup progress remains986/21:32. Report3f1c34b1 retains that observability discrepancy; completed receipt, provider-turn visibility and applied work are separate claims. No steering replay or replacement worker was started, and this gap does not block feasible systemic work.

Unshipped native-JSON timing and its concurrency semantic counterexample, instrumented startup phase/profile results, syntax/comparator/preparer/build failures and all measurement gaps are retained. None establish an underlying GIL/OS/disk/busy cause or deployed acceptance. Every remaining route, operation, page, startup/drain, native-commit, payload, provider/peer, log-loss/retention and unexercised-branch obligation stays open under systemic17d034a6. All original attachments remain.

## Preserved prior coverage and failures

# Session selection snapshot and native-WAL coverage — 2026-10-18

All **586 findings and 90 concerns remain open**. Every prior585row/owner/action is preserved; one verifier-integrity gap is added. [Complete source, tests, every prototype/timing and mixed measurement](benchmarks/systemic-session-list-snapshot-20261018.json) and [native-WAL diagnostic including its original failed verifier](benchmarks/systemic-native-wal-diagnostic-20261018.json) retain the full evidence.

Active/chats/all sidebar views now select using a read-only metadata type, then decode complete selected rows from the same SQLite read. Metrics, full configuration, selected-session inclusion, pin/human-activity ordering and live runtime precedence remain. Activity workflow filtering and custom Store contracts keep their original paths; full session APIs remain complete. Unselected JSON is opaque, not validated by this sidebar. Raw SQLite rows/bytes are still read and response bytes are unchanged: this bounds decode/model work, **not scan, memory or payload-size acceptance**.146tests and7subtests pass, including exact summary/full response, filters, snapshot consistency, live precedence, selected decoder/error and extension contracts.

Ten initial-source alternating pairs per limit preserve identical serialized responses. A later two-check custom-decoder guard leaves the default algorithm unchanged, but its extra caller cost is unmeasured by these pairs; hashes remain initialsource, not relabeledfinal. Both instance/subclass extension regressions fail against the pre-guard function; all146finaltests pass, and a separate fresh finalsource mixedrun follows. At16rows/1,085,733bytes, baseline wall p50/p95/p99/max233.854/296.053/296.053/296.053ms →75.490/86.898/86.898/86.898ms. At250rows/16,965,420bytes,407.889/673.181/673.181/673.181ms →352.294/438.815/438.815/438.815ms; one pair regression remains. At n10 p95/p99 equal max. The first model-based prototype regressed default250p95592→815ms and remains rejected/unshipped. Timing is warm same-process on an uncontrolled host; no global speedup or acceptance claim. The large unchanged payload remains a concern.

The exact-source100iteration mixed run preserves9,600ordered active events and verifies12,511cold hashes. It records **13worker warnings and 19universal500ms observations**; all failure/timeout/rejection counters,44timers/24routes, phases, bytes, boot records and journal overhead are retained in the artifact. In the separately retained initialsource run, one approved1second stack sample inspected only that private fixture during cold seeding; the finalsource run had no sampler; symbolication mayoverlap start of steady work and adds unmeasured observer cost. Sparse native stack symbols do not explain the whole prepdelay; the5,000event expired-seed batch lacks an explicit timer, a retained gap. No unchanged replay forgreen.

The corrected final mixed run completed **3,085 ordinary operations plus 100 diagnostics with zero failures/timeouts/rejections**, but it is not performance acceptance. Three premeasurement slow records remain separate: schema723.913ms, sync-ref813.736ms (service1.777ms/delivery810.469ms), and follow-up pending-read811.915ms (service0.097ms/delivery810.088ms). Fast-service completion delays are not evidence of slow SQL. No unchanged replay forgreen.

| Final residual family | n | p50 ms | p95 ms | p99 ms | max ms | ≥500 ms |
|---|---:|---:|---:|---:|---:|---:|
| sqlite.execution_binding_materialize | 100 | 75.746 | 203.991 | 879.467 | 2068.02 | 2 |
| sync.push_transaction | 100 | 76.148 | 213.13 | 466.496 | 787.752 | 1 |
| transcript.drain | 300 | 114.759 | 226.457 | 864.139 | 1984.652 | 7 |
| sync.reconcile_head | 100 | 55.235 | 173.94 | 322.284 | 2283.742 | 1 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 195.593 | 415.091 | 693.955 | 871.615 | 3 |
| sync.object_collect | 10 | 334.663 | 1535.611 | 1535.611 | 1535.611 | 4 |
| maintenance.run | 1 | 17064.114 | 17064.114 | 17064.114 | 17064.114 | 1 |

Complete slow phases retain native-commit/SQL/lock-wait propagation, including execution-binding wait1,937.727ms and reconcile wait1,792.090ms. The final mixed result is worse than the initial2warning/10observation stage (now13/19); neither host variation nor the small extra identity guard is established as its cause. Both stages remain in full; no global performance gain is claimed.

The separate native-WAL fixture retains6,144ordered events and291commit/close observations. Projection commits max13.108ms, transcript commits22.939ms; checkpoint progress appears in4/5intervals respectively. This **does not reproduce or explain the prior580ms commit tail**. Physical disk/syscalls/successfulbusy/GIL attribution remains unverified; checkpoint/durability policies are unchanged. The original verifier requested2,048events from one clamped page and failed before saving numerics. Its failure/unknown observations remain preserved; the corrected distinct run persists beforevalidation and paginates. Read-only header interpretation follows [SQLite's format table](https://www.sqlite.org/walformat.html#walidxhdr) and [source schematic](https://github.com/sqlite/sqlite/blob/master/src/wal.c), with duplicate-header/version guards and explicit race limits.

PR540 exacthead54c176/strictmergef1d5/newmainCI39166580290 are all independently verified green. Original537/538main failures and539initialhead failure remain failed and retained. New session-list source is not yet committed/integrated/deployed; no installation/restart/default/permission/live-data mutation or acceptance occurred. Original527failedhandoff and separate2eprocess proof remain distinct. Every nativecommit/schemaCPU/payload/provider/peer/log/receipt/backup/unobserved and normal independent acceptance obligation stays open.

## Preserved prior coverage and failures

# Corrected scalar-default source and complete residuals — 2026-10-18

All **585 findings and 89 concerns remain open**. Every retained row, owner and action is preserved. [Complete source, test, paired timing, mixed workload and callback evidence](benchmarks/systemic-schema-scalar-source-20261018.json) includes every measured operation, route, phase, byte counter and residual, with source/merge/deployment states separate.

The local source skips a TypeAdapter only for exact None/bool/int/str defaults without configured custom JSON encoders, within the existing framework-version fences. It uses the same native JSON conversion. Custom encoders, subclasses and all other values retain upstream behavior. The rejected unconditional prototype and custom-encoder counterexample remain in prior evidence. No request validation, readiness/admission, warning/deadline, durability or dependency policy changes. **52 tests and 9 subtests pass**, including 15 new scalar/fallback/full-schema contracts; a wheel builds without installation.

Eight alternating complete-schema pairs preserve all 482 paths and identical hashes without warnings. Wall p50/p95/p99/max: baseline **583.221/665.964/665.964/665.964 ms**, candidate **488.966/648.862/648.862/648.862 ms**. Three individual pair regressions remain. Separate untimed adapter counts fall **1,912→200**. These are warm schema calls on an uncontrolled host, not full cold-start or whole-PA acceptance.

The exact-source 100-iteration mixed run verifies 9,600 ordered active events and 12,511 cold content hashes, with zero failures/timeouts/rejections. It retains **5 worker warnings, 11 universal 500 ms observations and 2 premeasurement slow records**. The 80.033 s run includes observer work; 3.200 s validation is separate. All 44 timers, 24 registered route groups, 50 ordinary operation counters plus separate diagnostics, complete phases and bytes are in the artifact. Journal persistence adds 1.647 s total/max32.809 ms; it is flushed, not fsynced. No whole-path speedup claim versus the earlier 74.233 s fixture.

| Remaining timer | Samples | p50 ms | p95 ms | p99 ms | Max ms | ≥500 ms |
|---|---:|---:|---:|---:|---:|---:|
| Transcript drain | 300 | 83.478 | 181.824 | 331.565 | 918.646 | 3 |
| Sync reconcile | 100 | 43.994 | 153.807 | 277.447 | 539.906 | 1 |
| Message-boundary history | 100 | 132.106 | 328.476 | 449.210 | 615.494 | 1 |
| Object collection | 10 | 314.758 | 1408.555 | 1408.555 | 1408.555 | 4 |
| Fleet page | 100 | 82.003 | 162.502 | 319.889 | 673.713 | 1 |
| Maintenance, inclusive | 1 | 14294.063 | 14294.063 | 14294.063 | 14294.063 | 1 |

A captured transcript compatibility-mirror commit takes 580.172 ms while holding the projection lock for 580.563 ms. Subsequent appends wait 475.489/482.129 ms and reconciliation waits 487.154 ms on that same boundary: shared propagation is confirmed. The native commit cause remains unverified; zero busy failures and logical/rusage counters cannot rule out successful busy waits or establish physical I/O. Full timelines and GC overlap selection are retained. No durability/checkpoint policy changed.

Premeasurement schema startup remains 505.456 ms, and a 1.767 ms sync-ref service has 560.828 ms completion-delivery delay. A separate direct callback diagnostic observes only four callbacks ≥10 ms (max42.694 ms) and a 649.098 ms schema-worker warning; it does not reproduce or explain the earlier delivery delay. No GIL/lock cause inferred. Schema CPU, native commit/WAL/I/O attribution, remaining leaf/route/provider/peer/log/backup coverage and deployed acceptance remain open.

PR539 corrected exact head, strict merge proof and new main CI39165480869 are now all verified green (five jobs). Original537/538main failures and initial539head failure remain failed and preserved. These fixture/test corrections do not establish production acceptance. This scalar source is not yet committed/integrated/deployed; no install/restart/default/permission/live-data mutation occurred. Original527 failed handoff remains failed, distinct from the separately verified2e process; its scoped recovery response supplies no new restart authority.

## Preserved prior coverage and failures

# Snapshot test timing attribution — 2026-10-18

All **585 findings and 89 concerns remain open**. Every prior584row/owner/action remains unchanged. [Exact failed-head phases and corrected test evidence](benchmarks/systemic-snapshot-request-timing-20261018.json) preserve the original CI result and distinguish fixture setup from the production request.

Initial PR539 head `d96075e1` CI39164667883 passed four jobs and the settings/UX isolation cases, but failed an unchanged metadata snapshot responsiveness assertion. Its existing diagnostic message reports setup249.496ms, request3.306ms, teardown0.012ms, full loop-threadCPU248.757ms; total252.814ms exceeded the200ms assertion. The actual request completed within its existing200ms async deadline. The assertion's clock started before entering three mock contexts. Exact interpreter/GC/allocation cost within setup remains unverified; this is not evidence of a252ms production snapshot call.

The corrected assertion measures from the already recorded request-start timestamp. Both the200ms request assertion and `asyncio.wait_for` deadline remain unchanged, along with zero transcript reads, preserved4,096buffer entries, no transcript payload and correct sequence. Full setup/teardown/CPU diagnostics remain. The changed target test passes3.94s; the five unchanged fixture/browser cases from the prior stage remain separately validated. This is a test-boundary correction, **not a252→3ms product speedup or performance acceptance**. No application threshold, warning policy or product code changed.

The original head failure is retained, not retried. Corrected PR539 scope includes fixture singleton isolation and snapshot request timing; it still requires new exact-head CI, stable supervisor, independent current-base/review validation, exact integration and newmain verification. All prior startup/background/liveFleet/provider/peer/log/receipt/browser/backup/unexercised and normal independent acceptance obligations remain. No deployment/restart/default change/acceptance/live-data mutation occurred; corrected scalar-default CPU source remains unimplemented.

## Preserved prior coverage and failures

# Confirmed portable-fixture store lifetime failure — 2026-10-18

All **584 findings and 88 concerns remain open**. Every prior583row, owner, action and failure is preserved. [Complete before/after, CI, source and browser evidence](benchmarks/systemic-settings-fixture-isolation-20261018.json) separates this fixture repair from live PA performance and acceptance.

PR538 merged as `5a3f85a9` after five successful head checks, stable supervisor nine observations, independent current-base/rules/review/attribution validation and an exact parent/tree match. Its new main CI39163921671 failed the same390px UX case; the other four jobs passed. New diagnostics show correct Agent focus/selection but a panel error. Both537and538CI traces identify an actual SQLite error in the Agent partial: Fleet local repositories call a workspace reader whose store cannot open its database. The focus precondition did not repair this underlying binding failure; both failed main runs remain unchanged.

The settings/UX fixture creates a new app without resetting the process agent singleton. If an earlier test's TemporaryDirectory has expired, the new app reuses that manager and its old workspace/store binding. A deterministic private normal-lifetime reproduction confirms the prior manager survives, its database is absent, the new app's database exists, and its threaded admission read raises the same OperationalError. After test-only singleton/store reset before boot, a new manager and workspace reader bind to the current store and admission reads succeed. Both probes report zero network attempts; all roots are guarded temporary directories. This is a confirmed fixture cause, not proof of production database loss or every historical SQLite warning.

The new regression fails against immutable5a3fixture source. On the corrected source, that regression plus all four settings/UX browser journeys pass:fivecases/338warnings40.31s. It runs the normal lifespan and verifies a real Agent partial returns200with the ux-worker execution after the earlier fixture directory expires. No production manager/Kernel/admission/durability/default/permission change, warning suppression, generic exception fallback, input retry or timeout increase was made.

The original functional-before probe redirected stdout but not stderr; bounded fixture SQLite stacks and708.8/709.9msstartup/read warnings reached tool output. No separately hashed local stderr exists, so its stdout hash does not claim complete console coverage. Subsequent probe/test/CI consoles are local only with both streams retained. These functional samples are not performance acceptance.

Corrected scalar-default work remains unimplemented. Its fifteen unique prototype contracts now pass after correcting two expected serialization strings to reflect chained int→str custom encoders; base/candidate equality had already passed, and both original failures remain. The480direct comparisons and complete482path schema parity remain separate functional evidence. The unsafe unconditional variant, its counterexample, final-pair regression and531msresidual are retained; corrected-source timing/full startup validation is still required.

All previous three preboot slow calls, five background threshold observations, four live2eFleet violations, base-race limitation, provider/MCP/workspace/peer/log/receipt/browser/backup/unexercised concerns and normal PA independent acceptance remain required. No deployment/restart/live archive or store mutation/acceptance/default change occurred. Follow-up review/integration/newmain gates remain open before continuing startup source repairs.

## Preserved earlier inventory and failure history

# Concurrent integration, mobile CI and remaining schema CPU — 2026-10-18

All **583 findings and 87 concerns remain open**. The prior580rows, owners, actions and evidence are preserved. [Full integration and CI evidence](benchmarks/systemic-pr537-integration-followup-20261018.json) and [every schema probe/counterexample](benchmarks/systemic-schema-scalar-default-probe-20261018.json) separate current verification from historical measurements.

PR537 head `7766a227` passed five checks, stable supervisor eight observations and independent current-base/rules/review/attribution checks. Backup PR536 merged `9e0173f0` at20:21:34Z between the final `af14` base check and PR537 merge `a264b5c1` at20:21:43Z. The strict postmerge equality/parent proof **failed and remains unfulfilled**. A separate read-only audit confirms all1,070tracked path/mode/blob entries equal the disjoint536+537union; it does not retroactively establish a current-base gate or atomically solve the head-only merge API's base race. The existing backup owner has a durable queued coordination prompt, not claimed consumed; ownership and data-recovery limits are unchanged.

On the exact combined build,147tests+16subtests passed; five cases were blocked by an overly narrow runner guard because they created their own valid temporary directories. After tracking owned TemporaryDirectory constructors, only those five cases were rerun and passed14.23s. Thus152unique combined storage/health/GC/retention/backup contracts pass. All initial guard failures remain, with source unchanged and raw logs local only. No production mutation, recovery or acceptance is implied.

Combined main CI39162953668 has four successful jobs and one failed mobile390px UX journey. It timed out after ArrowRight while waiting for execution detail; the failed shard retains953passes/1skip/540warnings/98subtests179.02s. One instrumented unchanged mobile diagnostic passed12.90s/97warnings and saw correct focus/Agent selection plus a200Agent partial response in102.064ms. It did not reproduce or close the original CI cause, and added observation can change timing. The initial helper import failed before browser/server launch; the corrected diagnostic is separately recorded. There is no same-head CI retry or production-cause claim, and this failure is distinct from the fork fetch stall and visualization frame detachment.

The test-only follow-up checks that Summary has native focus before keyboard input. Focus and lazy-tab completion share the existing15-second transition budget. Every original keyboard/accessibility/layout/preference assertion remains; bounded timeout state records only focus/selection/loading counts. It adds no forced focus, input retry, sleep, product code or deadline increase. Both changed desktop/mobile journeys pass30.33s;216warnings are retained. Follow-up exact-head CI, stable supervisor, independent review, integration and newmain checks remain required.

A new schema-only profile (before536integration) confirms repeated default-serialization setup:1,912Pydantic schema-generator initializations; profiled1,513ms includes instrumentation overhead. Four alternating complete schema pairs are exactly equal, but the unconditional scalar prototype is **not safe to ship**: custom JSON encoders can transform primitive defaults. Baseline wall samples were587.748/594.036/560.767/478.720ms; prototype434.268/451.496/467.300/531.098ms, retaining the final-pair regression and500msresidual. A corrected functional-only guard preserves upstream whenever custom encoders are present;480value/warning/error comparisons pass. That corrected guard is unimplemented and not newly timed. Full startup/concurrency/readiness and host scheduling remain open.

The earlier health mixed fixture remains its exact7766/af14source, not relabeled as combined/deployed acceptance. Three preboot slow calls, five background threshold observations, four live2eFleet violations and every prior nativeCPU/disk/SQL/provider/MCP/peer/log/receipt/browser/backup/unexercised requirement remain. The original failed527 receipt and scoped process-only decision remain unchanged; no install/restart/independent acceptance/cleanup/default change occurred.

## Preserved prior coverage and failure history

# Fresh-health reference batching source evidence — 2026-10-18

All **580 findings and 84 concerns remain open**. Every earlier row, owner, action and failure is retained; current-status overrides point to the [complete source and mixed evidence](benchmarks/systemic-health-reference-source-20261018.json). PR535 is now integrated as `af14c179`, with all five head and new main checks successful. The original PR533 main failure remains recorded; the readiness repair changes only its test.

Fresh transcript health iterated one observed cursor step per cold reference. The repair uses bounded batches of128rows with the **same DISTINCT query and complete set**. Fresh missing-file and integrity checks remain uncached; GC, writes, locking, retention, warning thresholds and deadlines are unchanged. The actual function body matches the earlier successful prototype. That probe reduced observed execute/fetch calls12,515→102 for the same12,511rows; these are fetch steps, not fewer SQL queries. Its four worker warnings and concurrent CPU regression remain published below.

Six new contracts cover empty/single/multiple batches, duplicate references, hot NULLs, fresh missing-file detection/restoration and failure propagation without partial health. Final storage/GC/retention tests pass61cases; disjoint backup suites pass65cases+16subtests:126unique tests. Initial fixture failures reused an event id; the corrected fixture creates a new id and retains all count assertions. Raw logs remain local with hashes; no production data was changed.

One exact438source/38template wheel fixture ran100iterations in74.233s on this macbook, Python3.14.2/arm64/10CPUs. It retained9,600ordered active events, verified all12,511cold object hashes, and returned2,900HTTP200responses below500ms. All2,927ordinary and100diagnostic worker calls completed with zero failures/timeouts/rejections/steady slow warnings. This candidate-only run is **not a controlled whole-path speedup comparison or deployed acceptance**.

Five500ms background observations remain: four object collections (max1,200.539ms), one inclusive maintenance call13,037.839ms. Maintenance phase evidence includes retention object collection5,484.971ms, migration/health4,533.736ms and final fresh health1,702.435ms; parent/child intervals overlap. Kernel boot/seeding separately had three slow calls. Two reads had0.064/0.678ms service but561.164/643.246ms completion delivery; OpenAPI had586.517ms service/568.731ms threadCPU. These support shared scheduling-delay attribution for the reads, not an assertion that every warning is a slow query; exact CPU/GIL/host contribution remains unverified.

Loop last600max173.441ms; GC last256max107.181ms, while retained long-GC records include178.707ms. The full artifact preserves all windows/counters and phases. Logical I/O counters and reported0rusage disk blocks do not establish physical write amplification. The hash-chained journal retains7,274records and added1,500.833ms total observer wall time outside individual result timers. Sequential snapshot capture13.834ms and postmeasurement verification3,523.071ms are separate. Eleven executor sections plus separate diagnostics were captured; absent auxiliary fixture sections cannot be recovered, and the separate live2e21-section snapshot remains distinct.

No install/restart, live backup/archive mutation, provider workload, peer probing or acceptance submission occurred. The original failed527 receipt remains failed; the scoped process-only response remains applied. All prior native SQL/CPU/disk/startup/provider/peer/log-loss/receipt/browser/backup/unexercised findings and normal PA independent acceptance remain required.

| Operation/request | n | p50 ms | p95 ms | p99 ms | max ms | 500ms observations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| / | 100 | 7.413 | 16.585 | 38.079 | 40.154 | 0 |
| /agent | 100 | 52.996 | 116.844 | 171.278 | 229.663 | 0 |
| /api/agent/history/session-0?limit=64 | 100 | 27.785 | 69.616 | 138.647 | 161.841 | 0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 126.112 | 328.1 | 496.954 | 497.68 | 0 |
| /api/agent/preferences | 100 | 3.357 | 7.611 | 11.131 | 71.841 | 0 |
| /api/agent/sessions?view=chats | 100 | 67.241 | 120.183 | 156.337 | 188.466 | 0 |
| /api/cards | 100 | 6.083 | 10.175 | 21.899 | 23.357 | 0 |
| /api/cards/fixture-card | 100 | 3.757 | 7.211 | 15.577 | 19.242 | 0 |
| /api/cards/fixture-card/history | 100 | 20.766 | 61.304 | 75.509 | 96.727 | 0 |
| /api/execution/defaults | 100 | 3.858 | 6.708 | 12.971 | 27.739 | 0 |
| /api/fleet/dispatch-jobs | 100 | 24.011 | 43.151 | 58.991 | 213.755 | 0 |
| /api/fleet/dispatch-queue | 100 | 4.619 | 10.291 | 22.055 | 27.013 | 0 |
| /api/health | 100 | 2.25 | 3.533 | 4.914 | 28.96 | 0 |
| /api/health-journal/status | 100 | 5.502 | 16.171 | 28.958 | 41.715 | 0 |
| /api/notifications | 100 | 3.727 | 11.496 | 19.453 | 21.575 | 0 |
| /api/pr-supervisor/capabilities | 100 | 5.229 | 12.134 | 18.348 | 18.566 | 0 |
| /api/pr-supervisor/watches | 100 | 5.247 | 13.903 | 18.763 | 21.217 | 0 |
| /api/runtime | 100 | 21.777 | 29.447 | 36.373 | 57.281 | 0 |
| /api/runtime?section=sse | 100 | 3.437 | 5.172 | 8.962 | 17.557 | 0 |
| /api/status | 100 | 33.393 | 66.963 | 101.298 | 101.642 | 0 |
| /api/sync/refs | 100 | 3.203 | 8.556 | 21.925 | 40.385 | 0 |
| /api/sync/status | 100 | 9.141 | 30.753 | 50.572 | 56.998 | 0 |
| /api/workspaces | 100 | 4.638 | 11.885 | 24.64 | 29.475 | 0 |
| /fleet | 100 | 75.694 | 164.636 | 293.546 | 352.369 | 0 |
| /partials/home/sections | 100 | 13.096 | 35.869 | 46.667 | 74.224 | 0 |
| /pull-requests | 100 | 20.542 | 58.956 | 73.673 | 82.461 | 0 |
| /settings?section=agent | 100 | 11.911 | 32.969 | 60.251 | 61.702 | 0 |
| /settings?section=backups | 100 | 11.368 | 49.507 | 66.065 | 76.607 | 0 |
| /work | 100 | 8.342 | 21.47 | 41.831 | 42.208 | 0 |
| acp.wire_append | 100 | 3.679 | 12.173 | 16.15 | 34.267 | 0 |
| dispatch.runnable_read | 100 | 1.854 | 7.28 | 11.711 | 22.332 | 0 |
| fleet.overview_cache_read | 100 | 1.668 | 6.767 | 12.701 | 17.66 | 0 |
| fleet.overview_cache_write | 100 | 8.769 | 21.934 | 48.84 | 63.149 | 0 |
| maintenance.run | 1 | 13037.839 | 13037.839 | 13037.839 | 13037.839 | 1 |
| progress.checkpoint_write | 100 | 7.479 | 15.531 | 19.622 | 23.98 | 0 |
| progress.sequence_allocate | 100 | 5.942 | 18.942 | 40.974 | 48.908 | 0 |
| reconciliation.dispatch_read | 100 | 1.643 | 7.844 | 16.204 | 41.692 | 0 |
| sqlite.card_write | 10 | 44.144 | 416.136 | 416.136 | 416.136 | 0 |
| sqlite.execution_binding_materialize | 100 | 49.117 | 133.92 | 198.187 | 276.352 | 0 |
| sync.commit_read | 100 | 3.651 | 9.381 | 18.946 | 31.125 | 0 |
| sync.object_collect | 10 | 263.684 | 1200.539 | 1200.539 | 1200.539 | 4 |
| sync.push_transaction | 100 | 54.775 | 119.165 | 171.952 | 230.746 | 0 |
| sync.reconcile_head | 100 | 41.913 | 142.298 | 227.357 | 259.114 | 0 |
| transcript.drain | 300 | 88.923 | 146.368 | 176.225 | 213.126 | 0 |

## Preserved prior inventory and stage history

# Post-integration CI and fresh-health findings — 2026-10-18

All **580 findings and 84 concerns remain open**. The prior 578 rows, owners, actions and evidence are unchanged.

PR533 merged as `a89284fd` from exact reviewed head `992c6b46`; both trees are `1027513d`. Its five head checks passed, the supervisor reported stable green, independent rules/reviews/attribution/current-base validation passed, and the durable watch confirms merged. Main CI39159151624 then failed one visualization browser test; the other four jobs passed. [Full sanitized failure and readiness correction evidence](benchmarks/systemic-ci533-visualization-readiness-20261018.json) retains the original failure. Main is not claimed green.

After reload, the fixture waited for three iframe elements before resolving a child document. Its initial navigation already required the preview's authenticated ready signal. The narrow correction applies that same condition after reload within the existing15-second wait, retaining saved-state, controls and isolation assertions. It adds no sleep, exception retry, timeout increase or product recovery code. The changed real browser journey passed in38.14s (349 pytest deprecation warnings retained); six backend contracts passed in12.75s. Production source is unchanged. CI frame detachment is not equated with the separately reported fork fetch stall or earlier browser findings.

A separate [fresh-health reference-read prototype](benchmarks/systemic-health-reference-probe-20261018.json) remains **unimplemented**. It batches up to128cursor rows using the identical DISTINCT query and full reference set; fresh filesystem health checks remain uncached. All12,511cold and6,144hot payloads and sequences passed verification. Its observed calls include fetch steps, not distinct SQL statements:12,515→102calls still return12,511rows.

| Disposable health/reference workload | Trials per method | Baseline wall p50 / p95 / max ms | Batched wall p50 / p95 / max ms |
| --- | ---: | ---: | ---: |
| references | 8 | 53.444 / 57.918 / 57.918 | 31.63 / 35.61 / 35.61 |
| health | 8 | 406.497 / 446.639 / 446.639 | 360.626 / 407.461 / 407.461 |
| concurrent | 3 | 712.24 / 2836.348 / 2836.348 | 534.105 / 671.541 / 671.541 |

Concurrent append max was406.094ms versus287.528ms. Four worker slow warnings remain (two per method); batched concurrent health max671.541ms and CPU max506.424ms prevent a passing claim. Reference-probe timers include sorting/digest comparison; full-health timers do not. Small samples, uncontrolled host activity and unmeasured physical I/O limit causal attribution. Filesystem checkpoint creation/modification bounds are retained because producer UTC start/finish were not recorded; exact per-trial durations are available.

Fresh supported card metadata at19:41:28Z shows acceptancefalse under unchanged requirement17d034a6, in Waiting. No acceptance evidence, default change, deployment or restart was submitted. The original failed527 handoff remains failed and the correlated process-only decision remains applied. The original source mixed five violations, observed2e four Fleet violations, peers/log gaps/backup/browser/startup/receipt/unobserved paths and normal independent acceptance remain required.

## Preserved prior inventory

# Rejected GC hypothesis and current integration boundary — 2026-10-18

The [bounded positive-reference aggregate experiment](benchmarks/systemic-gc-positive-aggregate-probe-20261018.json) is **not implemented**. It tested one aggregate row per128hash page, leaving the fresh locked deletion/pin/unlink proof unchanged. All42timing cases and both helper failures are preserved. Earlier rejected fetch-all evidence remains unchanged.

| Disposable workload | Trials/method | Baseline wall p50 / p95 / max ms | Aggregate wall p50 / p95 / max ms |
| --- | ---: | ---: | ---: |
| live | 6 | 195.538 / 202.588 / 202.588 | 168.286 / 186.688 / 186.688 |
| mixed | 6 | 54.069 / 58.581 / 58.581 | 52.554 / 78.327 / 78.327 |
| orphan | 6 | 89.989 / 107.252 / 107.252 | 81.713 / 85.104 / 85.104 |
| concurrent | 3 | 306.878 / 321.049 / 321.049 | 240.428 / 253.409 / 253.409 |

Concurrent append max rose191.518→201.834ms and mixed max58.581→78.327ms in these small samples, despite improvements elsewhere. All-live observed SQLite calls fell12,709→198: those counters include cursor fetch steps and are not distinct query counts; aggregate98rows represent12,511logical reference hashes. No broad improvement is established. The final helper called nonexistent `TranscriptStorage.read` after all timings; cold-payload verification did not complete, the fixture was disposed, and complete correctness is not claimed. The initial source-needle assertion also failed before seeding/timing. Neither failed validation was hidden or rerun for a passing performance sample.

PR533's prior final head1af619c3 passed all5CI39158334146 jobs. Independent validation then stopped on current-base drift(PR862c2f6b/main0d6638cc), before any merge. Normal local mergee9ac00b8 brings PR534's reserved-dispatch context fix into the dedicated branch without conflicts. Scoped integration tests are retained with temporary-root fencing; the resulting evidence head still requires fresh CI, stable supervisor and independent revalidation. The source547mixed result remains labeled as that earlier source, not the later combined tree.

All **578 findings and82concerns remain open**. No product GC edit, install, restart, default/config/backup/admission change or whole-PA acceptance occurred. Every earlier source/live/CI failure, remaining latency and acceptance requirement is preserved.

## Preserved prior inventory

# Exact-source mixed result and remaining violations — 2026-10-18

All **578 findings / 82 concerns remain open**. [Full result, 7,274 verified journal records, logical I/O counters, runtime windows and reproduction source](benchmarks/systemic-fleet-config-mixed-20261018.json) retain the one completed exact547 candidate scenario. Prior artifacts and every coverage row are unchanged.

The 100-iteration fixture ran65.862s on Darwin25.6.0/arm64/Python3.14.2,10CPU/24GiB (Mac16,13), with30sessions carrying128KiB diagnostics and128KiB unrelated configuration proofs each. All9,600active transcript events remain ordered and all12,511retained cold payloads pass independent compressed-content-hash verification. All2,900HTTP responses returned200;2,879worker submissions completed with zero failures/timeouts/cancellations/rejections/slow-worker warnings. This is source-fixture evidence, not live performance acceptance.

Five unchanged500ms-threshold violations remain: message-history1(max556.894ms), sync objectcollection3(max927.791ms), maintenance1(11257.464ms inclusive parent). Fleet100requests max236.661ms; transcriptdrain300max180.394ms; executionbinding100max134.300ms. StartupOpenAPI497.324ms with zero premeasurement slow calls is one full Kernel fixture; actual provider/MCP/peer cold-start acceptance remains open. No before/after wholepath speedup is inferred.

| Explicit measured family/request | Samples | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| `/` | 100 | 6.959 | 10.98 | 27.78 | 29.076 |
| `/agent` | 100 | 50.069 | 109.71 | 184.339 | 268.378 |
| `/api/agent/history/session-0?limit=64` | 100 | 25.784 | 59.397 | 110.07 | 140.702 |
| `/api/agent/history/session-0?limit=64&message_boundaries=true` | 100 | 114.835 | 243.253 | 442.668 | 556.894 |
| `/api/agent/preferences` | 100 | 3.102 | 7.134 | 9.939 | 14.923 |
| `/api/agent/sessions?view=chats` | 100 | 60.21 | 90.394 | 141.787 | 143.467 |
| `/api/cards` | 100 | 5.542 | 10.359 | 13.245 | 16.159 |
| `/api/cards/fixture-card` | 100 | 3.401 | 5.673 | 10.918 | 11.024 |
| `/api/cards/fixture-card/history` | 100 | 17.065 | 41.634 | 72.314 | 100.228 |
| `/api/execution/defaults` | 100 | 3.696 | 5.884 | 8.631 | 29.423 |
| `/api/fleet/dispatch-jobs` | 100 | 22.203 | 31.59 | 49.683 | 65.399 |
| `/api/fleet/dispatch-queue` | 100 | 4.399 | 7.302 | 15.306 | 17.709 |
| `/api/health` | 100 | 2.171 | 3.452 | 4.316 | 7.067 |
| `/api/health-journal/status` | 100 | 5.227 | 9.816 | 18.178 | 22.283 |
| `/api/notifications` | 100 | 3.498 | 7.249 | 15.323 | 17.189 |
| `/api/pr-supervisor/capabilities` | 100 | 4.835 | 10.31 | 16.472 | 20.601 |
| `/api/pr-supervisor/watches` | 100 | 4.947 | 13.072 | 16.538 | 21.196 |
| `/api/runtime` | 100 | 19.899 | 23.496 | 29.176 | 33.543 |
| `/api/runtime?section=sse` | 100 | 3.243 | 4.862 | 9.978 | 13.798 |
| `/api/status` | 100 | 35.527 | 58.519 | 64.646 | 65.362 |
| `/api/sync/refs` | 100 | 2.993 | 5.904 | 15.197 | 16.347 |
| `/api/sync/status` | 100 | 8.274 | 31.892 | 46.664 | 64.996 |
| `/api/workspaces` | 100 | 4.567 | 9.96 | 19.657 | 24.225 |
| `/fleet` | 100 | 69.622 | 164.126 | 228.762 | 236.661 |
| `/partials/home/sections` | 100 | 12.536 | 22.984 | 46.357 | 48.408 |
| `/pull-requests` | 100 | 19.407 | 47.797 | 74.466 | 81.408 |
| `/settings?section=agent` | 100 | 10.853 | 26.848 | 49.668 | 57.808 |
| `/settings?section=backups` | 100 | 10.157 | 26.074 | 58.712 | 65.493 |
| `/work` | 100 | 7.931 | 10.918 | 49.122 | 81.876 |
| `acp.wire_append` | 100 | 3.242 | 11.956 | 31.047 | 34.189 |
| `dispatch.runnable_read` | 100 | 1.815 | 5.48 | 10.924 | 87.243 |
| `fleet.overview_cache_read` | 100 | 1.279 | 6.7 | 11.865 | 13.368 |
| `fleet.overview_cache_write` | 100 | 7.342 | 13.933 | 28.817 | 30.58 |
| `maintenance.run` | 1 | 11257.464 | 11257.464 | 11257.464 | 11257.464 |
| `progress.checkpoint_write` | 100 | 6.977 | 12.946 | 17.097 | 33.923 |
| `progress.sequence_allocate` | 100 | 5.476 | 15.209 | 24.785 | 31.97 |
| `reconciliation.dispatch_read` | 100 | 1.582 | 5.128 | 7.749 | 10.337 |
| `sqlite.card_write` | 10 | 28.494 | 261.398 | 261.398 | 261.398 |
| `sqlite.execution_binding_materialize` | 100 | 50.77 | 110.469 | 132.17 | 134.3 |
| `sync.commit_read` | 100 | 3.142 | 9.3 | 17.888 | 31.806 |
| `sync.object_collect` | 10 | 243.914 | 927.791 | 927.791 | 927.791 |
| `sync.push_transaction` | 100 | 45.86 | 110.711 | 140.053 | 143.862 |
| `sync.reconcile_head` | 100 | 37.301 | 145.902 | 201.195 | 213.505 |
| `transcript.drain` | 300 | 75.751 | 130.073 | 155.227 | 180.394 |

The journal observer added1.436s of awaited off-loop persistence across7,274records (max22.891ms); it sits outside individual result timers but adds workload. Validation took3.550s after the measured window. Host background activity was uncontrolled; one passive load snapshot is not an idle/pressure trace. Logical filesystem bytes/fsyncs, nested lock holds and reported0rusage disk blocks are not physical disk amplification or proof of no contention. The artifact contains all50executor operation and24canonical route-group windows; its11-section executor snapshot plus separate diagnostics is not the distinct live21-section service capture. Missing auxiliary fixture snapshots cannot be recovered retroactively.

Exact547headCI39157944577 passed all5jobs at attempt1 and supervisor observed stablegreen8times, but this subsequent evidence commit still requires fresh exact-head checks and independent final revalidation. Original4c24dispatch449msCI failure and isolated passing diagnostic remain unresolved evidence. No install/restart occurred; original527handoff stays failed. Every other latency/coverage/provider/peer/log/backup/browser/receipt obligation and normal independent acceptance remains required.

## Preserved prior findings

# Fleet configuration decoding and remaining acceptance — 2026-10-18

All **578 findings and 82 concerns remain open**. The prior 576 rows, owners, actions and historical evidence are unchanged. [Complete source comparison, phase probes and CI history](benchmarks/systemic-fleet-config-20261018.json) retain every measured sample and limitation.

Fleet's read-only session summary still decoded unrelated configuration proofs after the earlier metrics projection repair. It now reuses the existing guarded three-key startup projection and the same bounded selection, count and ordering query. Full session APIs (including `include_metrics=False`) retain complete configuration; durable prompt, admission, recovery, receipt and transcript behavior are unchanged. Malformed, duplicate-key and legacy JSON decoding keeps its existing behavior. The presenter matches complete reads exactly in the fixture.

| 160 retained sessions; 80 selected; 20 alternating pairs | Full configuration | Implemented summary |
| --- | ---: | ---: |
| Decoded configuration bytes/read | 10,510,504 | 21,704 |
| Wall p50 / p95 / p99 / max (ms) | 187.859 / 262.747 / 312.758 / 312.758 | 72.048 / 137.332 / 233.621 / 233.621 |
| Thread CPU p50 / p95 / p99 / max (ms) | 165.896 / 185.665 / 190.681 / 190.681 | 66.470 / 75.783 / 83.165 / 83.165 |

The exact implementation comparison overlapped the guarded regression run; retain that contention limitation. An earlier separate prototype measured wall p95 237.793→89.511ms. These are disposable leaf measurements, not causal live-page or deployed acceptance. SQLite still parses the stored configuration; decoded bytes are not physical disk or write-amplification measurements. Nested phase probes also retain automatic GC and SQL wall/CPU costs, without claiming isolated lock/GIL/disk causality.

21 focused tests and 148 broader Fleet/Workshop/startup tests plus 2 subtests pass (the broader set includes the focused set). Original evidence-only PR533 head4c24 CI39157390577 failed an unchanged dispatch admission assertion at449.269ms against200ms; other4jobs passed. Initial327e CI39157175222 passed. One immutable exact4c24-source diagnostic passes separately, but the timing cause remains unknown. No deadline/assertion was changed and no same-head CI retry was used.

The completed current2e read-only window still has four Fleet≥500ms requests, maximum1252.611ms. No candidate install, restart or live retest has occurred. The correlated process-verification decision is applied; the failed527 handoff remains failed. Every other startup/transcript/maintenance/provider/peer/logging/backup/browser/receipt/unobserved-path finding and normal independent acceptance remains required.

## Preserved prior inventory

# Current process verification and residual inventory — 2026-10-18

All **576 findings and80concerns remain open**. Original570rows, owners and actions are preserved. The [complete artifact](benchmarks/systemic-current-process-20261018.json) contains exact-process proof, failed receipt, all21runtime sections,95GET outcomes, cached maintenance, control overlays and helper failure history.

The correlated operator authorization permits separate process verification without restarting. The failed PR527 handoff stays failed; original strict handoff gate remains unfulfilled. While awaiting the response, another build became active: source2e285481/packagefb90a5db/PID14454/startupba25d624. It contains527 and has matching438installed source files/52unchangedversions/38compiled templates; original527head/main and currentmain CI are terminalgreen. These observations are not a causal PR527 before/after comparison.

All95ordinaryGETs returned200 with stableprocess; four Fleet pages exceeded500ms (max1252.611ms, decodedresponse~1.13MB). Server-Timing page_context spans276.3–1052.6ms with174.3–627.6ms threadCPU; this separates observed work, not nativebusy/GIL/disk causality. Other sampled routes stayed below500ms. Cached maintenance total35657.499ms, objectcollection6960.062ms/15178SQLcalls, projectionVACUUM8028.342ms, firstmigrationhealth8394.499ms/final2929.654ms remain. Projection reported compactedtrue/statusdeferred/reasonbusy; the complete sequence is retained without normalizing away that status. Missingcold1/migrationrunning remain.

Full runtime contains150primaryoperations/1diagnostic/23families/71routes/62slowrecords;38primaryprocess maxima>=500ms. Positive baseline coverage138/181operations and63/115routes is for this process only, not cumulative;43/52 respectively remain unobserved in it. Session-save7660.515ms/transcript-append8131.766ms/workspaceGC9229.930ms and2failedworkspaceprovisions/15submissions persist, causes unresolved. Earlier stages and all linked backup/peer/log/receipt/browser/unexercised obligations remain. No warning/deadline/default/data/acceptance weakening.

# Exact current-process inventory — 2026-10-18

Supported macbook owner API; source `2e28548166de66131d19b1c8867b9d3f0c871907`, package `sha256:fb90a5db90f19e2859234b175397ebb466b751d80ca494877822418e7d7b7d9b`, PID14454, startup `ba25d624-ebe6-4649-b4a4-04d2ee7c798f`. Runtime UTC2026-10-18T19:03:27.214110+00:00. Every returned numeric section and explicit opaque omission retained.

Correlated operator authorization permits separate process verification without restart; the failed PR527 handoff remains failed and continuation delivery/restart attribution unverified. The original strict handoff gate is unchanged/unfulfilled. External build advance means these are exact2e285481 descendant-process observations, not a causal PR527 retest. One small sequential window of95timedGETs plus2untimed fences; five HTML samples yieldp95=p99=max and first is not guaranteed cold. Full owner-client wall includes auth/transport/read/JSON decode; decoded response bytes are not wire bytes. Per-sample checkpoint writes occur outside timers and add work. No own fixtures/tests overlapped; ordinary host/background and other owners are uncontrolled, no pressure trace. Linked backup owner reports an earlier142.403s bounded test interval; precise alignment with every historical sample is not established. Separate cached maintenance and full21-section runtime snapshots have distinct UTC windows. Process maxima, bounded all-call windows and retained slow-only records are distinct; family all-call percentiles cannot be inferred. Parents include children; wall-minusCPU does not isolate physicalIO/GIL/nativebusy. Zero busy failures does not prove no successful busy waits. Initial proof sandbox socket denial and equivalent repository URL assertion failure remain failed, followed by a complete new successful proof. No timed observations were repeated. All prior CI/browser/peer/log/backup/receipt/testroot/negative/unknown and unexercised-branch obligations remain open; no independent acceptance or fleet-wide audit claim.

## Primary executor

| Operation | Submitted | Slow | Failed/timeout/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| acp.pa_mcp_owner_probe | 11 | 0 | 0/0/0 | 11 | 24.296 | 237.702 | 237.702 | 237.702 | 237.702 |
| acp.provider_resolve | 11 | 0 | 0/0/0 | 11 | 0.496 | 45.977 | 45.977 | 45.977 | 45.977 |
| acp.wire_append | 4437 | 0 | 0/0/0 | 256 | 1.816 | 38.974 | 90.476 | 126.924 | 342.742 |
| acp.wire_init | 11 | 0 | 0/0/0 | 11 | 0.473 | 16.958 | 16.958 | 16.958 | 16.958 |
| agent.history_payload | 7 | 0 | 0/0/0 | 7 | 98.846 | 146.17 | 146.17 | 146.17 | 146.17 |
| agent.project_recovery_availability | 636 | 0 | 0/0/0 | 256 | 1.018 | 19.159 | 39.548 | 88.364 | 123.099 |
| agent.prompt_compose | 11 | 0 | 0/0/0 | 11 | 13.389 | 37.471 | 37.471 | 37.471 | 37.471 |
| agent.provider_resolve | 11 | 0 | 0/0/0 | 11 | 4.697 | 83.192 | 83.192 | 83.192 | 83.192 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 24.658 | 24.658 | 24.658 | 24.658 | 24.658 |
| agent.recovery_sessions | 214 | 2 | 0/0/0 | 214 | 91.601 | 401.675 | 481.623 | 518.434 | 518.434 |
| agent.recovery_snapshot | 7 | 0 | 0/0/0 | 7 | 55.409 | 110.931 | 110.931 | 110.931 | 110.931 |
| agent.session_mkdir | 11 | 0 | 0/0/0 | 11 | 1.634 | 15.166 | 15.166 | 15.166 | 15.166 |
| agent.session_snapshot | 3 | 0 | 0/0/0 | 3 | 93.754 | 319.733 | 319.733 | 319.733 | 319.733 |
| agent.snapshot_payload | 7 | 0 | 0/0/0 | 7 | 22.492 | 75.507 | 75.507 | 75.507 | 75.507 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 1556.545 | 1556.545 | 1556.545 | 1556.545 | 1556.545 |
| conversation.issues | 225 | 0 | 0/0/0 | 225 | 12.879 | 99.375 | 308.896 | 324.116 | 324.116 |
| conversation.session | 225 | 0 | 0/0/0 | 225 | 7.355 | 34.733 | 54.977 | 64.782 | 64.782 |
| dispatch.cancel_check | 7 | 0 | 0/0/0 | 7 | 10.271 | 14.096 | 14.096 | 14.096 | 14.096 |
| dispatch.completion_pending_read | 229 | 0 | 0/0/0 | 229 | 1.442 | 20.091 | 49.319 | 289.334 | 289.334 |
| dispatch.followup_pending_read | 229 | 0 | 0/0/0 | 229 | 1.811 | 9.535 | 35.843 | 123.01 | 123.01 |
| dispatch.idempotency_read | 4 | 0 | 0/0/0 | 4 | 2.527 | 42.575 | 42.575 | 42.575 | 42.575 |
| dispatch.initial_prompt_ack | 2 | 0 | 0/0/0 | 1 | 289.673 | 289.673 | 289.673 | 289.673 | 289.673 |
| dispatch.initial_prompt_bind | 1 | 0 | 0/0/0 | 1 | 56.246 | 56.246 | 56.246 | 56.246 | 56.246 |
| dispatch.prompt_bind | 1 | 0 | 0/0/0 | 1 | 15.51 | 15.51 | 15.51 | 15.51 | 15.51 |
| dispatch.record_complete | 2 | 0 | 0/0/0 | 2 | 63.116 | 65.1 | 65.1 | 65.1 | 65.1 |
| dispatch.record_read | 3716 | 0 | 0/0/0 | 256 | 17.751 | 38.025 | 314.607 | 461.443 | 498.87 |
| dispatch.record_write | 17 | 0 | 0/0/0 | 17 | 57.937 | 394.317 | 394.317 | 394.317 | 394.317 |
| dispatch.response_json | 2 | 0 | 0/0/0 | 2 | 2.541 | 3.514 | 3.514 | 3.514 | 3.514 |
| dispatch.sandbox_admission_failure | 1 | 0 | 0/0/0 | 1 | 37.4 | 37.4 | 37.4 | 37.4 | 37.4 |
| dispatch.session_link | 1 | 0 | 0/0/0 | 1 | 74.354 | 74.354 | 74.354 | 74.354 | 74.354 |
| dispatch.session_reserve | 1 | 0 | 0/0/0 | 1 | 30.853 | 30.853 | 30.853 | 30.853 | 30.853 |
| dispatch.workspace_admission_read | 15 | 0 | 0/0/0 | 15 | 9.977 | 35.17 | 35.17 | 35.17 | 35.17 |
| filesystem.github_credentials_read | 476 | 0 | 0/0/0 | 256 | 1.202 | 18.421 | 51.975 | 92.981 | 92.981 |
| filesystem.sync_head_read | 2 | 0 | 0/0/0 | 2 | 3.636 | 13.358 | 13.358 | 13.358 | 13.358 |
| fleet.overview.activity | 4 | 0 | 0/0/0 | 4 | 99.879 | 255.572 | 255.572 | 255.572 | 255.572 |
| fleet.overview.mcp_bootstrap | 3 | 3 | 0/0/0 | 3 | 5243.9 | 5507.997 | 5507.997 | 5507.997 | 5507.997 |
| fleet.overview.reachability | 78 | 0 | 0/0/0 | 78 | 8.298 | 30.058 | 269.52 | 269.52 | 269.52 |
| fleet.overview.repositories | 4 | 1 | 0/0/0 | 4 | 268.457 | 1151.394 | 1151.394 | 1151.394 | 1151.394 |
| fleet.overview.status | 74 | 0 | 0/0/0 | 74 | 8.384 | 35.51 | 269.612 | 269.612 | 269.612 |
| fleet.overview.sync | 161 | 0 | 0/0/0 | 161 | 31.213 | 119.561 | 228.38 | 298.727 | 298.727 |
| fleet.overview_cache_read | 3256 | 1 | 0/0/0 | 256 | 1.458 | 17.128 | 26.628 | 35.279 | 619.576 |
| fleet.overview_cache_write | 994 | 2 | 0/0/0 | 256 | 9.042 | 59.241 | 156.492 | 246.89 | 900.079 |
| fleet.overview_response_json | 666 | 0 | 0/0/0 | 256 | 2.269 | 20.66 | 90.203 | 113.321 | 298.756 |
| fleet.placement_resolve | 4 | 0 | 2/0/0 | 4 | 421.72 | 484.321 | 484.321 | 484.321 | 484.321 |
| fleet.response_json | 10 | 0 | 0/0/0 | 10 | 1.875 | 19.693 | 19.693 | 19.693 | 19.693 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 11.123 | 11.123 | 11.123 | 11.123 | 11.123 |
| goal.dispatch_admission_trace | 4 | 0 | 0/0/0 | 4 | 0.328 | 8.344 | 8.344 | 8.344 | 8.344 |
| goal.dispatch_assigned_service_identity_bind | 1 | 0 | 0/0/0 | 1 | 4.466 | 4.466 | 4.466 | 4.466 | 4.466 |
| goal.dispatch_execution_identity_restore | 2 | 0 | 0/0/0 | 2 | 2.167 | 3.144 | 3.144 | 3.144 | 3.144 |
| goal.dispatch_lifecycle_index | 39 | 0 | 0/0/0 | 39 | 0.368 | 5.299 | 7.747 | 7.747 | 7.747 |
| goal.dispatch_reservation_release | 1 | 0 | 0/0/0 | 1 | 0.743 | 0.743 | 0.743 | 0.743 | 0.743 |
| goal.dispatch_reservation_validate | 7 | 0 | 0/0/0 | 7 | 5.468 | 29.864 | 29.864 | 29.864 | 29.864 |
| health.canonical_obligation | 161 | 3 | 0/0/0 | 161 | 9.834 | 180.663 | 1350.674 | 1580.459 | 1580.459 |
| history.card_page | 1 | 0 | 0/0/0 | 1 | 19.711 | 19.711 | 19.711 | 19.711 | 19.711 |
| http.dispatch_completion | 2 | 0 | 0/0/0 | 2 | 349.603 | 692.827 | 692.827 | 692.827 | 692.827 |
| http.dispatch_list | 2 | 1 | 0/0/0 | 2 | 61.87 | 541.773 | 541.773 | 541.773 | 541.773 |
| http.fleet_agent | 4 | 0 | 0/0/0 | 4 | 799.879 | 10471.925 | 10471.925 | 10471.925 | 10471.925 |
| http.fleet_dispatch | 2 | 0 | 0/0/0 | 2 | 92.971 | 108.997 | 108.997 | 108.997 | 108.997 |
| http.fleet_overview | 666 | 0 | 0/0/0 | 256 | 83.025 | 420.577 | 830.396 | 982.975 | 1211.187 |
| http.fleet_sync_head | 4 | 0 | 0/0/0 | 4 | 67.935 | 116.36 | 116.36 | 116.36 | 116.36 |
| http.github | 2 | 0 | 0/0/0 | 2 | 330.735 | 897.709 | 897.709 | 897.709 | 897.709 |
| http.pr_supervisor_peer | 3672 | 0 | 0/0/0 | 256 | 39.64 | 142.078 | 519.119 | 659.789 | 1086.705 |
| http.session_list | 146 | 1 | 6/0/0 | 146 | 32.526 | 199.274 | 416.509 | 645.617 | 645.617 |
| intake.web_prompt | 3 | 0 | 0/0/0 | 3 | 47.328 | 298.104 | 298.104 | 298.104 | 298.104 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 35671.841 | 35671.841 | 35671.841 | 35671.841 | 35671.841 |
| notifications.list_page | 89 | 0 | 0/0/0 | 89 | 1.922 | 26.976 | 281.008 | 281.008 | 281.008 |
| notifications.present_page | 89 | 3 | 0/0/0 | 89 | 54.454 | 371.708 | 1896.894 | 1896.894 | 1896.894 |
| pr_supervisor.github_response_json | 2 | 0 | 0/0/0 | 2 | 0.554 | 57.879 | 57.879 | 57.879 | 57.879 |
| pr_supervisor.response_json | 3672 | 0 | 0/0/0 | 256 | 0.751 | 13.513 | 22.218 | 48.365 | 429.718 |
| preferences.agent_auto_approve_read | 36 | 0 | 0/0/0 | 36 | 3.289 | 37.993 | 39.575 | 39.575 | 39.575 |
| preferences.agent_surface_read | 2 | 0 | 0/0/0 | 2 | 4.081 | 6.511 | 6.511 | 6.511 | 6.511 |
| progress.checkpoint_write | 73 | 1 | 0/0/0 | 73 | 46.3 | 331.438 | 811.682 | 811.682 | 811.682 |
| progress.dispatch_read | 480 | 0 | 0/0/0 | 256 | 19.516 | 84.927 | 151.088 | 428.294 | 428.294 |
| progress.final_report_build | 8 | 0 | 0/0/0 | 8 | 1.04 | 16.771 | 16.771 | 16.771 | 16.771 |
| progress.final_report_write | 8 | 0 | 0/0/0 | 8 | 49.41 | 82.696 | 82.696 | 82.696 | 82.696 |
| progress.heartbeat_write | 54 | 1 | 0/0/0 | 54 | 56.116 | 204.227 | 606.098 | 606.098 | 606.098 |
| progress.pending_read | 431 | 2 | 0/0/0 | 256 | 6.522 | 17.978 | 36.814 | 726.914 | 726.914 |
| progress.sequence_allocate | 127 | 0 | 0/0/0 | 127 | 38.036 | 165.975 | 303.478 | 384.964 | 384.964 |
| provider.status.codex | 4 | 4 | 0/0/0 | 4 | 1271.861 | 1474.794 | 1474.794 | 1474.794 | 1474.794 |
| provider.status.cursor | 4 | 4 | 0/0/0 | 4 | 2129.657 | 2482.496 | 2482.496 | 2482.496 | 2482.496 |
| provider.status.openinterpreter | 4 | 2 | 0/0/0 | 4 | 301.726 | 1438.948 | 1438.948 | 1438.948 | 1438.948 |
| reconciliation.card_read | 1 | 0 | 0/0/0 | 1 | 13.799 | 13.799 | 13.799 | 13.799 | 13.799 |
| reconciliation.completion_evidence_read | 678 | 0 | 0/0/0 | 256 | 3.208 | 30.676 | 76.91 | 92.509 | 144.934 |
| reconciliation.completion_queue | 7 | 0 | 0/0/0 | 7 | 29.119 | 89.287 | 89.287 | 89.287 | 89.287 |
| reconciliation.dispatch_read | 1147 | 1 | 0/0/0 | 256 | 1.647 | 12.754 | 61.784 | 137.559 | 1081.743 |
| reconciliation.dispatch_write | 5 | 1 | 0/0/0 | 5 | 60.534 | 901.222 | 901.222 | 901.222 | 901.222 |
| reconciliation.session_read | 681 | 0 | 0/0/0 | 256 | 1.878 | 19.123 | 57.26 | 298.123 | 370.952 |
| reconciliation.supervisor_health | 1 | 0 | 0/0/0 | 1 | 16.058 | 16.058 | 16.058 | 16.058 | 16.058 |
| selection.attempt_authority | 7 | 0 | 0/0/0 | 7 | 6.397 | 33.995 | 33.995 | 33.995 | 33.995 |
| selection.card_read | 1 | 0 | 0/0/0 | 1 | 2.447 | 2.447 | 2.447 | 2.447 | 2.447 |
| selection.catalog_enrichment | 4 | 0 | 0/0/0 | 4 | 4.562 | 7.569 | 7.569 | 7.569 | 7.569 |
| selection.configuration_receipt | 9 | 1 | 0/0/0 | 9 | 34.239 | 586.459 | 586.459 | 586.459 | 586.459 |
| selection.prompt_identity | 12 | 0 | 0/0/0 | 11 | 43.464 | 122.323 | 122.323 | 122.323 | 122.323 |
| selection.prompt_outcome | 9 | 0 | 0/0/0 | 9 | 26.782 | 83.132 | 83.132 | 83.132 | 83.132 |
| selection.receipt_write | 1 | 0 | 0/0/0 | 1 | 4.392 | 4.392 | 4.392 | 4.392 | 4.392 |
| selection.target_receipt | 1 | 0 | 0/0/0 | 1 | 9.229 | 9.229 | 9.229 | 9.229 | 9.229 |
| selection.target_revalidation | 1 | 0 | 0/0/0 | 1 | 158.198 | 158.198 | 158.198 | 158.198 | 158.198 |
| sqlite.agent_runtime_checkpoint | 41 | 0 | 0/0/0 | 41 | 15.022 | 63.343 | 94.877 | 94.877 | 94.877 |
| sqlite.agent_session_read | 44 | 0 | 0/0/0 | 44 | 6.044 | 36.748 | 45.329 | 45.329 | 45.329 |
| sqlite.agent_session_save | 237 | 3 | 0/0/0 | 237 | 14.53 | 124.757 | 4363.702 | 7660.515 | 7660.515 |
| sqlite.card_read | 5559 | 0 | 0/0/0 | 256 | 1.802 | 32.832 | 81.95 | 255.661 | 486.757 |
| sqlite.card_summary_read | 1 | 0 | 0/0/0 | 1 | 1.013 | 1.013 | 1.013 | 1.013 | 1.013 |
| sqlite.dispatch_queue_admit | 1 | 0 | 0/0/0 | 1 | 14.358 | 14.358 | 14.358 | 14.358 | 14.358 |
| sqlite.execution_binding_materialize | 1 | 0 | 0/0/0 | 1 | 225.664 | 225.664 | 225.664 | 225.664 | 225.664 |
| sqlite.notification_create | 1 | 0 | 0/0/0 | 1 | 43.834 | 43.834 | 43.834 | 43.834 | 43.834 |
| sqlite.notification_list | 2 | 0 | 0/0/0 | 2 | 1.532 | 3.202 | 3.202 | 3.202 | 3.202 |
| sqlite.pr_merge_evidence_read | 2 | 0 | 0/0/0 | 2 | 3.891 | 10.152 | 10.152 | 10.152 | 10.152 |
| sqlite.pr_supervisor_capability_write | 19 | 0 | 0/0/0 | 19 | 4.802 | 59.332 | 59.332 | 59.332 | 59.332 |
| sqlite.pr_supervisor_completion_due_read | 474 | 1 | 0/0/0 | 256 | 13.258 | 117.961 | 404.135 | 547.443 | 547.443 |
| sqlite.pr_supervisor_due_read | 473 | 0 | 0/0/0 | 256 | 3.139 | 22.835 | 55.192 | 98.314 | 98.314 |
| sqlite.pr_supervisor_event_write | 1837 | 0 | 0/0/0 | 256 | 3.114 | 36.712 | 68.97 | 114.457 | 337.512 |
| sqlite.pr_supervisor_watch_read | 1837 | 1 | 0/0/0 | 256 | 4.117 | 57.224 | 156.624 | 187.65 | 955.021 |
| sqlite.pr_supervisor_watch_write | 1837 | 1 | 0/0/0 | 256 | 8.268 | 58.417 | 122.834 | 215.013 | 538.547 |
| sqlite.project_read | 3739 | 0 | 0/0/0 | 256 | 0.892 | 21.602 | 83.559 | 111.486 | 218.85 |
| sqlite.projection_head_read | 2 | 0 | 0/0/0 | 2 | 22.135 | 30.982 | 30.982 | 30.982 | 30.982 |
| sqlite.prompt_acceptance_read | 9 | 0 | 0/0/0 | 9 | 20.05 | 121.464 | 121.464 | 121.464 | 121.464 |
| sqlite.repository_read | 3714 | 0 | 0/0/0 | 256 | 1.198 | 30.995 | 129.198 | 289.254 | 428.395 |
| sqlite.restart_continuation_authorization | 3 | 0 | 0/0/0 | 3 | 1.271 | 1.836 | 1.836 | 1.836 | 1.836 |
| sqlite.restart_handoff_completion | 484 | 0 | 0/0/0 | 256 | 2.907 | 31.246 | 121.766 | 329.518 | 329.518 |
| sqlite.restart_handoff_delivered | 1 | 0 | 0/0/0 | 1 | 5.901 | 5.901 | 5.901 | 5.901 | 5.901 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 8.449 | 8.449 | 8.449 | 8.449 | 8.449 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 10.843 | 10.843 | 10.843 | 10.843 | 10.843 |
| sqlite.restart_handoff_session | 242 | 0 | 0/0/0 | 242 | 1.646 | 7.298 | 32.786 | 51.155 | 51.155 |
| sqlite.restart_handoffs_pending | 215 | 0 | 0/0/0 | 215 | 1.868 | 19.946 | 72.616 | 149.958 | 149.958 |
| sqlite.restart_handoffs_watchdog | 214 | 0 | 0/0/0 | 214 | 1.422 | 22.763 | 113.886 | 439.303 | 439.303 |
| sqlite.transcript_append | 556 | 6 | 0/0/0 | 256 | 25.175 | 272.873 | 551.264 | 1062.77 | 8131.766 |
| sqlite.transcript_message_page | 7 | 0 | 0/0/0 | 7 | 4.429 | 164.506 | 164.506 | 164.506 | 164.506 |
| sqlite.transcript_read | 18 | 0 | 0/0/0 | 18 | 51.93 | 292.933 | 292.933 | 292.933 | 292.933 |
| sqlite.transcript_sequence | 11 | 0 | 0/0/0 | 11 | 4.629 | 147.624 | 147.624 | 147.624 | 147.624 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 3.863 | 3.863 | 3.863 | 3.863 | 3.863 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 238.947 | 238.947 | 238.947 | 238.947 | 238.947 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 761.965 | 761.965 | 761.965 | 761.965 | 761.965 |
| sync.commit_read | 15068 | 1 | 0/0/0 | 256 | 0.626 | 1.399 | 8.144 | 8.474 | 832.821 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 22.313 | 22.313 | 22.313 | 22.313 | 22.313 |
| sync.json_decode | 26 | 0 | 0/0/0 | 26 | 0.703 | 32.717 | 53.237 | 53.237 | 53.237 |
| sync.json_encode | 2 | 0 | 0/0/0 | 2 | 0.279 | 35.571 | 35.571 | 35.571 | 35.571 |
| sync.object_has | 30377 | 0 | 0/0/0 | 256 | 0.178 | 0.604 | 2.518 | 4.295 | 306.651 |
| sync.object_ingest | 2 | 0 | 0/0/0 | 2 | 6.398 | 12.166 | 12.166 | 12.166 | 12.166 |
| sync.peer_http | 26 | 0 | 0/0/0 | 26 | 26.31 | 1115.087 | 1116.747 | 1116.747 | 1116.747 |
| sync.reconcile_head | 24 | 7 | 0/0/0 | 24 | 374.317 | 1071.808 | 2186.88 | 2186.88 | 2186.88 |
| sync.ref_read | 412 | 0 | 0/0/0 | 256 | 0.86 | 15.009 | 59.577 | 137.534 | 137.534 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 29.272 | 29.272 | 29.272 | 29.272 | 29.272 |
| workspace.collect_garbage | 3 | 3 | 0/0/0 | 3 | 5067.366 | 9229.93 | 9229.93 | 9229.93 | 9229.93 |
| workspace.expire_session | 3 | 0 | 0/0/0 | 3 | 3.794 | 5.393 | 5.393 | 5.393 | 5.393 |
| workspace.lease_renew | 12 | 2 | 0/0/0 | 12 | 99.149 | 622.448 | 622.448 | 622.448 | 622.448 |
| workspace.project_provision | 15 | 0 | 2/0/0 | 15 | 1524.128 | 3157.775 | 3157.775 | 3157.775 | 3157.775 |
| workspace.reconcile_terminal_state | 3 | 0 | 0/0/0 | 3 | 88.994 | 1001.16 | 1001.16 | 1001.16 | 1001.16 |
| workspace.scratch_provision | 2 | 0 | 0/0/0 | 2 | 4.147 | 5.205 | 5.205 | 5.205 | 5.205 |
| workspace.session_fence | 2 | 0 | 0/0/0 | 2 | 20.688 | 27.592 | 27.592 | 27.592 | 27.592 |
| workspace.session_leases | 1 | 0 | 0/0/0 | 1 | 20.393 | 20.393 | 20.393 | 20.393 | 20.393 |

## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeout/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 50.274 | 63.818 | 99.36 | 99.36 | 99.36 |

## Prefix families — all-call percentiles unavailable

| Prefix | Names | Submitted | Slow | Failed | Timeouts | Process max ms |
| --- | --- | --- | --- | --- | --- | --- |
| acp | 4 | 4470 | 0 | 0 | 0 | 342.742 |
| agent | 11 | 909 | 3 | 0 | 0 | 1556.545 |
| conversation | 2 | 450 | 0 | 0 | 0 | 324.116 |
| dispatch | 15 | 4228 | 0 | 0 | 0 | 498.87 |
| filesystem | 2 | 478 | 0 | 0 | 0 | 92.981 |
| fleet | 12 | 5255 | 7 | 2 | 0 | 5507.997 |
| goal | 6 | 54 | 0 | 0 | 0 | 29.864 |
| health | 1 | 161 | 3 | 0 | 0 | 1580.459 |
| history | 1 | 1 | 0 | 0 | 0 | 19.711 |
| http | 9 | 4500 | 2 | 6 | 0 | 10471.925 |
| intake | 1 | 3 | 0 | 0 | 0 | 298.104 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 35671.841 |
| notifications | 2 | 178 | 3 | 0 | 0 | 1896.894 |
| pr_supervisor | 2 | 3674 | 0 | 0 | 0 | 429.718 |
| preferences | 2 | 38 | 0 | 0 | 0 | 39.575 |
| progress | 7 | 1181 | 4 | 0 | 0 | 811.682 |
| provider | 3 | 12 | 10 | 0 | 0 | 2482.496 |
| reconciliation | 7 | 2520 | 2 | 0 | 0 | 1081.743 |
| selection | 9 | 45 | 1 | 0 | 0 | 586.459 |
| sqlite | 32 | 21583 | 12 | 0 | 0 | 8131.766 |
| startup | 3 | 3 | 1 | 0 | 0 | 761.965 |
| sync | 10 | 45939 | 8 | 0 | 0 | 2186.88 |
| workspace | 8 | 41 | 5 | 2 | 0 | 9229.93 |

## Every registered route window

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| / | 7 | 155368 | 7 | 28.162 | 326.178 | 326.178 | 326.178 |
| /api/agent/history/{session_id} | 7 | 6131271 | 7 | 150.218 | 602.007 | 602.007 | 602.007 |
| /api/agent/observability/v1/sessions/{session_id} | 13 | 44316 | 13 | 38.406 | 99.555 | 99.555 | 99.555 |
| /api/agent/prompt-capabilities | 1 | 39 | 1 | 8.968 | 8.968 | 8.968 | 8.968 |
| /api/agent/session-events | 2 | 9258 | 2 | 3.138 | 43.435 | 43.435 | 43.435 |
| /api/agent/sessions | 148 | 3479119 | 148 | 35.718 | 277.614 | 7649.089 | 10373.635 |
| /api/agent/sessions/{session_id} | 7 | 2423116 | 7 | 25.866 | 82.563 | 82.563 | 82.563 |
| /api/agent/sessions/{session_id}/browser | 1 | 18 | 1 | 5.592 | 5.592 | 5.592 | 5.592 |
| /api/agent/sessions/{session_id}/commands | 3 | 206934 | 3 | 33.748 | 41.927 | 41.927 | 41.927 |
| /api/agent/sessions/{session_id}/dispatch-prompts | 1 | 425 | 1 | 716.426 | 716.426 | 716.426 | 716.426 |
| /api/agent/sessions/{session_id}/events | 3 | 9391 | 3 | 2.826 | 9.664 | 9.664 | 9.664 |
| /api/agent/sessions/{session_id}/execution-catalog | 5 | 25157 | 5 | 3.234 | 5.039 | 5.039 | 5.039 |
| /api/agent/sessions/{session_id}/issues | 1 | 32873 | 1 | 12.485 | 12.485 | 12.485 | 12.485 |
| /api/agent/sessions/{session_id}/prompt | 2 | 784 | 2 | 244.21 | 2918.152 | 2918.152 | 2918.152 |
| /api/agent/sessions/{session_id}/prompts/{client_prompt_id} | 1 | 295 | 1 | 53.034 | 53.034 | 53.034 | 53.034 |
| /api/agent/sessions/{session_id}/recover | 1 | 7666952 | 1 | 5342.397 | 5342.397 | 5342.397 | 5342.397 |
| /api/agent/sessions/{session_id}/restart-handoffs | 3 | 1235121 | 3 | 21.97 | 23.259 | 23.259 | 23.259 |
| /api/agent/sessions/{session_id}/threads | 224 | 324733 | 224 | 26.004 | 132.359 | 352.785 | 699.996 |
| /api/backups | 1 | 2225 | 1 | 4.673 | 4.673 | 4.673 | 4.673 |
| /api/backups/status | 1 | 56339 | 1 | 11.57 | 11.57 | 11.57 | 11.57 |
| /api/cards/summaries | 2 | 25695 | 2 | 6.915 | 65.132 | 65.132 | 65.132 |
| /api/cards/{card_id} | 11 | 317379 | 11 | 6.793 | 80.097 | 80.097 | 80.097 |
| /api/cards/{card_id}/history | 1 | 14918 | 1 | 22.098 | 22.098 | 22.098 | 22.098 |
| /api/configuration | 2 | 339898 | 2 | 12.342 | 13.729 | 13.729 | 13.729 |
| /api/execution/catalog | 1 | 51947 | 1 | 6585.501 | 6585.501 | 6585.501 | 6585.501 |
| /api/execution/defaults | 1 | 6308 | 1 | 24.981 | 24.981 | 24.981 | 24.981 |
| /api/fleet/dispatch | 3 | 235154 | 3 | 5676.608 | 6751.046 | 6751.046 | 6751.046 |
| /api/fleet/dispatch-jobs | 2 | 2472092 | 2 | 64.202 | 567.008 | 567.008 | 567.008 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 32 | 5132127 | 32 | 12.575 | 79.859 | 86.134 | 86.134 |
| /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint | 4 | 789 | 4 | 122.089 | 453.601 | 453.601 | 453.601 |
| /api/fleet/dispatch-jobs/{dispatch_id}/turn-end | 1 | 338 | 1 | 11.719 | 11.719 | 11.719 | 11.719 |
| /api/fleet/dispatch/materialize | 2 | 2950 | 2 | 77.065 | 91.543 | 91.543 | 91.543 |
| /api/fleet/dispatch/{dispatch_id}/complete | 2 | 7726 | 2 | 305.639 | 645.225 | 645.225 | 645.225 |
| /api/fleet/instances | 180 | 480420 | 180 | 2.356 | 39.128 | 118.238 | 176.863 |
| /api/fleet/instances/{authority_instance_id}/dispatch-jobs/{dispatch_id}/{operation} | 1 | 193819 | 1 | 58.483 | 58.483 | 58.483 | 58.483 |
| /api/fleet/overview | 5 | 5573827 | 5 | 131.309 | 345.239 | 345.239 | 345.239 |
| /api/fleet/overview/local | 223 | 2286250 | 223 | 45.341 | 166.343 | 485.792 | 695.934 |
| /api/fleet/placement/preview | 1 | 74129 | 1 | 6413.187 | 6413.187 | 6413.187 | 6413.187 |
| /api/fleet/session-route/{session_id} | 2 | 3101 | 2 | 4.491 | 11.732 | 11.732 | 11.732 |
| /api/health | 26 | 390 | 26 | 1.909 | 6.815 | 18.001 | 18.001 |
| /api/health-journal/reports | 3 | 964 | 3 | 93.008 | 105.787 | 105.787 | 105.787 |
| /api/health-journal/reports/{report_id} | 1 | 116765 | 1 | 63.116 | 63.116 | 63.116 | 63.116 |
| /api/instance | 3 | 1680 | 3 | 3.921 | 8.543 | 8.543 | 8.543 |
| /api/instance/maintenance | 1 | 7468 | 1 | 5.59 | 5.59 | 5.59 | 5.59 |
| /api/items/{item_id} | 4 | 30925 | 4 | 2.847 | 32.637 | 32.637 | 32.637 |
| /api/notifications | 89 | 1062098 | 89 | 63.978 | 399.794 | 1909.922 | 1909.922 |
| /api/notifications/{notification_id} | 4 | 25335 | 4 | 99.787 | 385.403 | 385.403 | 385.403 |
| /api/notifications/{notification_id}/read | 1 | 5344 | 1 | 35.741 | 35.741 | 35.741 | 35.741 |
| /api/notifications/{notification_id}/respond | 3 | 13051 | 3 | 5584.157 | 5688.639 | 5688.639 | 5688.639 |
| /api/operations/{idempotency_key:path} | 1 | 474 | 1 | 8.438 | 8.438 | 8.438 | 8.438 |
| /api/owner-ready | 11 | 781 | 11 | 2.225 | 34.352 | 34.352 | 34.352 |
| /api/pr-supervisor/capabilities | 8 | 21656 | 8 | 6.022 | 15.043 | 15.043 | 15.043 |
| /api/pr-supervisor/instances/heartbeat | 39 | 663 | 39 | 6.81 | 39.635 | 84.592 | 84.592 |
| /api/pr-supervisor/replicas | 3713 | 20563106 | 256 | 38.18 | 194.977 | 446.418 | 550.045 |
| /api/pr-supervisor/watches | 2 | 11434 | 2 | 3.187 | 3.8 | 3.8 | 3.8 |
| /api/pr-supervisor/watches/{watch_id} | 4 | 64986 | 4 | 22.538 | 34.955 | 34.955 | 34.955 |
| /api/pr-supervisor/watches/{watch_id}/merge-evidence | 2 | 6623 | 2 | 7.805 | 122.232 | 122.232 | 122.232 |
| /api/projects/{project_id} | 1 | 2083 | 1 | 5.899 | 5.899 | 5.899 | 5.899 |
| /api/ready | 223 | 29842 | 223 | 2.603 | 9.434 | 29.223 | 32.622 |
| /api/runtime | 20 | 10282879 | 20 | 65.88 | 78.358 | 113.165 | 113.165 |
| /api/status | 262 | 703424 | 256 | 55.283 | 198.858 | 292.637 | 415.193 |
| /api/sync/get | 40 | 300904 | 40 | 3.147 | 42.258 | 46.263 | 46.263 |
| /api/sync/refs | 11 | 2849 | 11 | 3.185 | 21.926 | 21.926 | 21.926 |
| /api/telemetry/live | 457 | 1439805 | 256 | 2.395 | 4.69 | 7.327 | 79.102 |
| /api/ui/assets | 26 | 2054 | 26 | 10.003 | 60.071 | 117.378 | 117.378 |
| /api/workspaces | 10 | 19576 | 10 | 5.802 | 58.221 | 58.221 | 58.221 |
| /fleet | 5 | 5643467 | 5 | 675.084 | 1211.377 | 1211.377 | 1211.377 |
| /partials/home/sections | 2 | 40076 | 2 | 3847.655 | 3952.821 | 3952.821 | 3952.821 |
| /pull-requests | 5 | 125605 | 5 | 108.859 | 162.488 | 162.488 | 162.488 |
| /{page_path:path} | 16 | 1332079 | 16 | 56.888 | 142.998 | 142.998 | 142.998 |
| __unmatched__ | 30 | 1114329 | 30 | 20.886 | 125.327 | 142.507 | 142.507 |

## Retained slow-record names ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent.recovery_sessions | 2 | 503.948 | 518.434 | 518.434 | 518.434 |
| agent.startup_state_read | 1 | 1556.545 | 1556.545 | 1556.545 | 1556.545 |
| fleet.overview.mcp_bootstrap | 3 | 5243.9 | 5507.997 | 5507.997 | 5507.997 |
| fleet.overview.repositories | 1 | 1151.394 | 1151.394 | 1151.394 | 1151.394 |
| fleet.overview_cache_read | 1 | 619.576 | 619.576 | 619.576 | 619.576 |
| fleet.overview_cache_write | 2 | 869.247 | 900.079 | 900.079 | 900.079 |
| health.canonical_obligation | 3 | 1350.674 | 1580.459 | 1580.459 | 1580.459 |
| http.dispatch_list | 1 | 541.773 | 541.773 | 541.773 | 541.773 |
| http.session_list | 1 | 645.617 | 645.617 | 645.617 | 645.617 |
| maintenance.run | 1 | 35671.841 | 35671.841 | 35671.841 | 35671.841 |
| notifications.present_page | 3 | 653.878 | 1896.894 | 1896.894 | 1896.894 |
| progress.checkpoint_write | 1 | 811.682 | 811.682 | 811.682 | 811.682 |
| progress.heartbeat_write | 1 | 606.098 | 606.098 | 606.098 | 606.098 |
| progress.pending_read | 2 | 661.2 | 726.914 | 726.914 | 726.914 |
| provider.status.codex | 4 | 1271.861 | 1474.794 | 1474.794 | 1474.794 |
| provider.status.cursor | 4 | 2129.657 | 2482.496 | 2482.496 | 2482.496 |
| provider.status.openinterpreter | 2 | 559.722 | 1438.948 | 1438.948 | 1438.948 |
| reconciliation.dispatch_read | 1 | 1081.743 | 1081.743 | 1081.743 | 1081.743 |
| reconciliation.dispatch_write | 1 | 901.222 | 901.222 | 901.222 | 901.222 |
| selection.configuration_receipt | 1 | 586.459 | 586.459 | 586.459 | 586.459 |
| sqlite.agent_session_save | 3 | 5759.064 | 7660.515 | 7660.515 | 7660.515 |
| sqlite.pr_supervisor_completion_due_read | 1 | 547.443 | 547.443 | 547.443 | 547.443 |
| sqlite.pr_supervisor_watch_read | 1 | 955.021 | 955.021 | 955.021 | 955.021 |
| sqlite.pr_supervisor_watch_write | 1 | 538.547 | 538.547 | 538.547 | 538.547 |
| sqlite.transcript_append | 6 | 551.264 | 8131.766 | 8131.766 | 8131.766 |
| startup.openapi | 1 | 761.965 | 761.965 | 761.965 | 761.965 |
| sync.commit_read | 1 | 832.821 | 832.821 | 832.821 | 832.821 |
| sync.reconcile_head | 7 | 808.892 | 2186.88 | 2186.88 | 2186.88 |
| workspace.collect_garbage | 3 | 5067.366 | 9229.93 | 9229.93 | 9229.93 |
| workspace.lease_renew | 2 | 583.751 | 622.448 | 622.448 | 622.448 |

## Retained slow-record families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent | 3 | 518.434 | 1556.545 | 1556.545 | 1556.545 |
| fleet | 7 | 1151.394 | 5507.997 | 5507.997 | 5507.997 |
| health | 3 | 1350.674 | 1580.459 | 1580.459 | 1580.459 |
| http | 2 | 541.773 | 645.617 | 645.617 | 645.617 |
| maintenance | 1 | 35671.841 | 35671.841 | 35671.841 | 35671.841 |
| notifications | 3 | 653.878 | 1896.894 | 1896.894 | 1896.894 |
| progress | 4 | 661.2 | 811.682 | 811.682 | 811.682 |
| provider | 10 | 1438.948 | 2482.496 | 2482.496 | 2482.496 |
| reconciliation | 2 | 901.222 | 1081.743 | 1081.743 | 1081.743 |
| selection | 1 | 586.459 | 586.459 | 586.459 | 586.459 |
| sqlite | 12 | 719.428 | 8131.766 | 8131.766 | 8131.766 |
| startup | 1 | 761.965 | 761.965 | 761.965 | 761.965 |
| sync | 8 | 808.892 | 2186.88 | 2186.88 | 2186.88 |
| workspace | 5 | 4616.598 | 9229.93 | 9229.93 | 9229.93 |

## Exact ordinary GET window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| / | 5 | 0 | 90.77 | 167.195 | 167.195 | 167.195 | 0 | 25464 |
| /agent | 5 | 0 | 77.533 | 222.802 | 222.802 | 222.802 | 0 | 116012 |
| /api/fleet/overview | 5 | 0 | 190.068 | 390.548 | 390.548 | 390.548 | 0 | 1133991 |
| /api/health | 20 | 0 | 17.169 | 89.92 | 308.964 | 308.964 | 0 | 15 |
| /api/runtime | 20 | 0 | 94.832 | 139.716 | 161.563 | 161.563 | 0 | 514178 |
| /api/status | 20 | 0 | 60.462 | 196.739 | 382.789 | 382.789 | 0 | 2824 |
| /fleet | 5 | 0 | 733.625 | 1252.611 | 1252.611 | 1252.611 | 4 | 1128695 |
| /pull-requests | 5 | 0 | 133.06 | 189.021 | 189.021 | 189.021 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 62.908 | 236.089 | 236.089 | 236.089 | 0 | 100559 |
| /work | 5 | 0 | 25.244 | 87.297 | 87.297 | 87.297 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQL calls | Rows | SQL wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| retention_selection | 1/0 | 88.264791 | 6.939041 | 273 | 271 | 86.5867521497421 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 29.656333 | 6.481792 | 2 | 0 | 29.392207972705364 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 64.876875 | 14.02525 | 5 | 0 | 63.194582937285304 | 0.0024170149117708206 | 64.79966698680073 |
| retention_object_collection | 1/0 | 6960.062333 | 779.157792 | 15178 | 14944 | 4682.346754940227 | 0.0014160177670419216 | 0.007499998901039362 |
| retention_canonical | 1/0 | 7025.076625 | 793.315417 | 15183 | 14944 | 4745.541337877512 | 0.0038330326788127422 | 64.80716698570177 |
| retention_mirror | 1/0 | 10.8565 | 3.597125 | 2 | 0 | 10.594542021863163 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 7124.381375 | 803.998834 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 0.476459 | 0.319125 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 8394.499208 | 2896.265791 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 927.729166 | 598.398416 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 11842.041125 | 6988.404167 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4438.46425 | 1870.68225 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2929.654042 | 1590.180167 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 35657.499125 | 14748.451291 | n/a | n/a | n/a | n/a | n/a |

## Projection native SQLite phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQL calls | Rows | SQL wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connect | 1/0 | 1.536416 | 1.03525 | n/a | n/a | n/a | n/a | n/a |
| size_initial | 1/0 | 5.8665 | 2.283083 | n/a | n/a | n/a | n/a | n/a |
| integrity_before | 1/0 | 2829.577667 | 1304.784667 | n/a | n/a | n/a | n/a | n/a |
| optimize | 1/0 | 0.49275 | 0.497042 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_before | 1/0 | 16.316042 | 7.774333 | n/a | n/a | n/a | n/a | n/a |
| size_pre_compact | 1/0 | 0.168916 | 0.13475 | n/a | n/a | n/a | n/a | n/a |
| disk_space | 1/0 | 0.124792 | 0.125708 | n/a | n/a | n/a | n/a | n/a |
| vacuum | 1/0 | 8028.341791 | 4864.674084 | n/a | n/a | n/a | n/a | n/a |
| integrity_after | 1/0 | 957.632666 | 805.31725 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_after | 1/0 | 0.266958 | 0.110375 | n/a | n/a | n/a | n/a | n/a |
| size_final | 1/0 | 0.142083 | 0.14275 | n/a | n/a | n/a | n/a | n/a |
| close | 1/0 | 1.037166 | 1.0405 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 11841.98075 | 6988.344584 | n/a | n/a | n/a | n/a | n/a |

## Transcript native SQLite phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQL calls | Rows | SQL wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connect | 1/0 | 0.566666 | 0.568333 | n/a | n/a | n/a | n/a | n/a |
| size_initial | 1/0 | 4.478792 | 0.641458 | n/a | n/a | n/a | n/a | n/a |
| integrity_before | 1/0 | 4425.879583 | 1864.447584 | n/a | n/a | n/a | n/a | n/a |
| optimize | 1/0 | 0.412666 | 0.401584 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_before | 1/0 | 6.657167 | 4.162625 | n/a | n/a | n/a | n/a | n/a |
| size_pre_compact | 1/0 | 0.05325 | 0.053542 | n/a | n/a | n/a | n/a | n/a |
| size_final | 1/0 | 0.0185 | 0.018708 | n/a | n/a | n/a | n/a | n/a |
| close | 1/0 | 0.184584 | 0.185125 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 4438.429125 | 1870.647958 | n/a | n/a | n/a | n/a | n/a |


## Preserved earlier coverage

# Main526 browser polling follow-up — 2026-10-18

All **570 findings and79concerns remain open**. Every prior569row, owner and action is unchanged. The [complete artifact](benchmarks/systemic-main526-browser-20261018.json) preserves the redacted failed shard, exact-source polling probe, all test outputs and activation stop.

PR526 merged exactea9918d7 after stable supervisor and independent5SUCCESS/0reviews0threads/currentrules/CLEAN; source/merge trees equal. Its mainCI39146283029 attempt1 failed one browser test while polling the login URL after a sign-in click: CDP reported “Inspected target navigated or closed”. Other4checks passed. No526install/restart or workflowretry.

The test-only pure-read poll now tolerates only that exact transition response within the same15second deadline and50ms cadence. All URL/auth/receipt/layout assertions remain; persistent closure/false predicates still fail, unrelated errors and cancellation propagate. Generic JavaScript/input/navigation commands do not replay. Five deterministic contracts and both real browser journeys pass (combined7tests/35.82s;274deprecationwarnings retained), under both temporary PA roots and settings-aware guards. The deterministic probe explains the poll behavior; original CI/browser timing remains unverified. Product437files unchanged, so no unchanged mixed performance rerun.

Every earlier latency outlier, failed/negative/unknown stage, peer/log/backup/receipt/browser/testroot gap and normal independentacceptance obligation remains open. Followup integration, fresh exact mainCI, safe combined activation and retest remain required.

# Full ancestry boundary discovery — 2026-10-18

All **569 findings and78concerns remain open**. Every prior568row, owner and action is preserved. The [complete artifact](benchmarks/systemic-ancestor-boundaries-20261018.json) retains source, full test/benchmark commands and failures, exact wheel and all mixed outcomes.

Imported forks previously copied the ready tip’s full ancestry for membership, then copied their exact boundary ancestry again. The repair uses the existing pinned indexed membership for discovery and retains exact boundary sets, canonical fallback and history bounds. At30k retained commits,20alternating pairs transfer59999→29999hashes/4.02→2.01MB; p50wall440.654→395.967ms, p95596.576→425.759ms and medianthreadCPU422.712→382.839ms. ObservedSQLcalls15→21; canonical result sets match each trial. This synthetic warm-index fixture does not establish production or whole-path speedup. Initial path-alias guard and test-import failures remain alongside the expected old-source regression failure. Five targeted contracts pass; broader suite99tests plus5subtests passes241.00s (overlapping, not104distinctcontracts); full mixed results are retained below.

Mixed workerwarnings: {"Slow off-loop operation": 1}. Thresholdviolations: {"/api/agent/history/session-0?limit=64&message_boundaries=true": 1, "maintenance.run": 1, "sync.object_collect": 3}. The9600orderedactiveevents remain; newrepair isunmerged/uninstalled anddoesnotcloseanyresidual. CompletePR525actualinventory follows separately; no completed observationwindow repeated. No warning/deadline/default/data/freshness/integrity/reclamation/admission/receipt/acceptance weakening. Everyotherfamily, external/peer/log/backup/browser/testroot/unexercised obligation and normal independentacceptance remains.

# PR525 actual loaded inventory — 2026-10-18

All **568 findings and77concerns remain open**. Every prior row, owner and action is unchanged. The [complete artifact](benchmarks/systemic-525-loaded-20261018.json) retains integration/installation/strict activation proof and all returned runtime sections separately.

All95 timed GETs returned200 with stable build; one Fleet page took738.203ms. The initial untimed observer fence failed before samples; it is preserved, with no duplicate timed samples. Cached maintenance took42213.485ms, object collection7203.064ms with13948SQLcalls, full projection VACUUM8023.222ms, first migration/health11109.834ms and final health4179.475ms. Workspace provisioning records2failures/7submissions, cause unknown. OpenAPI1715.659ms and session-save4408.177ms remain. No whole-path speedup or acceptance claim; every earlier failed/negative/unknown stage and all linked ownership remain.

# Actual PR525 runtime and ordinary GET inventory

Source: supported bound owner API on macbook0c7d8ecb; source bc5116a9c6d18bdae92082aef985ea1f7eb43724, package sha256:c90c070baa5fbcaf6c06df9528d56c7c9852ef97371c053edf056650d53ee271, PID 87866, startup 08b819dd-4a9d-4de6-af7a-422674eacf1b. Runtime UTC 2026-10-18T14:08:57.405270+00:00. All21 sections, request UTC/correlation IDs and explicit opaque omissions are retained.

One small sequential window of95 timed GETs plus2untimed build fences. Five HTML samples give p95=p99=max; first not guaranteed cold. Decoded bytes are not wire bytes. Observer checkpoints add work outside timers. No own fixtures/tests overlapped timedGETs; host/background uncontrolled, no pressure capture. Initial sandbox observer failed on its untimed initial fence before any timed sample. All95 still-missing samples were collected once after native escalation. Separate maintenance/runtime sandbox attempts failed at initial status fence with ConnectError Errno1 Operation not permitted; no snapshot was produced until escalation. These are not production-timeout observations; the initial observer retains only the exception type, so its exact underlying cause is not independently captured. Runtime and cached maintenance were then captured sequentially in separate read-only windows. Process totals/maxima, bounded all-call percentiles and retained slow-only records are distinct. Family all-call percentiles cannot be inferred. Parent phases include children; wall-minusCPU does not establish disk/GIL/nativebusy attribution. Zero busy failures does not exclude successful busy waits. Every prior negative/unknown, peer/log/backup/receipt/browser and unexercised-branch obligation remains open.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 98.562 | 98.562 | 98.562 | 98.562 | 98.562 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 1.873 | 1.873 | 1.873 | 1.873 | 1.873 |
| acp.wire_append | 924 | 0 | 0/0/0 | 256 | 0.918 | 57.163 | 149.406 | 194.278 | 199.016 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 1.003 | 1.003 | 1.003 | 1.003 | 1.003 |
| agent.project_recovery_availability | 48 | 0 | 0/0/0 | 48 | 1.803 | 93.354 | 190.141 | 190.141 | 190.141 |
| agent.prompt_compose | 2 | 0 | 0/0/0 | 2 | 47.947 | 53.74 | 53.74 | 53.74 | 53.74 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 1.371 | 1.371 | 1.371 | 1.371 | 1.371 |
| agent.recovery_sessions | 23 | 0 | 0/0/0 | 23 | 150.706 | 282.954 | 310.985 | 310.985 | 310.985 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 6.656 | 6.656 | 6.656 | 6.656 | 6.656 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 1817.908 | 1817.908 | 1817.908 | 1817.908 | 1817.908 |
| dispatch.completion_pending_read | 28 | 0 | 0/0/0 | 28 | 2.911 | 33.46 | 59.796 | 59.796 | 59.796 |
| dispatch.followup_pending_read | 28 | 0 | 0/0/0 | 28 | 2.484 | 84.08 | 499.05 | 499.05 | 499.05 |
| dispatch.record_read | 399 | 1 | 0/0/0 | 256 | 19.235 | 44.115 | 371.515 | 403.42 | 529.843 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 2.563 | 60.221 | 60.221 | 60.221 | 60.221 |
| filesystem.github_credentials_read | 52 | 0 | 0/0/0 | 52 | 2.26 | 32.007 | 59.828 | 59.828 | 59.828 |
| fleet.overview.reachability | 9 | 0 | 0/0/0 | 9 | 11.685 | 238.143 | 238.143 | 238.143 | 238.143 |
| fleet.overview.status | 9 | 0 | 0/0/0 | 9 | 14.259 | 287.458 | 287.458 | 287.458 | 287.458 |
| fleet.overview.sync | 15 | 0 | 0/0/0 | 15 | 38.12 | 176.555 | 176.555 | 176.555 | 176.555 |
| fleet.overview_cache_read | 384 | 5 | 0/0/0 | 256 | 2.302 | 26.148 | 42.768 | 115.901 | 1186.222 |
| fleet.overview_cache_write | 114 | 4 | 0/0/0 | 114 | 26.342 | 114.382 | 895.431 | 898.903 | 898.903 |
| fleet.overview_response_json | 81 | 1 | 0/0/0 | 81 | 15.227 | 69.152 | 508.024 | 508.024 | 508.024 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 13.834 | 13.834 | 13.834 | 13.834 | 13.834 |
| goal.dispatch_lifecycle_index | 6 | 0 | 0/0/0 | 6 | 0.684 | 61.947 | 61.947 | 61.947 | 61.947 |
| health.canonical_obligation | 31 | 1 | 0/0/0 | 31 | 47.417 | 397.245 | 1575.074 | 1575.074 | 1575.074 |
| http.fleet_overview | 81 | 0 | 0/0/0 | 81 | 159.752 | 2254.088 | 3518.073 | 3518.073 | 3518.073 |
| http.github | 1 | 0 | 0/0/0 | 1 | 658.976 | 658.976 | 658.976 | 658.976 | 658.976 |
| http.pr_supervisor_peer | 366 | 0 | 0/0/0 | 256 | 47.716 | 150.361 | 361.134 | 417.094 | 1530.442 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 42249.866 | 42249.866 | 42249.866 | 42249.866 | 42249.866 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 1.345 | 6.849 | 6.849 | 6.849 | 6.849 |
| notifications.present_page | 2 | 1 | 0/0/0 | 2 | 202.47 | 548.462 | 548.462 | 548.462 | 548.462 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 94.576 | 94.576 | 94.576 | 94.576 | 94.576 |
| pr_supervisor.response_json | 366 | 0 | 0/0/0 | 256 | 0.714 | 13.129 | 57.941 | 64.002 | 64.002 |
| preferences.agent_auto_approve_read | 4 | 0 | 0/0/0 | 4 | 2.205 | 9.261 | 9.261 | 9.261 | 9.261 |
| progress.checkpoint_write | 14 | 1 | 0/0/0 | 14 | 225.943 | 722.352 | 722.352 | 722.352 | 722.352 |
| progress.dispatch_read | 67 | 0 | 0/0/0 | 67 | 64.838 | 102.966 | 225.742 | 225.742 | 225.742 |
| progress.final_report_build | 2 | 0 | 0/0/0 | 2 | 3.225 | 12.286 | 12.286 | 12.286 | 12.286 |
| progress.final_report_write | 2 | 0 | 0/0/0 | 2 | 222.725 | 254.487 | 254.487 | 254.487 | 254.487 |
| progress.heartbeat_write | 10 | 0 | 0/0/0 | 10 | 147.941 | 436.199 | 436.199 | 436.199 | 436.199 |
| progress.pending_read | 54 | 0 | 0/0/0 | 54 | 12.948 | 73.211 | 96.217 | 96.217 | 96.217 |
| progress.sequence_allocate | 24 | 1 | 0/0/0 | 24 | 161.279 | 232.632 | 527.641 | 527.641 | 527.641 |
| reconciliation.completion_evidence_read | 81 | 0 | 0/0/0 | 81 | 4.978 | 72.86 | 176.464 | 176.464 | 176.464 |
| reconciliation.completion_queue | 1 | 0 | 0/0/0 | 1 | 497.014 | 497.014 | 497.014 | 497.014 | 497.014 |
| reconciliation.dispatch_read | 137 | 1 | 0/0/0 | 137 | 2.952 | 73.11 | 338.421 | 591.631 | 591.631 |
| reconciliation.session_read | 81 | 1 | 0/0/0 | 81 | 3.31 | 39.924 | 660.394 | 660.394 | 660.394 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 85.076 | 85.076 | 85.076 | 85.076 | 85.076 |
| selection.prompt_identity | 2 | 0 | 0/0/0 | 2 | 60.874 | 91.138 | 91.138 | 91.138 | 91.138 |
| selection.prompt_outcome | 1 | 0 | 0/0/0 | 1 | 74.052 | 74.052 | 74.052 | 74.052 | 74.052 |
| sqlite.agent_runtime_checkpoint | 5 | 0 | 0/0/0 | 5 | 49.138 | 171.109 | 171.109 | 171.109 | 171.109 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 7.256 | 35.228 | 49.313 | 49.313 | 49.313 |
| sqlite.agent_session_save | 53 | 2 | 0/0/0 | 53 | 23.952 | 214.084 | 4408.177 | 4408.177 | 4408.177 |
| sqlite.card_read | 583 | 0 | 0/0/0 | 256 | 1.703 | 23.443 | 68.936 | 161.599 | 376.894 |
| sqlite.pr_supervisor_capability_write | 3 | 0 | 0/0/0 | 3 | 6.078 | 18.751 | 18.751 | 18.751 | 18.751 |
| sqlite.pr_supervisor_completion_due_read | 50 | 1 | 0/0/0 | 50 | 24.665 | 182.584 | 631.237 | 631.237 | 631.237 |
| sqlite.pr_supervisor_due_read | 50 | 0 | 0/0/0 | 50 | 3.736 | 70.245 | 104.42 | 104.42 | 104.42 |
| sqlite.pr_supervisor_event_write | 183 | 0 | 0/0/0 | 183 | 3.48 | 52.789 | 201.092 | 216.68 | 216.68 |
| sqlite.pr_supervisor_watch_read | 183 | 0 | 0/0/0 | 183 | 5.195 | 94.554 | 206.567 | 402.043 | 402.043 |
| sqlite.pr_supervisor_watch_write | 183 | 0 | 0/0/0 | 183 | 9.942 | 117.182 | 378.461 | 471.335 | 471.335 |
| sqlite.project_read | 406 | 1 | 0/0/0 | 256 | 0.79 | 16.476 | 74.799 | 147.602 | 515.41 |
| sqlite.repository_read | 399 | 0 | 0/0/0 | 256 | 0.982 | 18.175 | 59.07 | 63.352 | 212.896 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 2.858 | 2.889 | 2.889 | 2.889 | 2.889 |
| sqlite.restart_handoff_completion | 96 | 0 | 0/0/0 | 96 | 6.157 | 66.601 | 182.758 | 182.758 | 182.758 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 8.684 | 8.684 | 8.684 | 8.684 | 8.684 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 3.78 | 3.78 | 3.78 | 3.78 | 3.78 |
| sqlite.restart_handoff_session | 48 | 0 | 0/0/0 | 48 | 7.24 | 21.794 | 25.484 | 25.484 | 25.484 |
| sqlite.restart_handoffs_pending | 24 | 0 | 0/0/0 | 24 | 2.827 | 82.555 | 101.253 | 101.253 | 101.253 |
| sqlite.restart_handoffs_watchdog | 23 | 0 | 0/0/0 | 23 | 2.058 | 26.928 | 83.116 | 83.116 | 83.116 |
| sqlite.transcript_append | 112 | 1 | 0/0/0 | 112 | 14.892 | 261.928 | 475.398 | 587.939 | 587.939 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 68.225 | 68.225 | 68.225 | 68.225 | 68.225 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.833 | 4.833 | 4.833 | 4.833 | 4.833 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 9.242 | 9.242 | 9.242 | 9.242 | 9.242 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1715.659 | 1715.659 | 1715.659 | 1715.659 | 1715.659 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 1.952 | 129.188 | 129.188 | 129.188 | 129.188 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 287.728 | 287.728 | 287.728 | 287.728 | 287.728 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 13.245 | 80.557 | 80.557 | 80.557 | 80.557 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 48.669 | 3506.553 | 3506.553 | 3506.553 | 3506.553 |
| sync.reconcile_head | 12 | 8 | 0/0/0 | 12 | 756.363 | 4312.076 | 4312.076 | 4312.076 | 4312.076 |
| sync.ref_read | 48 | 0 | 0/0/0 | 48 | 1.065 | 18.092 | 172.478 | 172.478 | 172.478 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 124.079 | 124.079 | 124.079 | 124.079 | 124.079 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7959.052 | 7959.052 | 7959.052 | 7959.052 | 7959.052 |
| workspace.lease_renew | 2 | 0 | 0/0/0 | 2 | 161.689 | 276.081 | 276.081 | 276.081 | 276.081 |
| workspace.project_provision | 7 | 0 | 2/0/0 | 7 | 1428.986 | 2948.899 | 2948.899 | 2948.899 | 2948.899 |
| workspace.reconcile_terminal_state | 1 | 1 | 0/0/0 | 1 | 2532.875 | 2532.875 | 2532.875 | 2532.875 | 2532.875 |

## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 31.453 | 43.891 | 65.349 | 65.349 | 65.349 |

## Primary prefix families (all-call percentiles unavailable)

| Prefix | Names | Submitted | Slow | Failed | Timeouts | Process max ms |
| --- | --- | --- | --- | --- | --- | --- |
| acp | 4 | 927 | 0 | 0 | 0 | 199.016 |
| agent | 6 | 76 | 1 | 0 | 0 | 1817.908 |
| dispatch | 4 | 462 | 1 | 0 | 0 | 529.843 |
| filesystem | 1 | 52 | 0 | 0 | 0 | 59.828 |
| fleet | 7 | 613 | 10 | 0 | 0 | 1186.222 |
| goal | 1 | 6 | 0 | 0 | 0 | 61.947 |
| health | 1 | 31 | 1 | 0 | 0 | 1575.074 |
| http | 3 | 448 | 0 | 0 | 0 | 3518.073 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 42249.866 |
| notifications | 2 | 4 | 1 | 0 | 0 | 548.462 |
| pr_supervisor | 2 | 367 | 0 | 0 | 0 | 94.576 |
| preferences | 1 | 4 | 0 | 0 | 0 | 9.261 |
| progress | 7 | 173 | 2 | 0 | 0 | 722.352 |
| reconciliation | 4 | 300 | 2 | 0 | 0 | 660.394 |
| selection | 3 | 4 | 0 | 0 | 0 | 91.138 |
| sqlite | 21 | 2427 | 5 | 0 | 0 | 4408.177 |
| startup | 3 | 3 | 1 | 0 | 0 | 1715.659 |
| sync | 7 | 98 | 8 | 0 | 0 | 4312.076 |
| workspace | 4 | 11 | 2 | 2 | 0 | 7959.052 |

## Every registered route window

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| / | 5 | 127320 | 5 | 10.247 | 12.293 | 12.293 | 12.293 |
| /api/agent/observability/v1/sessions/{session_id} | 3 | 12367 | 3 | 55.223 | 56.603 | 56.603 | 56.603 |
| /api/agent/sessions/{session_id}/restart-handoffs | 3 | 1185772 | 3 | 68.02 | 82.035 | 82.035 | 82.035 |
| /api/cards/{card_id} | 2 | 148432 | 2 | 9.206 | 27.309 | 27.309 | 27.309 |
| /api/configuration | 1 | 169949 | 1 | 17.221 | 17.221 | 17.221 | 17.221 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 3 | 538422 | 3 | 20.033 | 38.06 | 38.06 | 38.06 |
| /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint | 1 | 203 | 1 | 700.476 | 700.476 | 700.476 | 700.476 |
| /api/fleet/overview | 5 | 4680575 | 5 | 119.803 | 189.665 | 189.665 | 189.665 |
| /api/fleet/overview/local | 27 | 637471 | 27 | 61.838 | 212.768 | 217.142 | 217.142 |
| /api/health | 20 | 300 | 20 | 2.241 | 4.589 | 11.393 | 11.393 |
| /api/instance | 1 | 560 | 1 | 4.66 | 4.66 | 4.66 | 4.66 |
| /api/instance/maintenance | 1 | 7455 | 1 | 2.851 | 2.851 | 2.851 | 2.851 |
| /api/notifications | 2 | 22218 | 2 | 210.538 | 570.542 | 570.542 | 570.542 |
| /api/owner-ready | 1 | 71 | 1 | 5.263 | 5.263 | 5.263 | 5.263 |
| /api/pr-supervisor/capabilities | 1 | 2707 | 1 | 7.493 | 7.493 | 7.493 | 7.493 |
| /api/pr-supervisor/instances/heartbeat | 5 | 85 | 5 | 45.469 | 241.959 | 241.959 | 241.959 |
| /api/pr-supervisor/replicas | 399 | 2209568 | 256 | 37.724 | 150.087 | 411.092 | 462.009 |
| /api/ready | 27 | 3348 | 27 | 4.117 | 27.741 | 30.795 | 30.795 |
| /api/runtime | 20 | 6185985 | 20 | 41.397 | 54.743 | 78.546 | 78.546 |
| /api/status | 55 | 154555 | 55 | 77.235 | 520.682 | 600.736 | 600.736 |
| /api/sync/refs | 2 | 518 | 2 | 4.684 | 7.546 | 7.546 | 7.546 |
| /api/ui/assets | 2 | 158 | 2 | 2.091 | 8.227 | 8.227 | 8.227 |
| /api/workspaces | 1 | 1439 | 1 | 7.668 | 7.668 | 7.668 | 7.668 |
| /fleet | 5 | 5482935 | 5 | 269.681 | 712.167 | 712.167 | 712.167 |
| /pull-requests | 5 | 125605 | 5 | 37.534 | 143.385 | 143.385 | 143.385 |
| /{page_path:path} | 15 | 1258355 | 15 | 44.834 | 347.629 | 347.629 | 347.629 |

## Retained slow-record names

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent.startup_state_read | 1 | 1817.908 | 1817.908 | 1817.908 | 1817.908 |
| dispatch.record_read | 1 | 529.843 | 529.843 | 529.843 | 529.843 |
| fleet.overview_cache_read | 5 | 1181.921 | 1186.222 | 1186.222 | 1186.222 |
| fleet.overview_cache_write | 4 | 659.508 | 898.903 | 898.903 | 898.903 |
| fleet.overview_response_json | 1 | 508.024 | 508.024 | 508.024 | 508.024 |
| health.canonical_obligation | 1 | 1575.074 | 1575.074 | 1575.074 | 1575.074 |
| maintenance.run | 1 | 42249.866 | 42249.866 | 42249.866 | 42249.866 |
| notifications.present_page | 1 | 548.462 | 548.462 | 548.462 | 548.462 |
| progress.checkpoint_write | 1 | 722.352 | 722.352 | 722.352 | 722.352 |
| progress.sequence_allocate | 1 | 527.641 | 527.641 | 527.641 | 527.641 |
| reconciliation.dispatch_read | 1 | 591.631 | 591.631 | 591.631 | 591.631 |
| reconciliation.session_read | 1 | 660.394 | 660.394 | 660.394 | 660.394 |
| sqlite.agent_session_save | 2 | 2747.798 | 4408.177 | 4408.177 | 4408.177 |
| sqlite.pr_supervisor_completion_due_read | 1 | 631.237 | 631.237 | 631.237 | 631.237 |
| sqlite.project_read | 1 | 515.41 | 515.41 | 515.41 | 515.41 |
| sqlite.transcript_append | 1 | 587.939 | 587.939 | 587.939 | 587.939 |
| startup.openapi | 1 | 1715.659 | 1715.659 | 1715.659 | 1715.659 |
| sync.reconcile_head | 8 | 922.745 | 4312.076 | 4312.076 | 4312.076 |
| workspace.collect_garbage | 1 | 7959.052 | 7959.052 | 7959.052 | 7959.052 |
| workspace.reconcile_terminal_state | 1 | 2532.875 | 2532.875 | 2532.875 | 2532.875 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent | 1 | 1817.908 | 1817.908 | 1817.908 | 1817.908 |
| dispatch | 1 | 529.843 | 529.843 | 529.843 | 529.843 |
| fleet | 10 | 898.903 | 1186.222 | 1186.222 | 1186.222 |
| health | 1 | 1575.074 | 1575.074 | 1575.074 | 1575.074 |
| maintenance | 1 | 42249.866 | 42249.866 | 42249.866 | 42249.866 |
| notifications | 1 | 548.462 | 548.462 | 548.462 | 548.462 |
| progress | 2 | 527.641 | 722.352 | 722.352 | 722.352 |
| reconciliation | 2 | 591.631 | 660.394 | 660.394 | 660.394 |
| sqlite | 5 | 631.237 | 4408.177 | 4408.177 | 4408.177 |
| startup | 1 | 1715.659 | 1715.659 | 1715.659 | 1715.659 |
| sync | 8 | 922.745 | 4312.076 | 4312.076 | 4312.076 |
| workspace | 2 | 2532.875 | 7959.052 | 7959.052 | 7959.052 |

## Exact ordinary GET window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| / | 5 | 0 | 30.52 | 78.439 | 78.439 | 78.439 | 0 | 25464 |
| /agent | 5 | 0 | 238.087 | 371.632 | 371.632 | 371.632 | 0 | 119789 |
| /api/fleet/overview | 5 | 0 | 195.044 | 239.514 | 239.514 | 239.514 | 0 | 936115 |
| /api/health | 20 | 0 | 24.61 | 54.469 | 108.692 | 108.692 | 0 | 15 |
| /api/runtime | 20 | 0 | 72.974 | 185.176 | 384.562 | 384.562 | 0 | 309344 |
| /api/status | 20 | 0 | 85.195 | 152.041 | 391.624 | 391.624 | 0 | 2824 |
| /fleet | 5 | 0 | 287.636 | 738.203 | 738.203 | 738.203 | 1 | 1096587 |
| /pull-requests | 5 | 0 | 58.207 | 163.992 | 163.992 | 163.992 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 37.837 | 56.093 | 56.093 | 56.093 | 0 | 100559 |
| /work | 5 | 0 | 73.27 | 150.951 | 150.951 | 150.951 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQLite calls | Rows | SQLite wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| retention_selection | 1/0 | 1034.184875 | 5.948417 | 272 | 270 | 1029.871335835196 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 62.251333 | 6.734708 | 2 | 0 | 61.88541604205966 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 158.174042 | 30.62975 | 5 | 0 | 155.2326229866594 | 0.0023750471882522106 | 158.09912496479228 |
| retention_object_collection | 1/0 | 7203.064417 | 734.537875 | 13948 | 13732 | 5512.303899857216 | 0.001541979145258665 | 0.005125009920448065 |
| retention_canonical | 1/0 | 7361.402625 | 765.321417 | 13953 | 13732 | 5667.536522843875 | 0.003917026333510876 | 158.10424997471273 |
| retention_mirror | 1/0 | 28.609333 | 7.143083 | 2 | 0 | 28.08266697684303 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 8424.440375 | 778.644583 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 1.859083 | 0.716792 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 11109.833666 | 3496.30625 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 743.634417 | 605.919958 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12969.232042 | 8401.094125 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4784.613291 | 2116.14225 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 4179.474625 | 1889.643584 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 42213.484542 | 17288.760334 | n/a | n/a | n/a | n/a | n/a |

## Projection native SQLite phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQLite calls | Rows | SQLite wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connect | 1/0 | 2.360583 | 0.901083 | n/a | n/a | n/a | n/a | n/a |
| size_initial | 1/0 | 7.602458 | 3.712625 | n/a | n/a | n/a | n/a | n/a |
| integrity_before | 1/0 | 2764.45 | 1378.526625 | n/a | n/a | n/a | n/a | n/a |
| optimize | 1/0 | 2.516333 | 1.646458 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_before | 1/0 | 7.368833 | 4.850541 | n/a | n/a | n/a | n/a | n/a |
| size_pre_compact | 1/0 | 0.131375 | 0.132084 | n/a | n/a | n/a | n/a | n/a |
| disk_space | 1/0 | 0.203833 | 0.204833 | n/a | n/a | n/a | n/a | n/a |
| vacuum | 1/0 | 8023.222167 | 6113.348583 | n/a | n/a | n/a | n/a | n/a |
| integrity_after | 1/0 | 2054.08825 | 825.082833 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_after | 1/0 | 101.393208 | 67.912792 | n/a | n/a | n/a | n/a | n/a |
| size_final | 1/0 | 0.506667 | 0.418125 | n/a | n/a | n/a | n/a | n/a |
| close | 1/0 | 4.162667 | 3.177333 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 12969.172625 | 8400.979584 | n/a | n/a | n/a | n/a | n/a |

## Transcript native SQLite phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQLite calls | Rows | SQLite wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connect | 1/0 | 1.707333 | 1.07725 | n/a | n/a | n/a | n/a | n/a |
| size_initial | 1/0 | 0.76675 | 0.768125 | n/a | n/a | n/a | n/a | n/a |
| integrity_before | 1/0 | 4745.941125 | 2091.07325 | n/a | n/a | n/a | n/a | n/a |
| optimize | 1/0 | 0.5235 | 0.444333 | n/a | n/a | n/a | n/a | n/a |
| checkpoint_before | 1/0 | 35.027125 | 22.155708 | n/a | n/a | n/a | n/a | n/a |
| size_pre_compact | 1/0 | 0.086875 | 0.087458 | n/a | n/a | n/a | n/a | n/a |
| size_final | 1/0 | 0.043667 | 0.044209 | n/a | n/a | n/a | n/a | n/a |
| close | 1/0 | 0.184166 | 0.18525 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 4784.522125 | 2116.051584 | n/a | n/a | n/a | n/a | n/a |


## Preserved earlier coverage

# PR525 CI attribution remains open — 2026-10-18

All **568 findings and77concerns remain open**. Every previous567row, owner and action is preserved. The [full CI follow-up](benchmarks/systemic-ci525-followup-20261018.json) retains original failed-shard log, diagnostics, source, command/isolation limitations and explicit binding helpers.

First headcf9bbcd CI39133202193 attempt1 failed one snapshot responsiveness assertion at256.078ms against200ms; other4checks passed. That test and its product path were unchanged fromd6e4. The original timer includes testmocksetup/request/teardown; this fact does not attribute theCI outlier tosetup. Three isolated phase observations didnot reproduceCI(request8.692–14.661ms, setup0.670–1.525ms); one valueaudit found noMock objects inreturned data. Originalcause remainsUNKNOWN. No workflowretry, warning/deadline/budget change or inventedproductionrepair.

The test now adds setup/request/teardown and loop-threadCPU values to its failure message. Its original timed region,200ms wait/elapsed assertions and transcript-read/buffer/metadata assertions remain unchanged. Guardednewtest passes1/4.67s; fullnewheadCI ispending. An earlierdirectone-testcommand passed but omittedexplicittempPAroots/guard; it isnotisolationproof and its console istranscribedfromproviderresult ratherthanclaimedlosslesslocalraw. The correctedrunner usesbothtemproots anddeniesworkspaceconstruction. No observedlivewrites; previousisolationgaps/eightemptylocks remainopen.

Futureactivationhelper selectors nowusefrozen explicitsessionbindings;3puretests confirm PRnumber changesonlyGitHubselector andwronghistorical9524backupUUID isrejected. Generichelpers areprepared only, no liveactivationattempt. Originalhelperfailure remainsretained. Normal independentacceptance andallbroaderlatencyrequirements remainunsatisfied.

## Preserved source repair, actual deployment and earlier evidence

# Startup definition hashing repair — 2026-10-18

All **567 findings and76concerns remain open**. Every prior566row, owner and action is unchanged. The [complete source artifact](benchmarks/systemic-schema-remapping-20261018.json) retains sources, original failed/unexercised probes, corrected mechanism counts, all commands/exit codes, test failures/corrections and final measurements. The complete actual PR524 inventory below remains distinct from this uninstalled source.

Pydantic traverses the same definition under several candidate names while resolving schema collisions. The FastAPI0.136.3/Pydantic2.13.4 call-local generator keeps one hashable representation per schema within a pass, then clears every memo entry before upstream mutates the schemas. Strong references prevent identity reuse. Original hash/dedup/order/error behavior, full OpenAPI validation, security, routes, readiness, deadlines and warning thresholds remain. Unknown Pydantic versions use the original generator; framework globals are unchanged.

The corrected prototype exercises5616hash requests but936structural computations across3passes. The first candidate did not exercise its binding (all counters0); its timing supports no improvement and is retained unchanged with the original helper and assessment. One new test initially expected a single second-pass entry, while upstream retains both original entries; its failed run and source remain. The corrected test still requires fresh hashes on every pass. Final37tests plus9subtests pass28.66s; these suites overlap earlier runs. Documentation/formatting changed during that suite; executable function AST parity is verified, and final measurements use exact final bytes.

Four alternating fresh-process schema cases (previous/current/current/previous) took1175.949/968.139/943.029/1187.534ms wall and1112.684/943.758/926.359/1111.090ms threadCPU. All481paths/792795canonicalbytes retain SHA623e83b9. These are two samples per version on an uncontrolled host, not fleet or whole-path acceptance. No own fixtures/tests overlapped these timed cases. One full normal disposable startup reached readiness1257.927ms but retains an OpenAPI1046.817ms/995.814CPU warning. Rawstdout/stderr and childexit0 are durable; immediate lifespan shutdown defers fixture maintenance, so this is not full maintenance acceptance.

No production restart, defaults/configuration/dependency changes or selfacceptance from this source stage. Next: supervised integration, safe exact activation/retest and every remaining requirement, including nativebusy/GIL/physicalIO and other shared CPU costs. Normal scheduler independent acceptance remains unsatisfied.

## Preserved actual deployment and all earlier evidence

# PR524 actual loaded inventory — 2026-10-18

All **566 findings and75concerns remain open**. Every prior565row, owner and action is retained; the durable helper-identifier concern is now included. Exact source/package/newPID/config/lease were verified. The [complete artifact](benchmarks/systemic-524-loaded-20261018.json) separates integration, installation, activation and retest evidence.

All95GETs returned200/stable, but3threshold violations remain (Agent twice, max554.895ms; Fleetoverview613.791ms). Cached maintenance35849.515ms includes GC7151.908ms/13816SQLcalls and VACUUM8623.590ms. Workspace provision has1failure/7submissions with cause unknown. All prior failed/negative/unknown stages remain. No warning/deadline/default/data/integrity/reclamation/acceptance weakening.

One small sequential window of95 timed GETs plus2untimed build fences. Five HTML samples give p95=p99=max; first not guaranteed cold. Decoded bytes are not wire bytes. Observer checkpoints add work outside timers. No own tests/fixtures overlapped GETs; host/background uncontrolled, no pressure capture. Runtime and cached maintenance followed in a parallel read-only batch and are separate windows. Process totals/maxima, bounded all-call percentiles and retained slow-only records are distinct. Family all-call percentiles cannot be inferred. Parent phases include children; wall-minusCPU does not establish disk/GIL/nativebusy attribution. Zero busy failures does not exclude successful busy waits. Every prior negative/unknown, peer/log/backup/receipt/browser and unexercised-branch obligation remains open.

# Actual PR524 runtime and ordinary GET inventory

Source: supported bound owner API on macbook0c7d8ecb; source d6e4ca08af657e5963df0325ee742b63f19dc4c6, package sha256:b34c8068418e4a2162f6a94a48a2cdf7998873ff0c52e159d7aa49269cbf5ce4, PID 84056, startup 9a36d89d-3dc9-4117-b8a7-d861927ed1c1. Runtime snapshot UTC 2026-10-18T13:42:13.650189+00:00. Each request UTC/correlation and all21sections/explicit opaque omissions remain in the artifact.

One small sequential window of95 timed GETs plus2untimed build fences. Five HTML samples give p95=p99=max; first not guaranteed cold. Decoded bytes are not wire bytes. Observer checkpoints add work outside timers. No own tests/fixtures overlapped GETs; host/background uncontrolled, no pressure capture. Runtime and cached maintenance followed in a parallel read-only batch and are separate windows. Process totals/maxima, bounded all-call percentiles and retained slow-only records are distinct. Family all-call percentiles cannot be inferred. Parent phases include children; wall-minusCPU does not establish disk/GIL/nativebusy attribution. Zero busy failures does not exclude successful busy waits. Every prior negative/unknown, peer/log/backup/receipt/browser and unexercised-branch obligation remains open.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 59.138 | 59.138 | 59.138 | 59.138 | 59.138 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 0.529 | 0.529 | 0.529 | 0.529 | 0.529 |
| acp.wire_append | 439 | 0 | 0/0/0 | 256 | 1.241 | 20.051 | 67.935 | 149.477 | 149.477 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 0.699 | 0.699 | 0.699 | 0.699 | 0.699 |
| agent.project_recovery_availability | 28 | 0 | 0/0/0 | 28 | 2.082 | 30.071 | 63.325 | 63.325 | 63.325 |
| agent.prompt_compose | 1 | 0 | 0/0/0 | 1 | 22.98 | 22.98 | 22.98 | 22.98 | 22.98 |
| agent.provider_resolve | 1 | 0 | 0/0/0 | 1 | 2.177 | 2.177 | 2.177 | 2.177 | 2.177 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 2.674 | 2.674 | 2.674 | 2.674 | 2.674 |
| agent.recovery_sessions | 13 | 1 | 0/0/0 | 13 | 124.01 | 634.801 | 634.801 | 634.801 | 634.801 |
| agent.recovery_snapshot | 1 | 0 | 0/0/0 | 1 | 57.54 | 57.54 | 57.54 | 57.54 | 57.54 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 0.695 | 0.695 | 0.695 | 0.695 | 0.695 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 1858.492 | 1858.492 | 1858.492 | 1858.492 | 1858.492 |
| dispatch.completion_pending_read | 19 | 0 | 0/0/0 | 19 | 2.006 | 196.498 | 196.498 | 196.498 | 196.498 |
| dispatch.followup_pending_read | 19 | 0 | 0/0/0 | 19 | 2.298 | 349.885 | 349.885 | 349.885 | 349.885 |
| dispatch.record_read | 266 | 0 | 0/0/0 | 256 | 19.772 | 49.431 | 110.095 | 353.342 | 353.342 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 2.491 | 58.613 | 58.613 | 58.613 | 58.613 |
| filesystem.github_credentials_read | 35 | 0 | 0/0/0 | 35 | 1.928 | 63.097 | 72.896 | 72.896 | 72.896 |
| fleet.overview.reachability | 6 | 1 | 0/0/0 | 6 | 13.29 | 1235.668 | 1235.668 | 1235.668 | 1235.668 |
| fleet.overview.status | 6 | 1 | 0/0/0 | 6 | 12.334 | 1234.645 | 1234.645 | 1234.645 | 1234.645 |
| fleet.overview.sync | 6 | 0 | 0/0/0 | 6 | 59.657 | 381.893 | 381.893 | 381.893 | 381.893 |
| fleet.overview_cache_read | 256 | 5 | 0/0/0 | 256 | 3.923 | 43.776 | 1192.427 | 1231.427 | 1231.427 |
| fleet.overview_cache_write | 72 | 1 | 0/0/0 | 72 | 20.164 | 470.256 | 894.08 | 894.08 | 894.08 |
| fleet.overview_response_json | 54 | 1 | 0/0/0 | 54 | 14.553 | 147.61 | 961.951 | 961.951 | 961.951 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 12.541 | 12.541 | 12.541 | 12.541 | 12.541 |
| goal.dispatch_lifecycle_index | 5 | 0 | 0/0/0 | 5 | 0.755 | 70.95 | 70.95 | 70.95 | 70.95 |
| health.canonical_obligation | 29 | 1 | 0/0/0 | 29 | 51.133 | 362.422 | 1532.267 | 1532.267 | 1532.267 |
| http.fleet_overview | 54 | 0 | 0/0/0 | 54 | 195.024 | 2617.114 | 3604.636 | 3604.636 | 3604.636 |
| http.github | 1 | 0 | 0/0/0 | 1 | 750.212 | 750.212 | 750.212 | 750.212 | 750.212 |
| http.pr_supervisor_peer | 238 | 0 | 0/0/0 | 238 | 51.272 | 199.047 | 406.091 | 807.059 | 807.059 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 35889.214 | 35889.214 | 35889.214 | 35889.214 | 35889.214 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 2.927 | 9.896 | 9.896 | 9.896 | 9.896 |
| notifications.present_page | 2 | 0 | 0/0/0 | 2 | 203.076 | 210.726 | 210.726 | 210.726 | 210.726 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 8.697 | 8.697 | 8.697 | 8.697 | 8.697 |
| pr_supervisor.response_json | 238 | 0 | 0/0/0 | 238 | 0.749 | 15.117 | 44.282 | 108.247 | 108.247 |
| preferences.agent_auto_approve_read | 4 | 0 | 0/0/0 | 4 | 2.779 | 10.588 | 10.588 | 10.588 | 10.588 |
| progress.checkpoint_write | 3 | 0 | 0/0/0 | 3 | 288.928 | 322.578 | 322.578 | 322.578 | 322.578 |
| progress.dispatch_read | 35 | 0 | 0/0/0 | 35 | 59.747 | 86.771 | 96.897 | 96.897 | 96.897 |
| progress.heartbeat_write | 5 | 0 | 0/0/0 | 5 | 189.003 | 244.753 | 244.753 | 244.753 | 244.753 |
| progress.pending_read | 33 | 0 | 0/0/0 | 33 | 12.23 | 96.34 | 193.1 | 193.1 | 193.1 |
| progress.sequence_allocate | 8 | 0 | 0/0/0 | 8 | 140.107 | 183.695 | 183.695 | 183.695 | 183.695 |
| reconciliation.completion_evidence_read | 54 | 1 | 0/0/0 | 54 | 5.008 | 107.978 | 555.304 | 555.304 | 555.304 |
| reconciliation.dispatch_read | 91 | 2 | 0/0/0 | 91 | 2.702 | 43.432 | 1232.942 | 1232.942 | 1232.942 |
| reconciliation.session_read | 54 | 1 | 0/0/0 | 54 | 3.195 | 72.712 | 896.175 | 896.175 | 896.175 |
| selection.attempt_authority | 1 | 0 | 0/0/0 | 1 | 20.59 | 20.59 | 20.59 | 20.59 | 20.59 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 28.167 | 28.167 | 28.167 | 28.167 | 28.167 |
| selection.prompt_identity | 1 | 0 | 0/0/0 | 1 | 39.018 | 39.018 | 39.018 | 39.018 | 39.018 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0/0 | 2 | 15.251 | 28.8 | 28.8 | 28.8 | 28.8 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 4.717 | 14.354 | 39.137 | 39.137 | 39.137 |
| sqlite.agent_session_save | 38 | 1 | 0/0/0 | 38 | 12.721 | 225.581 | 4515.367 | 4515.367 | 4515.367 |
| sqlite.card_read | 386 | 0 | 0/0/0 | 256 | 1.625 | 16.213 | 83.778 | 138.599 | 385.535 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0/0 | 2 | 19.363 | 166.122 | 166.122 | 166.122 | 166.122 |
| sqlite.pr_supervisor_completion_due_read | 33 | 1 | 0/0/0 | 33 | 35.503 | 249.171 | 518.365 | 518.365 | 518.365 |
| sqlite.pr_supervisor_due_read | 33 | 0 | 0/0/0 | 33 | 3.836 | 41.928 | 49.451 | 49.451 | 49.451 |
| sqlite.pr_supervisor_event_write | 119 | 0 | 0/0/0 | 119 | 3.456 | 68.323 | 121.932 | 131.926 | 131.926 |
| sqlite.pr_supervisor_watch_read | 119 | 1 | 0/0/0 | 119 | 5.456 | 115.571 | 151.332 | 1431.587 | 1431.587 |
| sqlite.pr_supervisor_watch_write | 119 | 0 | 0/0/0 | 119 | 9.927 | 124.41 | 168.751 | 380.737 | 380.737 |
| sqlite.project_read | 273 | 0 | 0/0/0 | 256 | 0.953 | 13.783 | 62.722 | 94.961 | 153.779 |
| sqlite.repository_read | 266 | 0 | 0/0/0 | 256 | 1.358 | 59.776 | 110.896 | 121.83 | 149.959 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 2.245 | 91.03 | 91.03 | 91.03 | 91.03 |
| sqlite.restart_handoff_completion | 56 | 0 | 0/0/0 | 56 | 6.406 | 89.185 | 120.761 | 120.761 | 120.761 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 6.711 | 6.711 | 6.711 | 6.711 | 6.711 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 2.622 | 2.622 | 2.622 | 2.622 | 2.622 |
| sqlite.restart_handoff_session | 28 | 0 | 0/0/0 | 28 | 7.429 | 74.386 | 113.272 | 113.272 | 113.272 |
| sqlite.restart_handoffs_pending | 14 | 0 | 0/0/0 | 14 | 3.729 | 35.872 | 35.872 | 35.872 | 35.872 |
| sqlite.restart_handoffs_watchdog | 13 | 0 | 0/0/0 | 13 | 3.898 | 58.969 | 58.969 | 58.969 | 58.969 |
| sqlite.transcript_append | 69 | 0 | 0/0/0 | 69 | 13.914 | 73.437 | 288.079 | 288.079 | 288.079 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 12.102 | 12.102 | 12.102 | 12.102 | 12.102 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.413 | 4.413 | 4.413 | 4.413 | 4.413 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 8.827 | 8.827 | 8.827 | 8.827 | 8.827 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1647.557 | 1647.557 | 1647.557 | 1647.557 | 1647.557 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 1.278 | 143.818 | 143.818 | 143.818 | 143.818 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 341.341 | 341.341 | 341.341 | 341.341 | 341.341 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 0.685 | 133.012 | 133.012 | 133.012 | 133.012 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 59.295 | 2444.703 | 2444.703 | 2444.703 | 2444.703 |
| sync.reconcile_head | 12 | 9 | 0/0/0 | 12 | 715.308 | 4055.574 | 4055.574 | 4055.574 | 4055.574 |
| sync.ref_read | 32 | 0 | 0/0/0 | 32 | 0.849 | 3.993 | 5.116 | 5.116 | 5.116 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 139.979 | 139.979 | 139.979 | 139.979 | 139.979 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7682.431 | 7682.431 | 7682.431 | 7682.431 | 7682.431 |
| workspace.lease_renew | 1 | 0 | 0/0/0 | 1 | 63.603 | 63.603 | 63.603 | 63.603 | 63.603 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 2155.242 | 2683.458 | 2683.458 | 2683.458 | 2683.458 |
| workspace.reconcile_terminal_state | 1 | 1 | 0/0/0 | 1 | 2556.887 | 2556.887 | 2556.887 | 2556.887 | 2556.887 |

## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 28.676 | 43.226 | 48.107 | 48.107 | 48.107 |

## Primary prefix families (all-call percentiles unavailable)

| Prefix | Names | Submitted | Slow | Failed | Timeouts | Process max ms |
| --- | --- | --- | --- | --- | --- | --- |
| acp | 4 | 442 | 0 | 0 | 0 | 149.477 |
| agent | 8 | 47 | 2 | 0 | 0 | 1858.492 |
| dispatch | 4 | 311 | 0 | 0 | 0 | 353.342 |
| filesystem | 1 | 35 | 0 | 0 | 0 | 72.896 |
| fleet | 7 | 401 | 9 | 0 | 0 | 1235.668 |
| goal | 1 | 5 | 0 | 0 | 0 | 70.95 |
| health | 1 | 29 | 1 | 0 | 0 | 1532.267 |
| http | 3 | 293 | 0 | 0 | 0 | 3604.636 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 35889.214 |
| notifications | 2 | 4 | 0 | 0 | 0 | 210.726 |
| pr_supervisor | 2 | 239 | 0 | 0 | 0 | 108.247 |
| preferences | 1 | 4 | 0 | 0 | 0 | 10.588 |
| progress | 5 | 84 | 0 | 0 | 0 | 322.578 |
| reconciliation | 3 | 199 | 4 | 0 | 0 | 1232.942 |
| selection | 3 | 3 | 0 | 0 | 0 | 39.018 |
| sqlite | 21 | 1596 | 3 | 0 | 0 | 4515.367 |
| startup | 3 | 3 | 1 | 0 | 0 | 1647.557 |
| sync | 7 | 82 | 9 | 0 | 0 | 4055.574 |
| workspace | 4 | 10 | 2 | 1 | 0 | 7682.431 |

## Every registered route window

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| / | 5 | 127320 | 5 | 13.626 | 68.419 | 68.419 | 68.419 |
| /api/agent/observability/v1/sessions/{session_id} | 1 | 4115 | 1 | 63.898 | 63.898 | 63.898 | 63.898 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 732656 | 2 | 75.654 | 89.888 | 89.888 | 89.888 |
| /api/configuration | 1 | 169949 | 1 | 28.97 | 28.97 | 28.97 | 28.97 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 15.688 | 44.254 | 44.254 | 44.254 |
| /api/fleet/overview | 5 | 5123665 | 5 | 151.591 | 350.466 | 350.466 | 350.466 |
| /api/fleet/overview/local | 20 | 601354 | 20 | 83.981 | 612.753 | 681.829 | 681.829 |
| /api/health | 20 | 300 | 20 | 2.16 | 2.981 | 7.854 | 7.854 |
| /api/instance | 1 | 560 | 1 | 4.087 | 4.087 | 4.087 | 4.087 |
| /api/instance/maintenance | 1 | 7454 | 1 | 9.171 | 9.171 | 9.171 | 9.171 |
| /api/notifications | 2 | 22218 | 2 | 225.145 | 237.376 | 237.376 | 237.376 |
| /api/owner-ready | 1 | 71 | 1 | 4.263 | 4.263 | 4.263 | 4.263 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 24.062 | 98.227 | 98.227 | 98.227 |
| /api/pr-supervisor/replicas | 266 | 1474634 | 256 | 42.857 | 199.852 | 477.216 | 508.296 |
| /api/ready | 20 | 2072 | 20 | 3.227 | 10.078 | 11.911 | 11.911 |
| /api/runtime | 20 | 5995316 | 20 | 39.144 | 66.533 | 87.526 | 87.526 |
| /api/status | 45 | 125040 | 45 | 89.432 | 402.528 | 716.112 | 716.112 |
| /api/sync/refs | 2 | 518 | 2 | 4.82 | 35.846 | 35.846 | 35.846 |
| /api/ui/assets | 2 | 158 | 2 | 8.752 | 14.666 | 14.666 | 14.666 |
| /api/workspaces | 1 | 1439 | 1 | 7.96 | 7.96 | 7.96 | 7.96 |
| /fleet | 5 | 5482945 | 5 | 221.667 | 329.347 | 329.347 | 329.347 |
| /pull-requests | 5 | 125605 | 5 | 62.432 | 100.453 | 100.453 | 100.453 |
| /{page_path:path} | 15 | 1257455 | 15 | 18.41 | 536.342 | 536.342 | 536.342 |

## Retained slow-record names

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent.recovery_sessions | 1 | 634.801 | 634.801 | 634.801 | 634.801 |
| agent.startup_state_read | 1 | 1858.492 | 1858.492 | 1858.492 | 1858.492 |
| fleet.overview.reachability | 1 | 1235.668 | 1235.668 | 1235.668 | 1235.668 |
| fleet.overview.status | 1 | 1234.645 | 1234.645 | 1234.645 | 1234.645 |
| fleet.overview_cache_read | 5 | 1192.427 | 1231.427 | 1231.427 | 1231.427 |
| fleet.overview_cache_write | 1 | 894.08 | 894.08 | 894.08 | 894.08 |
| fleet.overview_response_json | 1 | 961.951 | 961.951 | 961.951 | 961.951 |
| health.canonical_obligation | 1 | 1532.267 | 1532.267 | 1532.267 | 1532.267 |
| maintenance.run | 1 | 35889.214 | 35889.214 | 35889.214 | 35889.214 |
| reconciliation.completion_evidence_read | 1 | 555.304 | 555.304 | 555.304 | 555.304 |
| reconciliation.dispatch_read | 2 | 555.838 | 1232.942 | 1232.942 | 1232.942 |
| reconciliation.session_read | 1 | 896.175 | 896.175 | 896.175 | 896.175 |
| sqlite.agent_session_save | 1 | 4515.367 | 4515.367 | 4515.367 | 4515.367 |
| sqlite.pr_supervisor_completion_due_read | 1 | 518.365 | 518.365 | 518.365 | 518.365 |
| sqlite.pr_supervisor_watch_read | 1 | 1431.587 | 1431.587 | 1431.587 | 1431.587 |
| startup.openapi | 1 | 1647.557 | 1647.557 | 1647.557 | 1647.557 |
| sync.reconcile_head | 9 | 849.89 | 4055.574 | 4055.574 | 4055.574 |
| workspace.collect_garbage | 1 | 7682.431 | 7682.431 | 7682.431 | 7682.431 |
| workspace.reconcile_terminal_state | 1 | 2556.887 | 2556.887 | 2556.887 | 2556.887 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
| --- | --- | --- | --- | --- | --- |
| agent | 2 | 634.801 | 1858.492 | 1858.492 | 1858.492 |
| fleet | 9 | 1192.427 | 1235.668 | 1235.668 | 1235.668 |
| health | 1 | 1532.267 | 1532.267 | 1532.267 | 1532.267 |
| maintenance | 1 | 35889.214 | 35889.214 | 35889.214 | 35889.214 |
| reconciliation | 4 | 555.838 | 1232.942 | 1232.942 | 1232.942 |
| sqlite | 3 | 1431.587 | 4515.367 | 4515.367 | 4515.367 |
| startup | 1 | 1647.557 | 1647.557 | 1647.557 | 1647.557 |
| sync | 9 | 849.89 | 4055.574 | 4055.574 | 4055.574 |
| workspace | 2 | 2556.887 | 7682.431 | 7682.431 | 7682.431 |

## Exact ordinary GET window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| / | 5 | 0 | 75.72 | 212.225 | 212.225 | 212.225 | 0 | 25464 |
| /agent | 5 | 0 | 258.559 | 554.895 | 554.895 | 554.895 | 2 | 119609 |
| /api/fleet/overview | 5 | 0 | 193.13 | 613.791 | 613.791 | 613.791 | 1 | 1024733 |
| /api/health | 20 | 0 | 20.203 | 51.388 | 57.566 | 57.566 | 0 | 15 |
| /api/runtime | 20 | 0 | 73.488 | 132.249 | 141.746 | 141.746 | 0 | 299935 |
| /api/status | 20 | 0 | 77.481 | 426.435 | 469.099 | 469.099 | 0 | 2824 |
| /fleet | 5 | 0 | 239.906 | 369.606 | 369.606 | 369.606 | 0 | 1096589 |
| /pull-requests | 5 | 0 | 91.982 | 134.385 | 134.385 | 134.385 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 37.196 | 44.736 | 44.736 | 44.736 | 0 | 100559 |
| /work | 5 | 0 | 35.073 | 55.691 | 55.691 | 55.691 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | SQLite calls | Rows | SQLite wall ms | Lock wait | Lock hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| retention_selection | 1/0 | 1025.4925 | 14.950875 | 273 | 270 | 1013.9779060264118 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 25.301958 | 6.5475 | 2 | 0 | 24.994665989652276 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 196.514 | 30.988625 | 5 | 0 | 194.73554205615073 | 0.002667016815394163 | 196.43233297392726 |
| retention_object_collection | 1/0 | 7151.907667 | 715.583417 | 13816 | 13602 | 5185.67327898927 | 0.0032910029403865337 | 0.004334026016294956 |
| retention_canonical | 1/0 | 7348.801208 | 746.938 | 13821 | 13602 | 5380.408821045421 | 0.005958019755780697 | 196.43666699994355 |
| retention_mirror | 1/0 | 50.976959 | 8.783125 | 2 | 0 | 49.388165993150324 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 8426.011334 | 771.230208 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 1.081333 | 0.775 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 7065.555375 | 3041.455709 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 916.199083 | 719.064125 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12456.964333 | 8269.082042 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 5195.400708 | 2148.719375 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 1787.661958 | 1612.298209 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 35849.515 | 16563.211708 | n/a | n/a | n/a | n/a | n/a |



## Preserved earlier coverage

# Remove unused prior-card reconstruction from versioned appends — 2026-10-18

All **565 findings and74concerns remain open**. Every prior563row, owner and action is unchanged. The [full artifact](benchmarks/systemic-append-prior-20261018.json) retains the source, complete pair data, test stages, rejected SQL alternative, failed probe/assessment, earlier startup diagnostics and new mixed-load evidence.

EventLog.append_event unconditionally reconstructed prior card state from retained indexed event history. For non-create events with a supplied causal_card_version, that result was discarded: only duplicate-create detection and omitted-version derivation consume it. Keep index readiness, duplicate-create lookups, absent/empty-version fallback, ref/parent fence, object durability, index advancement and full projection unchanged.

With128KiB card bodies and100/1000retained updates,20alternatingpairs per class preserve body/summary/semanticversion and matching durable/projection heads after every write. At1000history, p50/p95/p99/max wall83.688/104.569/114.905/114.905ms becomes30.601/50.541/79.599/79.599ms; median processCPU78.968->27.964ms, prior snapshots1->0. This removes measured work while the global mutation lock is held, without claiming sole attribution of historical summary convoys.

The new regression fails against old source exactly on the unused supplied-version read (1failed2passed); final5contracts pass, and the broader151tests plus5subtests pass142.77s. Those counts overlap. Duplicate creation, missing/empty version derivation, moved-parent fencing, card CAS, manual-summary fencing, durable receipts and completion protection retain their contracts.

The row-upsert alternative is **not implemented**. Its partial128KiB20pair evidence has lower logical WALgrowth but no clear latency gain. Its full probe failed after40trials because +00:00 and Z were compared as strings; the1MiB class never ran. A separate2write diagnostic confirms equivalent instants; its copied literal-parity flag is explicitly corrected by a companion assessment. Original failure/raw remain unchanged. Previously unpublished loop-profile and callback diagnostics are now retained with their original inconclusive/observer-overhead limits; no lossless first-profile console recovery is claimed.

Twenty alternating pairs per100/1000retained-card-update class with128KiB body; temporary setup uses the same prototype outside timers solely to avoid quadratic seeding while retaining all canonical events/ref/index/projection writes. Pertrial semantic timestamp normalization and full body/summary/head checks outside timers. Process CPU is not worker-thread CPU or GIL/nativebusy isolation. Exact final function AST equals measured prototype; only comments differ. Mixed workload is one changed-source regression, not paired whole-path speedup or deployed acceptance. All437wheel source bytes/38templates/30runtime module hashes and journals attested. No other own tests/probes overlapped timed mixed case; metadata-only progress and tracer/hashjournal add work. Physical disk IO/successful native busy waits/raw locks/GIL remain unmeasured. All prior failed/unknown/negative outcomes remain. The actual523 observer has95timed GETs plus2untimed build-fence GETs; separate21section runtime/cachedmaintenance are not the same window.

One changed-source mixed workload took103.994s, with capture/validation separate. It preserved9600ordered events/12511cold hashes/5000expired deletions/16audit proofs and zero operation failure/timeout/rejection deltas, but warnings{'Slow off-loop operation': 3} and threshold violations{'transcript.drain': 1, '/api/agent/history/session-0?limit=64&message_boundaries=true': 5, 'sync.object_collect': 4, 'maintenance.run': 1} remain. Full premeasurement bootstrap residuals are retained. No performance acceptance or deployed improvement is claimed.

Normal PA scheduler independent acceptance17d034a6 remains unsatisfied. Exactbfe0/PR523 is loaded and separately observed with3live GET violations, cached36.270smaintenance and all prior coverage/peer/log/backup/receipt/browser obligations preserved. Next: supervised integration, exact safe activation/retest and all remaining shared causes; no warning/deadline/default/configuration/durable-data/acceptance weakening.

## Every mixed workload latency family

| Name | n | p50ms | p95ms | p99ms | maxms | >=500ms |
|---|---:|---:|---:|---:|---:|---:|
| / | 100 | 11.065 | 21.733 | 49.868 | 89.037 | 0 |
| /agent | 100 | 65.895 | 163.925 | 247.15 | 249.245 | 0 |
| /api/agent/history/session-0?limit=64 | 100 | 38.761 | 77.877 | 177.366 | 235.905 | 0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 183.741 | 468.342 | 804.827 | 1019.364 | 5 |
| /api/agent/preferences | 100 | 5.016 | 9.855 | 14.891 | 15.479 | 0 |
| /api/agent/sessions?view=chats | 100 | 79.72 | 129.055 | 177.631 | 334.322 | 0 |
| /api/cards | 100 | 9.062 | 17.288 | 26.833 | 27.358 | 0 |
| /api/cards/fixture-card | 100 | 5.21 | 15.283 | 24.873 | 29.216 | 0 |
| /api/cards/fixture-card/history | 100 | 28.826 | 71.177 | 133.785 | 159.825 | 0 |
| /api/execution/defaults | 100 | 5.796 | 9.138 | 51.413 | 128.332 | 0 |
| /api/fleet/dispatch-jobs | 100 | 33.946 | 63.022 | 108.139 | 227.605 | 0 |
| /api/fleet/dispatch-queue | 100 | 7.238 | 14.397 | 24.076 | 26.427 | 0 |
| /api/health | 100 | 3.456 | 5.43 | 8.312 | 57.332 | 0 |
| /api/health-journal/status | 100 | 8.291 | 21.605 | 42.534 | 76.168 | 0 |
| /api/notifications | 100 | 5.741 | 17.893 | 35.057 | 68.539 | 0 |
| /api/pr-supervisor/capabilities | 100 | 8.101 | 17.264 | 24.426 | 49.795 | 0 |
| /api/pr-supervisor/watches | 100 | 7.718 | 15.636 | 26.214 | 26.692 | 0 |
| /api/runtime | 100 | 32.461 | 38.563 | 55.386 | 113.496 | 0 |
| /api/runtime?section=sse | 100 | 5.114 | 8.103 | 10.893 | 20.573 | 0 |
| /api/status | 100 | 50.751 | 84.072 | 137.019 | 200.279 | 0 |
| /api/sync/refs | 100 | 4.829 | 10.413 | 26.529 | 28.403 | 0 |
| /api/sync/status | 100 | 13.566 | 58.168 | 78.413 | 79.871 | 0 |
| /api/workspaces | 100 | 6.891 | 16.906 | 29.183 | 32.685 | 0 |
| /fleet | 100 | 96.097 | 207.049 | 357.826 | 387.124 | 0 |
| /partials/home/sections | 100 | 19.558 | 52.264 | 75.273 | 92.091 | 0 |
| /pull-requests | 100 | 29.593 | 95.132 | 154.217 | 213.951 | 0 |
| /settings?section=agent | 100 | 17.752 | 38.19 | 83.362 | 116.876 | 0 |
| /settings?section=backups | 100 | 16.615 | 38.037 | 94.193 | 108.977 | 0 |
| /work | 100 | 12.893 | 38.49 | 68.401 | 89.614 | 0 |
| acp.wire_append | 100 | 6.513 | 18.794 | 29.469 | 68.169 | 0 |
| dispatch.runnable_read | 100 | 2.813 | 14.559 | 19.082 | 72.592 | 0 |
| fleet.overview_cache_read | 100 | 2.888 | 11.758 | 13.416 | 14.323 | 0 |
| fleet.overview_cache_write | 100 | 15.592 | 40.32 | 54.454 | 78.72 | 0 |
| maintenance.run | 1 | 18773.735 | 18773.735 | 18773.735 | 18773.735 | 1 |
| progress.checkpoint_write | 100 | 13.253 | 26.57 | 37.461 | 55.265 | 0 |
| progress.sequence_allocate | 100 | 9.956 | 32.009 | 119.669 | 149.154 | 0 |
| reconciliation.dispatch_read | 100 | 2.676 | 13.822 | 22.517 | 43.684 | 0 |
| sqlite.card_write | 10 | 49.451 | 212.672 | 212.672 | 212.672 | 0 |
| sqlite.execution_binding_materialize | 100 | 67.876 | 215.955 | 343.823 | 363.42 | 0 |
| sync.commit_read | 100 | 5.256 | 18.572 | 38.969 | 45.665 | 0 |
| sync.object_collect | 10 | 376.446 | 1554.412 | 1554.412 | 1554.412 | 4 |
| sync.push_transaction | 100 | 75.431 | 201.475 | 310.38 | 391.149 | 0 |
| sync.reconcile_head | 100 | 47.634 | 200.621 | 337.977 | 368.889 | 0 |
| transcript.drain | 300 | 129.447 | 254.84 | 402.751 | 513.56 | 1 |

## Preserved earlier coverage

# PR523 actual activation and remaining latency — 2026-10-18

All **563 findings and 72 concerns remain open**. Every prior row, owner and action is preserved. The [complete actual-build artifact](benchmarks/systemic-523-loaded-20261018.json) records sourcebfe0a81a/package5fd609b1/PID79888/startupbffb5ec0/config86968/lease290, independent integration checks and distinct installation/loaded proofs. Both exact-head/main CI passed5/5 without retries. Normal PA scheduler independent acceptance17d034a6 remains unsatisfied.

The once-only95 ordinary GETs all returned200 with stable build fences, but runtime628.164ms, Fleet612.692ms and Home706.045ms exceeded500ms. Status p95=137.811ms; runtime p95=225.639ms. Fleet returned1,120,902 decoded bytes. One small sequential window, five HTML samples give p95=p99=max; first not guaranteed cold. Decoded response bytes are not wire bytes. Local sample checkpoint persistence adds observer work outside timers. No own tests/fixtures overlapped GETs, host/background uncontrolled and no pressure capture; separate runtime and cached-maintenance snapshots followed in a parallel read-only batch. Process totals/maxima, bounded all-call percentile windows and slow-only retained records are distinct. Family all-call percentiles cannot be inferred. Parent phases include children; wall-minusCPU is not disk/GIL attribution, and zero busy failures does not exclude successful busy waits. Every prior negative result, unknown child exit, older nested/log/rotation gaps, workspace failure, peer/backup/receipt and coverage obligation remains open.

Separate runtime capture retains81 primary plus1diagnostic operation,19prefix families,22 route groups,25slow records and25slow counters. Positive baseline coverage in THIS process is76/181operations and19/115routes; unobserved names/branches remain open. Workspace provision still has1failure/7submissions with cause unknown. Full per-name/family/route/count/percentile/max tables follow.

Startup OpenAPI remains1478.644ms/1317.585ms threadCPU; state read1965.636ms/1778.038ms SQLwall/344.873ms CPU returns365rows. Cache read0.047ms service/1021.343ms completion and cache write5.549ms service/1601.464ms completion preserve shared scheduling evidence without native/GIL/physicalIO isolation. Session-save3235.604ms includes3226.340ms lockhold/3195.789ms SQLwall/18.236ms CPU. Process loopmax1532ms is separate from the GET window.

Cached maintenance36269.591ms includes objectcollection5885.922ms/3292.748ms SQLwall/13683SQLAPIcalls/13471rows, projection15435.851ms including VACUUM11149.191ms/6970.371msCPU, integrity2803.662+1383.800ms, transcriptintegrity4556.754ms, firsthealth6547.741ms/final2359.671ms. Missingcold1/migrationrunning remains; no freshness, integrity or reclamation contract was skipped. Linked backup acceptance remains separate and open.

## Every observed operation, endpoint and family

# Actual523 runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, sourcebfe0a81a/PID79888/startupbffb5ec0/package5fd609b1. Runtime snapshot UTC 2026-10-18T13:13:23.967983+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

No own disposable tests or fixtures overlapped the95GET window; cached maintenance and runtime collectors follow it in a parallel read-only batch and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 57.083 | 57.083 | 57.083 | 57.083 | 57.083 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 4.501 | 4.501 | 4.501 | 4.501 | 4.501 |
| acp.wire_append | 443 | 0 | 0/0/0 | 256 | 0.971 | 19.875 | 128.271 | 155.812 | 270.374 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 12.249 | 12.249 | 12.249 | 12.249 | 12.249 |
| agent.project_recovery_availability | 26 | 0 | 0/0/0 | 26 | 1.735 | 24.487 | 53.15 | 53.15 | 53.15 |
| agent.prompt_compose | 1 | 0 | 0/0/0 | 1 | 25.885 | 25.885 | 25.885 | 25.885 | 25.885 |
| agent.provider_resolve | 1 | 0 | 0/0/0 | 1 | 2.397 | 2.397 | 2.397 | 2.397 | 2.397 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 1.559 | 1.559 | 1.559 | 1.559 | 1.559 |
| agent.recovery_sessions | 12 | 0 | 0/0/0 | 12 | 151.324 | 293.929 | 293.929 | 293.929 | 293.929 |
| agent.recovery_snapshot | 1 | 0 | 0/0/0 | 1 | 42.729 | 42.729 | 42.729 | 42.729 | 42.729 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 0.578 | 0.578 | 0.578 | 0.578 | 0.578 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 1965.636 | 1965.636 | 1965.636 | 1965.636 | 1965.636 |
| dispatch.completion_pending_read | 17 | 0 | 0/0/0 | 17 | 4.856 | 262.867 | 262.867 | 262.867 | 262.867 |
| dispatch.followup_pending_read | 17 | 0 | 0/0/0 | 17 | 5.447 | 195.932 | 195.932 | 195.932 | 195.932 |
| dispatch.record_read | 244 | 1 | 0/0/0 | 244 | 20.716 | 95.771 | 380.777 | 543.795 | 543.795 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 7.595 | 155.066 | 155.066 | 155.066 | 155.066 |
| filesystem.github_credentials_read | 32 | 0 | 0/0/0 | 32 | 3.858 | 130.564 | 134.903 | 134.903 | 134.903 |
| fleet.overview.reachability | 6 | 0 | 0/0/0 | 6 | 9.598 | 29.541 | 29.541 | 29.541 | 29.541 |
| fleet.overview.status | 6 | 0 | 0/0/0 | 6 | 15.667 | 25.283 | 25.283 | 25.283 | 25.283 |
| fleet.overview.sync | 14 | 0 | 0/0/0 | 14 | 37.825 | 399.674 | 399.674 | 399.674 | 399.674 |
| fleet.overview_cache_read | 262 | 1 | 0/0/0 | 256 | 3.257 | 51.134 | 197.129 | 1022.501 | 1022.501 |
| fleet.overview_cache_write | 80 | 2 | 0/0/0 | 80 | 26.068 | 142.735 | 1607.48 | 1607.48 | 1607.48 |
| fleet.overview_response_json | 54 | 1 | 0/0/0 | 54 | 12.818 | 38.036 | 696.718 | 696.718 | 696.718 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 12.255 | 12.255 | 12.255 | 12.255 | 12.255 |
| goal.dispatch_lifecycle_index | 4 | 0 | 0/0/0 | 4 | 0.916 | 8.109 | 8.109 | 8.109 | 8.109 |
| health.canonical_obligation | 31 | 1 | 0/0/0 | 31 | 51.072 | 319.152 | 1341.138 | 1341.138 | 1341.138 |
| http.fleet_overview | 54 | 0 | 0/0/0 | 54 | 187.349 | 1883.883 | 2057.307 | 2057.307 | 2057.307 |
| http.github | 1 | 0 | 0/0/0 | 1 | 612.131 | 612.131 | 612.131 | 612.131 | 612.131 |
| http.pr_supervisor_peer | 224 | 0 | 0/0/0 | 224 | 59.256 | 154.747 | 241.3 | 346.952 | 346.952 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 36273.075 | 36273.075 | 36273.075 | 36273.075 | 36273.075 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 9.227 | 19.058 | 19.058 | 19.058 | 19.058 |
| notifications.present_page | 2 | 0 | 0/0/0 | 2 | 248.847 | 274.004 | 274.004 | 274.004 | 274.004 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 57.167 | 57.167 | 57.167 | 57.167 | 57.167 |
| pr_supervisor.response_json | 224 | 0 | 0/0/0 | 224 | 0.853 | 11.948 | 25.573 | 28.512 | 28.512 |
| preferences.agent_auto_approve_read | 5 | 0 | 0/0/0 | 5 | 3.491 | 231.927 | 231.927 | 231.927 | 231.927 |
| progress.checkpoint_write | 2 | 0 | 0/0/0 | 2 | 282.045 | 344.738 | 344.738 | 344.738 | 344.738 |
| progress.dispatch_read | 32 | 0 | 0/0/0 | 32 | 62.997 | 117.661 | 142.999 | 142.999 | 142.999 |
| progress.heartbeat_write | 7 | 2 | 0/0/0 | 7 | 302.7 | 748.565 | 748.565 | 748.565 | 748.565 |
| progress.pending_read | 29 | 1 | 0/0/0 | 29 | 13.548 | 120.998 | 602.148 | 602.148 | 602.148 |
| progress.sequence_allocate | 9 | 0 | 0/0/0 | 9 | 248.368 | 407.857 | 407.857 | 407.857 | 407.857 |
| reconciliation.completion_evidence_read | 51 | 0 | 0/0/0 | 51 | 5.068 | 97.023 | 124.537 | 124.537 | 124.537 |
| reconciliation.dispatch_read | 86 | 0 | 0/0/0 | 86 | 3.145 | 44.087 | 450.265 | 450.265 | 450.265 |
| reconciliation.session_read | 51 | 1 | 0/0/0 | 51 | 3.613 | 96.55 | 1033.42 | 1033.42 | 1033.42 |
| selection.attempt_authority | 1 | 0 | 0/0/0 | 1 | 10.651 | 10.651 | 10.651 | 10.651 | 10.651 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 27.914 | 27.914 | 27.914 | 27.914 | 27.914 |
| selection.prompt_identity | 1 | 0 | 0/0/0 | 1 | 172.804 | 172.804 | 172.804 | 172.804 | 172.804 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0/0 | 2 | 25.788 | 34.438 | 34.438 | 34.438 | 34.438 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 6.121 | 41.065 | 275.628 | 275.628 | 275.628 |
| sqlite.agent_session_save | 37 | 1 | 0/0/0 | 37 | 15.878 | 212.155 | 3235.604 | 3235.604 | 3235.604 |
| sqlite.card_read | 357 | 2 | 0/0/0 | 256 | 1.766 | 20.763 | 74.662 | 639.713 | 1193.721 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0/0 | 2 | 10.15 | 37.632 | 37.632 | 37.632 | 37.632 |
| sqlite.pr_supervisor_completion_due_read | 30 | 0 | 0/0/0 | 30 | 32.565 | 138.976 | 269.25 | 269.25 | 269.25 |
| sqlite.pr_supervisor_due_read | 30 | 0 | 0/0/0 | 30 | 3.788 | 89.334 | 117.714 | 117.714 | 117.714 |
| sqlite.pr_supervisor_event_write | 112 | 0 | 0/0/0 | 112 | 4.02 | 76.984 | 97.928 | 238.752 | 238.752 |
| sqlite.pr_supervisor_watch_read | 112 | 0 | 0/0/0 | 112 | 5.843 | 101.89 | 329.084 | 472.9 | 472.9 |
| sqlite.pr_supervisor_watch_write | 112 | 0 | 0/0/0 | 112 | 11.151 | 109.815 | 260.558 | 314.838 | 314.838 |
| sqlite.project_read | 251 | 0 | 0/0/0 | 251 | 1.127 | 47.56 | 94.734 | 158.957 | 158.957 |
| sqlite.repository_read | 244 | 0 | 0/0/0 | 244 | 1.565 | 66.963 | 150.306 | 164.53 | 164.53 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 1.996 | 2.255 | 2.255 | 2.255 | 2.255 |
| sqlite.restart_handoff_completion | 52 | 0 | 0/0/0 | 52 | 5.976 | 34.301 | 112.065 | 112.065 | 112.065 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 5.96 | 5.96 | 5.96 | 5.96 | 5.96 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 2.069 | 2.069 | 2.069 | 2.069 | 2.069 |
| sqlite.restart_handoff_session | 26 | 0 | 0/0/0 | 26 | 4.964 | 9.957 | 10.695 | 10.695 | 10.695 |
| sqlite.restart_handoffs_pending | 13 | 0 | 0/0/0 | 13 | 2.656 | 8.768 | 8.768 | 8.768 | 8.768 |
| sqlite.restart_handoffs_watchdog | 12 | 0 | 0/0/0 | 12 | 3.412 | 17.967 | 17.967 | 17.967 | 17.967 |
| sqlite.transcript_append | 64 | 0 | 0/0/0 | 64 | 18.472 | 153.703 | 295.95 | 295.95 | 295.95 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 9.19 | 9.19 | 9.19 | 9.19 | 9.19 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 3.633 | 3.633 | 3.633 | 3.633 | 3.633 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 8.854 | 8.854 | 8.854 | 8.854 | 8.854 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1478.644 | 1478.644 | 1478.644 | 1478.644 | 1478.644 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 8.364 | 125.405 | 125.405 | 125.405 | 125.405 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 19.7 | 19.7 | 19.7 | 19.7 | 19.7 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 5.814 | 85.703 | 85.703 | 85.703 | 85.703 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 81.25 | 1788.708 | 1788.708 | 1788.708 | 1788.708 |
| sync.reconcile_head | 12 | 8 | 0/0/0 | 12 | 707.363 | 4441.408 | 4441.408 | 4441.408 | 4441.408 |
| sync.ref_read | 28 | 0 | 0/0/0 | 28 | 0.993 | 41.091 | 50.87 | 50.87 | 50.87 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 35.246 | 35.246 | 35.246 | 35.246 | 35.246 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 6838.275 | 6838.275 | 6838.275 | 6838.275 | 6838.275 |
| workspace.lease_renew | 1 | 0 | 0/0/0 | 1 | 75.307 | 75.307 | 75.307 | 75.307 | 75.307 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 2124.899 | 2598.777 | 2598.777 | 2598.777 | 2598.777 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1674.519 | 1674.519 | 1674.519 | 1674.519 | 1674.519 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 26.994 | 113.487 | 181.96 | 181.96 | 181.96 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 446 | 0 | 0 | 0 | 270.374 |
| agent | 8 | 44 | 1 | 0 | 0 | 1965.636 |
| dispatch | 4 | 285 | 1 | 0 | 0 | 543.795 |
| filesystem | 1 | 32 | 0 | 0 | 0 | 134.903 |
| fleet | 7 | 423 | 4 | 0 | 0 | 1607.48 |
| goal | 1 | 4 | 0 | 0 | 0 | 8.109 |
| health | 1 | 31 | 1 | 0 | 0 | 1341.138 |
| http | 3 | 279 | 0 | 0 | 0 | 2057.307 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 36273.075 |
| notifications | 2 | 4 | 0 | 0 | 0 | 274.004 |
| pr_supervisor | 2 | 225 | 0 | 0 | 0 | 57.167 |
| preferences | 1 | 5 | 0 | 0 | 0 | 231.927 |
| progress | 5 | 79 | 3 | 0 | 0 | 748.565 |
| reconciliation | 3 | 188 | 1 | 0 | 0 | 1033.42 |
| selection | 3 | 3 | 0 | 0 | 0 | 172.804 |
| sqlite | 21 | 1482 | 3 | 0 | 0 | 3235.604 |
| startup | 3 | 3 | 1 | 0 | 0 | 1478.644 |
| sync | 7 | 78 | 8 | 0 | 0 | 4441.408 |
| workspace | 4 | 10 | 1 | 1 | 0 | 6838.275 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 90.727 | 638.039 | 638.039 | 638.039 |
| /api/agent/observability/v1/sessions/{session_id} | 1 | 4020 | 1 | 68.579 | 68.579 | 68.579 | 68.579 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 680310 | 2 | 29.244 | 83.946 | 83.946 | 83.946 |
| /api/configuration | 1 | 169949 | 1 | 9.969 | 9.969 | 9.969 | 9.969 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 16.87 | 57.029 | 57.029 | 57.029 |
| /api/fleet/overview | 5 | 4796785 | 5 | 139.489 | 303.122 | 303.122 | 303.122 |
| /api/fleet/overview/local | 19 | 412959 | 19 | 74.31 | 539.92 | 539.92 | 539.92 |
| /api/health | 20 | 300 | 20 | 2.035 | 3.173 | 5.155 | 5.155 |
| /api/instance | 2 | 1120 | 2 | 5.947 | 23.367 | 23.367 | 23.367 |
| /api/notifications | 2 | 22218 | 2 | 274.451 | 327.658 | 327.658 | 327.658 |
| /api/owner-ready | 1 | 71 | 1 | 7.244 | 7.244 | 7.244 | 7.244 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 73.093 | 196.964 | 196.964 | 196.964 |
| /api/pr-supervisor/replicas | 244 | 1351363 | 244 | 45.745 | 403.113 | 744.062 | 1432.306 |
| /api/ready | 19 | 2017 | 19 | 3.538 | 13.175 | 13.175 | 13.175 |
| /api/runtime | 20 | 5808633 | 20 | 37.508 | 134.562 | 194.401 | 194.401 |
| /api/status | 44 | 122471 | 44 | 64.543 | 449.887 | 644.511 | 644.511 |
| /api/sync/refs | 3 | 777 | 3 | 16.162 | 131.095 | 131.095 | 131.095 |
| /api/ui/assets | 2 | 158 | 2 | 6.295 | 8.052 | 8.052 | 8.052 |
| /api/workspaces | 1 | 1439 | 1 | 13.044 | 13.044 | 13.044 | 13.044 |
| /fleet | 5 | 5604510 | 5 | 281.348 | 592.573 | 592.573 | 592.573 |
| /pull-requests | 5 | 125605 | 5 | 39.094 | 60.171 | 60.171 | 60.171 |
| /{page_path:path} | 15 | 1256550 | 15 | 32.997 | 313.746 | 313.746 | 313.746 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.startup_state_read | 1 | 1965.636 | 1965.636 | 1965.636 | 1965.636 |
| dispatch.record_read | 1 | 543.795 | 543.795 | 543.795 | 543.795 |
| fleet.overview_cache_read | 1 | 1022.501 | 1022.501 | 1022.501 | 1022.501 |
| fleet.overview_cache_write | 2 | 1031.392 | 1607.48 | 1607.48 | 1607.48 |
| fleet.overview_response_json | 1 | 696.718 | 696.718 | 696.718 | 696.718 |
| health.canonical_obligation | 1 | 1341.138 | 1341.138 | 1341.138 | 1341.138 |
| maintenance.run | 1 | 36273.075 | 36273.075 | 36273.075 | 36273.075 |
| progress.heartbeat_write | 2 | 718.262 | 748.565 | 748.565 | 748.565 |
| progress.pending_read | 1 | 602.148 | 602.148 | 602.148 | 602.148 |
| reconciliation.session_read | 1 | 1033.42 | 1033.42 | 1033.42 | 1033.42 |
| sqlite.agent_session_save | 1 | 3235.604 | 3235.604 | 3235.604 | 3235.604 |
| sqlite.card_read | 2 | 639.713 | 1193.721 | 1193.721 | 1193.721 |
| startup.openapi | 1 | 1478.644 | 1478.644 | 1478.644 | 1478.644 |
| sync.reconcile_head | 8 | 975.127 | 4441.408 | 4441.408 | 4441.408 |
| workspace.collect_garbage | 1 | 6838.275 | 6838.275 | 6838.275 | 6838.275 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 1 | 1965.636 | 1965.636 | 1965.636 | 1965.636 |
| dispatch | 1 | 543.795 | 543.795 | 543.795 | 543.795 |
| fleet | 4 | 1022.501 | 1607.48 | 1607.48 | 1607.48 |
| health | 1 | 1341.138 | 1341.138 | 1341.138 | 1341.138 |
| maintenance | 1 | 36273.075 | 36273.075 | 36273.075 | 36273.075 |
| progress | 3 | 718.262 | 748.565 | 748.565 | 748.565 |
| reconciliation | 1 | 1033.42 | 1033.42 | 1033.42 | 1033.42 |
| sqlite | 3 | 1193.721 | 3235.604 | 3235.604 | 3235.604 |
| startup | 1 | 1478.644 | 1478.644 | 1478.644 | 1478.644 |
| sync | 8 | 975.127 | 4441.408 | 4441.408 | 4441.408 |
| workspace | 1 | 6838.275 | 6838.275 | 6838.275 | 6838.275 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 109.378 | 706.045 | 706.045 | 706.045 | 1 | 25464 |
| /agent | 5 | 0 | 116.983 | 378.152 | 378.152 | 378.152 | 0 | 119428 |
| /api/fleet/overview | 5 | 0 | 172.053 | 344.113 | 344.113 | 344.113 | 0 | 959357 |
| /api/health | 20 | 0 | 20.758 | 49.499 | 59.4 | 59.4 | 0 | 15 |
| /api/runtime | 20 | 0 | 71.177 | 225.639 | 628.164 | 628.164 | 1 | 293493 |
| /api/status | 20 | 0 | 74.559 | 137.811 | 154.05 | 154.05 | 0 | 2824 |
| /fleet | 5 | 0 | 303.137 | 612.692 | 612.692 | 612.692 | 1 | 1120902 |
| /pull-requests | 5 | 0 | 59.518 | 98.769 | 98.769 | 98.769 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 36.756 | 84.501 | 84.501 | 84.501 | 0 | 100559 |
| /work | 5 | 0 | 37.373 | 70.862 | 70.862 | 70.862 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 102.213833 | 9.214792 | 273 | 270 | 52.7222960954532 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 27.087625 | 6.662083 | 2 | 0 | 26.836665987502784 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 137.069 | 28.430833 | 5 | 0 | 135.41125098709017 | 0.0018749851733446121 | 137.01170805143192 |
| retention_object_collection | 1/0 | 5885.921875 | 647.97975 | 13683 | 13471 | 3292.748429987114 | 0.001291045919060707 | 0.003583962097764015 |
| retention_canonical | 1/0 | 6023.639541 | 676.588833 | 13688 | 13471 | 3428.159680974204 | 0.003166031092405319 | 137.0152920135297 |
| retention_mirror | 1/0 | 41.604667 | 7.643583 | 2 | 0 | 40.28612497495487 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 6167.730625 | 693.746208 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 0.736291 | 0.58175 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 6547.741083 | 3039.596208 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1182.284625 | 666.120208 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 15435.850709 | 9231.793917 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4575.097 | 2117.102375 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2359.670709 | 1691.869542 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 36269.591459 | 17441.172208 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# OpenAPI JSON filtering CPU repair — 2026-10-18

All **563 findings and 72 concerns remain open**. Every previous562 row, owner and action is unchanged. The [source artifact](benchmarks/systemic-openapi-json-20261018.json) contains both profiles, every prototype/source timing, commands/exit codes, exact source snapshots, parity hash, test log and guarded temporary roots. This source is not deployed; the actual PR522 loaded evidence and complete runtime inventory are preserved immediately below.

FastAPI’s final generic encoder traversed an already JSON-serialized OpenAPI model dump. The version-fenced, call-local replacement keeps complete OpenAPI validation and Pydantic JSON serialization, then preserves the framework’s recursive null/private-key filtering. Array nulls remain; other objects/options use the public encoder. Framework globals, route/auth/validation contracts, readiness, deadlines and warning thresholds are unchanged.

Four alternating fresh-process schema cases (previous/current/current/previous) kept identical481-path/792795-byte schema hash623e83b9. Wall times were1290.846/1167.272/1196.959/1358.254ms; threadCPU1239.314/1098.357/1123.609/1247.904ms. Between current samples only module documentation/import ordering changed; function ASTs are equal, and the final sample/full startup match final source bytes. Tiny samples and uncontrolled host load do not establish fleet or whole-path improvement. Profile/prototype stages remain separately labeled, with profiler overhead included.

The34 regressions plus9subtests pass, including whole-PA/framework schema equality, recursive/generic/discriminator/webhook variants, extension serialization, fallback, concurrency, security/idempotency and readiness. One full disposable normal startup reached readiness in1318.720ms but retained2warnings: OpenAPI1090.861ms and sync-ref1140.204ms (service0.378ms/completion1139.464ms). This is **not startup acceptance**; remaining native/model/schema/GIL scheduling work stays open. No mixed workload was rerun to hide these warnings. Full numeric runtime is durable; full startup console stdout has no lossless local archive, only provider history, and is not claimed recovered.

Next: supervised review/integration and safe exact activation, then continue shared startup and summary-write causes and every remaining criterion. Normal PA independent acceptance remains unsatisfied; no Done/selfacceptance/default/provider/peer/backup or configuration changes.

## Preserved loaded deployment and all earlier evidence

# PR522 actual activation and remaining latency — 2026-10-18

All **562 findings and 71 concerns remain open**; every existing row, owner and next action is preserved. The [complete loaded-build artifact](benchmarks/systemic-522-loaded-20261018.json) records exact source de576ebb, package6439e272, new PID76726/startup2acd7a30 and unchanged configuration/lease290. Head and main CI each passed all5 checks without retries; integration and installed/loaded proofs remain distinct.

The once-only95 ordinary sequential GETs all returned HTTP200 with stable build fences, but Agent659.990ms and Fleet632.784ms exceeded500ms. Status p95=186.025ms, runtime129.334ms; Fleet returned1,096,583 decoded bytes and overview1,019,876. Single sequential95GET window, all200/stable but Agent659.990ms/Fleet632.784ms violate500ms. Not acceptance or a causal paired comparison. No own tests or fixtures overlapped GETs; host/background uncontrolled, no pressure capture. Per-sample persistence adds work outside timers. Five HTML p95=p99=max, first not guaranteed cold, decoded bytes not wire bytes. Runtime/cachedmaintenance were separate following parallel read-only snapshots. Per-process maxima differ from bounded windows. Zero busy failures does not exclude successful busy waits. Parent phases include children; wall-minusCPU is not disk/GIL attribution. All prior CI failures, negative experiments, old unknown child exit, log gaps, workspace failures, peer/backup/receipt/coverage gaps remain open.

Separate runtime inventory: 81 primary operations plus1 diagnostic,19 prefix families,23 registered routes,25 retained slow records and25 slow counters. Positive current-process baseline coverage is76/181 operations and19/115 routes; unobserved paths/branches remain open. workspace.project_provision has1 failure of7 with cause unknown. Full per-name/family/route count and percentile tables follow; all-call family percentiles cannot be inferred from per-operation summaries.

Startup dispatch recovery took1557.043ms overall but14.613ms worker service/14.417ms threadCPU; completion delivery alone was1346.684ms and dispatch195.831ms. OpenAPI remained1610.451ms/1378.312ms CPU; session read970.791ms had7.042ms service/961.199ms completion delivery. These retain shared scheduling evidence without claiming native busy, disk or GIL isolation. Startup state read1674.196ms/1621.886ms SQLitewall/341.688ms CPU still returned365 rows. Runtime loop max1375ms is a process maximum, separate from GET samples.

Cached startup maintenance took33125.874ms: object collection5016.190ms/3881.801ms SQLwall across13559 API calls and13349 rows; projection maintenance12268.468ms including VACUUM8715.093ms/6207.265ms CPU; initial fresh health6906.627ms/final2535.744ms; transcript integrity4463.092ms. No fresh checks/reclamation were skipped. Missingcold1/migration and linked backup acceptance remain open. Normal PA scheduler independent acceptance17d034a6 remains unsatisfied; accepted-original-scope database89957 is separate.

## Every observed operation, endpoint and family

# Actual522 runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, sourcede576ebb/PID76726/startup2acd7a30/package6439e272. Runtime snapshot UTC 2026-10-18T12:50:48.580225+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

No own disposable tests or fixtures overlapped the95GET window; cached maintenance and runtime collectors follow it in a parallel read-only batch and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 57.902 | 57.902 | 57.902 | 57.902 | 57.902 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 0.434 | 0.434 | 0.434 | 0.434 | 0.434 |
| acp.wire_append | 333 | 0 | 0/0/0 | 256 | 1.066 | 21.815 | 134.65 | 239.549 | 239.549 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 0.541 | 0.541 | 0.541 | 0.541 | 0.541 |
| agent.project_recovery_availability | 30 | 0 | 0/0/0 | 30 | 1.725 | 49.897 | 69.461 | 69.461 | 69.461 |
| agent.prompt_compose | 1 | 0 | 0/0/0 | 1 | 116.375 | 116.375 | 116.375 | 116.375 | 116.375 |
| agent.provider_resolve | 1 | 0 | 0/0/0 | 1 | 2.341 | 2.341 | 2.341 | 2.341 | 2.341 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 0.946 | 0.946 | 0.946 | 0.946 | 0.946 |
| agent.recovery_sessions | 14 | 1 | 0/0/0 | 14 | 169.254 | 676.943 | 676.943 | 676.943 | 676.943 |
| agent.recovery_snapshot | 1 | 0 | 0/0/0 | 1 | 53.402 | 53.402 | 53.402 | 53.402 | 53.402 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 3.951 | 3.951 | 3.951 | 3.951 | 3.951 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 1674.196 | 1674.196 | 1674.196 | 1674.196 | 1674.196 |
| dispatch.completion_pending_read | 19 | 0 | 0/0/0 | 19 | 3.753 | 90.859 | 90.859 | 90.859 | 90.859 |
| dispatch.followup_pending_read | 19 | 0 | 0/0/0 | 19 | 2.531 | 208.584 | 208.584 | 208.584 | 208.584 |
| dispatch.record_read | 281 | 0 | 0/0/0 | 256 | 19.692 | 61.472 | 342.479 | 401.834 | 401.834 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 2.222 | 57.644 | 57.644 | 57.644 | 57.644 |
| filesystem.github_credentials_read | 37 | 0 | 0/0/0 | 37 | 1.941 | 228.358 | 258.23 | 258.23 | 258.23 |
| fleet.overview.reachability | 7 | 0 | 0/0/0 | 7 | 9.115 | 21.83 | 21.83 | 21.83 | 21.83 |
| fleet.overview.status | 7 | 0 | 0/0/0 | 7 | 11.578 | 14.109 | 14.109 | 14.109 | 14.109 |
| fleet.overview.sync | 13 | 0 | 0/0/0 | 13 | 58.627 | 264.053 | 264.053 | 264.053 | 264.053 |
| fleet.overview_cache_read | 298 | 1 | 0/0/0 | 256 | 1.452 | 24.857 | 59.626 | 118.263 | 840.905 |
| fleet.overview_cache_write | 90 | 2 | 0/0/0 | 90 | 14.668 | 192.349 | 856.583 | 856.583 | 856.583 |
| fleet.overview_response_json | 63 | 0 | 0/0/0 | 63 | 6.538 | 51.026 | 217.983 | 217.983 | 217.983 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 12.537 | 12.537 | 12.537 | 12.537 | 12.537 |
| goal.dispatch_lifecycle_index | 5 | 0 | 0/0/0 | 5 | 0.661 | 13.49 | 13.49 | 13.49 | 13.49 |
| health.canonical_obligation | 29 | 1 | 0/0/0 | 29 | 35.269 | 258.699 | 1260.486 | 1260.486 | 1260.486 |
| http.fleet_overview | 63 | 0 | 0/0/0 | 63 | 119.131 | 2080.608 | 2108.327 | 2108.327 | 2108.327 |
| http.github | 1 | 0 | 0/0/0 | 1 | 828.578 | 828.578 | 828.578 | 828.578 | 828.578 |
| http.pr_supervisor_peer | 274 | 0 | 0/0/0 | 256 | 44.957 | 145.191 | 369.112 | 401.05 | 401.05 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 33129.466 | 33129.466 | 33129.466 | 33129.466 | 33129.466 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 2.364 | 19.331 | 19.331 | 19.331 | 19.331 |
| notifications.present_page | 2 | 1 | 0/0/0 | 2 | 234.383 | 777.33 | 777.33 | 777.33 | 777.33 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 21.664 | 21.664 | 21.664 | 21.664 | 21.664 |
| pr_supervisor.response_json | 274 | 0 | 0/0/0 | 256 | 0.8 | 14.564 | 95.875 | 285.169 | 285.169 |
| preferences.agent_auto_approve_read | 4 | 0 | 0/0/0 | 4 | 3.223 | 4.54 | 4.54 | 4.54 | 4.54 |
| progress.checkpoint_write | 4 | 0 | 0/0/0 | 4 | 296.039 | 308.049 | 308.049 | 308.049 | 308.049 |
| progress.dispatch_read | 37 | 0 | 0/0/0 | 37 | 61.892 | 113.342 | 156.373 | 156.373 | 156.373 |
| progress.heartbeat_write | 7 | 0 | 0/0/0 | 7 | 176.73 | 285.003 | 285.003 | 285.003 | 285.003 |
| progress.pending_read | 33 | 0 | 0/0/0 | 33 | 13.118 | 52.834 | 90.802 | 90.802 | 90.802 |
| progress.sequence_allocate | 11 | 0 | 0/0/0 | 11 | 151.903 | 242.984 | 242.984 | 242.984 | 242.984 |
| reconciliation.completion_evidence_read | 57 | 0 | 0/0/0 | 57 | 4.077 | 40.961 | 73.122 | 73.122 | 73.122 |
| reconciliation.dispatch_read | 96 | 1 | 0/0/0 | 96 | 2.224 | 23.616 | 1557.043 | 1557.043 | 1557.043 |
| reconciliation.session_read | 57 | 1 | 0/0/0 | 57 | 2.157 | 33.383 | 970.791 | 970.791 | 970.791 |
| selection.attempt_authority | 1 | 0 | 0/0/0 | 1 | 12.931 | 12.931 | 12.931 | 12.931 | 12.931 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 27.133 | 27.133 | 27.133 | 27.133 | 27.133 |
| selection.prompt_identity | 1 | 0 | 0/0/0 | 1 | 41.708 | 41.708 | 41.708 | 41.708 | 41.708 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0/0 | 2 | 23.262 | 39.029 | 39.029 | 39.029 | 39.029 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 3.501 | 32.111 | 36.974 | 36.974 | 36.974 |
| sqlite.agent_session_save | 39 | 1 | 0/0/0 | 39 | 13.601 | 232.932 | 1783.073 | 1783.073 | 1783.073 |
| sqlite.card_read | 419 | 0 | 0/0/0 | 256 | 1.681 | 9.871 | 68.305 | 82.587 | 285.685 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0/0 | 2 | 4.645 | 59.124 | 59.124 | 59.124 | 59.124 |
| sqlite.pr_supervisor_completion_due_read | 35 | 0 | 0/0/0 | 35 | 19.57 | 381.242 | 446.696 | 446.696 | 446.696 |
| sqlite.pr_supervisor_due_read | 34 | 0 | 0/0/0 | 34 | 3.802 | 71.659 | 99.983 | 99.983 | 99.983 |
| sqlite.pr_supervisor_event_write | 137 | 0 | 0/0/0 | 137 | 3.469 | 36.114 | 62.783 | 107.321 | 107.321 |
| sqlite.pr_supervisor_watch_read | 137 | 0 | 0/0/0 | 137 | 5.442 | 65.27 | 101.132 | 140.825 | 140.825 |
| sqlite.pr_supervisor_watch_write | 137 | 1 | 0/0/0 | 137 | 10.329 | 120.917 | 240.938 | 1246.777 | 1246.777 |
| sqlite.project_read | 289 | 0 | 0/0/0 | 256 | 0.98 | 12.374 | 35.941 | 137.909 | 188.086 |
| sqlite.repository_read | 282 | 0 | 0/0/0 | 256 | 1.201 | 29.844 | 151.829 | 260.566 | 260.566 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 1.303 | 5.377 | 5.377 | 5.377 | 5.377 |
| sqlite.restart_handoff_completion | 60 | 0 | 0/0/0 | 60 | 7.399 | 65.296 | 155.133 | 155.133 | 155.133 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 10.847 | 10.847 | 10.847 | 10.847 | 10.847 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 2.547 | 2.547 | 2.547 | 2.547 | 2.547 |
| sqlite.restart_handoff_session | 30 | 0 | 0/0/0 | 30 | 7.725 | 25.033 | 31.06 | 31.06 | 31.06 |
| sqlite.restart_handoffs_pending | 15 | 0 | 0/0/0 | 15 | 3.591 | 399.503 | 399.503 | 399.503 | 399.503 |
| sqlite.restart_handoffs_watchdog | 14 | 0 | 0/0/0 | 14 | 2.239 | 140.298 | 140.298 | 140.298 | 140.298 |
| sqlite.transcript_append | 54 | 2 | 0/0/0 | 54 | 16.241 | 253.9 | 824.402 | 824.402 | 824.402 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 17.747 | 17.747 | 17.747 | 17.747 | 17.747 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.43 | 4.43 | 4.43 | 4.43 | 4.43 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 8.242 | 8.242 | 8.242 | 8.242 | 8.242 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1610.451 | 1610.451 | 1610.451 | 1610.451 | 1610.451 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 2.172 | 50.169 | 50.169 | 50.169 | 50.169 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 25.157 | 25.157 | 25.157 | 25.157 | 25.157 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 3.929 | 23.283 | 23.283 | 23.283 | 23.283 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 63.466 | 1898.522 | 1898.522 | 1898.522 | 1898.522 |
| sync.reconcile_head | 12 | 9 | 0/0/0 | 12 | 742.359 | 3307.301 | 3307.301 | 3307.301 | 3307.301 |
| sync.ref_read | 32 | 0 | 0/0/0 | 32 | 0.984 | 5.348 | 23.692 | 23.692 | 23.692 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 96.347 | 96.347 | 96.347 | 96.347 | 96.347 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7231.688 | 7231.688 | 7231.688 | 7231.688 | 7231.688 |
| workspace.lease_renew | 1 | 0 | 0/0/0 | 1 | 197.967 | 197.967 | 197.967 | 197.967 | 197.967 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 2172.451 | 2495.192 | 2495.192 | 2495.192 | 2495.192 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1280.976 | 1280.976 | 1280.976 | 1280.976 | 1280.976 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 30.622 | 73.678 | 87.31 | 87.31 | 87.31 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 336 | 0 | 0 | 0 | 239.549 |
| agent | 8 | 50 | 2 | 0 | 0 | 1674.196 |
| dispatch | 4 | 326 | 0 | 0 | 0 | 401.834 |
| filesystem | 1 | 37 | 0 | 0 | 0 | 258.23 |
| fleet | 7 | 479 | 3 | 0 | 0 | 856.583 |
| goal | 1 | 5 | 0 | 0 | 0 | 13.49 |
| health | 1 | 29 | 1 | 0 | 0 | 1260.486 |
| http | 3 | 338 | 0 | 0 | 0 | 2108.327 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 33129.466 |
| notifications | 2 | 4 | 1 | 0 | 0 | 777.33 |
| pr_supervisor | 2 | 275 | 0 | 0 | 0 | 285.169 |
| preferences | 1 | 4 | 0 | 0 | 0 | 4.54 |
| progress | 5 | 92 | 0 | 0 | 0 | 308.049 |
| reconciliation | 3 | 210 | 2 | 0 | 0 | 1557.043 |
| selection | 3 | 3 | 0 | 0 | 0 | 41.708 |
| sqlite | 21 | 1712 | 4 | 0 | 0 | 1783.073 |
| startup | 3 | 3 | 1 | 0 | 0 | 1610.451 |
| sync | 7 | 82 | 9 | 0 | 0 | 3307.301 |
| workspace | 4 | 10 | 1 | 1 | 0 | 7231.688 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 11.959 | 57.843 | 57.843 | 57.843 |
| /api/agent/observability/v1/sessions/{session_id} | 1 | 4020 | 1 | 66.337 | 66.337 | 66.337 | 66.337 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 639138 | 2 | 22.17 | 37.765 | 37.765 | 37.765 |
| /api/configuration | 1 | 169949 | 1 | 16.331 | 16.331 | 16.331 | 16.331 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 37.765 | 40.609 | 40.609 | 40.609 |
| /api/fleet/overview | 5 | 5099380 | 5 | 192.501 | 241.567 | 241.567 | 241.567 |
| /api/fleet/overview/local | 20 | 575728 | 20 | 59.615 | 185.921 | 191.281 | 191.281 |
| /api/health | 20 | 300 | 20 | 2.158 | 2.86 | 2.916 | 2.916 |
| /api/instance | 1 | 560 | 1 | 4.77 | 4.77 | 4.77 | 4.77 |
| /api/instance/maintenance | 1 | 7454 | 1 | 175.55 | 175.55 | 175.55 | 175.55 |
| /api/notifications | 2 | 22218 | 2 | 247.604 | 916.618 | 916.618 | 916.618 |
| /api/owner-ready | 1 | 71 | 1 | 3.919 | 3.919 | 3.919 | 3.919 |
| /api/pr-supervisor/capabilities | 1 | 2707 | 1 | 16.929 | 16.929 | 16.929 | 16.929 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 8.147 | 136.251 | 136.251 | 136.251 |
| /api/pr-supervisor/replicas | 281 | 1556480 | 256 | 43.709 | 182.134 | 424.474 | 599.083 |
| /api/ready | 20 | 2234 | 20 | 3.384 | 32.966 | 145.478 | 145.478 |
| /api/runtime | 20 | 5837092 | 20 | 42.778 | 84.07 | 99.283 | 99.283 |
| /api/status | 45 | 125550 | 45 | 67.302 | 476.049 | 590.834 | 590.834 |
| /api/ui/assets | 2 | 158 | 2 | 6.026 | 19.575 | 19.575 | 19.575 |
| /api/workspaces | 1 | 1439 | 1 | 9.844 | 9.844 | 9.844 | 9.844 |
| /fleet | 5 | 5482915 | 5 | 328.142 | 598.672 | 598.672 | 598.672 |
| /pull-requests | 5 | 125605 | 5 | 94.628 | 172.065 | 172.065 | 172.065 |
| /{page_path:path} | 15 | 1255660 | 15 | 20.748 | 554.324 | 554.324 | 554.324 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.recovery_sessions | 1 | 676.943 | 676.943 | 676.943 | 676.943 |
| agent.startup_state_read | 1 | 1674.196 | 1674.196 | 1674.196 | 1674.196 |
| fleet.overview_cache_read | 1 | 840.905 | 840.905 | 840.905 | 840.905 |
| fleet.overview_cache_write | 2 | 844.215 | 856.583 | 856.583 | 856.583 |
| health.canonical_obligation | 1 | 1260.486 | 1260.486 | 1260.486 | 1260.486 |
| maintenance.run | 1 | 33129.466 | 33129.466 | 33129.466 | 33129.466 |
| notifications.present_page | 1 | 777.33 | 777.33 | 777.33 | 777.33 |
| reconciliation.dispatch_read | 1 | 1557.043 | 1557.043 | 1557.043 | 1557.043 |
| reconciliation.session_read | 1 | 970.791 | 970.791 | 970.791 | 970.791 |
| sqlite.agent_session_save | 1 | 1783.073 | 1783.073 | 1783.073 | 1783.073 |
| sqlite.pr_supervisor_watch_write | 1 | 1246.777 | 1246.777 | 1246.777 | 1246.777 |
| sqlite.transcript_append | 2 | 614.313 | 824.402 | 824.402 | 824.402 |
| startup.openapi | 1 | 1610.451 | 1610.451 | 1610.451 | 1610.451 |
| sync.reconcile_head | 9 | 763.025 | 3307.301 | 3307.301 | 3307.301 |
| workspace.collect_garbage | 1 | 7231.688 | 7231.688 | 7231.688 | 7231.688 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 2 | 676.943 | 1674.196 | 1674.196 | 1674.196 |
| fleet | 3 | 844.215 | 856.583 | 856.583 | 856.583 |
| health | 1 | 1260.486 | 1260.486 | 1260.486 | 1260.486 |
| maintenance | 1 | 33129.466 | 33129.466 | 33129.466 | 33129.466 |
| notifications | 1 | 777.33 | 777.33 | 777.33 | 777.33 |
| reconciliation | 2 | 970.791 | 1557.043 | 1557.043 | 1557.043 |
| sqlite | 4 | 824.402 | 1783.073 | 1783.073 | 1783.073 |
| startup | 1 | 1610.451 | 1610.451 | 1610.451 | 1610.451 |
| sync | 9 | 763.025 | 3307.301 | 3307.301 | 3307.301 |
| workspace | 1 | 7231.688 | 7231.688 | 7231.688 | 7231.688 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 77.369 | 119.152 | 119.152 | 119.152 | 0 | 25464 |
| /agent | 5 | 0 | 159.585 | 659.99 | 659.99 | 659.99 | 1 | 119250 |
| /api/fleet/overview | 5 | 0 | 283.073 | 452.208 | 452.208 | 452.208 | 0 | 1019876 |
| /api/health | 20 | 0 | 21.04 | 49.561 | 71.476 | 71.476 | 0 | 15 |
| /api/runtime | 20 | 0 | 76.314 | 129.334 | 183.381 | 183.381 | 0 | 291994 |
| /api/status | 20 | 0 | 72.471 | 186.025 | 247.943 | 247.943 | 0 | 2824 |
| /fleet | 5 | 0 | 343.088 | 632.784 | 632.784 | 632.784 | 1 | 1096583 |
| /pull-requests | 5 | 0 | 114.351 | 193.288 | 193.288 | 193.288 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 39.128 | 47.259 | 47.259 | 47.259 | 0 | 100559 |
| /work | 5 | 0 | 39.106 | 45.733 | 45.733 | 45.733 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 802.031708 | 7.45625 | 272 | 270 | 71.46986777661368 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 36.875667 | 7.219458 | 2 | 0 | 36.620792059693485 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 155.377666 | 28.658833 | 5 | 0 | 152.26266701938584 | 0.001125037670135498 | 155.2906669676304 |
| retention_object_collection | 1/0 | 5016.190333 | 634.001375 | 13559 | 13349 | 3881.8012553965673 | 0.0015420373529195786 | 0.003958004526793957 |
| retention_canonical | 1/0 | 5172.01175 | 663.092708 | 13564 | 13349 | 4034.063922415953 | 0.0026670750230550766 | 155.29462497215718 |
| retention_mirror | 1/0 | 25.138 | 7.457583 | 2 | 0 | 24.228582973591983 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 5999.443125 | 678.25475 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 0.596625 | 0.483208 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 6906.62675 | 3044.206417 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 935.521625 | 642.256542 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12268.467916 | 8352.112209 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4479.048959 | 2055.966458 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2535.743625 | 1746.9955 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 33125.874417 | 16520.649667 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# Startup reconciliation inventory repair — 2026-10-18

All **562 findings and71 concerns remain open**. Every prior560 row remains unchanged. The [source and complete measurement artifact](benchmarks/systemic-reconciliation-inventory-20261018.json) retains the targeted improvement, all mixed-workload outliers, both test stages and the original coroutine warning.

Startup completion recovery copied the newest1000 full dispatch records before filtering. The new explicit reader applies the same newest window before eligibility filtering and returns full detached candidate records. Recovery parsing, transcript lookup, saves and outbox behavior retain their contracts; generic dispatch listing is unchanged. Candidate copies occur outside the indexed lock. Dynamic legacy adapters use the existing list contract unless the optional reader is explicitly declared.

In20 alternating paired reads over1005 retained dispatches, full snapshots fall1000 to3 per read. p50/p95/p99/max567.750/704.942/737.371/737.371 to7.951/10.103/10.162/10.162ms; total threadCPU10713.984 to144.688ms. Full candidate data/order match and DB/WAL hashes stay unchanged. Immutable strings are not counted as newly copied bytes; all-eligible inventories still need full copies. Actual startup dispatch read3.612s/1.756CPU overlaps other startup work, but causal GIL/nativeIO attribution and deployed improvement remain unverified.

The176-test suite plus6subtests passed in85.66s with one coroutine warning in an existing mocked timeout. The fake receiver now closes its rejected coroutine and asserts closure;502/recoverable assertions and deadlines remain unchanged. Five final edited-test/reader contracts passed in4.90s with no warnings. Original warning evidence remains; no production defect is claimed.

A separate exact-wheel100-iteration mixed run preserves9600 ordered active events,12511 cold hashes,5000 expired deletions and16 audit proofs, with zero operation failure/timeout/rejection deltas. However, **four worker warnings and eight threshold violations remain**: transcript drain1, message history2, object collection4 and maintenance1. Workload101.478s, snapshot capture24.020ms and validation4077.774ms are separate. Bootstrap OpenAPI1058.581ms and a1117.162ms sync-ref completion remain outside the steady window. This is not a controlled wholepath speedup or acceptance.

All23923 lock events and7276 sample records have verified hash chains and lossless journals.30 waits>=100ms overlap summary/append/card holders; nested spans and edge counts overlap. Tracer/journal persistence adds work; raw locks, successful native busy waits, physicalIO and GIL remain gaps. This new source case durably records childexit0 before verification and completes verification. The older parent failure and unknown child exit remain unchanged, with no reconstruction or repeat to seek green.

The activated6b7 GET window remains95HTTP200 with zero threshold violations, but its separate runtime has29 slow counters and2 unexplained workspace failures; cached maintenance37.079s and all earlier provider/peer/backup/log/receipt/browser/retention/unexercised concerns remain. No warning/threshold/deadline/default/configuration/durable data or acceptance gate was weakened. Normal PA scheduler independent acceptance remains required.

## Every mixed workload latency family

| Name | n | p50ms | p95ms | p99ms | maxms | >=500ms |
|---|---:|---:|---:|---:|---:|---:|
| / | 100 | 11.176 | 39.114 | 117.264 | 119.726 | 0 |
| /agent | 100 | 64.774 | 142.244 | 229.503 | 272.118 | 0 |
| /api/agent/history/session-0?limit=64 | 100 | 40.191 | 82.876 | 139.983 | 152.137 | 0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 182.112 | 417.967 | 658.581 | 759.803 | 2 |
| /api/agent/preferences | 100 | 4.899 | 9.989 | 18.219 | 19.253 | 0 |
| /api/agent/sessions?view=chats | 100 | 80.451 | 140.355 | 254.689 | 269.651 | 0 |
| /api/cards | 100 | 8.952 | 14.656 | 40.199 | 120.77 | 0 |
| /api/cards/fixture-card | 100 | 5.155 | 9.98 | 23.492 | 26.149 | 0 |
| /api/cards/fixture-card/history | 100 | 28.507 | 66.181 | 103.47 | 152.601 | 0 |
| /api/execution/defaults | 100 | 5.795 | 9.308 | 15.417 | 36.87 | 0 |
| /api/fleet/dispatch-jobs | 100 | 34.383 | 44.915 | 61.788 | 69.133 | 0 |
| /api/fleet/dispatch-queue | 100 | 7.236 | 14.406 | 22.037 | 22.702 | 0 |
| /api/health | 100 | 3.419 | 5.587 | 6.887 | 25.464 | 0 |
| /api/health-journal/status | 100 | 8.344 | 21.611 | 42.361 | 120.158 | 0 |
| /api/notifications | 100 | 5.515 | 11.107 | 19.367 | 32.805 | 0 |
| /api/pr-supervisor/capabilities | 100 | 7.839 | 14.545 | 23.755 | 31.242 | 0 |
| /api/pr-supervisor/watches | 100 | 7.848 | 16.789 | 23.037 | 27.027 | 0 |
| /api/runtime | 100 | 32.712 | 43.015 | 51.456 | 103.459 | 0 |
| /api/runtime?section=sse | 100 | 5.099 | 7.11 | 22.097 | 33.675 | 0 |
| /api/status | 100 | 51.68 | 75.862 | 110.637 | 151.855 | 0 |
| /api/sync/refs | 100 | 4.784 | 12.045 | 17.965 | 21.015 | 0 |
| /api/sync/status | 100 | 13.06 | 41.061 | 84.692 | 86.596 | 0 |
| /api/workspaces | 100 | 7.068 | 16.73 | 33.17 | 43.445 | 0 |
| /fleet | 100 | 96.789 | 309.061 | 395.831 | 409.754 | 0 |
| /partials/home/sections | 100 | 19.556 | 45.625 | 98.029 | 103.202 | 0 |
| /pull-requests | 100 | 28.958 | 62.074 | 145.677 | 194.331 | 0 |
| /settings?section=agent | 100 | 16.985 | 56.726 | 87.552 | 96.169 | 0 |
| /settings?section=backups | 100 | 15.975 | 51.74 | 96.371 | 102.086 | 0 |
| /work | 100 | 12.602 | 28.071 | 64.075 | 91.932 | 0 |
| acp.wire_append | 100 | 5.142 | 15.562 | 38.896 | 65.572 | 0 |
| dispatch.runnable_read | 100 | 3.128 | 10.808 | 23.539 | 65.986 | 0 |
| fleet.overview_cache_read | 100 | 2.381 | 14.903 | 31.144 | 59.173 | 0 |
| fleet.overview_cache_write | 100 | 14.055 | 30.421 | 48.11 | 56.992 | 0 |
| maintenance.run | 1 | 18069.507 | 18069.507 | 18069.507 | 18069.507 | 1 |
| progress.checkpoint_write | 100 | 10.628 | 21.981 | 34.229 | 59.809 | 0 |
| progress.sequence_allocate | 100 | 9.406 | 28.85 | 54.1 | 65.95 | 0 |
| reconciliation.dispatch_read | 100 | 2.292 | 13.385 | 44.178 | 57.822 | 0 |
| sqlite.card_write | 10 | 49.896 | 181.385 | 181.385 | 181.385 | 0 |
| sqlite.execution_binding_materialize | 100 | 83.485 | 188.045 | 261.595 | 419.431 | 0 |
| sync.commit_read | 100 | 4.995 | 11.966 | 42.16 | 65.382 | 0 |
| sync.object_collect | 10 | 427.673 | 1260.465 | 1260.465 | 1260.465 | 4 |
| sync.push_transaction | 100 | 81.552 | 172.528 | 231.579 | 462.6 | 0 |
| sync.reconcile_head | 100 | 50.701 | 172.738 | 291.627 | 332.909 | 0 |
| transcript.drain | 300 | 125.105 | 201.964 | 345.708 | 504.719 | 1 |

## Preserved earlier coverage

# PR521 actual activation and remaining latency — 2026-10-18

All **560 findings and 69 concerns remain open**. Every existing row is unchanged. The [complete loaded-build artifact](benchmarks/systemic-521-loaded-20261018.json) records server source6b7ec220, package8abdf60e, new PID72162/startup8fd8d8da and unchanged configuration/lease290. The earlier failed head, all CI failures, the install sequencing error and all historical negative measurements remain preserved.

The once-only95 ordinary sequential GET window returned95 HTTP200 with no500ms violations and stable build fences. Status p95=148.432ms, runtime192.304ms, Agent max196.133ms, Fleet max475.639ms. Fleet still returned1,245,872 decoded bytes; overview max1,017,514 bytes. These small samples do not establish a causal speedup or whole-PA acceptance. Five HTML samples have p95=p99=max; the first sample is not guaranteed cold, decoded bytes are not wire bytes, and observer persistence adds work outside request timers. No own fixture or test overlapped this window.

The separate runtime snapshot at2026-10-18T12:16:59.861265+00:00 retains all21 sections,77 primary operations plus1 diagnostic,19 prefix families and22 route groups. It contains29 retained slow observations/counters and2 failed workspace.project_provision calls of7, with their causes unknown. Positive baseline coverage is73/181 operations and19/115 route groups; unobserved paths and branches remain open. All-call family percentiles cannot be inferred from operation summaries; the full tables below label slow-only families and bounded windows separately.

Startup state read remained3837.499ms, including3779.149ms SQLite wall/328.619ms threadCPU for365 rows. Startup reconciliation dispatch read took3612.045ms/1755.972ms threadCPU with only1.571ms indexed-lock hold. OpenAPI warm-up took1317.803ms/1244.790ms CPU. Fleet cache calls with submillisecond service still had up to1201.866ms completion delivery. These observations identify residual work and scheduling delay; they do not isolate every native busy, disk or GIL cause.

Cached startup maintenance took37079.239ms, including object GC5620.436ms/4414.684ms SQL wall, projection maintenance12017.652ms (VACUUM7254.759ms/5909.185ms CPU), first fresh health7228.733ms, transcript integrity5255.202ms and final health5049.112ms. Missing cold content and migration remain open. Fresh checks are retained; parent spans include children. No checks, reclamation, warnings, thresholds or deadlines were skipped or changed.

Activation receiptbbcc5388 is error-free and continuation_queued while an older supervisor notice runs. Loaded source is already independently proved; do not restart again or repeat the95GET window when that continuation arrives. Original preinstall lane gate failed and the orchestrator nevertheless launched the exact install. Fresh supported lane CAS restored Active and post-install gates passed; report84bb1b70 and the failed stage remain explicit. Normal PA scheduler independent acceptance17d034a6 remains unsatisfied. Backup0331 and accepted-original-scope database89957 ownership are unchanged.

## Every observed operation, endpoint and family

# Actual521 runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, source6b7ec220/PID72162/startup8fd8d8da/package8abdf60e. Runtime snapshot UTC 2026-10-18T12:16:59.861265+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

No own disposable tests or fixtures overlapped the95GET window; cached maintenance and runtime collectors follow it in a parallel read-only batch and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 100.474 | 100.474 | 100.474 | 100.474 | 100.474 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 2.004 | 2.004 | 2.004 | 2.004 | 2.004 |
| acp.wire_append | 163 | 0 | 0/0/0 | 163 | 1.02 | 85.078 | 139.421 | 149.073 | 149.073 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 1.111 | 1.111 | 1.111 | 1.111 | 1.111 |
| agent.project_recovery_availability | 32 | 0 | 0/0/0 | 32 | 2.488 | 44.969 | 48.794 | 48.794 | 48.794 |
| agent.prompt_compose | 1 | 0 | 0/0/0 | 1 | 55.846 | 55.846 | 55.846 | 55.846 | 55.846 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 11.977 | 11.977 | 11.977 | 11.977 | 11.977 |
| agent.recovery_sessions | 15 | 0 | 0/0/0 | 15 | 135.537 | 477.114 | 477.114 | 477.114 | 477.114 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 1.719 | 1.719 | 1.719 | 1.719 | 1.719 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 3837.499 | 3837.499 | 3837.499 | 3837.499 | 3837.499 |
| dispatch.completion_pending_read | 20 | 0 | 0/0/0 | 20 | 3.476 | 21.676 | 41.483 | 41.483 | 41.483 |
| dispatch.followup_pending_read | 20 | 0 | 0/0/0 | 20 | 2.387 | 62.376 | 196.453 | 196.453 | 196.453 |
| dispatch.record_read | 269 | 0 | 0/0/0 | 256 | 20.256 | 42.229 | 109.751 | 388.877 | 388.877 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 9.83 | 61.305 | 61.305 | 61.305 | 61.305 |
| filesystem.github_credentials_read | 36 | 0 | 0/0/0 | 36 | 3.936 | 69.276 | 86.581 | 86.581 | 86.581 |
| fleet.overview.reachability | 7 | 0 | 0/0/0 | 7 | 19.493 | 25.225 | 25.225 | 25.225 | 25.225 |
| fleet.overview.status | 7 | 0 | 0/0/0 | 7 | 20.56 | 25.78 | 25.78 | 25.78 | 25.78 |
| fleet.overview.sync | 12 | 0 | 0/0/0 | 12 | 52.217 | 219.418 | 219.418 | 219.418 | 219.418 |
| fleet.overview_cache_read | 293 | 7 | 0/0/0 | 256 | 4.569 | 54.972 | 101.582 | 132.918 | 1517.825 |
| fleet.overview_cache_write | 89 | 0 | 0/0/0 | 89 | 37.516 | 257.641 | 414.471 | 414.471 | 414.471 |
| fleet.overview_response_json | 63 | 0 | 0/0/0 | 63 | 21.555 | 56.648 | 77.403 | 77.403 | 77.403 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 11.9 | 11.9 | 11.9 | 11.9 | 11.9 |
| goal.dispatch_lifecycle_index | 5 | 0 | 0/0/0 | 5 | 0.85 | 13.445 | 13.445 | 13.445 | 13.445 |
| health.canonical_obligation | 29 | 1 | 0/0/0 | 29 | 45.726 | 314.737 | 1411.028 | 1411.028 | 1411.028 |
| http.fleet_overview | 63 | 0 | 0/0/0 | 63 | 282.417 | 1914.695 | 1981.503 | 1981.503 | 1981.503 |
| http.github | 1 | 0 | 0/0/0 | 1 | 600.767 | 600.767 | 600.767 | 600.767 | 600.767 |
| http.pr_supervisor_peer | 240 | 0 | 0/0/0 | 240 | 56.979 | 197.903 | 552.296 | 1172.675 | 1172.675 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 37085.426 | 37085.426 | 37085.426 | 37085.426 | 37085.426 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 3.593 | 87.794 | 87.794 | 87.794 | 87.794 |
| notifications.present_page | 2 | 1 | 0/0/0 | 2 | 226.989 | 594.95 | 594.95 | 594.95 | 594.95 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 1.23 | 1.23 | 1.23 | 1.23 | 1.23 |
| pr_supervisor.response_json | 240 | 0 | 0/0/0 | 240 | 1.604 | 23.131 | 65.67 | 68.365 | 68.365 |
| preferences.agent_auto_approve_read | 5 | 0 | 0/0/0 | 5 | 3.355 | 138.223 | 138.223 | 138.223 | 138.223 |
| progress.checkpoint_write | 2 | 0 | 0/0/0 | 2 | 288.039 | 313.955 | 313.955 | 313.955 | 313.955 |
| progress.dispatch_read | 38 | 0 | 0/0/0 | 38 | 60.845 | 138.606 | 227.019 | 227.019 | 227.019 |
| progress.heartbeat_write | 6 | 0 | 0/0/0 | 6 | 172.762 | 370.075 | 370.075 | 370.075 | 370.075 |
| progress.pending_read | 34 | 1 | 0/0/0 | 34 | 14.2 | 113.273 | 1024.009 | 1024.009 | 1024.009 |
| progress.sequence_allocate | 8 | 0 | 0/0/0 | 8 | 147.015 | 324.508 | 324.508 | 324.508 | 324.508 |
| reconciliation.completion_evidence_read | 57 | 0 | 0/0/0 | 57 | 4.622 | 144.974 | 409.908 | 409.908 | 409.908 |
| reconciliation.dispatch_read | 96 | 1 | 0/0/0 | 96 | 2.682 | 58.221 | 3612.045 | 3612.045 | 3612.045 |
| reconciliation.session_read | 57 | 1 | 0/0/0 | 57 | 2.968 | 71.036 | 1537.152 | 1537.152 | 1537.152 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 159.303 | 159.303 | 159.303 | 159.303 | 159.303 |
| selection.prompt_identity | 1 | 0 | 0/0/0 | 1 | 94.066 | 94.066 | 94.066 | 94.066 | 94.066 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0/0 | 2 | 34.418 | 37.869 | 37.869 | 37.869 | 37.869 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 9.634 | 36.791 | 65.123 | 65.123 | 65.123 |
| sqlite.agent_session_save | 34 | 0 | 0/0/0 | 34 | 23.43 | 194.556 | 329.302 | 329.302 | 329.302 |
| sqlite.card_read | 390 | 1 | 0/0/0 | 256 | 1.838 | 19.48 | 90.131 | 156.668 | 515.747 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0/0 | 2 | 48.581 | 116.073 | 116.073 | 116.073 | 116.073 |
| sqlite.pr_supervisor_completion_due_read | 34 | 0 | 0/0/0 | 34 | 37.068 | 211.753 | 276.678 | 276.678 | 276.678 |
| sqlite.pr_supervisor_due_read | 34 | 0 | 0/0/0 | 34 | 3.93 | 109.816 | 117.961 | 117.961 | 117.961 |
| sqlite.pr_supervisor_event_write | 120 | 0 | 0/0/0 | 120 | 3.532 | 93.517 | 225.175 | 248.319 | 248.319 |
| sqlite.pr_supervisor_watch_read | 120 | 1 | 0/0/0 | 120 | 5.332 | 96.948 | 204.684 | 583.789 | 583.789 |
| sqlite.pr_supervisor_watch_write | 120 | 0 | 0/0/0 | 120 | 9.891 | 141.209 | 184.396 | 369.666 | 369.666 |
| sqlite.project_read | 276 | 0 | 0/0/0 | 256 | 1.048 | 19.976 | 92.627 | 141.431 | 141.431 |
| sqlite.repository_read | 269 | 0 | 0/0/0 | 256 | 1.543 | 41.713 | 151.293 | 219.703 | 219.703 |
| sqlite.restart_handoff_completion | 64 | 0 | 0/0/0 | 64 | 6.609 | 62.401 | 160.145 | 160.145 | 160.145 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 11.166 | 11.166 | 11.166 | 11.166 | 11.166 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 2.643 | 2.643 | 2.643 | 2.643 | 2.643 |
| sqlite.restart_handoff_session | 32 | 0 | 0/0/0 | 32 | 6.756 | 32.954 | 73.528 | 73.528 | 73.528 |
| sqlite.restart_handoffs_pending | 16 | 0 | 0/0/0 | 16 | 2.736 | 75.272 | 75.272 | 75.272 | 75.272 |
| sqlite.restart_handoffs_watchdog | 15 | 0 | 0/0/0 | 15 | 1.849 | 50.228 | 50.228 | 50.228 | 50.228 |
| sqlite.transcript_append | 17 | 1 | 0/0/0 | 17 | 24.662 | 1608.216 | 1608.216 | 1608.216 | 1608.216 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 47.098 | 47.098 | 47.098 | 47.098 | 47.098 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.507 | 4.507 | 4.507 | 4.507 | 4.507 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 8.542 | 8.542 | 8.542 | 8.542 | 8.542 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1317.803 | 1317.803 | 1317.803 | 1317.803 | 1317.803 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 2.028 | 20.695 | 20.695 | 20.695 | 20.695 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 81.329 | 81.329 | 81.329 | 81.329 | 81.329 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 4.098 | 53.821 | 53.821 | 53.821 | 53.821 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 76.094 | 1758.149 | 1758.149 | 1758.149 | 1758.149 |
| sync.reconcile_head | 12 | 10 | 0/0/0 | 12 | 714.885 | 3430.336 | 3430.336 | 3430.336 | 3430.336 |
| sync.ref_read | 32 | 0 | 0/0/0 | 32 | 1.035 | 45.598 | 119.377 | 119.377 | 119.377 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 85.695 | 85.695 | 85.695 | 85.695 | 85.695 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7287.439 | 7287.439 | 7287.439 | 7287.439 | 7287.439 |
| workspace.lease_renew | 1 | 0 | 0/0/0 | 1 | 280.233 | 280.233 | 280.233 | 280.233 | 280.233 |
| workspace.project_provision | 7 | 0 | 2/0/0 | 7 | 1510.776 | 3133.768 | 3133.768 | 3133.768 | 3133.768 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1828.013 | 1828.013 | 1828.013 | 1828.013 | 1828.013 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 31.48 | 88.185 | 111.614 | 111.614 | 111.614 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 166 | 0 | 0 | 0 | 149.073 |
| agent | 6 | 51 | 1 | 0 | 0 | 3837.499 |
| dispatch | 4 | 316 | 0 | 0 | 0 | 388.877 |
| filesystem | 1 | 36 | 0 | 0 | 0 | 86.581 |
| fleet | 7 | 472 | 7 | 0 | 0 | 1517.825 |
| goal | 1 | 5 | 0 | 0 | 0 | 13.445 |
| health | 1 | 29 | 1 | 0 | 0 | 1411.028 |
| http | 3 | 304 | 0 | 0 | 0 | 1981.503 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 37085.426 |
| notifications | 2 | 4 | 1 | 0 | 0 | 594.95 |
| pr_supervisor | 2 | 241 | 0 | 0 | 0 | 68.365 |
| preferences | 1 | 5 | 0 | 0 | 0 | 138.223 |
| progress | 5 | 88 | 1 | 0 | 0 | 1024.009 |
| reconciliation | 3 | 210 | 2 | 0 | 0 | 3612.045 |
| selection | 2 | 2 | 0 | 0 | 0 | 159.303 |
| sqlite | 20 | 1569 | 3 | 0 | 0 | 1608.216 |
| startup | 3 | 3 | 1 | 0 | 0 | 1317.803 |
| sync | 7 | 82 | 10 | 0 | 0 | 3430.336 |
| workspace | 4 | 10 | 1 | 2 | 0 | 7287.439 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 11.25 | 336.339 | 336.339 | 336.339 |
| /api/agent/observability/v1/sessions/{session_id} | 2 | 9245 | 2 | 61.762 | 67.929 | 67.929 | 67.929 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 602594 | 2 | 24.143 | 31.148 | 31.148 | 31.148 |
| /api/configuration | 1 | 169949 | 1 | 16.551 | 16.551 | 16.551 | 16.551 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 18.966 | 28.128 | 28.128 | 28.128 |
| /api/fleet/overview | 5 | 5087570 | 5 | 109.471 | 224.295 | 224.295 | 224.295 |
| /api/fleet/overview/local | 18 | 64782 | 18 | 33.744 | 408.855 | 408.855 | 408.855 |
| /api/health | 20 | 300 | 20 | 2.185 | 2.75 | 3.118 | 3.118 |
| /api/instance | 1 | 560 | 1 | 5.606 | 5.606 | 5.606 | 5.606 |
| /api/notifications | 2 | 22218 | 2 | 247.437 | 917.032 | 917.032 | 917.032 |
| /api/owner-ready | 1 | 71 | 1 | 6.007 | 6.007 | 6.007 | 6.007 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 143.584 | 319.286 | 319.286 | 319.286 |
| /api/pr-supervisor/replicas | 269 | 1490053 | 256 | 45.611 | 278.561 | 582.486 | 642.007 |
| /api/ready | 18 | 2205 | 18 | 3.532 | 12.082 | 12.082 | 12.082 |
| /api/runtime | 20 | 5729851 | 20 | 41.022 | 122.36 | 122.441 | 122.441 |
| /api/status | 44 | 123491 | 44 | 60.777 | 330.472 | 474.016 | 474.016 |
| /api/sync/refs | 2 | 518 | 2 | 5.82 | 37.891 | 37.891 | 37.891 |
| /api/ui/assets | 2 | 158 | 2 | 6.703 | 93.495 | 93.495 | 93.495 |
| /api/workspaces | 1 | 1439 | 1 | 7.453 | 7.453 | 7.453 | 7.453 |
| /fleet | 5 | 6229360 | 5 | 278.766 | 373.284 | 373.284 | 373.284 |
| /pull-requests | 5 | 125605 | 5 | 46.729 | 145.184 | 145.184 | 145.184 |
| /{page_path:path} | 15 | 1254750 | 15 | 52.15 | 136.311 | 136.311 | 136.311 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.startup_state_read | 1 | 3837.499 | 3837.499 | 3837.499 | 3837.499 |
| fleet.overview_cache_read | 7 | 1018.62 | 1517.825 | 1517.825 | 1517.825 |
| health.canonical_obligation | 1 | 1411.028 | 1411.028 | 1411.028 | 1411.028 |
| maintenance.run | 1 | 37085.426 | 37085.426 | 37085.426 | 37085.426 |
| notifications.present_page | 1 | 594.95 | 594.95 | 594.95 | 594.95 |
| progress.pending_read | 1 | 1024.009 | 1024.009 | 1024.009 | 1024.009 |
| reconciliation.dispatch_read | 1 | 3612.045 | 3612.045 | 3612.045 | 3612.045 |
| reconciliation.session_read | 1 | 1537.152 | 1537.152 | 1537.152 | 1537.152 |
| sqlite.card_read | 1 | 515.747 | 515.747 | 515.747 | 515.747 |
| sqlite.pr_supervisor_watch_read | 1 | 583.789 | 583.789 | 583.789 | 583.789 |
| sqlite.transcript_append | 1 | 1608.216 | 1608.216 | 1608.216 | 1608.216 |
| startup.openapi | 1 | 1317.803 | 1317.803 | 1317.803 | 1317.803 |
| sync.reconcile_head | 10 | 722.494 | 3430.336 | 3430.336 | 3430.336 |
| workspace.collect_garbage | 1 | 7287.439 | 7287.439 | 7287.439 | 7287.439 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 1 | 3837.499 | 3837.499 | 3837.499 | 3837.499 |
| fleet | 7 | 1018.62 | 1517.825 | 1517.825 | 1517.825 |
| health | 1 | 1411.028 | 1411.028 | 1411.028 | 1411.028 |
| maintenance | 1 | 37085.426 | 37085.426 | 37085.426 | 37085.426 |
| notifications | 1 | 594.95 | 594.95 | 594.95 | 594.95 |
| progress | 1 | 1024.009 | 1024.009 | 1024.009 | 1024.009 |
| reconciliation | 2 | 1537.152 | 3612.045 | 3612.045 | 3612.045 |
| sqlite | 3 | 583.789 | 1608.216 | 1608.216 | 1608.216 |
| startup | 1 | 1317.803 | 1317.803 | 1317.803 | 1317.803 |
| sync | 10 | 722.494 | 3430.336 | 3430.336 | 3430.336 |
| workspace | 1 | 7287.439 | 7287.439 | 7287.439 | 7287.439 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 30.771 | 416.368 | 416.368 | 416.368 | 0 | 25464 |
| /agent | 5 | 0 | 155.217 | 196.133 | 196.133 | 196.133 | 0 | 119068 |
| /api/fleet/overview | 5 | 0 | 148.023 | 321.762 | 321.762 | 321.762 | 0 | 1017514 |
| /api/health | 20 | 0 | 20.604 | 74.443 | 204.3 | 204.3 | 0 | 15 |
| /api/runtime | 20 | 0 | 89.889 | 192.304 | 209.189 | 209.189 | 0 | 286646 |
| /api/status | 20 | 0 | 75.249 | 148.432 | 184.105 | 184.105 | 0 | 2824 |
| /fleet | 5 | 0 | 298.199 | 475.639 | 475.639 | 475.639 | 0 | 1245872 |
| /pull-requests | 5 | 0 | 74.636 | 167.053 | 167.053 | 167.053 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 35.105 | 358.184 | 358.184 | 358.184 | 0 | 100559 |
| /work | 5 | 0 | 80.823 | 136.275 | 136.275 | 136.275 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 59.537625 | 8.2025 | 273 | 270 | 16.35283202631399 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 25.818792 | 6.405834 | 2 | 0 | 25.573916034772992 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 137.028334 | 29.053833 | 5 | 0 | 134.77458205306903 | 0.0014579854905605316 | 136.91716705216095 |
| retention_object_collection | 1/0 | 5620.436042 | 629.296042 | 13434 | 13226 | 4414.683858572971 | 0.0016669509932398796 | 0.0037500285543501377 |
| retention_canonical | 1/0 | 5757.784625 | 658.659 | 13439 | 13226 | 4549.45844062604 | 0.0031249364838004112 | 136.9209170807153 |
| retention_mirror | 1/0 | 21.824958 | 6.285542 | 2 | 0 | 21.339791011996567 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 5839.454958 | 673.44525 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 0.641333 | 0.47975 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 7228.733458 | 3100.852458 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1663.865208 | 1116.435625 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12017.651875 | 8140.911708 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 5279.2975 | 2049.112917 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 5049.111791 | 2481.535541 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 37079.239417 | 17563.151209 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# Startup inventory compatibility correction — 2026-10-18

All **560 findings and 69 concerns remain open**. The previous 559 rows remain unchanged. The [correction artifact](benchmarks/systemic-startup-config-correction-20261018.json) preserves all eight failures from original PR521 head3618/CI39125018198, the native before/after regressions, expanded tests, and corrected-source measurements. The failed head was never merged or deployed; its CI was not retried.

The optional reader lookup fabricated a callable on dynamic legacy adapters, producing an empty startup inventory. The corrected helper checks that the reader is declared before binding it; legacy adapters retain their existing list_sessions contract. Three unchanged recovery tests failed before and passed after. The expanded suite passes151 tests in72.80s, including the full live-event module; the workspace GC protection case passes in8.04s. Two adapter contracts were added, with no assertion or deadline relaxation.

The guarded SQL is unchanged. A separate20-pair fixture measures the corrected helper and implemented read together:160 rows/models per read,41,988,864 to35,744 configuration characters; baseline p50/p95/p99/max 552.815/615.272/649.022/649.022ms, corrected 174.925/231.039/258.911/258.911ms. Ordering, snapshots, recovery decisions/actions, nonconfiguration fields and15 legacy JSON cases match. This is fixture evidence, not deployed or full startup acceptance. Every original measurement and failure remains linked below.

Actual c43f still has three threshold violations in its95-GET window,35.414s cached maintenance and an unexplained workspace failure. The instrumented mixed fixture still has six threshold violations and an unknown child exit status after the parent verifier failed. All native busy/IO/GIL/GC, startup/schema, history/sync, endpoint/provider/peer, log-loss/receipt/retention, browser and backup concerns retain their owners and next actions. Normal PA scheduler acceptance remains independent and unsatisfied.

The [complete disposable sample/lock journals and fixture log](benchmarks/systemic-all-holder-journals-20261018.json) retain every sourcefixture sample and lock event; losslessgzip manifests include originalSHA256s. Parentverifierfailed/childexituncaptured remain explicit.

# Startup configuration repair and complete lock-holder evidence — 2026-10-18

All **559 findings and 68 concerns remain open**. Every preceding558row/owner/action is preserved. The [full source/measurement/failure artifact](benchmarks/systemic-startup-config-20261018.json) and [lossless full lock journal](benchmarks/systemic-all-holder-locks-20261018.jsonl.gz) distinguish actualc43f observations, instrumentedfixture outcomes, a rejectedrepair and the finalstartup source repair. No warning/deadline/default/dependency/configuration change, selfacceptance orDone.

The existing startup read already excluded diagnosticmetrics. It still materialized unrelated configuration proofs. A newexplicitread-only startup inventory selects configuration/durable_runtime/provisioning while preserving everyclosed/archivedID, status/controlmode/provenance/binding/ordering and snapshot input. General APIs returnfullconfig; recovery stillre-fetches acompletecurrent session before creatingorsavingruntime. Duplicate selectedkeys/scalar/root/malformedJSON pass through theexistingdecoder to preserve Pythonlast-key/legacy/error behavior. NoSQLstatusfilter ornewadmission policy.

Finalsource160sessions/20alternatingpairedreads materializes41,988,864->35,744configurationcharacters (160models/rows each), p50/p95/p99/max594.507/687.640/697.018/697.018->193.899/287.739/314.016/314.016ms. Guardedprototypep95645.196->194.823 andinitialunguardedprototype are retained separately; theunguardedprototype wasnotshipped. 22focusedtests9.25s and107broader64.88s (includesfocused) pass: recoveryobligations/paused/human/archive/closedIDs/snapshots/defaults/fullfreshrefetch/legacyadapters plus15JSONedgecases. Actualstartup5.969s configurationvolumeshareunverified; nofullKernel/provider/deployedacceptance. Runningc43f remainsunchanged.

Oneactualinstrumented100iterationfixture preserved9600orderedactiveevents/12511coldhashes/5000expired/16audit with0primary+diagnosticfailure/timeout/rejection deltas,0steadyworkerwarnings BUT6targetviolations: messagehistory2/objectcollection3/maintenance1. Workload100.056s, captureand4.020svalidation separate. BootstrapOpenAPI warningremains inpremeasurement/rawlog. 23,923locksections/0drops:6,702before/17,197overlappingworkload/24after. Calibration20pairs median3.089->11.759ms/1000locksections, observer3.994stotal/max213.746ms add scheduling/work; this isnot acontrolledspeedup/acceptance comparison. Raw/unmeasuredlocks/nativebusy/physicalIO/GIL remainunattributed.

The22workloadwaits>=100ms include8summarywrites,3transcriptappends and2syncpushes waitingbehindcapturedsummary-writeholders. Largest405.016ms wait has403.930ms holderunion plus1.086msgap. Actualc43f separatewindowshows firstsession-save4839.143mshold/4822.398SQLwall/5.682CPU andnextsave4867.450mswait: confirmedpropagation, notproof ofnativebusy/disk/GIL cause. Nestedholdsareinclusive; do notsumthem asdistinctincidents.

A projectiontransaction-grouping prototype was **rejected**:10alternatingpairs/300updatespercase reduced600->300SQLiteCOMMITs butkept25,585,520WALbytes andworsenedp9525.837->27.187/max34.468->86.340ms. It isNOTinproductsource. Output-blockcounter0 isnotzeroIOproof. Allpartialcasecheckpoints/rawtimings kept; onlycompletepairs used in statistics.

Evidencefailures remain explicit. FirstfixtureguardstoppedbeforePAimports onresolved/privateversusunresolved/varalias; correctedboth.resolve withsamecontainmentpolicy. Actualv2fixturewrotefullresult/journals, butparentthenFAILEDlen(int) on anumeric sampleeventsfield beforepersistingchildexitstatus. Childexitstatus remains**unknown**. Separateverificationchecks7,276sample/2,623lockbatchhashchains,29runtimehashes/437sourceattestation/38templates/roots/fullfunctionalcontent withoutrepeatingworkload. Originalparentexit1 isnotretroactivelygreen. Correctedfuturev3runner storeschildexitbeforeverification andtypechecksarrays, PARSEDONLY/notexecuted. Fullraw numericjournal23923events preservedlosslessly; initialfailure/proofs/runnerversions areembedded.

Actualc43f95GETwindowstillhas3targetviolations/35.414smaintenance, workspacefailureunknown,77/181operations21/115routespositive;104/94unobserved and everyobservedname hasuncoveredbranches. Nativebusy/WAL/fsync/disk/GIL/GC/fullfreshhealth/integrity/VACUUM/schema/MCP/provider/placement/workspace/watch/peers/logloss5.64MB/statusfailures2396/olderrotation/receipt/lostraw/attachment/testroot/browserCI/backup gaps retainowners/actions. Backup0331 sourcegap/pausedfailedarchives/independentacceptance andDB89957 acceptedoriginalPR471scope unchanged. NormalPAscheduler owns17d034a6 independent_agent_v1 acceptance. Sourceintegration/install/activation cannot closewholecard.

## Every instrumented workload latency family

| Name | n | p50 ms | p95 ms | p99 ms | max ms | >=500ms |
|---|---:|---:|---:|---:|---:|---:|
| / | 100 | 10.899 | 22.416 | 50.591 | 82.069 | 0 |
| /agent | 100 | 65.825 | 153.562 | 246.556 | 304.14 | 0 |
| /api/agent/history/session-0?limit=64 | 100 | 38.737 | 83.895 | 157.802 | 217.503 | 0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 182.94 | 399.613 | 540.182 | 586.475 | 2 |
| /api/agent/preferences | 100 | 4.978 | 10.573 | 18.234 | 22.109 | 0 |
| /api/agent/sessions?view=chats | 100 | 79.369 | 126.474 | 178.934 | 192.249 | 0 |
| /api/cards | 100 | 8.934 | 16.298 | 23.992 | 39.165 | 0 |
| /api/cards/fixture-card | 100 | 5.28 | 9.999 | 19.317 | 24.699 | 0 |
| /api/cards/fixture-card/history | 100 | 28.158 | 64.918 | 140.08 | 141.383 | 0 |
| /api/execution/defaults | 100 | 5.855 | 8.523 | 21.927 | 40.7 | 0 |
| /api/fleet/dispatch-jobs | 100 | 33.777 | 69.459 | 132.74 | 240.074 | 0 |
| /api/fleet/dispatch-queue | 100 | 7.16 | 17.913 | 23.597 | 26.953 | 0 |
| /api/health | 100 | 3.382 | 5.033 | 5.281 | 11.575 | 0 |
| /api/health-journal/status | 100 | 8.363 | 12.488 | 22.374 | 43.687 | 0 |
| /api/notifications | 100 | 5.715 | 13.432 | 29.932 | 140.475 | 0 |
| /api/pr-supervisor/capabilities | 100 | 8.06 | 15.109 | 22.482 | 22.688 | 0 |
| /api/pr-supervisor/watches | 100 | 7.803 | 18.092 | 27.908 | 30.862 | 0 |
| /api/runtime | 100 | 32.507 | 39.331 | 45.096 | 227.752 | 0 |
| /api/runtime?section=sse | 100 | 5.037 | 6.566 | 9.482 | 20.353 | 0 |
| /api/status | 100 | 51.109 | 82.543 | 119.181 | 131.319 | 0 |
| /api/sync/refs | 100 | 4.714 | 9.926 | 25.269 | 129.307 | 0 |
| /api/sync/status | 100 | 13.345 | 26.678 | 81.345 | 138.435 | 0 |
| /api/workspaces | 100 | 6.925 | 20.342 | 31.353 | 37.135 | 0 |
| /fleet | 100 | 94.223 | 231.452 | 322.532 | 328.49 | 0 |
| /partials/home/sections | 100 | 19.135 | 52.923 | 64.592 | 69.301 | 0 |
| /pull-requests | 100 | 29.742 | 89.608 | 115.111 | 263.327 | 0 |
| /settings?section=agent | 100 | 17.339 | 26.291 | 87.942 | 98.689 | 0 |
| /settings?section=backups | 100 | 16.498 | 32.431 | 87.162 | 99.477 | 0 |
| /work | 100 | 12.3 | 37.885 | 104.432 | 125.432 | 0 |
| acp.wire_append | 100 | 4.402 | 17.633 | 21.639 | 25.325 | 0 |
| dispatch.runnable_read | 100 | 2.667 | 11.441 | 16.555 | 20.9 | 0 |
| fleet.overview_cache_read | 100 | 3.342 | 11.946 | 25.079 | 58.068 | 0 |
| fleet.overview_cache_write | 100 | 13.459 | 37.976 | 43.489 | 63.151 | 0 |
| maintenance.run | 1 | 18055.507 | 18055.507 | 18055.507 | 18055.507 | 1 |
| progress.checkpoint_write | 100 | 12.374 | 22.296 | 35.591 | 39.203 | 0 |
| progress.sequence_allocate | 100 | 8.787 | 28.618 | 61.818 | 120.579 | 0 |
| reconciliation.dispatch_read | 100 | 2.071 | 7.911 | 16.921 | 21.756 | 0 |
| sqlite.card_write | 10 | 89.257 | 115.43 | 115.43 | 115.43 | 0 |
| sqlite.execution_binding_materialize | 100 | 62.519 | 153.184 | 201.39 | 254.673 | 0 |
| sync.commit_read | 100 | 4.962 | 13.832 | 22.136 | 27.765 | 0 |
| sync.object_collect | 10 | 349.571 | 1451.855 | 1451.855 | 1451.855 | 3 |
| sync.push_transaction | 100 | 80.289 | 219.118 | 427.466 | 443.15 | 0 |
| sync.reconcile_head | 100 | 53.516 | 173.235 | 281.945 | 328.333 | 0 |
| transcript.drain | 300 | 123.709 | 219.106 | 266.614 | 401.031 | 0 |

## Preserved earlier coverage

# Exact combined PR519/520 loaded inventory — 2026-10-18

All **558 findings and 67 concerns remain open**. The [full raw artifact](benchmarks/systemic-c43f-loaded-20261018.json) records actual loadedc43f15ca/package9092/PID65909/startup75dd5b9d with unchangedconfig86968/lease290. All558existingrows/owners/actions remain unchanged; status overrides record deployment separately from acceptance. OriginalCI519failure causes, allolderwindows and negativeGCexperiments remain open and retained.

The declared95ordinarysequentialGET window completed95HTTP200/stablebuild, but **three >=500ms violations** remain: runtime561.834ms, Agent1076.758ms and Fleet652.048ms. Agent context598.1wall/87.5CPU/server749.9wall/125.2CPU, Fleetcontext463.1wall/184.7CPU/server589.4wall/238.4CPU; runtimehasno perrequestphase split. Agent118889decodedbytes, Fleet1096580bytes and Fleetoverview1013248bytes maxima are retained. Full helper wall includes transport/auth/decode. This is not a controlled wholepathspeedup or cold-start acceptance; five-pagep95=p99=max, firstnotguaranteedcold, decodedbytesnotwire. No ownfixture/test/probe overlapped95GETwindow; normalhost/background uncontrolled/nohostpressurecapture. Every sample/error/status/UTC/correlation/timing/bytecount/SHA checkpoint remains.

Separate runtime snapshot2026-10-18T11:31:25.987375+00:00 retains all21sections,82primary+1diagnosticoperations,19prefixfamilies,25routes,34slowrecords(cap64),34processslowcounters and25primaryprocessmaxima>=500ms. Workspaceprovision1failed/7submitted causeunknown. Positive baseline observations cover77/181operations and21/115routes, leaving104/94unobserved; names do not cover every branch. All-call family percentiles cannot be recovered from per-operation summaries; slow-only family percentiles below have their own declared scope.

Cached startup maintenance2026-10-18T11:29:19.650175+00:00–2026-10-18T11:29:55.069082+00:00 took35414.303ms: retention7195.893ms includingGC6961.170ms/664.829CPU/13277observedSQLiteAPIcalls13071rows/5128.319SQLwall/publicationhold0.015459ms; firstfreshhealth6854.379 andfinalfreshhealth3122.271ms; projection12141.253 includingVACUUM7509.797/5959.896CPU/initialintegrity3709.601/finalintegrity816.161; transcriptDB4933.778 includingintegrity4919.956ms. Bothcheckresultsok/WALbusy0, but zero busyfailures doesnotprove successful native busywaitabsence. Parent spans include children. WallminusCPU is not physicalIO/GIL/nativebusy attribution. Missingcold1/migrationrunning, independentbackupsourcegap and allolderretention/log/peer limitations remain. Cachedstartup differs from request/runtimewindows; do not sum inclusive spans or infer causalpair.

PR520head39122631739/main39122908151 each5terminalSUCCESS/no retry; stable9/exactheadindependentgreen/normalCAS/equalheadmergetree799219a6 recorded. Exactimmutableinstall437sourcebytesVCS/52versionsunchanged/38compiler-freetemplates/config/lease verified beforemanagedhandoffb705; serverloadedproof follows, no repeat restart. Nativeinitialblankreadiness repair is source/native-tested, while originalmetadata272ms/visualizationstalenode/Workshopnull CI causes remain unverified; no deadline/assertion relaxed. NormalPAscheduler owns17d034a6 independent_agent_v1 finalacceptance; no selfacceptance/Done/cleanup. Backup0331/DB89957 scopes/owners unchanged.

## Every actual c43f runtime observation

# Actualc43f runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, sourcec43f/PID65909/startup75dd5b9d/package9092. Runtime snapshot UTC 2026-10-18T11:31:25.987375+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

The95GET window precedes the current turn disposable tests; cached maintenance and runtime collectors follow it sequentially and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 91.261 | 91.261 | 91.261 | 91.261 | 91.261 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 34.824 | 34.824 | 34.824 | 34.824 | 34.824 |
| acp.wire_append | 735 | 0 | 0/0/0 | 256 | 1.087 | 57.275 | 117.234 | 396.913 | 396.913 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 0.714 | 0.714 | 0.714 | 0.714 | 0.714 |
| agent.project_recovery_availability | 40 | 0 | 0/0/0 | 40 | 1.61 | 121.971 | 477.234 | 477.234 | 477.234 |
| agent.prompt_compose | 2 | 0 | 0/0/0 | 2 | 19.781 | 26.457 | 26.457 | 26.457 | 26.457 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 3.139 | 3.139 | 3.139 | 3.139 | 3.139 |
| agent.recovery_sessions | 19 | 1 | 0/0/0 | 19 | 129.275 | 544.261 | 544.261 | 544.261 | 544.261 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 1.768 | 1.768 | 1.768 | 1.768 | 1.768 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 5968.821 | 5968.821 | 5968.821 | 5968.821 | 5968.821 |
| dispatch.completion_pending_read | 26 | 0 | 0/0/0 | 26 | 1.912 | 67.87 | 79.141 | 79.141 | 79.141 |
| dispatch.followup_pending_read | 26 | 0 | 0/0/0 | 26 | 2.484 | 44.977 | 273.752 | 273.752 | 273.752 |
| dispatch.record_read | 370 | 0 | 0/0/0 | 256 | 19.465 | 36.151 | 96.778 | 364.655 | 364.655 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 6.469 | 374.671 | 374.671 | 374.671 | 374.671 |
| filesystem.github_credentials_read | 49 | 0 | 0/0/0 | 49 | 2.816 | 73.495 | 854.594 | 854.594 | 854.594 |
| fleet.overview.reachability | 9 | 0 | 0/0/0 | 9 | 14.097 | 78.112 | 78.112 | 78.112 | 78.112 |
| fleet.overview.status | 9 | 0 | 0/0/0 | 9 | 10.308 | 78.154 | 78.154 | 78.154 | 78.154 |
| fleet.overview.sync | 14 | 1 | 0/0/0 | 14 | 30.126 | 1239.888 | 1239.888 | 1239.888 | 1239.888 |
| fleet.overview_cache_read | 379 | 4 | 0/0/0 | 256 | 1.638 | 38.959 | 55.123 | 82.088 | 1369.463 |
| fleet.overview_cache_write | 113 | 3 | 0/0/0 | 113 | 15.495 | 147.125 | 1094.684 | 1119.317 | 1119.317 |
| fleet.overview_response_json | 81 | 0 | 0/0/0 | 81 | 11.333 | 74.853 | 150.709 | 150.709 | 150.709 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 13.46 | 13.46 | 13.46 | 13.46 | 13.46 |
| goal.dispatch_lifecycle_index | 6 | 0 | 0/0/0 | 6 | 0.694 | 14.683 | 14.683 | 14.683 | 14.683 |
| health.canonical_obligation | 29 | 2 | 0/0/0 | 29 | 45.457 | 873.162 | 1459.896 | 1459.896 | 1459.896 |
| http.fleet_overview | 81 | 0 | 0/0/0 | 81 | 185.039 | 3745.945 | 3819.021 | 3819.021 | 3819.021 |
| http.github | 1 | 0 | 0/0/0 | 1 | 740.261 | 740.261 | 740.261 | 740.261 | 740.261 |
| http.pr_supervisor_peer | 342 | 0 | 0/0/0 | 256 | 48.651 | 148.346 | 304.117 | 398.46 | 529.543 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 35417.94 | 35417.94 | 35417.94 | 35417.94 | 35417.94 |
| notifications.list_page | 3 | 0 | 0/0/0 | 3 | 4.273 | 140.503 | 140.503 | 140.503 | 140.503 |
| notifications.present_page | 3 | 1 | 0/0/0 | 3 | 226.331 | 899.056 | 899.056 | 899.056 | 899.056 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 24.069 | 24.069 | 24.069 | 24.069 | 24.069 |
| pr_supervisor.response_json | 342 | 0 | 0/0/0 | 256 | 0.742 | 14.841 | 57.023 | 101.881 | 101.881 |
| preferences.agent_auto_approve_read | 5 | 0 | 0/0/0 | 5 | 4.944 | 41.797 | 41.797 | 41.797 | 41.797 |
| progress.checkpoint_write | 10 | 0 | 0/0/0 | 10 | 184.413 | 297.62 | 297.62 | 297.62 | 297.62 |
| progress.dispatch_read | 58 | 0 | 0/0/0 | 58 | 58.702 | 128.391 | 155.861 | 155.861 | 155.861 |
| progress.final_report_build | 2 | 0 | 0/0/0 | 2 | 1.617 | 3.036 | 3.036 | 3.036 | 3.036 |
| progress.final_report_write | 2 | 0 | 0/0/0 | 2 | 185.13 | 188.639 | 188.639 | 188.639 | 188.639 |
| progress.heartbeat_write | 8 | 0 | 0/0/0 | 8 | 150.824 | 282.141 | 282.141 | 282.141 | 282.141 |
| progress.pending_read | 49 | 0 | 0/0/0 | 49 | 11.506 | 59.929 | 175.581 | 175.581 | 175.581 |
| progress.sequence_allocate | 18 | 2 | 0/0/0 | 18 | 139.844 | 695.069 | 695.069 | 695.069 | 695.069 |
| reconciliation.completion_evidence_read | 72 | 1 | 0/0/0 | 72 | 4.299 | 196.919 | 1407.819 | 1407.819 | 1407.819 |
| reconciliation.completion_queue | 1 | 0 | 0/0/0 | 1 | 58.833 | 58.833 | 58.833 | 58.833 | 58.833 |
| reconciliation.dispatch_read | 122 | 1 | 0/0/0 | 122 | 2.197 | 70.472 | 275.448 | 3398.83 | 3398.83 |
| reconciliation.session_read | 72 | 0 | 0/0/0 | 72 | 2.486 | 72.434 | 477.703 | 477.703 | 477.703 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 101.09 | 101.09 | 101.09 | 101.09 | 101.09 |
| selection.prompt_identity | 2 | 0 | 0/0/0 | 2 | 33.079 | 59.789 | 59.789 | 59.789 | 59.789 |
| selection.prompt_outcome | 1 | 0 | 0/0/0 | 1 | 33.081 | 33.081 | 33.081 | 33.081 | 33.081 |
| sqlite.agent_runtime_checkpoint | 5 | 0 | 0/0/0 | 5 | 25.287 | 57.101 | 57.101 | 57.101 | 57.101 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 6.815 | 114.972 | 391.607 | 391.607 | 391.607 |
| sqlite.agent_session_save | 46 | 2 | 0/0/0 | 46 | 23.965 | 307.094 | 4891.232 | 4891.232 | 4891.232 |
| sqlite.card_read | 542 | 1 | 0/0/0 | 256 | 1.582 | 13.431 | 42.768 | 88.485 | 560.487 |
| sqlite.pr_supervisor_capability_write | 3 | 0 | 0/0/0 | 3 | 7.893 | 158.97 | 158.97 | 158.97 | 158.97 |
| sqlite.pr_supervisor_completion_due_read | 47 | 0 | 0/0/0 | 47 | 25.03 | 201.483 | 384.739 | 384.739 | 384.739 |
| sqlite.pr_supervisor_due_read | 47 | 0 | 0/0/0 | 47 | 3.845 | 27.381 | 41.852 | 41.852 | 41.852 |
| sqlite.pr_supervisor_event_write | 171 | 0 | 0/0/0 | 171 | 3.523 | 51.626 | 145.954 | 228.039 | 228.039 |
| sqlite.pr_supervisor_watch_read | 171 | 0 | 0/0/0 | 171 | 4.985 | 92.621 | 179.967 | 231.094 | 231.094 |
| sqlite.pr_supervisor_watch_write | 171 | 0 | 0/0/0 | 171 | 9.989 | 112.234 | 172.836 | 295.172 | 295.172 |
| sqlite.project_read | 377 | 0 | 0/0/0 | 256 | 0.861 | 12.665 | 75.972 | 88.222 | 191.472 |
| sqlite.repository_read | 370 | 0 | 0/0/0 | 256 | 1.043 | 16.412 | 73.625 | 102.284 | 142.492 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 0.96 | 1.375 | 1.375 | 1.375 | 1.375 |
| sqlite.restart_handoff_completion | 80 | 0 | 0/0/0 | 80 | 6.068 | 100.395 | 361.208 | 361.208 | 361.208 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 3.591 | 3.591 | 3.591 | 3.591 | 3.591 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 2.278 | 2.278 | 2.278 | 2.278 | 2.278 |
| sqlite.restart_handoff_session | 40 | 0 | 0/0/0 | 40 | 5.778 | 16.659 | 83.019 | 83.019 | 83.019 |
| sqlite.restart_handoffs_pending | 20 | 0 | 0/0/0 | 20 | 5.202 | 37.828 | 67.277 | 67.277 | 67.277 |
| sqlite.restart_handoffs_watchdog | 19 | 0 | 0/0/0 | 19 | 2.742 | 72.678 | 72.678 | 72.678 | 72.678 |
| sqlite.transcript_append | 83 | 1 | 0/0/0 | 83 | 15.978 | 245.045 | 921.041 | 921.041 | 921.041 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 47.224 | 47.224 | 47.224 | 47.224 | 47.224 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.252 | 4.252 | 4.252 | 4.252 | 4.252 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 8.507 | 8.507 | 8.507 | 8.507 | 8.507 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1120.491 | 1120.491 | 1120.491 | 1120.491 | 1120.491 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 1.838 | 75.295 | 75.295 | 75.295 | 75.295 |
| sync.gc_resume | 1 | 1 | 0/0/0 | 1 | 1156.662 | 1156.662 | 1156.662 | 1156.662 | 1156.662 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 2.657 | 111.942 | 111.942 | 111.942 | 111.942 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 90.984 | 3763.763 | 3763.763 | 3763.763 | 3763.763 |
| sync.reconcile_head | 12 | 9 | 0/0/0 | 12 | 810.212 | 3247.233 | 3247.233 | 3247.233 | 3247.233 |
| sync.ref_read | 44 | 0 | 0/0/0 | 44 | 1.166 | 37.254 | 208.762 | 208.762 | 208.762 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 49.006 | 49.006 | 49.006 | 49.006 | 49.006 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 8018.516 | 8018.516 | 8018.516 | 8018.516 | 8018.516 |
| workspace.lease_renew | 2 | 0 | 0/0/0 | 2 | 31.546 | 183.183 | 183.183 | 183.183 | 183.183 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 2370.6 | 2947.416 | 2947.416 | 2947.416 | 2947.416 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1408.852 | 1408.852 | 1408.852 | 1408.852 | 1408.852 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 30.909 | 85.527 | 87.597 | 87.597 | 87.597 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 738 | 0 | 0 | 0 | 396.913 |
| agent | 6 | 64 | 2 | 0 | 0 | 5968.821 |
| dispatch | 4 | 429 | 0 | 0 | 0 | 374.671 |
| filesystem | 1 | 49 | 0 | 0 | 0 | 854.594 |
| fleet | 7 | 606 | 8 | 0 | 0 | 1369.463 |
| goal | 1 | 6 | 0 | 0 | 0 | 14.683 |
| health | 1 | 29 | 2 | 0 | 0 | 1459.896 |
| http | 3 | 424 | 0 | 0 | 0 | 3819.021 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 35417.94 |
| notifications | 2 | 6 | 1 | 0 | 0 | 899.056 |
| pr_supervisor | 2 | 343 | 0 | 0 | 0 | 101.881 |
| preferences | 1 | 5 | 0 | 0 | 0 | 41.797 |
| progress | 7 | 147 | 2 | 0 | 0 | 695.069 |
| reconciliation | 4 | 267 | 2 | 0 | 0 | 3398.83 |
| selection | 3 | 4 | 0 | 0 | 0 | 101.09 |
| sqlite | 21 | 2218 | 4 | 0 | 0 | 4891.232 |
| startup | 3 | 3 | 1 | 0 | 0 | 1120.491 |
| sync | 7 | 94 | 10 | 0 | 0 | 3763.763 |
| workspace | 4 | 11 | 1 | 1 | 0 | 8018.516 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 11.311 | 19.991 | 19.991 | 19.991 |
| /api/agent/observability/v1/sessions/{session_id} | 2 | 8364 | 2 | 57.061 | 80.819 | 80.819 | 80.819 |
| /api/agent/sessions/{session_id}/restart-handoffs | 4 | 1144158 | 4 | 18.504 | 60.948 | 60.948 | 60.948 |
| /api/configuration | 2 | 339898 | 2 | 15.981 | 21.055 | 21.055 | 21.055 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 16.31 | 29.871 | 29.871 | 29.871 |
| /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint | 1 | 203 | 1 | 1057.56 | 1057.56 | 1057.56 | 1057.56 |
| /api/fleet/overview | 5 | 5066238 | 5 | 161.37 | 287.006 | 287.006 | 287.006 |
| /api/fleet/overview/local | 25 | 483406 | 25 | 93.899 | 770.585 | 819.693 | 819.693 |
| /api/health | 20 | 300 | 20 | 2.629 | 6.939 | 12.272 | 12.272 |
| /api/instance | 2 | 1120 | 2 | 8.941 | 9.015 | 9.015 | 9.015 |
| /api/instance/maintenance | 1 | 7458 | 1 | 4.934 | 4.934 | 4.934 | 4.934 |
| /api/notifications | 3 | 33327 | 3 | 241.137 | 1360.742 | 1360.742 | 1360.742 |
| /api/owner-ready | 1 | 71 | 1 | 5.458 | 5.458 | 5.458 | 5.458 |
| /api/pr-supervisor/capabilities | 1 | 2707 | 1 | 24.57 | 24.57 | 24.57 | 24.57 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 8.118 | 111.808 | 111.808 | 111.808 |
| /api/pr-supervisor/replicas | 370 | 2048702 | 256 | 39.361 | 168.774 | 270.795 | 376.996 |
| /api/ready | 24 | 2778 | 24 | 3.208 | 8.651 | 9.623 | 9.623 |
| /api/runtime | 20 | 6226142 | 20 | 40.259 | 98.309 | 453.337 | 453.337 |
| /api/status | 51 | 142494 | 51 | 73.918 | 501.845 | 732.908 | 732.908 |
| /api/sync/refs | 2 | 518 | 2 | 8.725 | 305.484 | 305.484 | 305.484 |
| /api/ui/assets | 3 | 237 | 3 | 7.095 | 74.501 | 74.501 | 74.501 |
| /api/workspaces | 2 | 2878 | 2 | 7.043 | 39.697 | 39.697 | 39.697 |
| /fleet | 5 | 5482900 | 5 | 297.302 | 625.982 | 625.982 | 625.982 |
| /pull-requests | 5 | 125605 | 5 | 36.416 | 67.508 | 67.508 | 67.508 |
| /{page_path:path} | 15 | 1253855 | 15 | 27.269 | 995.184 | 995.184 | 995.184 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.recovery_sessions | 1 | 544.261 | 544.261 | 544.261 | 544.261 |
| agent.startup_state_read | 1 | 5968.821 | 5968.821 | 5968.821 | 5968.821 |
| fleet.overview.sync | 1 | 1239.888 | 1239.888 | 1239.888 | 1239.888 |
| fleet.overview_cache_read | 4 | 1118.916 | 1369.463 | 1369.463 | 1369.463 |
| fleet.overview_cache_write | 3 | 1094.684 | 1119.317 | 1119.317 | 1119.317 |
| health.canonical_obligation | 2 | 873.162 | 1459.896 | 1459.896 | 1459.896 |
| maintenance.run | 1 | 35417.94 | 35417.94 | 35417.94 | 35417.94 |
| notifications.present_page | 1 | 899.056 | 899.056 | 899.056 | 899.056 |
| progress.sequence_allocate | 2 | 522.68 | 695.069 | 695.069 | 695.069 |
| reconciliation.completion_evidence_read | 1 | 1407.819 | 1407.819 | 1407.819 | 1407.819 |
| reconciliation.dispatch_read | 1 | 3398.83 | 3398.83 | 3398.83 | 3398.83 |
| sqlite.agent_session_save | 2 | 4890.116 | 4891.232 | 4891.232 | 4891.232 |
| sqlite.card_read | 1 | 560.487 | 560.487 | 560.487 | 560.487 |
| sqlite.transcript_append | 1 | 921.041 | 921.041 | 921.041 | 921.041 |
| startup.openapi | 1 | 1120.491 | 1120.491 | 1120.491 | 1120.491 |
| sync.gc_resume | 1 | 1156.662 | 1156.662 | 1156.662 | 1156.662 |
| sync.reconcile_head | 9 | 1045.976 | 3247.233 | 3247.233 | 3247.233 |
| workspace.collect_garbage | 1 | 8018.516 | 8018.516 | 8018.516 | 8018.516 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 2 | 544.261 | 5968.821 | 5968.821 | 5968.821 |
| fleet | 8 | 1118.916 | 1369.463 | 1369.463 | 1369.463 |
| health | 2 | 873.162 | 1459.896 | 1459.896 | 1459.896 |
| maintenance | 1 | 35417.94 | 35417.94 | 35417.94 | 35417.94 |
| notifications | 1 | 899.056 | 899.056 | 899.056 | 899.056 |
| progress | 2 | 522.68 | 695.069 | 695.069 | 695.069 |
| reconciliation | 2 | 1407.819 | 3398.83 | 3398.83 | 3398.83 |
| sqlite | 4 | 921.041 | 4891.232 | 4891.232 | 4891.232 |
| startup | 1 | 1120.491 | 1120.491 | 1120.491 | 1120.491 |
| sync | 10 | 1045.976 | 3247.233 | 3247.233 | 3247.233 |
| workspace | 1 | 8018.516 | 8018.516 | 8018.516 | 8018.516 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 39.649 | 113.539 | 113.539 | 113.539 | 0 | 25464 |
| /agent | 5 | 0 | 197.755 | 1076.758 | 1076.758 | 1076.758 | 1 | 118889 |
| /api/fleet/overview | 5 | 0 | 201.685 | 398.795 | 398.795 | 398.795 | 0 | 1013248 |
| /api/health | 20 | 0 | 21.325 | 66.992 | 113.262 | 113.262 | 0 | 15 |
| /api/runtime | 20 | 0 | 74.322 | 174.474 | 561.834 | 561.834 | 1 | 311333 |
| /api/status | 20 | 0 | 76.607 | 243.87 | 445.079 | 445.079 | 0 | 2824 |
| /fleet | 5 | 0 | 320.798 | 652.048 | 652.048 | 652.048 | 1 | 1096580 |
| /pull-requests | 5 | 0 | 55.571 | 88.184 | 88.184 | 88.184 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 42.313 | 75.515 | 75.515 | 75.515 | 0 | 100559 |
| /work | 5 | 0 | 34.133 | 51.714 | 51.714 | 51.714 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 75.000459 | 7.412083 | 273 | 270 | 71.7189522110857 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 30.222875 | 6.703083 | 2 | 0 | 29.983625980094075 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 133.365125 | 27.304 | 5 | 0 | 132.04625097569078 | 0.003834022209048271 | 133.26191599480808 |
| retention_object_collection | 1/0 | 6961.170459 | 664.828917 | 13277 | 13071 | 5128.319241339341 | 0.0019579892978072166 | 0.015459023416042328 |
| retention_canonical | 1/0 | 7094.842334 | 692.388833 | 13282 | 13071 | 5260.365492315032 | 0.005792011506855488 | 133.27737501822412 |
| retention_mirror | 1/0 | 25.785292 | 6.534625 | 2 | 0 | 22.57587597705424 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 7195.892625 | 706.594208 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 31.122709 | 2.157875 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 6854.379375 | 2969.045 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1134.910625 | 700.562875 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12141.252916 | 8224.48 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4933.777708 | 1982.717375 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 3122.271167 | 1928.000834 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 35414.303209 | 16514.184916 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# Browser startup readiness and retained merge-CI failures — 2026-10-18

All **558 findings and 67 concerns remain open**. Every preceding556row is preserved unchanged. The [complete raw source/native/CI evidence](benchmarks/systemic-browser-navigation-20261018.json) separates a confirmed initial-browser readiness defect from three original CI failures whose causes remain unverified. This source repair is not installed; actualrunningf7/package3346 is unchanged.

A native private Chromium profile and bounded loopback-only header barrier reproduced initialattachment returning a completed **about:blank** document while the requested page's response headers were withheld. Synthetic protocol evidence also shows the old readiness helper can accept an oldcomplete document after a newloader acknowledgement. The sameURLnativeheld-header reload did **not** reproduce earlycompletion, so that outcome is not claimed fixed or causally attributed. OriginalCIhadnoURL/body/readyState/loadercapture; do not infer that the originalWorkshopnull orvisualizationnodefailure was this exactinitialrace.

The candidate launchesblank, establishesCDP/viewport, then sends the requestednavigationonce before publishing the attachment. Readiness requires a new committedmainframeloader whennavigation creates a document, with stablebefore/afterloaderidentity aroundthecomplete-pageprobe. Redirectloaders may differfromthefirstacknowledgement; same-documentnavigation legitimatelykeeps itsloader. No input/navigation is replayed. The addedread shares the existing15secondcommandbudget; allreadinessprobes fit the existing10secondbudget. No default or timeout is increased. The corrected nativefixture didnotpublishbeforetargetheaders; both delivered documents were subsequentlyverified.

Eightfocusedcontracts pass0.80s;54broaderbrowsertransport/session/ownership/CSRF/module/nativeviewport tests plus11subtests pass8.74s (includesfocused);5nativeUI/Workshop/visualizationisolation/E2E/unchanged200msmetadata tests pass50.66s. BothPAroots/workspaceguard and every outcome are retained. Initialfocusedrun hit the sandbox socketbind boundary (5pass/3fail); unchangedtests were rerun only after nativeapproval. No workaround or assertion/deadline weakening.

PR519 integrated exact00e01133 from856609c3 afterstable8/independent5green/normalheadCAS; merge/head treeec68b691 identical. MainCI39121383664 attempt1failedtwo shards/three tests:metadata272.137msvsunchanged200ms; visualizationbackendnode detached afterreload; Workshopinitialrootnull.dataset. Nativeunchangedthree-case checks pass46.13s oncurrent00e0 and42.50s onimmutablef7, with437sourcefiles/importroots attested. NativeMac/Python3.14.2 differsCI Linux3.14.8. After preservingthese failures andstaticpathchecks, **one** justifiedfailed-shardretry passed; threeoriginalsuccessjobs andtworetriedsuccessjobs nowterminalgreen. This is not aflaky/CI-source-repair or rootcauseclaim. Originalfailures remain open. PublicCIlogpayload masks ephemeraltesthandles; originalfullraw hash is retained.

No newunchangedcore100iteration workload was rerun: the currentdiagnosticfixture still has7workerwarnings/11sampledviolations, andactualf7 stillhas3of95GETtargetviolations/startup36.021s. All nativebusy/WAL/fsync/disk/GIL/GC/fullfreshhealth/integrity/VACUUM/schema/MCP/provider/placement/workspace/watch/peers/logloss/statusfailures/rotation/receipt/lostraw/backup/testroot/olderbrowserCI/unexercised obligations remain. Downloads/externalnavigation/otherbrowserbranches are not blanketverified. Backup0331 missing-source/failedarchive independentacceptance andDB89957 acceptedoriginalscope retainowners. NormalPAscheduler owns finalindependentacceptance under unchanged17d034a6; no selfacceptance/Done/cleanup. Latestcombinedsource mustfollowreadyPR/durablewatch/exactheadindependentgreen/mainCI/safeexactactivation/retest. Do not installorrestartold519fromqueuedstalenotices.

## Preserved earlier coverage

# Fleet diagnostic hydration repair and retained experiments — 2026-10-18

All **556 findings and 65 concerns remain open**. Every preceding555row is preserved unchanged; the new finding records a separate Fleet/background activity caller of diagnostic session hydration. The [complete numeric artifact](benchmarks/systemic-workshop-summary-20261018.json) retains every source/prototype/mixed sample, error, warning, durable-root audit and journal proof. Currentrunningf7 does not contain this local repair.

Fleet/Workshop bounded session selection fetched and parsed diagnostic metrics unused by its presentation. The new opt-in read-only summary excludes only that column, reusing the existing startup projection fields. Complete default reads and full re-fetches remain unchanged. All lifecycle, recovery, config, binding, provenance, sorting, total/omission counts and old extension interfaces are preserved. A160session/128KiBdiagnostic fixture returns80rows: diagnostic characters transferred/materialized percall fall10,485,760 to0. Twenty alternating actualsource pairs show full p95187.837ms vs summary26.414ms, with exact complete nonmetrics model/count/order equality and preserved fullgetdiagnostics. This is not proof of sole cause for live Fleet515mscontextCPU.

Fourfocused contracts pass; broaderFleet/Workshop/startupinventory/watchsnapshot suite passes112tests+2subtests34.72s (includesfocused, not additive). Both temporaryroots and workspaceconstructor guard retained. Exact437source/38template wheel scenario adds30sessions128KiBmetrics to the retained12511cold/5000expired fixture and runs100iterations once. Duration100.805s, retained9600ordered activeevents, warnings{'Slow off-loop operation': 7}, thresholdviolations{'sqlite.execution_binding_materialize': 1, 'transcript.drain': 1, '/api/agent/history/session-0?limit=64&message_boundaries=true': 2, 'sync.object_collect': 5, '/fleet': 1, 'maintenance.run': 1}; failed/timeout/rejected deltas{'operation_counter_deltas': {'failed': 0, 'timed_out': 0, 'rejected': 0}, 'diagnostics_counter_deltas': {'failed': 0, 'timed_out': 0, 'rejected': 0}}. Capture and durableverification windows are separate; everycoldhash/projection/title/order assertion and chainedjournal proof is retained. This changed diagnosticworkload is not a paired speedup comparison or passing acceptance scenario.

The new post-lock-removal speculativequery fetchall experiment is **not implemented**.20all-live plus46correctedmixed/orphan/concurrent timingtrials show liveCPU benefit but worse orphanmaximum/concurrentappendmaximum. Both pre-seeding source-needleassertions and invalidsharedobjectsroot fixture failure are retained; the failing mixed result checked before checkpoint waslost and is explicitlynot reconstructed. Corrected remainingtimings checkpoint eachreturnbeforeassertions; their finalsinglepage6144row validation wasinvalid because publiclist is capped. A separate functional-only two-method proof paginates1024orderedhot events/exactcoldpayload; it doesnot fabricate a successful6144row verification or rerun timings forgreen. Earlierpre-PR518fetchall negative/emptytraceinvalid stages remain.

All previous555rows/nativeSQLbusy/WAL/fsync/disk/GIL/GC/retention/fullfreshhealth/integrity/VACUUM/startup/schema/provider/placement/workspace/watch/peers/logloss/statusfailures/rotation/receipt/lostraw/backup/testroots/browser/CI/unexercised concerns remain. Runningf7 ordinary95GETs still have3targetviolations andstartup36.021s, separately documentedbelow. Backup0331 andDB89957 originalscope retainowners; normalPAscheduler owns independent acceptance under unchanged17d034a6. No warningssuppressed, thresholdsraised, diagnosticsdiscardedfromdurability or admission/default/freshnesscontract changed.

## Every current mixed operation and request

| Operation/request | n | p50ms | p95ms | p99ms | maxms | target violations |
|---|---:|---:|---:|---:|---:|---:|
| / | 100 | 11.391 | 29.592 | 49.186 | 113.726 | 0 |
| /agent | 100 | 66.31 | 169.626 | 243.681 | 244.463 | 0 |
| /api/agent/history/session-0?limit=64 | 100 | 39.714 | 88.022 | 122.705 | 127.724 | 0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100 | 183.369 | 437.783 | 593.082 | 644.011 | 2 |
| /api/agent/preferences | 100 | 5.149 | 9.526 | 27.115 | 28.994 | 0 |
| /api/agent/sessions?view=chats | 100 | 79.438 | 133.248 | 188.863 | 210.474 | 0 |
| /api/cards | 100 | 9.21 | 13.743 | 19.697 | 41.029 | 0 |
| /api/cards/fixture-card | 100 | 5.443 | 16.891 | 20.327 | 22.565 | 0 |
| /api/cards/fixture-card/history | 100 | 29.041 | 63.873 | 70.226 | 92.019 | 0 |
| /api/execution/defaults | 100 | 5.923 | 8.615 | 44.694 | 55.745 | 0 |
| /api/fleet/dispatch-jobs | 100 | 34.63 | 50.256 | 82.439 | 259.207 | 0 |
| /api/fleet/dispatch-queue | 100 | 7.223 | 19.116 | 23.725 | 42.714 | 0 |
| /api/health | 100 | 3.537 | 5.254 | 8.332 | 13.211 | 0 |
| /api/health-journal/status | 100 | 8.74 | 16.993 | 30.932 | 34.684 | 0 |
| /api/notifications | 100 | 5.781 | 10.896 | 27.554 | 27.76 | 0 |
| /api/pr-supervisor/capabilities | 100 | 8.063 | 12.571 | 32.617 | 42.214 | 0 |
| /api/pr-supervisor/watches | 100 | 8.128 | 14.601 | 36.093 | 38.646 | 0 |
| /api/runtime | 100 | 33.316 | 47.396 | 219.618 | 235.1 | 0 |
| /api/runtime?section=sse | 100 | 5.347 | 7.805 | 9.597 | 22.888 | 0 |
| /api/status | 100 | 51.499 | 73.393 | 92.928 | 105.097 | 0 |
| /api/sync/refs | 100 | 4.864 | 12.0 | 20.459 | 23.003 | 0 |
| /api/sync/status | 100 | 13.104 | 44.048 | 73.626 | 150.995 | 0 |
| /api/workspaces | 100 | 7.243 | 16.449 | 34.446 | 43.371 | 0 |
| /fleet | 100 | 96.954 | 212.287 | 359.467 | 515.352 | 1 |
| /partials/home/sections | 100 | 19.888 | 45.58 | 71.295 | 87.332 | 0 |
| /pull-requests | 100 | 29.922 | 85.74 | 129.281 | 139.678 | 0 |
| /settings?section=agent | 100 | 17.745 | 50.633 | 109.581 | 125.492 | 0 |
| /settings?section=backups | 100 | 16.468 | 53.97 | 95.443 | 129.839 | 0 |
| /work | 100 | 12.931 | 26.162 | 68.185 | 71.946 | 0 |
| acp.wire_append | 100 | 5.4 | 14.154 | 30.433 | 30.443 | 0 |
| dispatch.runnable_read | 100 | 2.534 | 9.589 | 15.045 | 80.324 | 0 |
| fleet.overview_cache_read | 100 | 2.017 | 11.791 | 40.388 | 50.313 | 0 |
| fleet.overview_cache_write | 100 | 11.967 | 32.955 | 50.651 | 133.443 | 0 |
| maintenance.run | 1 | 17345.348 | 17345.348 | 17345.348 | 17345.348 | 1 |
| progress.checkpoint_write | 100 | 11.286 | 30.449 | 38.04 | 60.308 | 0 |
| progress.sequence_allocate | 100 | 8.669 | 24.408 | 34.907 | 44.961 | 0 |
| reconciliation.dispatch_read | 100 | 2.197 | 19.022 | 41.697 | 54.633 | 0 |
| sqlite.card_write | 10 | 76.022 | 131.838 | 131.838 | 131.838 | 0 |
| sqlite.execution_binding_materialize | 100 | 80.707 | 162.055 | 204.281 | 542.926 | 1 |
| sync.commit_read | 100 | 5.138 | 16.112 | 43.146 | 50.388 | 0 |
| sync.object_collect | 10 | 483.384 | 1505.045 | 1505.045 | 1505.045 | 5 |
| sync.push_transaction | 100 | 68.846 | 178.665 | 219.496 | 262.902 | 0 |
| sync.reconcile_head | 100 | 65.849 | 231.757 | 306.31 | 320.715 | 0 |
| transcript.drain | 300 | 118.004 | 195.811 | 255.211 | 503.467 | 1 |

## Preserved earlier coverage

# Exact PR518 loaded inventory — 2026-10-18

All **555 findings and 64 concerns remain open**. PR518 is merged and actually loaded as f7e87710/package3346/PID58866/startupc3bee533, with unchanged configuration and leased workspace. The [complete raw artifact](benchmarks/systemic-f7e8-loaded-20261018.json) separates installation, loaded proof, each request window, the later full runtime snapshot and cached startup phases. Every preceding row/owner/action and failed stage is preserved unchanged; current status overrides supersede only deployment stages.

The single95 ordinary sequential GET window completed with95HTTP200 and stable build, but **three >=500ms violations**: status506.658ms and Fleet694.389/576.001ms. Fleet maximum includes567.4ms contextwall/515.1ms contextCPU and636ms serverwall/560ms CPU; the second has496.7/146.9ms context. These individual phase observations do not partition disk/GIL/busy or explain the status outlier. Each raw timestamp, helper wall, ServerTiming, correlation, decoded bytes and SHA checkpoint is retained. Five-page p95=p99=max; first not guaranteed cold; decoded bytes are not wire bytes; observer persistence adds work outside timers. No own fixture/test/probe overlapped this window. No host-pressure capture was made.

The distinct10:42:26Z runtime snapshot retains all21sections,82primary+1diagnostic operations,19primary prefix families,24routes,47slow records (cap64),47process slow counters and30primaryprocess maxima >=500ms. Workspace provision has1failure/7submissions, causeunknown. Positive baseline observations cover77/181operations and20/115routegroups, leaving104/95unobserved here; observed names do not prove every branch. All-call family percentiles are unavailable from summary-only API data; slow-only family percentiles have their own declared window below. Older rotation/peer logs/build/observability gaps remain.

Cached startup maintenance took36021.453ms: retention6898.651ms includingGC6739.139ms/691.231ms threadCPU/13039observedSQLiteAPIcalls12837rows/3926.215ms SQLitewall. GCpublicationlock hold was0.003709ms; APIcalls include lazy row production, not13039SQLstatements. Full firsthealth8888.988ms/finalfreshhealth2280.172ms, projection11348ms includingVACUUM6259.622 andinitialintegrity4052.440, transcriptDB5348.055 includingcheck5334.292 remain slow. Projection WAL checkpoint was busy/deferred after compaction. Missingcold1/migrationrunning remain. Parent phases include children; wall minusCPU is not physicalIO/nativebusy/GIL attribution. Different startup layouts/windows cannot establish controlled causal speedup.

Protected PR518 head39119023802/main39119310154 each5terminalSUCCESS, exact merge/head tree equality and immutable437source/52version/38template installation checks were recorded before managed activation. Existing receipt9ee8 remains queued while its own continuation runs, errornull; no repeat restart. Normal PA scheduling owns independent acceptance under unchanged17d034a6; no repairer self-acceptance. Backup0331 missing-source/failedarchive acceptance and DB89957 accepted original scope retain their owners. Full startup/provider/peer/history/drain/sync/nativeIO/GC/Fleet/receipt/log/testroot/browser/CI/unexercised obligations remain.

## Every actual f7e8 runtime observation

# Actualf7e8 runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, sourcef7e8/PID58866/startupc3bee533/package3346. Runtime snapshot UTC 2026-10-18T10:42:26.057557+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

The95GET window precedes the current turn disposable tests; cached maintenance and runtime collectors follow it sequentially and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 39.424 | 39.424 | 39.424 | 39.424 | 39.424 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 1.584 | 1.584 | 1.584 | 1.584 | 1.584 |
| acp.wire_append | 781 | 0 | 0/0/0 | 256 | 1.141 | 64.224 | 200.356 | 289.327 | 289.327 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 0.679 | 0.679 | 0.679 | 0.679 | 0.679 |
| agent.project_recovery_availability | 44 | 0 | 0/0/0 | 44 | 1.716 | 42.683 | 99.772 | 99.772 | 99.772 |
| agent.prompt_compose | 2 | 0 | 0/0/0 | 2 | 25.024 | 132.234 | 132.234 | 132.234 | 132.234 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 2.04 | 2.04 | 2.04 | 2.04 | 2.04 |
| agent.recovery_sessions | 21 | 0 | 0/0/0 | 21 | 143.484 | 396.569 | 443.182 | 443.182 | 443.182 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 2.583 | 2.583 | 2.583 | 2.583 | 2.583 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 5025.904 | 5025.904 | 5025.904 | 5025.904 | 5025.904 |
| dispatch.completion_pending_read | 27 | 0 | 0/0/0 | 27 | 1.984 | 79.648 | 110.103 | 110.103 | 110.103 |
| dispatch.followup_pending_read | 27 | 1 | 0/0/0 | 27 | 2.396 | 87.412 | 994.313 | 994.313 | 994.313 |
| dispatch.record_read | 364 | 2 | 0/0/0 | 256 | 19.815 | 46.997 | 117.767 | 140.433 | 532.158 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 2.59 | 496.412 | 496.412 | 496.412 | 496.412 |
| filesystem.github_credentials_read | 50 | 0 | 0/0/0 | 50 | 1.869 | 61.577 | 248.575 | 248.575 | 248.575 |
| fleet.overview.reachability | 9 | 0 | 0/0/0 | 9 | 12.997 | 45.02 | 45.02 | 45.02 | 45.02 |
| fleet.overview.status | 9 | 0 | 0/0/0 | 9 | 11.925 | 21.592 | 21.592 | 21.592 | 21.592 |
| fleet.overview.sync | 9 | 1 | 0/0/0 | 9 | 22.727 | 1074.656 | 1074.656 | 1074.656 | 1074.656 |
| fleet.overview_cache_read | 380 | 8 | 0/0/0 | 256 | 1.892 | 24.726 | 27.677 | 32.527 | 1025.225 |
| fleet.overview_cache_write | 108 | 0 | 0/0/0 | 108 | 10.143 | 99.019 | 115.491 | 303.121 | 303.121 |
| fleet.overview_response_json | 81 | 2 | 0/0/0 | 81 | 3.786 | 38.316 | 1342.822 | 1342.822 | 1342.822 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 12.602 | 12.602 | 12.602 | 12.602 | 12.602 |
| goal.dispatch_lifecycle_index | 6 | 0 | 0/0/0 | 6 | 0.492 | 26.705 | 26.705 | 26.705 | 26.705 |
| health.canonical_obligation | 31 | 1 | 0/0/0 | 31 | 37.795 | 490.592 | 1231.988 | 1231.988 | 1231.988 |
| http.fleet_overview | 81 | 0 | 0/0/0 | 81 | 112.363 | 2477.843 | 2503.881 | 2503.881 | 2503.881 |
| http.github | 1 | 0 | 0/0/0 | 1 | 576.141 | 576.141 | 576.141 | 576.141 | 576.141 |
| http.pr_supervisor_peer | 338 | 0 | 0/0/0 | 256 | 47.526 | 172.701 | 457.901 | 799.983 | 799.983 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 36027.086 | 36027.086 | 36027.086 | 36027.086 | 36027.086 |
| notifications.list_page | 3 | 0 | 0/0/0 | 3 | 44.477 | 45.429 | 45.429 | 45.429 | 45.429 |
| notifications.present_page | 3 | 1 | 0/0/0 | 3 | 276.037 | 985.633 | 985.633 | 985.633 | 985.633 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 6.238 | 6.238 | 6.238 | 6.238 | 6.238 |
| pr_supervisor.response_json | 338 | 0 | 0/0/0 | 256 | 0.715 | 16.224 | 55.675 | 135.428 | 135.428 |
| preferences.agent_auto_approve_read | 6 | 0 | 0/0/0 | 6 | 7.446 | 169.811 | 169.811 | 169.811 | 169.811 |
| progress.checkpoint_write | 16 | 1 | 0/0/0 | 16 | 213.151 | 563.124 | 563.124 | 563.124 | 563.124 |
| progress.dispatch_read | 61 | 0 | 0/0/0 | 61 | 56.854 | 143.885 | 402.088 | 402.088 | 402.088 |
| progress.final_report_build | 2 | 0 | 0/0/0 | 2 | 2.347 | 4.113 | 4.113 | 4.113 | 4.113 |
| progress.final_report_write | 2 | 0 | 0/0/0 | 2 | 183.969 | 280.462 | 280.462 | 280.462 | 280.462 |
| progress.heartbeat_write | 8 | 1 | 0/0/0 | 8 | 145.904 | 698.329 | 698.329 | 698.329 | 698.329 |
| progress.pending_read | 56 | 1 | 0/0/0 | 56 | 12.482 | 83.427 | 1086.892 | 1086.892 | 1086.892 |
| progress.sequence_allocate | 24 | 0 | 0/0/0 | 24 | 140.729 | 200.963 | 352.702 | 352.702 | 352.702 |
| reconciliation.completion_evidence_read | 78 | 1 | 0/0/0 | 78 | 4.433 | 67.349 | 896.333 | 896.333 | 896.333 |
| reconciliation.completion_queue | 1 | 0 | 0/0/0 | 1 | 57.848 | 57.848 | 57.848 | 57.848 | 57.848 |
| reconciliation.dispatch_read | 132 | 1 | 0/0/0 | 132 | 3.308 | 59.729 | 117.315 | 3700.767 | 3700.767 |
| reconciliation.session_read | 78 | 1 | 0/0/0 | 78 | 3.364 | 117.523 | 1354.148 | 1354.148 | 1354.148 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 217.398 | 217.398 | 217.398 | 217.398 | 217.398 |
| selection.prompt_identity | 2 | 0 | 0/0/0 | 2 | 45.241 | 122.85 | 122.85 | 122.85 | 122.85 |
| selection.prompt_outcome | 1 | 0 | 0/0/0 | 1 | 44.408 | 44.408 | 44.408 | 44.408 | 44.408 |
| sqlite.agent_runtime_checkpoint | 5 | 0 | 0/0/0 | 5 | 35.625 | 157.628 | 157.628 | 157.628 | 157.628 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 8.568 | 71.241 | 107.002 | 107.002 | 107.002 |
| sqlite.agent_session_save | 44 | 2 | 0/0/0 | 44 | 24.238 | 211.554 | 538.924 | 538.924 | 538.924 |
| sqlite.card_read | 534 | 1 | 0/0/0 | 256 | 1.676 | 23.032 | 82.826 | 114.87 | 557.377 |
| sqlite.pr_supervisor_capability_write | 3 | 0 | 0/0/0 | 3 | 5.608 | 116.386 | 116.386 | 116.386 | 116.386 |
| sqlite.pr_supervisor_completion_due_read | 48 | 0 | 0/0/0 | 48 | 18.935 | 193.333 | 226.248 | 226.248 | 226.248 |
| sqlite.pr_supervisor_due_read | 48 | 0 | 0/0/0 | 48 | 3.931 | 64.831 | 111.588 | 111.588 | 111.588 |
| sqlite.pr_supervisor_event_write | 169 | 0 | 0/0/0 | 169 | 3.481 | 32.799 | 181.627 | 245.349 | 245.349 |
| sqlite.pr_supervisor_watch_read | 169 | 0 | 0/0/0 | 169 | 5.181 | 90.188 | 217.584 | 253.34 | 253.34 |
| sqlite.pr_supervisor_watch_write | 169 | 2 | 0/0/0 | 169 | 10.081 | 102.62 | 555.71 | 1146.705 | 1146.705 |
| sqlite.project_read | 371 | 1 | 0/0/0 | 256 | 0.911 | 38.041 | 154.946 | 332.618 | 573.498 |
| sqlite.repository_read | 364 | 1 | 0/0/0 | 256 | 1.176 | 42.381 | 144.195 | 552.68 | 552.68 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 2.497 | 3.132 | 3.132 | 3.132 | 3.132 |
| sqlite.restart_handoff_completion | 88 | 0 | 0/0/0 | 88 | 6.539 | 105.959 | 363.64 | 363.64 | 363.64 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 3.643 | 3.643 | 3.643 | 3.643 | 3.643 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 3.573 | 3.573 | 3.573 | 3.573 | 3.573 |
| sqlite.restart_handoff_session | 44 | 0 | 0/0/0 | 44 | 7.886 | 44.032 | 53.241 | 53.241 | 53.241 |
| sqlite.restart_handoffs_pending | 22 | 0 | 0/0/0 | 22 | 2.861 | 73.19 | 216.643 | 216.643 | 216.643 |
| sqlite.restart_handoffs_watchdog | 21 | 0 | 0/0/0 | 21 | 4.073 | 77.213 | 119.857 | 119.857 | 119.857 |
| sqlite.transcript_append | 62 | 5 | 0/0/0 | 62 | 38.693 | 709.645 | 3437.534 | 3437.534 | 3437.534 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 44.52 | 44.52 | 44.52 | 44.52 | 44.52 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 5.436 | 5.436 | 5.436 | 5.436 | 5.436 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 10.458 | 10.458 | 10.458 | 10.458 | 10.458 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1087.149 | 1087.149 | 1087.149 | 1087.149 | 1087.149 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 1.947 | 23.881 | 23.881 | 23.881 | 23.881 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 16.672 | 16.672 | 16.672 | 16.672 | 16.672 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 1.962 | 42.173 | 42.173 | 42.173 | 42.173 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 218.694 | 2482.627 | 2482.627 | 2482.627 | 2482.627 |
| sync.reconcile_head | 12 | 9 | 0/0/0 | 12 | 672.505 | 3355.93 | 3355.93 | 3355.93 | 3355.93 |
| sync.ref_read | 44 | 0 | 0/0/0 | 44 | 1.118 | 3.418 | 27.754 | 27.754 | 27.754 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 68.866 | 68.866 | 68.866 | 68.866 | 68.866 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7781.948 | 7781.948 | 7781.948 | 7781.948 | 7781.948 |
| workspace.lease_renew | 2 | 0 | 0/0/0 | 2 | 61.342 | 166.357 | 166.357 | 166.357 | 166.357 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 1678.746 | 3428.99 | 3428.99 | 3428.99 | 3428.99 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1354.309 | 1354.309 | 1354.309 | 1354.309 | 1354.309 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 31.172 | 91.372 | 94.508 | 94.508 | 94.508 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 784 | 0 | 0 | 0 | 289.327 |
| agent | 6 | 70 | 1 | 0 | 0 | 5025.904 |
| dispatch | 4 | 425 | 3 | 0 | 0 | 994.313 |
| filesystem | 1 | 50 | 0 | 0 | 0 | 248.575 |
| fleet | 7 | 597 | 11 | 0 | 0 | 1342.822 |
| goal | 1 | 6 | 0 | 0 | 0 | 26.705 |
| health | 1 | 31 | 1 | 0 | 0 | 1231.988 |
| http | 3 | 420 | 0 | 0 | 0 | 2503.881 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 36027.086 |
| notifications | 2 | 6 | 1 | 0 | 0 | 985.633 |
| pr_supervisor | 2 | 339 | 0 | 0 | 0 | 135.428 |
| preferences | 1 | 6 | 0 | 0 | 0 | 169.811 |
| progress | 7 | 169 | 3 | 0 | 0 | 1086.892 |
| reconciliation | 4 | 289 | 3 | 0 | 0 | 3700.767 |
| selection | 3 | 4 | 0 | 0 | 0 | 217.398 |
| sqlite | 21 | 2187 | 12 | 0 | 0 | 3437.534 |
| startup | 3 | 3 | 1 | 0 | 0 | 1087.149 |
| sync | 7 | 94 | 9 | 0 | 0 | 3355.93 |
| workspace | 4 | 11 | 1 | 1 | 0 | 7781.948 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 15.825 | 42.14 | 42.14 | 42.14 |
| /api/agent/observability/v1/sessions/{session_id} | 3 | 12531 | 3 | 79.527 | 262.054 | 262.054 | 262.054 |
| /api/agent/sessions/{session_id}/restart-handoffs | 4 | 1091826 | 4 | 27.068 | 39.658 | 39.658 | 39.658 |
| /api/configuration | 2 | 339898 | 2 | 17.544 | 17.719 | 17.719 | 17.719 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 3 | 538422 | 3 | 27.316 | 61.07 | 61.07 | 61.07 |
| /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint | 1 | 207 | 1 | 841.819 | 841.819 | 841.819 | 841.819 |
| /api/fleet/overview | 5 | 4680585 | 5 | 120.638 | 259.936 | 259.936 | 259.936 |
| /api/fleet/overview/local | 28 | 767839 | 28 | 57.826 | 257.28 | 278.787 | 278.787 |
| /api/health | 20 | 300 | 20 | 2.265 | 3.207 | 3.313 | 3.313 |
| /api/instance | 2 | 1120 | 2 | 6.605 | 9.991 | 9.991 | 9.991 |
| /api/instance/maintenance | 1 | 7478 | 1 | 5.905 | 5.905 | 5.905 | 5.905 |
| /api/notifications | 3 | 33327 | 3 | 324.078 | 1294.341 | 1294.341 | 1294.341 |
| /api/owner-ready | 1 | 71 | 1 | 5.752 | 5.752 | 5.752 | 5.752 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 10.658 | 609.234 | 609.234 | 609.234 |
| /api/pr-supervisor/replicas | 364 | 2015947 | 256 | 40.718 | 222.084 | 757.32 | 813.66 |
| /api/ready | 28 | 3241 | 28 | 2.943 | 5.568 | 11.534 | 11.534 |
| /api/runtime | 20 | 6510254 | 20 | 41.489 | 108.242 | 116.402 | 116.402 |
| /api/status | 55 | 153535 | 55 | 67.686 | 508.174 | 554.965 | 554.965 |
| /api/sync/refs | 3 | 777 | 3 | 84.982 | 240.109 | 240.109 | 240.109 |
| /api/ui/assets | 3 | 237 | 3 | 4.148 | 150.261 | 150.261 | 150.261 |
| /api/workspaces | 2 | 2878 | 2 | 7.891 | 9.104 | 9.104 | 9.104 |
| /fleet | 5 | 5482945 | 5 | 420.544 | 674.246 | 674.246 | 674.246 |
| /pull-requests | 5 | 125605 | 5 | 57.084 | 168.841 | 168.841 | 168.841 |
| /{page_path:path} | 15 | 1252055 | 15 | 19.188 | 254.71 | 254.71 | 254.71 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.startup_state_read | 1 | 5025.904 | 5025.904 | 5025.904 | 5025.904 |
| dispatch.followup_pending_read | 1 | 994.313 | 994.313 | 994.313 | 994.313 |
| dispatch.record_read | 2 | 529.822 | 532.158 | 532.158 | 532.158 |
| fleet.overview.sync | 1 | 1074.656 | 1074.656 | 1074.656 | 1074.656 |
| fleet.overview_cache_read | 8 | 887.284 | 1025.225 | 1025.225 | 1025.225 |
| fleet.overview_response_json | 2 | 638.873 | 1342.822 | 1342.822 | 1342.822 |
| health.canonical_obligation | 1 | 1231.988 | 1231.988 | 1231.988 | 1231.988 |
| maintenance.run | 1 | 36027.086 | 36027.086 | 36027.086 | 36027.086 |
| notifications.present_page | 1 | 985.633 | 985.633 | 985.633 | 985.633 |
| progress.checkpoint_write | 1 | 563.124 | 563.124 | 563.124 | 563.124 |
| progress.heartbeat_write | 1 | 698.329 | 698.329 | 698.329 | 698.329 |
| progress.pending_read | 1 | 1086.892 | 1086.892 | 1086.892 | 1086.892 |
| reconciliation.completion_evidence_read | 1 | 896.333 | 896.333 | 896.333 | 896.333 |
| reconciliation.dispatch_read | 1 | 3700.767 | 3700.767 | 3700.767 | 3700.767 |
| reconciliation.session_read | 1 | 1354.148 | 1354.148 | 1354.148 | 1354.148 |
| sqlite.agent_session_save | 2 | 521.72 | 538.924 | 538.924 | 538.924 |
| sqlite.card_read | 1 | 557.377 | 557.377 | 557.377 | 557.377 |
| sqlite.pr_supervisor_watch_write | 2 | 555.71 | 1146.705 | 1146.705 | 1146.705 |
| sqlite.project_read | 1 | 573.498 | 573.498 | 573.498 | 573.498 |
| sqlite.repository_read | 1 | 552.68 | 552.68 | 552.68 | 552.68 |
| sqlite.transcript_append | 5 | 752.148 | 3437.534 | 3437.534 | 3437.534 |
| startup.openapi | 1 | 1087.149 | 1087.149 | 1087.149 | 1087.149 |
| sync.reconcile_head | 9 | 987.985 | 3355.93 | 3355.93 | 3355.93 |
| workspace.collect_garbage | 1 | 7781.948 | 7781.948 | 7781.948 | 7781.948 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 1 | 5025.904 | 5025.904 | 5025.904 | 5025.904 |
| dispatch | 3 | 532.158 | 994.313 | 994.313 | 994.313 |
| fleet | 11 | 1002.069 | 1342.822 | 1342.822 | 1342.822 |
| health | 1 | 1231.988 | 1231.988 | 1231.988 | 1231.988 |
| maintenance | 1 | 36027.086 | 36027.086 | 36027.086 | 36027.086 |
| notifications | 1 | 985.633 | 985.633 | 985.633 | 985.633 |
| progress | 3 | 698.329 | 1086.892 | 1086.892 | 1086.892 |
| reconciliation | 3 | 1354.148 | 3700.767 | 3700.767 | 3700.767 |
| sqlite | 12 | 573.498 | 3437.534 | 3437.534 | 3437.534 |
| startup | 1 | 1087.149 | 1087.149 | 1087.149 | 1087.149 |
| sync | 9 | 987.985 | 3355.93 | 3355.93 | 3355.93 |
| workspace | 1 | 7781.948 | 7781.948 | 7781.948 | 7781.948 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 54.549 | 78.748 | 78.748 | 78.748 | 0 | 25464 |
| /agent | 5 | 0 | 148.725 | 329.717 | 329.717 | 329.717 | 0 | 118529 |
| /api/fleet/overview | 5 | 0 | 160.389 | 314.158 | 314.158 | 314.158 | 0 | 936117 |
| /api/health | 20 | 0 | 21.127 | 24.969 | 27.533 | 27.533 | 0 | 15 |
| /api/runtime | 20 | 0 | 75.442 | 157.244 | 167.066 | 167.066 | 0 | 325547 |
| /api/status | 20 | 0 | 80.394 | 199.002 | 506.658 | 506.658 | 1 | 2824 |
| /fleet | 5 | 0 | 444.436 | 694.389 | 694.389 | 694.389 | 2 | 1096589 |
| /pull-requests | 5 | 0 | 83.219 | 200.66 | 200.66 | 200.66 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 36.898 | 44.813 | 44.813 | 44.813 | 0 | 100559 |
| /work | 5 | 0 | 33.955 | 39.963 | 39.963 | 39.963 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 29.198166 | 6.434125 | 272 | 270 | 27.420454716775566 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 24.615625 | 6.43325 | 2 | 0 | 24.413917039055377 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 105.713584 | 27.888791 | 5 | 0 | 104.34462502598763 | 0.0015830155462026596 | 105.65824998775497 |
| retention_object_collection | 1/0 | 6739.139083 | 691.231458 | 13039 | 12837 | 3926.2153014424257 | 0.0015830155462026596 | 0.003708992153406143 |
| retention_canonical | 1/0 | 6845.105709 | 719.3495 | 13044 | 12837 | 4030.5599264684133 | 0.003166031092405319 | 105.66195897990838 |
| retention_mirror | 1/0 | 24.113209 | 7.378125 | 2 | 0 | 23.314207966905087 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 6898.65075 | 733.384875 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 1.36575 | 0.552667 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 8888.988458 | 3354.769334 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1255.788834 | 744.260625 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 11348.079667 | 7295.600708 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 5348.055334 | 2096.93075 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2280.171708 | 1719.597208 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 36021.45325 | 15945.406708 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# GC publication-lock evidence — 2026-10-18

All **555 findings and64concerns remain open**. Every554preceding row is unchanged; explicit current status overrides separate prior integration and activation from historical stage labels. The [complete artifact](benchmarks/systemic-gc-prefilter-20261018.json) preserves every new trial, lock event, timing, byte/count, source/root proof, journal and failure. This prefilter is a local candidate, not yet integrated or deployed. PR517 benchmark correction is separately integrated with five green head/main checks; running product remainsbcd5.

The candidate reads each bounded live-hash batch outside the append publication lock and can only keep bytes based on that speculation. Every possible deletion still requires the original fresh locked query and reader-pin check. Stale positive rows can only defer byte collection until a later pass. Initial pin check is unchanged; later wholly-live batches may continue scanning under a new pin, without unlink. No refcount/audit/transaction/durability/SQLpredicate/batchlimit/threshold/default change.

Complete numeric lock timelines confirm mutual GC/append contention in a disposable workload. Three alternating concurrent pairs reduced median GC168.434->134.889ms and appendtotal242.242->197.764ms, but maxima worsened200.619->236.151 and268.804->280.638ms. GCbatchlockhold35-38ms became0.001-0.005ms and appendlockwait~28ms became0.6-1.1ms. A separate orphan-heavy prototype worsened median138.080->151.678ms. These small samples and all regressions remain retained; no live causal share or whole-path tail success is claimed.

The first publication-race controller started the paused publisher before GC initial pin lock, so its speculative-query latch was unreachable. Original source/log/results are retained. Corrected sequencing starts GC first and pauses enumeration, then starts the publisher; commit and failed-publication cases pass. Late reference, late pin and stale positive contracts also pass. Ten focused,67 broader,65 backup plus16subtests and four nativeSQLite tests pass. Build cache sandboxdenial and approved native retry are retained; no live backup action occurred.

| Mixed observation | Exact517 product baseline | Prefilter candidate |
|---|---:|---:|
| Workload seconds | 96.259 | 96.993 |
| Worker warning count | 4 | 3 |
| Sampled target violations | 12 | 10 |
| GC wall ms | 10611.193584 | 7283.442708 |
| GC CPU ms | 908.085041 | 1095.157542 |
| GC lockwait ms | 5808.851956971921 | 138.76312505453825 |
| GC lockhold ms | 3926.3510448508896 | 0.014833000022917986 |
| GC SQLAPI wall ms | 3801.8784732557833 | 5728.137255238835 |
| GC SQLAPI calls | 12707 | 12707 |
| GC rows | 12511 | 12511 |

GC SQL API wall increased from3.802s to5.728s and thread CPU from908ms to1095ms. Full workload duration increased96.259s to96.993s; GC lock elimination does not establish a whole-path speedup. Baseline/candidate recorded4/3workerwarnings and12/10sampled violations. Each100iteration case ran once, serialized after all own tests. Both retain9600ordered active events,12511cold objects with every content hash verified,5000policy-expired events removed and16audit proofs. Each7274record journal is hash-chain validated. Workload finish, sequential capture and postvalidation times are distinct and checked. All operation failure/timeout/rejection totals, warnings/violations, CPU/logical block/SQL/filesystem counters, raw route/operation windows and bytes remain in the artifact. Hostactivity is uncontrolled; object count alone does not match production417MB/318MB databases. Inclusive phases overlap; nonCPU wall is not physical disk/nativebusy/GIL attribution. Existing warning overrides differ from declared500ms sample target; neither was changed.

## Every measured operation/request and byte distribution

Each cell is n/p50/p95/p99/max in ms. These are exact workload samples, separate from process maxima and last256runtime windows.

| Operation/request | Baseline | Candidate | >=500ms baseline/candidate |
|---|---|---|---:|
| / | 100/13.628/27.133/42.954/79.63 | 100/13.479/25.87/53.908/71.719 | 0/0 |
| /agent | 100/61.716/139.851/214.804/220.364 | 100/62.398/130.928/190.546/205.019 | 0/0 |
| /api/agent/history/session-0?limit=64 | 100/38.26/83.162/128.487/138.954 | 100/37.557/77.321/131.585/178.076 | 0/0 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100/179.684/407.353/605.991/612.574 | 100/180.682/430.913/516.301/579.267 | 3/3 |
| /api/agent/preferences | 100/5.264/9.313/19.729/25.796 | 100/5.161/9.169/18.086/24.56 | 0/0 |
| /api/agent/sessions?view=chats | 100/33.064/85.351/104.141/138.342 | 100/32.465/82.107/125.646/138.342 | 0/0 |
| /api/cards | 100/9.166/16.534/28.186/110.755 | 100/9.163/14.866/21.641/76.608 | 0/0 |
| /api/cards/fixture-card | 100/5.515/12.052/16.734/18.014 | 100/5.463/14.554/36.039/101.592 | 0/0 |
| /api/cards/fixture-card/history | 100/29.002/52.724/110.282/121.204 | 100/28.846/74.896/114.667/121.635 | 0/0 |
| /api/execution/defaults | 100/6.015/9.048/38.409/102.313 | 100/5.861/8.534/15.179/40.614 | 0/0 |
| /api/fleet/dispatch-jobs | 100/36.806/86.925/135.088/142.221 | 100/36.667/98.745/146.915/163.426 | 0/0 |
| /api/fleet/dispatch-queue | 100/7.297/15.846/19.363/22.915 | 100/7.203/14.824/25.145/31.374 | 0/0 |
| /api/health | 100/3.648/5.992/7.306/38.609 | 100/3.617/6.461/8.414/16.87 | 0/0 |
| /api/health-journal/status | 100/8.447/16.573/20.799/44.621 | 100/8.764/14.465/37.711/38.603 | 0/0 |
| /api/notifications | 100/5.778/12.455/19.8/21.587 | 100/5.613/14.777/33.532/36.801 | 0/0 |
| /api/pr-supervisor/capabilities | 100/8.276/21.104/29.174/39.811 | 100/8.481/13.854/21.125/33.939 | 0/0 |
| /api/pr-supervisor/watches | 100/8.257/18.362/27.366/38.124 | 100/8.155/16.778/44.78/50.727 | 0/0 |
| /api/runtime | 100/33.243/40.303/52.989/78.273 | 100/32.984/39.542/44.07/45.648 | 0/0 |
| /api/runtime?section=sse | 100/5.348/8.659/20.415/45.313 | 100/5.369/7.987/14.554/22.6 | 0/0 |
| /api/status | 100/49.62/85.66/103.715/173.035 | 100/49.923/85.162/95.666/102.246 | 0/0 |
| /api/sync/refs | 100/4.816/12.463/19.783/19.795 | 100/4.838/8.244/17.958/22.728 | 0/0 |
| /api/sync/status | 100/13.112/39.783/73.129/75.571 | 100/13.435/48.523/80.79/96.229 | 0/0 |
| /api/workspaces | 100/7.597/20.565/28.822/124.002 | 100/7.458/14.359/45.029/46.688 | 0/0 |
| /fleet | 100/93.596/215.27/290.286/306.268 | 100/93.24/266.651/418.547/493.505 | 0/0 |
| /partials/home/sections | 100/20.049/49.276/73.106/85.198 | 100/19.898/58.676/83.269/97.576 | 0/0 |
| /pull-requests | 100/32.268/91.11/135.36/141.813 | 100/32.122/69.734/140.558/140.633 | 0/0 |
| /settings?section=agent | 100/20.434/59.254/85.031/105.668 | 100/20.259/74.449/116.376/117.473 | 0/0 |
| /settings?section=backups | 100/19.099/48.074/90.202/104.39 | 100/19.086/41.463/100.117/109.553 | 0/0 |
| /work | 100/15.101/33.58/47.694/78.453 | 100/15.17/31.992/71.066/76.407 | 0/0 |
| acp.wire_append | 100/4.647/14.966/33.17/58.643 | 100/4.012/13.337/23.919/29.285 | 0/0 |
| dispatch.runnable_read | 100/2.688/28.521/34.472/105.353 | 100/2.966/11.416/16.342/23.25 | 0/0 |
| fleet.overview_cache_read | 100/2.248/11.318/28.631/77.475 | 100/2.055/8.273/23.304/30.345 | 0/0 |
| fleet.overview_cache_write | 100/12.44/34.097/42.928/60.604 | 100/13.581/32.86/37.759/43.329 | 0/0 |
| maintenance.run | 1/20645.674/20645.674/20645.674/20645.674 | 1/18057.533/18057.533/18057.533/18057.533 | 1/1 |
| progress.checkpoint_write | 100/9.969/22.232/28.115/40.588 | 100/10.658/21.881/30.753/87.762 | 0/0 |
| progress.sequence_allocate | 100/9.429/27.321/60.664/60.686 | 100/8.207/32.269/38.901/43.146 | 0/0 |
| reconciliation.dispatch_read | 100/1.93/12.81/30.318/77.487 | 100/1.763/7.274/9.999/16.4 | 0/0 |
| sqlite.card_write | 10/50.478/113.583/113.583/113.583 | 10/73.141/112.901/112.901/112.901 | 0/0 |
| sqlite.execution_binding_materialize | 100/31.688/108.664/141.294/572.445 | 100/62.149/162.744/214.372/440.972 | 1/0 |
| sync.commit_read | 100/4.189/15.75/23.896/27.441 | 100/4.617/14.537/18.601/19.443 | 0/0 |
| sync.object_collect | 10/559.511/1334.429/1334.429/1334.429 | 10/346.187/1445.595/1445.595/1445.595 | 6/3 |
| sync.push_transaction | 100/77.824/158.884/276.48/364.091 | 100/74.19/183.308/432.965/524.153 | 0/1 |
| sync.reconcile_head | 100/56.368/186.005/243.064/423.809 | 100/51.341/212.99/665.114/670.384 | 0/2 |
| transcript.drain | 300/116.966/222.96/280.181/529.045 | 300/118.239/183.205/221.243/420.178 | 1/0 |

Decoded response bytes are payload bytes, not wire bytes. Cells are n/p50/p95/p99/max.

| Request | Baseline decoded bytes | Candidate decoded bytes |
|---|---|---|
| / | 100/23368/23368/23368/23368 | 100/23368/23368/23368/23368 |
| /agent | 100/120912/120912/120942/120942 | 100/120912/120912/120942/120942 |
| /api/agent/history/session-0?limit=64 | 100/260542/260543/260543/260544 | 100/260542/260543/260543/260543 |
| /api/agent/history/session-0?limit=64&message_boundaries=true | 100/2775466/2775466/2775466/2775466 | 100/2775466/2775466/2775466/2775466 |
| /api/agent/preferences | 100/351/351/351/351 | 100/351/351/351/351 |
| /api/agent/sessions?view=chats | 100/75522/75522/75522/75522 | 100/75522/75522/75522/75522 |
| /api/cards | 100/23031/23031/23031/23031 | 100/23031/23031/23031/23031 |
| /api/cards/fixture-card | 100/2196/2196/2196/2196 | 100/2196/2196/2196/2196 |
| /api/cards/fixture-card/history | 100/102882/102882/102882/102882 | 100/102882/102882/102882/102882 |
| /api/execution/defaults | 100/2733/2733/2733/2733 | 100/2733/2733/2733/2733 |
| /api/fleet/dispatch-jobs | 100/247350/247350/247350/247350 | 100/247350/247350/247350/247350 |
| /api/fleet/dispatch-queue | 100/1072/1072/1072/1072 | 100/1073/1073/1073/1073 |
| /api/health | 100/15/15/15/15 | 100/15/15/15/15 |
| /api/health-journal/status | 100/521/521/521/521 | 100/521/521/521/521 |
| /api/notifications | 100/75/75/75/75 | 100/75/75/75/75 |
| /api/pr-supervisor/capabilities | 100/1284/1284/1284/1284 | 100/1284/1284/1284/1284 |
| /api/pr-supervisor/watches | 100/2/2/2/2 | 100/2/2/2/2 |
| /api/runtime | 100/183211/183249/183252/183497 | 100/189969/190069/190072/190324 |
| /api/runtime?section=sse | 100/303/303/303/303 | 100/303/303/303/303 |
| /api/status | 100/2133/2133/2133/2133 | 100/2134/2134/2134/2135 |
| /api/sync/refs | 100/259/259/259/260 | 100/259/259/259/259 |
| /api/sync/status | 100/3618/3618/3619/3621 | 100/3603/3617/3618/3619 |
| /api/workspaces | 100/26/26/26/26 | 100/26/26/26/26 |
| /fleet | 100/278789/278790/278790/278790 | 100/278774/278775/278775/278784 |
| /partials/home/sections | 100/3791/3791/3791/3791 | 100/3791/3791/3791/3791 |
| /pull-requests | 100/23352/23352/23352/23352 | 100/23352/23352/23352/23352 |
| /settings?section=agent | 100/94049/94049/94049/94049 | 100/94051/94051/94051/94051 |
| /settings?section=backups | 100/97682/97682/97682/97682 | 100/97685/97685/97685/97685 |
| /work | 100/28419/28419/28419/28419 | 100/28419/28419/28419/28419 |

## Remaining acceptance and coverage

All prior measured and unmeasured families remain open: nativebusy/WAL/fsync/disk/GIL/GC, freshhealth/integrity/VACUUM, fullKernel/OpenAPI/MCP/modelCPU, history/drain/sync/inventory amplification, realprovider/placement/workspace/activewatch, peers/builds/logs,5.64MBdroppedlogs/2396statusfailures/olderrotation, receipts/lostraw/attachments/testroots and unexplained browser/CI failures. Loadedbcd5 ordinary95GET window had3thresholdviolations; its runtime snapshot77/181baselineops and22/115routes positive still leaves104/93unobserved. Named observations do not cover every branch. Backup0331 retains separate ownership/missingcoldsource/paused failedarchives and independent acceptance; DB89957 accepted original scope remains closed. Normal PA scheduling owns final independent verification under unchanged17d034a6; no repairer self-acceptance, no Done from any PR or activation.

## Preserved earlier coverage

# Current loaded bcd5 inventory and measurement correction — 2026-10-18

All **554 findings and 63 concerns remain open**. The full JSON preserves every preceding553 row unchanged; this stage appends an explicit measurement-boundary finding and the [complete numeric artifact](benchmarks/systemic-bcd5-boundaries-20261018.json). Earlier candidate/integration/deployment labels below are historical stages. PR516 is now merged and loaded, independently verified at exact source/package/PID/startup/config/lease; that does not establish performance acceptance.

The single planned95ordinary sequential GET window returned95HTTP200, with **three >=500ms requests**: Agent2 (max687.949ms) and Fleet1 (max677.954ms). Every attempt, error/status, UTC timestamp, helperwall/ServerTiming wall+CPU, decoded bytes, correlation and partial SHA archive is retained. The build stayed bcd5/PID53309/startup7164/package2c2c. This small sample is not a cold-start or fleet acceptance distribution; decoded bytes are not wire bytes and five-page p95=p99=max. Observer persistence adds work outside request timers.

The separate full runtime capture at10:03:26Z contains82primary operations, one diagnostic operation,26route groups,19primary prefixes,43retained slow records (cap64),43primary process slow calls and31primary process maxima>=500ms. Workspace provisioning has one failure whose cause is unverified. Positive coverage is77/181 baseline operations and22/115 route groups;104/93 remain unobserved here, and a measured name does not cover every branch. No new names appeared relative to the ledger. All received21sections are retained with three explicit opaque privacy omission paths; older lost auxiliary values are not reconstructed. Runtime counts/maxima, rolling windows and slow-only quantiles remain separate, and all names/families are tabulated below.

Cached startup maintenance from09:59:32Z took32.516s: retention5.896s, cold-object collection5.720s (635ms threadCPU,12,904SQLite API calls/12,704rows,3.967s observed SQLwall,4.072s lockhold), initial fresh migration/health6.326s, dispatch compaction1.208s, projection12.454s including VACUUM7.848s, transcriptDB4.257s and final fresh health2.374s. Parent spans include children. Successful native busy, physical IO, GIL and host scheduling shares are unverified. Missingcold1/migrationrunning remain. Different object counts/database sizes/activity preclude a controlled before/after claim against earlier sweeps.

The benchmark formerly took its primary runtime/resource snapshot immediately after workload completion but evaluated returned finish time and diagnostic snapshots after validation. Its96.683/97.628s workload durations were correct;100.9595/101.6342s returned spans were wider. Original raw artifacts are unchanged. The local benchmark-only correction captures an explicit workload finish, sequential diagnostic/storage/resource snapshots before validation, closes warning/GC collectors and reports capture and validation durations separately. Snapshot capture is not atomic. Three guarded contracts pass11.78s, including real96-event order/three cold-hash verification and deliberate postmeasurement diagnostic/warning/GC activity excluded from captured results. Final SQL/object counter freeze also passes3guarded contracts in11.55s, including a real validation-only SQLite VALUES statement. The intermediate expandedtest failed because EXPLAIN yielded zero trace callbacks on this SQLite; that failure and the first passing result remain retained. No assertion/deadline budget was relaxed. No performance rerun or product/threshold/default change was made.

Protected PR516 headCI39116444919 and mainCI39116683724 each have five terminal SUCCESS checks with no retry/bypass. Exacthead ed78 merged bcd5, matching tree393ed474; watch64b86dd3 stablegreen and independent checks/reviews/threads/protection were recorded before merge. Exact immutable install/source437/dependency52/template38 verification preceded the managed handoff1ea6a273. Actualloaded source/package proof is separate from installation metadata. This benchmark correction still needs its own review/integration. Normal PA scheduling owns independent acceptance under unchanged requirement17d034a6; the repairer does not self-accept. Backup0331 ownership/missing-source/paused failed archives remain separate, DB89957 accepted original scope remains closed, and every earlier peer/log/receipt/test-root/browser/CI/valid-branch/latency residual remains required.

## Every actual bcd5 runtime observation

# Actualbcd5 runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, sourcebcd5/PID53309/startup7164/package2c2c. Runtime snapshot UTC 2026-10-18T10:03:26.718689+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

The95GET window precedes the current turn disposable tests; cached maintenance and runtime collectors follow it sequentially and represent separate windows. No host-pressure capture was made in this window. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 70.82 | 70.82 | 70.82 | 70.82 | 70.82 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 2.664 | 2.664 | 2.664 | 2.664 | 2.664 |
| acp.wire_append | 741 | 1 | 0/0/0 | 256 | 1.121 | 112.446 | 215.864 | 425.346 | 534.927 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 1.531 | 1.531 | 1.531 | 1.531 | 1.531 |
| agent.project_recovery_availability | 82 | 0 | 0/0/0 | 82 | 1.257 | 14.503 | 76.292 | 76.292 | 76.292 |
| agent.prompt_compose | 2 | 0 | 0/0/0 | 2 | 21.195 | 26.489 | 26.489 | 26.489 | 26.489 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 3.167 | 3.167 | 3.167 | 3.167 | 3.167 |
| agent.recovery_sessions | 40 | 0 | 0/0/0 | 40 | 123.034 | 191.647 | 469.134 | 469.134 | 469.134 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 2.309 | 2.309 | 2.309 | 2.309 | 2.309 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 4379.911 | 4379.911 | 4379.911 | 4379.911 | 4379.911 |
| dispatch.completion_pending_read | 47 | 0 | 0/0/0 | 47 | 1.786 | 73.837 | 356.805 | 356.805 | 356.805 |
| dispatch.followup_pending_read | 47 | 0 | 0/0/0 | 47 | 2.049 | 51.65 | 154.518 | 154.518 | 154.518 |
| dispatch.record_read | 703 | 1 | 0/0/0 | 256 | 19.458 | 37.16 | 336.595 | 385.638 | 517.019 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 11.151 | 57.314 | 57.314 | 57.314 | 57.314 |
| filesystem.github_credentials_read | 90 | 0 | 0/0/0 | 90 | 1.76 | 51.265 | 671.568 | 671.568 | 671.568 |
| fleet.overview.reachability | 16 | 0 | 0/0/0 | 16 | 8.295 | 19.283 | 19.283 | 19.283 | 19.283 |
| fleet.overview.status | 16 | 0 | 0/0/0 | 16 | 8.718 | 21.883 | 21.883 | 21.883 | 21.883 |
| fleet.overview.sync | 17 | 0 | 0/0/0 | 17 | 56.623 | 215.943 | 215.943 | 215.943 | 215.943 |
| fleet.overview_cache_read | 675 | 4 | 0/0/0 | 256 | 1.952 | 22.592 | 34.802 | 43.765 | 1720.071 |
| fleet.overview_cache_write | 193 | 1 | 0/0/0 | 193 | 10.515 | 60.287 | 157.471 | 670.876 | 670.876 |
| fleet.overview_response_json | 144 | 3 | 0/0/0 | 144 | 3.147 | 27.067 | 1715.934 | 1720.365 | 1720.365 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 11.985 | 11.985 | 11.985 | 11.985 | 11.985 |
| goal.dispatch_lifecycle_index | 9 | 0 | 0/0/0 | 9 | 0.708 | 12.966 | 12.966 | 12.966 | 12.966 |
| health.canonical_obligation | 33 | 1 | 0/0/0 | 33 | 30.431 | 328.484 | 1211.392 | 1211.392 | 1211.392 |
| http.fleet_overview | 144 | 0 | 0/0/0 | 144 | 104.785 | 918.157 | 2567.908 | 2592.832 | 2592.832 |
| http.github | 1 | 0 | 0/0/0 | 1 | 505.61 | 505.61 | 505.61 | 505.61 | 505.61 |
| http.pr_supervisor_peer | 664 | 0 | 0/0/0 | 256 | 46.897 | 144.743 | 367.356 | 417.125 | 1114.039 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 32521.342 | 32521.342 | 32521.342 | 32521.342 | 32521.342 |
| notifications.list_page | 5 | 0 | 0/0/0 | 5 | 6.129 | 40.824 | 40.824 | 40.824 | 40.824 |
| notifications.present_page | 5 | 1 | 0/0/0 | 5 | 262.935 | 1341.615 | 1341.615 | 1341.615 | 1341.615 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 13.527 | 13.527 | 13.527 | 13.527 | 13.527 |
| pr_supervisor.response_json | 664 | 0 | 0/0/0 | 256 | 0.672 | 13.858 | 19.162 | 22.833 | 162.932 |
| preferences.agent_auto_approve_read | 6 | 0 | 0/0/0 | 6 | 3.137 | 113.552 | 113.552 | 113.552 | 113.552 |
| progress.checkpoint_write | 14 | 1 | 0/0/0 | 14 | 228.611 | 638.9 | 638.9 | 638.9 | 638.9 |
| progress.dispatch_read | 93 | 0 | 0/0/0 | 93 | 54.116 | 145.509 | 209.88 | 209.88 | 209.88 |
| progress.final_report_build | 2 | 0 | 0/0/0 | 2 | 2.317 | 3.134 | 3.134 | 3.134 | 3.134 |
| progress.final_report_write | 2 | 0 | 0/0/0 | 2 | 170.053 | 271.971 | 271.971 | 271.971 | 271.971 |
| progress.heartbeat_write | 17 | 1 | 0/0/0 | 17 | 141.16 | 587.535 | 587.535 | 587.535 | 587.535 |
| progress.pending_read | 84 | 1 | 0/0/0 | 84 | 10.179 | 63.197 | 624.272 | 624.272 | 624.272 |
| progress.sequence_allocate | 31 | 1 | 0/0/0 | 31 | 142.88 | 284.875 | 654.688 | 654.688 | 654.688 |
| reconciliation.completion_evidence_read | 138 | 2 | 0/0/0 | 138 | 4.225 | 84.274 | 597.671 | 1736.544 | 1736.544 |
| reconciliation.completion_queue | 1 | 0 | 0/0/0 | 1 | 60.943 | 60.943 | 60.943 | 60.943 | 60.943 |
| reconciliation.dispatch_read | 232 | 2 | 0/0/0 | 232 | 1.941 | 34.627 | 413.354 | 3455.584 | 3455.584 |
| reconciliation.session_read | 138 | 0 | 0/0/0 | 138 | 2.778 | 18.893 | 99.407 | 358.765 | 358.765 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 41.033 | 41.033 | 41.033 | 41.033 | 41.033 |
| selection.prompt_identity | 2 | 0 | 0/0/0 | 2 | 34.544 | 80.242 | 80.242 | 80.242 | 80.242 |
| selection.prompt_outcome | 1 | 0 | 0/0/0 | 1 | 33.886 | 33.886 | 33.886 | 33.886 | 33.886 |
| sqlite.agent_runtime_checkpoint | 5 | 1 | 0/0/0 | 5 | 54.691 | 606.355 | 606.355 | 606.355 | 606.355 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 12.104 | 40.767 | 57.618 | 57.618 | 57.618 |
| sqlite.agent_session_save | 51 | 2 | 0/0/0 | 51 | 21.513 | 271.063 | 6431.58 | 6431.58 | 6431.58 |
| sqlite.card_read | 1036 | 1 | 0/0/0 | 256 | 1.667 | 30.368 | 154.784 | 192.728 | 599.518 |
| sqlite.pr_supervisor_capability_write | 4 | 0 | 0/0/0 | 4 | 5.481 | 10.707 | 10.707 | 10.707 | 10.707 |
| sqlite.pr_supervisor_completion_due_read | 88 | 0 | 0/0/0 | 88 | 16.232 | 159.058 | 469.982 | 469.982 | 469.982 |
| sqlite.pr_supervisor_due_read | 88 | 0 | 0/0/0 | 88 | 3.803 | 36.704 | 78.729 | 78.729 | 78.729 |
| sqlite.pr_supervisor_event_write | 332 | 0 | 0/0/0 | 256 | 3.123 | 25.374 | 72.233 | 228.3 | 228.3 |
| sqlite.pr_supervisor_watch_read | 332 | 1 | 0/0/0 | 256 | 4.669 | 83.522 | 437.674 | 637.047 | 637.047 |
| sqlite.pr_supervisor_watch_write | 332 | 2 | 0/0/0 | 256 | 8.892 | 91.294 | 164.497 | 285.853 | 595.507 |
| sqlite.project_read | 710 | 0 | 0/0/0 | 256 | 0.789 | 14.979 | 109.597 | 404.909 | 480.044 |
| sqlite.repository_read | 703 | 0 | 0/0/0 | 256 | 1.001 | 23.101 | 103.116 | 236.782 | 314.268 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0/0 | 2 | 2.03 | 3.496 | 3.496 | 3.496 | 3.496 |
| sqlite.restart_handoff_completion | 164 | 0 | 0/0/0 | 164 | 5.362 | 37.473 | 114.029 | 319.255 | 319.255 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 14.656 | 14.656 | 14.656 | 14.656 | 14.656 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 16.291 | 16.291 | 16.291 | 16.291 | 16.291 |
| sqlite.restart_handoff_session | 82 | 0 | 0/0/0 | 82 | 6.231 | 15.089 | 47.049 | 47.049 | 47.049 |
| sqlite.restart_handoffs_pending | 41 | 0 | 0/0/0 | 41 | 2.239 | 6.405 | 8.301 | 8.301 | 8.301 |
| sqlite.restart_handoffs_watchdog | 40 | 0 | 0/0/0 | 40 | 1.712 | 13.879 | 21.385 | 21.385 | 21.385 |
| sqlite.transcript_append | 60 | 4 | 0/0/0 | 60 | 32.184 | 512.573 | 1044.407 | 1044.407 | 1044.407 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 35.847 | 35.847 | 35.847 | 35.847 | 35.847 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.504 | 4.504 | 4.504 | 4.504 | 4.504 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 7.615 | 7.615 | 7.615 | 7.615 | 7.615 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1086.242 | 1086.242 | 1086.242 | 1086.242 | 1086.242 |
| sync.commit_read | 15 | 0 | 0/0/0 | 15 | 2.547 | 40.877 | 40.877 | 40.877 | 40.877 |
| sync.gc_resume | 1 | 0 | 0/0/0 | 1 | 17.783 | 17.783 | 17.783 | 17.783 | 17.783 |
| sync.json_decode | 15 | 0 | 0/0/0 | 15 | 1.908 | 23.96 | 23.96 | 23.96 | 23.96 |
| sync.peer_http | 15 | 0 | 0/0/0 | 15 | 78.897 | 2556.477 | 2556.477 | 2556.477 | 2556.477 |
| sync.reconcile_head | 15 | 8 | 0/0/0 | 15 | 619.693 | 4121.233 | 4121.233 | 4121.233 | 4121.233 |
| sync.ref_read | 86 | 0 | 0/0/0 | 86 | 0.809 | 2.626 | 53.352 | 53.352 | 53.352 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 41.778 | 41.778 | 41.778 | 41.778 | 41.778 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7391.275 | 7391.275 | 7391.275 | 7391.275 | 7391.275 |
| workspace.lease_renew | 2 | 0 | 0/0/0 | 2 | 77.877 | 114.375 | 114.375 | 114.375 | 114.375 |
| workspace.project_provision | 7 | 0 | 1/0/0 | 7 | 2224.622 | 3288.41 | 3288.41 | 3288.41 | 3288.41 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1750.404 | 1750.404 | 1750.404 | 1750.404 | 1750.404 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 35.263 | 74.623 | 322.109 | 322.109 | 322.109 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 744 | 1 | 0 | 0 | 534.927 |
| agent | 6 | 127 | 1 | 0 | 0 | 4379.911 |
| dispatch | 4 | 804 | 1 | 0 | 0 | 517.019 |
| filesystem | 1 | 90 | 0 | 0 | 0 | 671.568 |
| fleet | 7 | 1062 | 8 | 0 | 0 | 1720.365 |
| goal | 1 | 9 | 0 | 0 | 0 | 12.966 |
| health | 1 | 33 | 1 | 0 | 0 | 1211.392 |
| http | 3 | 809 | 0 | 0 | 0 | 2592.832 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 32521.342 |
| notifications | 2 | 10 | 1 | 0 | 0 | 1341.615 |
| pr_supervisor | 2 | 665 | 0 | 0 | 0 | 162.932 |
| preferences | 1 | 6 | 0 | 0 | 0 | 113.552 |
| progress | 7 | 243 | 4 | 0 | 0 | 654.688 |
| reconciliation | 4 | 509 | 4 | 0 | 0 | 3455.584 |
| selection | 3 | 4 | 0 | 0 | 0 | 80.242 |
| sqlite | 21 | 4094 | 11 | 0 | 0 | 6431.58 |
| startup | 3 | 3 | 1 | 0 | 0 | 1086.242 |
| sync | 7 | 148 | 8 | 0 | 0 | 4121.233 |
| workspace | 4 | 11 | 1 | 1 | 0 | 7391.275 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 10.771 | 96.317 | 96.317 | 96.317 |
| /api/agent/observability/v1/sessions/{session_id} | 2 | 8540 | 2 | 69.441 | 69.968 | 69.968 | 69.968 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 527335 | 2 | 23.913 | 35.674 | 35.674 | 35.674 |
| /api/configuration | 2 | 339898 | 2 | 23.334 | 80.053 | 80.053 | 80.053 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 4 | 717896 | 4 | 18.987 | 28.137 | 28.137 | 28.137 |
| /api/fleet/dispatch-jobs/{dispatch_id}/checkpoint | 1 | 213 | 1 | 582.948 | 582.948 | 582.948 | 582.948 |
| /api/fleet/overview | 5 | 4680540 | 5 | 156.886 | 291.659 | 291.659 | 291.659 |
| /api/fleet/overview/local | 49 | 803855 | 49 | 37.661 | 219.901 | 512.318 | 512.318 |
| /api/health | 20 | 300 | 20 | 2.172 | 3.323 | 3.336 | 3.336 |
| /api/health-journal/reports | 1 | 322 | 1 | 24.09 | 24.09 | 24.09 | 24.09 |
| /api/instance | 2 | 1120 | 2 | 3.726 | 6.586 | 6.586 | 6.586 |
| /api/instance/maintenance | 1 | 7453 | 1 | 4.609 | 4.609 | 4.609 | 4.609 |
| /api/notifications | 5 | 55545 | 5 | 301.797 | 1480.895 | 1480.895 | 1480.895 |
| /api/owner-ready | 1 | 71 | 1 | 7.033 | 7.033 | 7.033 | 7.033 |
| /api/pr-supervisor/capabilities | 1 | 2707 | 1 | 8.341 | 8.341 | 8.341 | 8.341 |
| /api/pr-supervisor/instances/heartbeat | 8 | 136 | 8 | 20.057 | 491.277 | 491.277 | 491.277 |
| /api/pr-supervisor/replicas | 703 | 3894220 | 256 | 38.375 | 244.341 | 552.675 | 890.296 |
| /api/ready | 49 | 6097 | 49 | 2.72 | 9.2 | 52.631 | 52.631 |
| /api/runtime | 20 | 6522510 | 20 | 46.024 | 106.519 | 395.015 | 395.015 |
| /api/status | 76 | 212839 | 76 | 68.098 | 316.878 | 475.525 | 475.525 |
| /api/sync/refs | 3 | 777 | 3 | 5.075 | 62.926 | 62.926 | 62.926 |
| /api/ui/assets | 5 | 395 | 5 | 6.093 | 40.584 | 40.584 | 40.584 |
| /api/workspaces | 2 | 2878 | 2 | 5.221 | 8.99 | 8.99 | 8.99 |
| /fleet | 5 | 5482960 | 5 | 224.906 | 544.762 | 544.762 | 544.762 |
| /pull-requests | 5 | 125605 | 5 | 42.612 | 257.122 | 257.122 | 257.122 |
| /{page_path:path} | 15 | 1250255 | 15 | 32.407 | 654.511 | 654.511 | 654.511 |

## Retained slow-record names (bounded cap64; counts shown per name)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| acp.wire_append | 1 | 534.927 | 534.927 | 534.927 | 534.927 |
| agent.startup_state_read | 1 | 4379.911 | 4379.911 | 4379.911 | 4379.911 |
| dispatch.record_read | 1 | 517.019 | 517.019 | 517.019 | 517.019 |
| fleet.overview_cache_read | 4 | 670.206 | 1720.071 | 1720.071 | 1720.071 |
| fleet.overview_cache_write | 1 | 670.876 | 670.876 | 670.876 | 670.876 |
| fleet.overview_response_json | 3 | 1715.934 | 1720.365 | 1720.365 | 1720.365 |
| health.canonical_obligation | 1 | 1211.392 | 1211.392 | 1211.392 | 1211.392 |
| maintenance.run | 1 | 32521.342 | 32521.342 | 32521.342 | 32521.342 |
| notifications.present_page | 1 | 1341.615 | 1341.615 | 1341.615 | 1341.615 |
| progress.checkpoint_write | 1 | 638.9 | 638.9 | 638.9 | 638.9 |
| progress.heartbeat_write | 1 | 587.535 | 587.535 | 587.535 | 587.535 |
| progress.pending_read | 1 | 624.272 | 624.272 | 624.272 | 624.272 |
| progress.sequence_allocate | 1 | 654.688 | 654.688 | 654.688 | 654.688 |
| reconciliation.completion_evidence_read | 2 | 597.671 | 1736.544 | 1736.544 | 1736.544 |
| reconciliation.dispatch_read | 2 | 677.97 | 3455.584 | 3455.584 | 3455.584 |
| sqlite.agent_runtime_checkpoint | 1 | 606.355 | 606.355 | 606.355 | 606.355 |
| sqlite.agent_session_save | 2 | 4375.947 | 6431.58 | 6431.58 | 6431.58 |
| sqlite.card_read | 1 | 599.518 | 599.518 | 599.518 | 599.518 |
| sqlite.pr_supervisor_watch_read | 1 | 637.047 | 637.047 | 637.047 | 637.047 |
| sqlite.pr_supervisor_watch_write | 2 | 545.412 | 595.507 | 595.507 | 595.507 |
| sqlite.transcript_append | 4 | 713.87 | 1044.407 | 1044.407 | 1044.407 |
| startup.openapi | 1 | 1086.242 | 1086.242 | 1086.242 | 1086.242 |
| sync.reconcile_head | 8 | 853.233 | 4121.233 | 4121.233 | 4121.233 |
| workspace.collect_garbage | 1 | 7391.275 | 7391.275 | 7391.275 | 7391.275 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| acp | 1 | 534.927 | 534.927 | 534.927 | 534.927 |
| agent | 1 | 4379.911 | 4379.911 | 4379.911 | 4379.911 |
| dispatch | 1 | 517.019 | 517.019 | 517.019 | 517.019 |
| fleet | 8 | 1385.709 | 1720.365 | 1720.365 | 1720.365 |
| health | 1 | 1211.392 | 1211.392 | 1211.392 | 1211.392 |
| maintenance | 1 | 32521.342 | 32521.342 | 32521.342 | 32521.342 |
| notifications | 1 | 1341.615 | 1341.615 | 1341.615 | 1341.615 |
| progress | 4 | 624.272 | 654.688 | 654.688 | 654.688 |
| reconciliation | 4 | 677.97 | 3455.584 | 3455.584 | 3455.584 |
| sqlite | 11 | 637.047 | 6431.58 | 6431.58 | 6431.58 |
| startup | 1 | 1086.242 | 1086.242 | 1086.242 | 1086.242 |
| sync | 8 | 853.233 | 4121.233 | 4121.233 | 4121.233 |
| workspace | 1 | 7391.275 | 7391.275 | 7391.275 | 7391.275 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 29.398 | 162.213 | 162.213 | 162.213 | 0 | 25464 |
| /agent | 5 | 0 | 140.988 | 687.949 | 687.949 | 687.949 | 2 | 118169 |
| /api/fleet/overview | 5 | 0 | 214.176 | 381.833 | 381.833 | 381.833 | 0 | 936108 |
| /api/health | 20 | 0 | 20.869 | 47.572 | 68.829 | 68.829 | 0 | 15 |
| /api/runtime | 20 | 0 | 76.352 | 454.697 | 479.7 | 479.7 | 0 | 326301 |
| /api/status | 20 | 0 | 72.433 | 182.084 | 496.702 | 496.702 | 0 | 2824 |
| /fleet | 5 | 0 | 245.196 | 677.954 | 677.954 | 677.954 | 1 | 1096592 |
| /pull-requests | 5 | 0 | 61.948 | 275.812 | 275.812 | 275.812 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 49.97 | 175.859 | 175.859 | 175.859 | 0 | 100559 |
| /work | 5 | 0 | 36.92 | 75.011 | 75.011 | 75.011 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 23.890125 | 6.248708 | 272 | 270 | 22.2922051907517 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 32.777542 | 6.644041 | 2 | 0 | 32.53795800264925 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 123.120875 | 27.635834 | 5 | 0 | 122.03533301362768 | 0.002208980731666088 | 123.07199998758733 |
| retention_object_collection | 1/0 | 5720.045417 | 634.975542 | 12904 | 12704 | 3966.9939925079234 | 0.2978351549245417 | 4071.949368924834 |
| retention_canonical | 1/0 | 5843.449042 | 662.871041 | 12909 | 12704 | 4089.029325521551 | 0.3000441356562078 | 4195.021368912421 |
| retention_mirror | 1/0 | 28.02025 | 8.559167 | 2 | 0 | 25.704542000312358 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 5896.048417 | 678.354375 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 0.653083 | 0.505917 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 6326.441583 | 2890.057375 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1207.984459 | 685.750208 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 12454.197834 | 7865.975959 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 4256.876916 | 1904.0625 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2373.561834 | 1613.970083 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 32516.322042 | 15639.169084 | n/a | n/a | n/a | n/a | n/a |

Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projection VACUUM, missing cold content and all older residuals remain open.

Whole-card acceptance remains open. Runtime counts and slow-only observations are separate from the95GET sample window. Unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages remain.


## Preserved earlier coverage

# PR516 CI correction — 2026-10-18

Initial head841ed238 CI39116112746 failed the existing directory-walk concurrency test: it still paused `Path.glob` after the collector moved to `iglob`, so its entry latch never opened. The original test reproduces locally (one failure); this is a confirmed obsolete hook, not an unexplained timing failure. The initial selected suite omitted this existing database performance file.

The test-only correction pauses the actual iterator and yields the same orphan while preserving every two/five-second deadline and the concurrent append/history assertion. The full database performance file plus five GC path contracts now passes16 tests in8.50s under both temporary roots and workspace guard. Product and benchmark bytes are identical to the measured initial head; no performance rerun was needed. Original CI log, original test, failing local reproduction and corrected results/root audits are retained in [the correction artifact](benchmarks/systemic-gc-path-ci-correction-20261018.json). No failed-head retry was requested. New-head CI, supervisor integration, deployment and whole-card acceptance remain separate and pending; all553 findings/62 concerns remain open.

# Current GC path evidence — 2026-10-18

All **553 findings and 62 concerns remain open**. This stage adds a small path-representation repair and an expanded cold-object workload. The full JSON preserves every preceding row unchanged and links every raw stage in [the GC artifact](benchmarks/systemic-gc-paths-20261018.json). Commit, integration, activation and acceptance are separate facts; the candidate is not yet integrated or deployed at measurement time.

The path-only prototype used 12,511 retained objects and 30 alternating pairs through the actual worker runtime. Method p95 fell from 526.122 to 431.967 ms; median CPU fell from 414.587 to 306.149 ms. Maximum wall time worsened from 537.722 to 624.070 ms. All 98 real executed SQL trace statements matched. A separate bulk-fetch prototype worsened p95/max and was rejected; its original empty SQL trace is explicitly invalid evidence. The patch keeps the existing 128-path batch, publication lock, reader pins, fresh live-reference query and unlink behavior. Python semantics references: [Path.glob](https://docs.python.org/3.14/library/pathlib.html#pathlib.Path.glob), [glob.iglob](https://docs.python.org/3.14/library/glob.html#glob.iglob).

Each expanded mixed case ran once with 100 iterations, 12,511 retained cold objects, 5,000 policy-expired events and real concurrent maintenance. Both retained 9,600 ordered active events, verified every cold content hash, preserved 16 audit proofs, and recorded zero failed/timed-out/rejected primary and diagnostic operation deltas. Baseline/candidate worker warnings were 5/1, but sampled threshold violations were **9/9**. This is not performance acceptance and is not a controlled causal comparison of entire paths.

| Mixed observation | Baseline17a | Path candidate |
| --- | ---: | ---: |
| Measured workload seconds | 96.683 | 97.628 |
| GC wall ms | 10,977.326 | 10,959.770 |
| GC thread CPU ms | 1,102.903 | 903.035 |
| GC publication-lock wait ms | 5,909.966 | 6,028.540 |
| GC outer lock hold ms | 4,083.219 | 4,036.081 |
| GC observed SQLite API wall ms | 3,891.745 | 3,913.304 |
| GC API calls / rows | 12,707 / 12,511 | 12,707 / 12,511 |
| Retention wall ms | 11,287.059 | 11,290.559 |
| Total maintenance wall ms | 20,281.001 | 19,799.973 |

The baseline violations were push1, message-boundary history2, bounded sync inventory5 and maintenance1; candidate violations were reconcile2, history2, bounded inventory4 and maintenance1. `sync.object_collect` in this workload is bounded sync inventory, distinct from transcript filesystem GC. API call observations include fetch iteration, not that many SQL statements. Inclusive parent spans overlap children; non-CPU wall time is not proof of disk, SQLite busy or GIL cost. Fixture databases are about25MB/11MB versus live417MB/318MB. Journal persistence and postmeasurement cold verification add work; every 7,274-record hash chain per case and all raw samples, bytes, warnings, route/operation windows, SQL/fsync counters, sources and root audits are preserved.

Validation passed: 62 broad tests (including5 new matching/error/pin/publication contracts), 65 backup tests plus16 subtests, four native SQLite tests and two benchmark guards. The original platform-dependent unlink-error assertion failed, then was corrected to compare the original native error class/errno. Initial probe setup, one static introspection import without temporary roots, and empty-trace mistakes remain disclosed. No live state was constructed by that introspection. Production thresholds/defaults/durability were unchanged.

Actual loaded17a observation preceded these fixtures:95 sequential GETs all200, Agent619.653ms and Fleet684.100ms exceeded500ms. Server captured17a/PID47080/startup6409f032/package2e7a, config86968 and lease290 agree. The ba0 restart continuation remains queued behind this old notification; no new restart is requested. Full runtime at09:14:31Z has77 primary operations, one diagnostic operation,24 routes,23 retained slow records (cap64),23 process slow-call counters and20 primary process maxima >=500ms; workspace provisioning failed twice, cause unknown.73/181 baseline operation names and20/115 routes had positive current coverage; all unexercised branches remain open. Cached startup maintenance took33.736s, GC6.142s, first/final fresh health7.723/2.220s, projection11.220s including VACUUM6.983s, and transcript DB5.033s. Missing cold object1 and migration state remain explicit. Every operation/route/name/family is tabulated below, with process maxima separate from rolling windows; this is not a new fleet/log audit.

Remaining work includes lock/SQL/native busy/fsync/disk/GIL/GC attribution; full fresh health/integrity/VACUUM; startup schema/import/model CPU; history/drain/sync/inventory amplification; real provider/placement/workspace/active-watch branches; peers and their unknown builds/logs; dropped logs/status failures/older retention; receipts/attachments/lost raw evidence/test-root gaps and all previous unexplained CI/browser failures. Backup0331 retains its owner, missing-source question, failed eight archives/207 failures/zero verified, paused automation and independent acceptance gate. No backup/recovery/retry/restore/delete/ref-drop action was performed. Database89957 remains closed only for its accepted original scope. Normal PA scheduling owns independent final verification; integrated evidence alone cannot complete this card.

## Every actual17a runtime observation

# Actual17a runtime and ordinary GET inventory

Source: supported bound ownerAPI on macbook0c7d8ecb, source17a/PID47080/startup6409/package2e7a. Runtime snapshot UTC 2026-10-18T09:14:31.450157+00:00; each GET has its own raw UTC timestamp. Every received21section is retained with three explicit opaque privacy omissions. Older window values are not reconstructed.

Runtime counts/maxima are process totals; latency percentiles are per-operation bounded windows. Primary and diagnostic executors are separate namespaces. Slow-record percentiles are from the retained slow-only window, not all requests/operations or distinct incidents. No peer/log-rotation/fleet completeness claim.

The95GET window precedes disposable probes/tests; supplementary metadata collectors overlap each other after it. Passive host counters cover only that later ordinary GET window, not startup maintenance. Local per-sample checkpoint writes add observer work outside request timers. Decoded bytes are not wire bytes. Five-page p95=p99=max; first sample is not guaranteed cold.

## Primary executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0/0 | 1 | 60.451 | 60.451 | 60.451 | 60.451 | 60.451 |
| acp.provider_resolve | 1 | 0 | 0/0/0 | 1 | 1.727 | 1.727 | 1.727 | 1.727 | 1.727 |
| acp.wire_append | 184 | 0 | 0/0/0 | 184 | 0.997 | 68.858 | 129.777 | 129.867 | 129.867 |
| acp.wire_init | 1 | 0 | 0/0/0 | 1 | 0.837 | 0.837 | 0.837 | 0.837 | 0.837 |
| agent.project_recovery_availability | 32 | 0 | 0/0/0 | 32 | 1.606 | 51.447 | 111.677 | 111.677 | 111.677 |
| agent.prompt_compose | 1 | 0 | 0/0/0 | 1 | 28.26 | 28.26 | 28.26 | 28.26 | 28.26 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0/0 | 1 | 9.62 | 9.62 | 9.62 | 9.62 | 9.62 |
| agent.recovery_sessions | 15 | 1 | 0/0/0 | 15 | 155.566 | 526.862 | 526.862 | 526.862 | 526.862 |
| agent.session_mkdir | 1 | 0 | 0/0/0 | 1 | 2.701 | 2.701 | 2.701 | 2.701 | 2.701 |
| agent.startup_state_read | 1 | 1 | 0/0/0 | 1 | 4625.374 | 4625.374 | 4625.374 | 4625.374 | 4625.374 |
| dispatch.completion_pending_read | 20 | 0 | 0/0/0 | 20 | 1.791 | 14.388 | 51.606 | 51.606 | 51.606 |
| dispatch.followup_pending_read | 20 | 0 | 0/0/0 | 20 | 2.356 | 167.395 | 360.433 | 360.433 | 360.433 |
| dispatch.record_read | 287 | 0 | 0/0/0 | 256 | 19.88 | 40.643 | 139.617 | 429.421 | 429.421 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0/0 | 7 | 2.927 | 51.95 | 51.95 | 51.95 | 51.95 |
| filesystem.github_credentials_read | 38 | 0 | 0/0/0 | 38 | 2.414 | 54.179 | 144.76 | 144.76 | 144.76 |
| fleet.overview.reachability | 7 | 0 | 0/0/0 | 7 | 10.228 | 39.822 | 39.822 | 39.822 | 39.822 |
| fleet.overview.status | 7 | 0 | 0/0/0 | 7 | 8.218 | 269.46 | 269.46 | 269.46 | 269.46 |
| fleet.overview.sync | 7 | 2 | 0/0/0 | 7 | 17.952 | 1010.211 | 1010.211 | 1010.211 | 1010.211 |
| fleet.overview_cache_read | 298 | 1 | 0/0/0 | 256 | 4.814 | 102.245 | 208.511 | 270.209 | 602.48 |
| fleet.overview_cache_write | 84 | 1 | 0/0/0 | 84 | 23.159 | 200.886 | 1404.752 | 1404.752 | 1404.752 |
| fleet.overview_response_json | 63 | 0 | 0/0/0 | 63 | 7.538 | 64.188 | 89.864 | 89.864 | 89.864 |
| fleet.update_recovery | 1 | 0 | 0/0/0 | 1 | 10.229 | 10.229 | 10.229 | 10.229 | 10.229 |
| goal.dispatch_lifecycle_index | 5 | 0 | 0/0/0 | 5 | 1.181 | 13.34 | 13.34 | 13.34 | 13.34 |
| health.canonical_obligation | 29 | 1 | 0/0/0 | 29 | 41.887 | 305.209 | 1089.544 | 1089.544 | 1089.544 |
| http.fleet_overview | 63 | 0 | 0/0/0 | 63 | 158.509 | 2468.092 | 2656.242 | 2656.242 | 2656.242 |
| http.github | 1 | 0 | 0/0/0 | 1 | 702.767 | 702.767 | 702.767 | 702.767 | 702.767 |
| http.pr_supervisor_peer | 268 | 0 | 0/0/0 | 256 | 55.729 | 151.301 | 289.163 | 327.416 | 1067.331 |
| maintenance.run | 1 | 1 | 0/0/0 | 1 | 33739.9 | 33739.9 | 33739.9 | 33739.9 | 33739.9 |
| notifications.list_page | 2 | 0 | 0/0/0 | 2 | 4.88 | 19.101 | 19.101 | 19.101 | 19.101 |
| notifications.present_page | 2 | 0 | 0/0/0 | 2 | 300.286 | 475.825 | 475.825 | 475.825 | 475.825 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0/0 | 1 | 29.697 | 29.697 | 29.697 | 29.697 | 29.697 |
| pr_supervisor.response_json | 268 | 0 | 0/0/0 | 256 | 0.809 | 12.663 | 19.858 | 46.029 | 75.323 |
| preferences.agent_auto_approve_read | 4 | 0 | 0/0/0 | 4 | 6.37 | 66.417 | 66.417 | 66.417 | 66.417 |
| progress.checkpoint_write | 3 | 0 | 0/0/0 | 3 | 274.721 | 361.002 | 361.002 | 361.002 | 361.002 |
| progress.dispatch_read | 38 | 0 | 0/0/0 | 38 | 53.816 | 126.485 | 143.315 | 143.315 | 143.315 |
| progress.heartbeat_write | 5 | 0 | 0/0/0 | 5 | 127.119 | 260.381 | 260.381 | 260.381 | 260.381 |
| progress.pending_read | 35 | 0 | 0/0/0 | 35 | 12.217 | 55.581 | 60.206 | 60.206 | 60.206 |
| progress.sequence_allocate | 8 | 0 | 0/0/0 | 8 | 148.95 | 405.986 | 405.986 | 405.986 | 405.986 |
| reconciliation.completion_evidence_read | 57 | 0 | 0/0/0 | 57 | 4.057 | 97.126 | 173.992 | 173.992 | 173.992 |
| reconciliation.dispatch_read | 96 | 1 | 0/0/0 | 96 | 2.268 | 52.75 | 3434.565 | 3434.565 | 3434.565 |
| reconciliation.session_read | 57 | 1 | 0/0/0 | 57 | 2.828 | 131.962 | 1406.023 | 1406.023 | 1406.023 |
| selection.configuration_receipt | 1 | 0 | 0/0/0 | 1 | 48.48 | 48.48 | 48.48 | 48.48 | 48.48 |
| selection.prompt_identity | 1 | 0 | 0/0/0 | 1 | 151.444 | 151.444 | 151.444 | 151.444 | 151.444 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0/0 | 2 | 27.587 | 36.608 | 36.608 | 36.608 | 36.608 |
| sqlite.agent_session_read | 21 | 0 | 0/0/0 | 21 | 7.845 | 51.178 | 85.684 | 85.684 | 85.684 |
| sqlite.agent_session_save | 33 | 0 | 0/0/0 | 33 | 25.375 | 125.428 | 218.339 | 218.339 | 218.339 |
| sqlite.card_read | 422 | 1 | 0/0/0 | 256 | 1.58 | 3.971 | 53.486 | 57.195 | 523.153 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0/0 | 2 | 4.838 | 134.78 | 134.78 | 134.78 | 134.78 |
| sqlite.pr_supervisor_completion_due_read | 36 | 0 | 0/0/0 | 36 | 19.811 | 166.344 | 166.885 | 166.885 | 166.885 |
| sqlite.pr_supervisor_due_read | 36 | 0 | 0/0/0 | 36 | 4.197 | 71.598 | 72.404 | 72.404 | 72.404 |
| sqlite.pr_supervisor_event_write | 134 | 0 | 0/0/0 | 134 | 3.691 | 38.301 | 84.946 | 161.892 | 161.892 |
| sqlite.pr_supervisor_watch_read | 134 | 0 | 0/0/0 | 134 | 5.13 | 92.729 | 425.239 | 443.813 | 443.813 |
| sqlite.pr_supervisor_watch_write | 134 | 0 | 0/0/0 | 134 | 10.051 | 102.062 | 189.348 | 207.296 | 207.296 |
| sqlite.project_read | 294 | 0 | 0/0/0 | 256 | 0.863 | 10.398 | 56.99 | 393.218 | 393.218 |
| sqlite.repository_read | 287 | 0 | 0/0/0 | 256 | 1.111 | 28.064 | 87.729 | 140.373 | 140.373 |
| sqlite.restart_handoff_completion | 64 | 0 | 0/0/0 | 64 | 5.309 | 37.638 | 223.637 | 223.637 | 223.637 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0/0 | 1 | 10.099 | 10.099 | 10.099 | 10.099 | 10.099 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0/0 | 1 | 3.546 | 3.546 | 3.546 | 3.546 | 3.546 |
| sqlite.restart_handoff_session | 32 | 0 | 0/0/0 | 32 | 5.649 | 14.673 | 28.458 | 28.458 | 28.458 |
| sqlite.restart_handoffs_pending | 16 | 0 | 0/0/0 | 16 | 2.21 | 16.5 | 16.5 | 16.5 | 16.5 |
| sqlite.restart_handoffs_watchdog | 15 | 0 | 0/0/0 | 15 | 1.862 | 5.313 | 5.313 | 5.313 | 5.313 |
| sqlite.transcript_append | 18 | 0 | 0/0/0 | 18 | 45.155 | 319.122 | 319.122 | 319.122 | 319.122 |
| sqlite.transcript_sequence | 1 | 0 | 0/0/0 | 1 | 45.515 | 45.515 | 45.515 | 45.515 | 45.515 |
| startup.agent_manager | 1 | 0 | 0/0/0 | 1 | 4.431 | 4.431 | 4.431 | 4.431 | 4.431 |
| startup.async_backend_prepare | 1 | 0 | 0/0/0 | 1 | 7.768 | 7.768 | 7.768 | 7.768 | 7.768 |
| startup.openapi | 1 | 1 | 0/0/0 | 1 | 1159.704 | 1159.704 | 1159.704 | 1159.704 | 1159.704 |
| sync.commit_read | 12 | 0 | 0/0/0 | 12 | 2.079 | 71.009 | 71.009 | 71.009 | 71.009 |
| sync.gc_resume | 1 | 1 | 0/0/0 | 1 | 776.97 | 776.97 | 776.97 | 776.97 | 776.97 |
| sync.json_decode | 12 | 0 | 0/0/0 | 12 | 3.607 | 27.247 | 27.247 | 27.247 | 27.247 |
| sync.peer_http | 12 | 0 | 0/0/0 | 12 | 38.266 | 2398.472 | 2398.472 | 2398.472 | 2398.472 |
| sync.reconcile_head | 12 | 9 | 0/0/0 | 12 | 765.435 | 2858.107 | 2858.107 | 2858.107 | 2858.107 |
| sync.ref_read | 32 | 0 | 0/0/0 | 32 | 1.638 | 53.452 | 106.063 | 106.063 | 106.063 |
| sync.startup_reconcile | 1 | 0 | 0/0/0 | 1 | 36.326 | 36.326 | 36.326 | 36.326 | 36.326 |
| workspace.collect_garbage | 1 | 1 | 0/0/0 | 1 | 7418.404 | 7418.404 | 7418.404 | 7418.404 | 7418.404 |
| workspace.lease_renew | 1 | 0 | 0/0/0 | 1 | 101.264 | 101.264 | 101.264 | 101.264 | 101.264 |
| workspace.project_provision | 7 | 0 | 2/0/0 | 7 | 1679.427 | 2826.234 | 2826.234 | 2826.234 | 2826.234 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0/0 | 1 | 1422.572 | 1422.572 | 1422.572 | 1422.572 | 1422.572 |
## Diagnostic executor

| Operation | Submitted | Slow | Failed/timeouts/rejected | Window n | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| runtime.status_snapshot | 21 | 0 | 0/0/0 | 20 | 27.101 | 83.098 | 142.597 | 142.597 | 142.597 |

## Primary operation prefix families

Family all-call percentiles cannot be derived from per-operation percentile summaries; raw all-call samples are not exposed by this snapshot. The separate slow-only family table below has its exact declared sample scope.

| Prefix | Names | Submitted | Slow calls | Failed | Timeouts | Maximum process ms |
|---|---:|---:|---:|---:|---:|---:|
| acp | 4 | 187 | 0 | 0 | 0 | 129.867 |
| agent | 6 | 51 | 2 | 0 | 0 | 4625.374 |
| dispatch | 4 | 334 | 0 | 0 | 0 | 429.421 |
| filesystem | 1 | 38 | 0 | 0 | 0 | 144.76 |
| fleet | 7 | 467 | 4 | 0 | 0 | 1404.752 |
| goal | 1 | 5 | 0 | 0 | 0 | 13.34 |
| health | 1 | 29 | 1 | 0 | 0 | 1089.544 |
| http | 3 | 332 | 0 | 0 | 0 | 2656.242 |
| maintenance | 1 | 1 | 1 | 0 | 0 | 33739.9 |
| notifications | 2 | 4 | 0 | 0 | 0 | 475.825 |
| pr_supervisor | 2 | 269 | 0 | 0 | 0 | 75.323 |
| preferences | 1 | 4 | 0 | 0 | 0 | 66.417 |
| progress | 5 | 89 | 0 | 0 | 0 | 405.986 |
| reconciliation | 3 | 210 | 2 | 0 | 0 | 3434.565 |
| selection | 2 | 2 | 0 | 0 | 0 | 151.444 |
| sqlite | 20 | 1683 | 1 | 0 | 0 | 523.153 |
| startup | 3 | 3 | 1 | 0 | 0 | 1159.704 |
| sync | 7 | 82 | 10 | 0 | 0 | 2858.107 |
| workspace | 4 | 10 | 1 | 2 | 0 | 7418.404 |

## Registered route windows

| Route | Process count | Decoded bytes total | Window n | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 14.33 | 47.771 | 47.771 | 47.771 |
| /api/agent/observability/v1/sessions/{session_id} | 1 | 4507 | 1 | 74.843 | 74.843 | 74.843 | 74.843 |
| /api/agent/sessions/{session_id}/restart-handoffs | 2 | 499592 | 2 | 18.338 | 32.732 | 32.732 | 32.732 |
| /api/configuration | 1 | 169949 | 1 | 36.026 | 36.026 | 36.026 | 36.026 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 16.875 | 26.994 | 26.994 | 26.994 |
| /api/fleet/overview | 5 | 5021015 | 5 | 129.476 | 203.622 | 203.622 | 203.622 |
| /api/fleet/overview/local | 23 | 420084 | 23 | 91.934 | 1221.626 | 1321.706 | 1321.706 |
| /api/health | 20 | 300 | 20 | 2.108 | 3.009 | 7.268 | 7.268 |
| /api/instance | 1 | 560 | 1 | 5.04 | 5.04 | 5.04 | 5.04 |
| /api/instance/maintenance | 1 | 7448 | 1 | 5.792 | 5.792 | 5.792 | 5.792 |
| /api/notifications | 2 | 22218 | 2 | 316.819 | 520.02 | 520.02 | 520.02 |
| /api/owner-ready | 1 | 71 | 1 | 6.472 | 6.472 | 6.472 | 6.472 |
| /api/pr-supervisor/capabilities | 1 | 2707 | 1 | 12.726 | 12.726 | 12.726 | 12.726 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 17.091 | 258.848 | 258.848 | 258.848 |
| /api/pr-supervisor/replicas | 289 | 1597621 | 256 | 40.908 | 183.325 | 516.458 | 1232.649 |
| /api/ready | 23 | 2480 | 23 | 3.907 | 6.847 | 7.887 | 7.887 |
| /api/runtime | 20 | 5848648 | 20 | 36.925 | 93.219 | 159.234 | 159.234 |
| /api/status | 48 | 133512 | 48 | 67.973 | 587.719 | 617.115 | 617.115 |
| /api/sync/refs | 1 | 259 | 1 | 24.395 | 24.395 | 24.395 | 24.395 |
| /api/ui/assets | 2 | 158 | 2 | 9.391 | 11.612 | 11.612 | 11.612 |
| /api/workspaces | 1 | 1439 | 1 | 6.922 | 6.922 | 6.922 | 6.922 |
| /fleet | 5 | 5483163 | 5 | 335.425 | 661.581 | 661.581 | 661.581 |
| /pull-requests | 5 | 125605 | 5 | 34.538 | 35.699 | 35.699 | 35.699 |
| /{page_path:path} | 15 | 1249350 | 15 | 51.909 | 576.809 | 576.809 | 576.809 |

## Retained slow-record names (cap64;23retained/23processslowcalls)

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.recovery_sessions | 1 | 526.862 | 526.862 | 526.862 | 526.862 |
| agent.startup_state_read | 1 | 4625.374 | 4625.374 | 4625.374 | 4625.374 |
| fleet.overview.sync | 2 | 566.317 | 1010.211 | 1010.211 | 1010.211 |
| fleet.overview_cache_read | 1 | 602.48 | 602.48 | 602.48 | 602.48 |
| fleet.overview_cache_write | 1 | 1404.752 | 1404.752 | 1404.752 | 1404.752 |
| health.canonical_obligation | 1 | 1089.544 | 1089.544 | 1089.544 | 1089.544 |
| maintenance.run | 1 | 33739.9 | 33739.9 | 33739.9 | 33739.9 |
| reconciliation.dispatch_read | 1 | 3434.565 | 3434.565 | 3434.565 | 3434.565 |
| reconciliation.session_read | 1 | 1406.023 | 1406.023 | 1406.023 | 1406.023 |
| sqlite.card_read | 1 | 523.153 | 523.153 | 523.153 | 523.153 |
| startup.openapi | 1 | 1159.704 | 1159.704 | 1159.704 | 1159.704 |
| sync.gc_resume | 1 | 776.97 | 776.97 | 776.97 | 776.97 |
| sync.reconcile_head | 9 | 901.612 | 2858.107 | 2858.107 | 2858.107 |
| workspace.collect_garbage | 1 | 7418.404 | 7418.404 | 7418.404 | 7418.404 |

## Retained slow-record prefix families ONLY

| Name | Retained records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent | 2 | 526.862 | 4625.374 | 4625.374 | 4625.374 |
| fleet | 4 | 602.48 | 1404.752 | 1404.752 | 1404.752 |
| health | 1 | 1089.544 | 1089.544 | 1089.544 | 1089.544 |
| maintenance | 1 | 33739.9 | 33739.9 | 33739.9 | 33739.9 |
| reconciliation | 2 | 1406.023 | 3434.565 | 3434.565 | 3434.565 |
| sqlite | 1 | 523.153 | 523.153 | 523.153 | 523.153 |
| startup | 1 | 1159.704 | 1159.704 | 1159.704 | 1159.704 |
| sync | 10 | 862.007 | 2858.107 | 2858.107 | 2858.107 |
| workspace | 1 | 7418.404 | 7418.404 | 7418.404 | 7418.404 |

## Exact ordinary GET sample window

| Request | n | Errors | p50 | p95 | p99 | Max ms | >=500ms | Decoded bytes max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 0 | 39.861 | 127.048 | 127.048 | 127.048 | 0 | 25464 |
| /agent | 5 | 0 | 156.313 | 619.653 | 619.653 | 619.653 | 1 | 117988 |
| /api/fleet/overview | 5 | 0 | 194.041 | 247.214 | 247.214 | 247.214 | 0 | 1004203 |
| /api/health | 20 | 0 | 20.221 | 59.845 | 85.324 | 85.324 | 0 | 15 |
| /api/runtime | 20 | 0 | 67.522 | 144.486 | 240.086 | 240.086 | 0 | 292501 |
| /api/status | 20 | 0 | 69.401 | 188.468 | 257.351 | 257.351 | 0 | 2824 |
| /fleet | 5 | 0 | 354.307 | 684.1 | 684.1 | 684.1 | 1 | 1096636 |
| /pull-requests | 5 | 0 | 52.447 | 54.66 | 54.66 | 54.66 | 0 | 25121 |
| /settings?section=agent | 5 | 0 | 71.642 | 488.952 | 488.952 | 488.952 | 0 | 100559 |
| /work | 5 | 0 | 45.785 | 343.838 | 343.838 | 343.838 | 0 | 31323 |

## Cached maintenance inclusive phases

| Phase | Calls/errors | Wall ms | ThreadCPU ms | Observed SQLite API calls | Rows | SQLite wall ms | Lock wait ms | Lock hold ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| retention_selection | 1/0 | 27.526917 | 8.041708 | 273 | 270 | 24.74018483189866 | 0.0 | 0.0 |
| retention_discovery | 1/0 | 30.106083 | 6.510333 | 2 | 0 | 29.870833968743682 | 0.0 | 0.0 |
| retention_transaction | 1/0 | 116.854292 | 27.881041 | 5 | 0 | 115.63120904611424 | 0.001291045919060707 | 116.80629197508097 |
| retention_object_collection | 1/0 | 6141.834209 | 828.204291 | 12707 | 12511 | 3529.1482629836537 | 0.4397900775074959 | 3722.3282551276498 |
| retention_canonical | 1/0 | 6258.91025 | 856.295667 | 12712 | 12511 | 3644.779472029768 | 0.4410811234265566 | 3839.1345471027307 |
| retention_mirror | 1/0 | 48.396917 | 7.96225 | 2 | 0 | 47.396750072948635 | 0.0 | 0.0 |
| transcript_retention | 1/0 | 6335.098541 | 872.553458 | n/a | n/a | n/a | n/a | n/a |
| mutation_retention | 1/0 | 1.7265 | 0.69775 | n/a | n/a | n/a | n/a | n/a |
| transcript_migration_and_health | 1/0 | 7723.198166 | 2999.092708 | n/a | n/a | n/a | n/a | n/a |
| dispatch_compact | 1/0 | 1202.076166 | 709.376125 | n/a | n/a | n/a | n/a | n/a |
| projection_database | 1/0 | 11220.248 | 7703.021166 | n/a | n/a | n/a | n/a | n/a |
| transcript_database | 1/0 | 5032.737292 | 1977.797459 | n/a | n/a | n/a | n/a | n/a |
| transcript_health_final | 1/0 | 2220.322542 | 1641.316125 | n/a | n/a | n/a | n/a | n/a |
| total | 1/0 | 33735.88825 | 15904.278583 | n/a | n/a | n/a | n/a | n/a |

Cold-object collection6.142s/828msCPU dominates retention6.335s in this startup sweep; its12707SQLite API observations include lazy row fetches and are not12707distinct SQL statements. Path and fetch prototypes use separate disposable runs; no inferred live speedup. Busy failures0 does not prove absence of successful native busy waits. Parent spans include children; wall-minusCPU is not physical IO/GIL attribution. Full fresh checks, projectionVACUUM6.983s and missingcold1 remain.

Whole-card acceptance remains open, including73of181baseline runtime names positive here (108unobserved),20of115baseline route groups (95unobserved), unexercised branches/providers/peers/backup/log loss/older retention/receipts and all prior stages. No new operation or route names were introduced by this snapshot.


## Preserved earlier coverage

# Runtime coverage correction and CI contract validation — 2026-10-18

**552rows/61concerns remain open**, preservingall551priorrows andaddingonecollector-coverageconcern. FirstPR515head555451a5 CI39112403148 failed anexistingempty-sweep testthatassertedtheoldexactphasekeyset. Intentionalnewretention_selection/retention_canonical keysare nowaddedtotheexactexpectedset, witha parent-spancontainmentassertion; distinctfirst/finalhealth/finalizedtimings/JSON/call-errorchecks remain. All4nativeSQLtestspass. Initial57broaderchecks(including6focusedcontracts) plus4nativeSQLchecks ranseparately;65backup+16subtestspass. Productsource/wheel/29runtimehashes unchanged: no newmixedreplay orsame-headCIretry. OriginalfailedlogSHA067f3bc1 andfullfailure remain inthenumericartifact, notclassifiedasflaky.

Sourceinspectionconfirmedtheearliercollector retainedonly5of21runtime sections. The81primary-operation/24route/38slowrecord snapshotafter95GETs remainscorrectforitsdeclaredsubset; auxiliaryexecutor/namedlocktimeline/backendinit/queues/health/storage/SSE values fromthatwindow were discardedandcannotbereconstructed. Aninitialsupplement over-redacted205internal locknames plusknownmetadata; itremainsasunknowns. Acorrectedlaterbuild-fencedsnapshot at08:49:38Z retainsall21sectionswithonly3intentionalopaqueprivacyomissions(sandboxsignature/recovery/metadata):94primaryoperations,1diagnosticoperation(runtime.status_snapshot),31routes,42namedlocktimelines. Primaryprocess35maxima>=500ms/85slowcallcounters and64retainedslowrecords(cap64) arenotdistinctincidentcounts orpercentiles ofthe95GETscenario. Latercontrolplane/backgroundtraffic andourdisposablehostworkload overlap; no causalpairedcomparison.

[Everylaterprimary/diagnosticoperation,route andretainedslowname/count/p50/p95/p99/max](benchmarks/systemic-retention-phases-20261018.md) islistedwithnamespaces/windows distinct. Thenumericartifactretainseveryoriginalsnapshot/firstsupplement/correctedcapture,allcounter/phase/bytefields,3opaqueomissionpaths andbothcollector sources. Futurebuildcollectors mustuse thecorrectedschema, nottheoldfive-key helper. ProviderMCPstate ischecking/providercontextpending whileownerprobeconnected; currentproviderPA tools absentthiscatalog. Cause,duration andimpact remainunverified; supportedboundownerAPI used, no restart/reconnect/config/defaultchange.

Allprevious551-row/sourcefixture/live9e5/backup/log/peer/receipt/independentacceptance limits below remain. Newcollector source remainsuninstalled; currentloaded9e5 unchanged. Nextcorrectedhead mustpassnormaldurablesupervisor/exactheadindependentchecks/mainCI beforeanyintegration/activation. No threshold,timeout,integrity,reclamation,SQL,lock,policy oracceptance relaxation.

---

# Loaded PR514 retest and bounded retention subphases — 2026-10-18

All **551 rows remain open**: the original545rows/60concerns/140slow names/181baselineoperations/115baselineroutes/6pagefamilies/14startuprecords are preserved, with6new maintenance-subphase rows. The full numeric/raw stage is [systemic-retention-phases-20261018.json](benchmarks/systemic-retention-phases-20261018.json). [Every returned operation, route and retained slow-record name/count/percentiles/max](benchmarks/systemic-retention-phases-20261018.md) is listed separately from process maxima and the95sample window.

PR514 exactmerge9e5c7c55 is actually loaded onmacbook0c7d8ecb/PID40587/startupc24bcd97, package583f5faf, unchangedconfig86968/lease290. Protectedhead checks and exactmain39110469459 are5terminalsuccess. Originalmainfailure (missingstatus/541ms outcome_read) remainsunattributed: isolatedtest+12filetests passed and exactlyone failed-shard same-head retry passed; originalfailure/localresponses/retrylog retained. Installation verified437files/52unchangedversions/38compiledtemplates. Restart9887errornull; owncontinuationreceipt mayremainqueued whilethisturnruns, not groundsforanotherrestart.

Actual95ordinarysequentialGETs allHTTP200, butFleet1of5 reached764.512ms/1,165,099decodedHTMLbytes. WorstFleetcontext680.0wall/191.6threadCPUms; servertotal729.8/239.3; helper764.512. Other9explicitrequestvariants stayedbelow500ms inthissmallwindow. Five-pagep95=p99=max; decodedpayload isnotwirebytes/firstnotguaranteedcold. Fullruntime81operations/19prefixfamilies/24routes/38retainedslowrecords(cap64) includes24processmax>=500ms andworkspace.project_provision1failedof7, causeunknown. Currentpositiveobservations cover76of181baselineoperations and20of115baselineroutegroups;105/95remaincurrentlyunobserved, and namedobservations do notcovereverybranch. Allsixpageshell families wereexplicitlysampled; Settingsagentvariant isnotbackupsacceptance. Supplementarymetadata collectors mayoverlap afterthe95window. NativePA MCP tools absentthiscatalog; boundownerAPI used withoutfallbackcredentials.

Cachedmaintenance34.233s wall/16.229s threadCPU: retention7.414s/0.889s, migration-firsthealth5.887s/2.771s, projection8.867s/6.673s, transcriptDB5.635s/2.713s, finalfreshhealth5.568s/2.537s. Projectionintegrity2.378/0.893s/VACUUM5.578s/finalcheckpointbusy-deferred; transcriptintegrity4.498s/optimize1.134s/noVACUUM. Bothfreshhealthcontracts/missingobject1/migrationrunning retained. Parenttimingsincludechildren; wall-minusCPU isnotnativebusy/disk/GIL attribution. Passivewholehostcounterwindow retainsbufferedinterval/UTCendpointlimits, notperPAphysicalIOorpairedcausalproof.

The newcandidate exposes ONLYsixfixedname/contextlocalaggregates duringretention, in theexistingcachedmaintenancephase mapping: inclusivewall/threadCPU and observedSQL/commit/rows/busy-failure/Python-lock deltas. NoextraSQL/newlocks/per-session arrays/logs/retention/default/threshold changes. AST equality afterremovingonlyobservationcontexts (stringwhitespace normalized) andactualSQLtrace/data/audit/pin/error/context-isolation tests preservealgorithms. Exceptionsstillpropagate; servicecachedsuccessfulresult doesnotarchivepartialfailedsweep phases, a remaininglimit. Successfulnativebusy and physicalIO remainunpartitioned. Sixfocusedcontracts/57broaderchecks/65backupchecks+16subtests pass; initial2fixture-setupfailures(missingsubdirectory andnestedworkspacevalidation) preserved, withno productioncodechange forcorrection.

Oneexact437file-wheel/29runtimehash/38template mixed replay: **92.208s concurrentworkload**, 9600ordered activeevents retained,5000policy-expired events removed,16independentauditproofs,0operationfailures/timeouts/rejections and0steadyworkerwarnings. **Four500ms sample violations remain:** history1/max579.286ms; objectcollection3/max1171.593ms. Onepremeasurementstartup.openapi warning1164.356wall/1080.354CPUms andloopmax206.372ms are separate. Start–finish timestampinterval92.615s alsoincludespost-workloadorder validation aftercountersnapshot. Notperformanceacceptance orcontrolledspeedcomparison. All7224hashjournalrecords retained, including3maintenancecallerresults (notnecessarily3distinctsweeps); boundedawaitedoffloopphase export addswaiterwork. No fsync/powerloss guarantee or green-huntingrepeat.

The5000event/oneexpired-ID sourcefixture retention took185.306ms (30main session fixtures plusoneexpired retention session). Its canonicaltransaction includes74.131msPythonwriter-lockwait/41.195mshold; mirror52.942ms. Thisdoesnotattribute live7.414s or coverlargecoldGC/pin/walkbranches (sourcefixture has0coldobjects afterprune). Newspan observations:

| Inclusive source span | Wall ms | ThreadCPU ms | SQL calls | SQL ms | Commit ms | Python lock wait ms |
|---|---:|---:|---:|---:|---:|---:|
| retention_canonical | 116.693 | 23.273 | 10 | 35.283 | 5.26 | 74.131 |
| retention_discovery | 1.606 | 1.08 | 3 | 1.555 | 0.0 | 0.0 |
| retention_mirror | 52.942 | 22.589 | 8 | 31.786 | 18.038 | 0.0 |
| retention_object_collection | 1.284 | 0.436 | 0 | 0.0 | 0.0 | 0.0 |
| retention_selection | 15.575 | 3.263 | 3 | 11.422 | 0.0 | 0.0 |
| retention_transaction | 115.359 | 22.793 | 10 | 35.283 | 5.26 | 74.131 |

Currentnewcollector isnotintegrated/installed/deployed. Next: protectedreview/supervisor/exacthead+mainCI, safeexactactivation andactualphaseattribution, thenconfirmedsharedcostrepairs. ALLnativebusy/WAL/fsync/disk/GIL/GC/integrity/VACUUM/schemaCPU/history/drain/sync/objectGC/provider/placement/workspace/activewatch/peer/logloss5.64MB/statusfailures2396/olderretention/receipts/attachments/testroots/unexercisedbranches andold505DOM/507nativeTab/508CIcontradictioncauses remain. Backup0331 retainsmissing5cbb/ref1/819bytes/Sep20/nointactsource/failed355/eightarchives/207failures0verified/paused/b527independentacceptance; no backup/recovery/restore/delete/refdrop/placeholder/newworker/admission/fault action. DB89957accepted ORIGINALPR471scope staysclosed. Requirement17d034a6/independent_agent_v1 unchanged; normalPAscheduler owns finalverifier, notrepairerselfacceptance. Existing10attachments/eightoldemptylocks/allunknownreceipts andeveryearlierstage remainpreserved.

---

# PR 513 activation and cold-reference query repair — 2026-10-18

**All 545 findings and 60 concerns remain open.** The [full matrix](performance-coverage.json) preserves every previous row, owner and action. The [new evidence artifact](benchmarks/systemic-reference-index-20261018.json) contains every request, runtime family, maintenance phase, native comparison, test, sample journal and retained failure. Older checkpoints below remain historical evidence.

PR 513 is integrated as `6d2febce` from head `cea5b15c`, watch `e9c3f52a`. Independent protected-merge validation and five successful checks each on head CI 39108399380 and main CI 39108646308 are recorded separately. Immutable installation verifies 437 source files, VCS identity, 38 compiled templates, 52 unchanged dependency versions and unchanged configuration. Server capture independently proves source `6d2`, package `8003`, new PID 35604, startup `63967eed`, configuration `86968`, lease 290 and handoff `36489b59` with no error. Its continuation can remain queued while this turn runs. Installation, activation and acceptance are separate states.

The local source is macbook `0c7d8ecb`, using supported owner HTTP reads. The request window is 2026-10-18T07:51:15.516088+00:00–2026-10-18T07:51:47.972025+00:00; the full runtime snapshot is from 2026-10-18T07:51:56.418437+00:00. No peer logs/builds were read and no extra provider, backup or peer workload was injected.

All 95 sequential requests returned HTTP 200, but status (621.730ms), Fleet (697.381ms) and fleet overview (650.208ms) exceed 500ms. The Fleet outlier has context 402.2ms wall/173.4ms CPU, shell 108.2/8.5, template 51.3/50.5 and total server 564.5/235.3; helper time is 697.381ms. Sizes are decoded bytes, not wire bytes. With five page samples, p95 and p99 equal maximum. The first sample is not guaranteed cold, and this window is not acceptance.

| Request | n | p50 / p95 / p99 / max ms | >=500ms | Max decoded bytes |
|---|---:|---|---:|---:|
|/|5|29.639 / 35.134 / 35.134 / 35.134|0|25464|
|/agent|5|107.822 / 169.314 / 169.314 / 169.314|0|117631|
|/api/fleet/overview|5|163.861 / 650.208 / 650.208 / 650.208|1|936106|
|/api/health|20|21.304 / 53.954 / 180.961 / 180.961|0|15|
|/api/runtime|20|77.582 / 120.636 / 173.812 / 173.812|0|330225|
|/api/status|20|78.169 / 263.554 / 621.73 / 621.73|1|2824|
|/fleet|5|310.973 / 697.381 / 697.381 / 697.381|1|1185134|
|/pull-requests|5|52.429 / 109.106 / 109.106 / 109.106|0|25121|
|/settings?section=agent|5|56.697 / 73.379 / 73.379 / 73.379|0|100559|
|/work|5|42.984 / 461.229 / 461.229 / 461.229|0|31323|

The separate runtime inventory retains all 81 operations, 24 route groups and 47 slow observations (capacity 64). Thirty-one process maxima exceed 500ms. Workspace provisioning failed once in seven submissions; its cause remains unknown. Process counters and maxima differ from bounded rolling percentiles, operation budgets and the request window above. An initial report said 30 tails; the programmatic correction to 31 preserves the unchanged raw snapshot. Every earlier name and failure remains open.

| Operation | Submitted | Failed / timed out | Window p50 / p95 / p99 ms | Process max ms | Budget slow calls |
|---|---:|---|---|---:|---:|
|agent.startup_state_read|1|0 / 0|6224.207 / 6224.207 / 6224.207|6224.207|1|
|dispatch.completion_pending_read|38|0 / 0|1.762 / 391.299 / 595.297|595.297|1|
|dispatch.followup_pending_read|38|0 / 0|2.512 / 46.443 / 849.481|849.481|1|
|dispatch.record_read|560|0 / 0|19.29 / 39.401 / 348.713|711.908|2|
|filesystem.github_credentials_read|75|0 / 0|1.522 / 57.743 / 717.261|717.261|0|
|fleet.overview.reachability|13|0 / 0|8.815 / 1217.209 / 1217.209|1217.209|1|
|fleet.overview.status|13|0 / 0|9.075 / 1223.577 / 1223.577|1223.577|1|
|fleet.overview.sync|16|0 / 0|44.145 / 1851.481 / 1851.481|1851.481|1|
|fleet.overview_cache_read|545|0 / 0|1.474 / 9.539 / 101.867|1336.031|8|
|fleet.overview_cache_write|159|0 / 0|12.464 / 179.606 / 608.575|1198.017|3|
|fleet.overview_response_json|117|0 / 0|5.718 / 65.19 / 1319.814|1320.128|2|
|goal.dispatch_lifecycle_index|8|0 / 0|0.492 / 1216.277 / 1216.277|1216.277|1|
|health.canonical_obligation|33|0 / 0|44.383 / 890.316 / 2188.179|2188.179|3|
|http.fleet_overview|117|0 / 0|137.885 / 2604.206 / 2669.586|3786.88|0|
|http.github|1|0 / 0|659.008 / 659.008 / 659.008|659.008|0|
|http.pr_supervisor_peer|544|0 / 0|46.099 / 103.13 / 268.348|801.701|0|
|maintenance.run|1|0 / 0|36911.944 / 36911.944 / 36911.944|36911.944|1|
|progress.checkpoint_write|4|0 / 0|279.495 / 558.775 / 558.775|558.775|1|
|progress.pending_read|63|0 / 0|11.018 / 47.92 / 531.807|531.807|1|
|reconciliation.dispatch_read|186|0 / 0|1.878 / 77.863 / 1222.372|3540.773|3|
|reconciliation.session_read|111|0 / 0|2.043 / 56.027 / 776.712|1336.7|2|
|sqlite.agent_session_save|36|0 / 0|11.828 / 113.766 / 5347.708|5347.708|1|
|sqlite.pr_supervisor_watch_write|272|0 / 0|8.885 / 78.0 / 117.155|1296.594|1|
|sqlite.project_read|567|0 / 0|0.832 / 15.978 / 104.1|871.792|1|
|startup.openapi|1|0 / 0|1094.203 / 1094.203 / 1094.203|1094.203|1|
|sync.peer_http|15|0 / 0|76.177 / 2094.408 / 2094.408|2094.408|0|
|sync.reconcile_head|15|0 / 0|562.06 / 4196.046 / 4196.046|4196.046|8|
|sync.startup_reconcile|1|0 / 0|1277.43 / 1277.43 / 1277.43|1277.43|1|
|workspace.collect_garbage|1|0 / 0|9831.295 / 9831.295 / 9831.295|9831.295|1|
|workspace.project_provision|7|1 / 0|1993.277 / 2550.837 / 2550.837|2550.837|0|
|workspace.reconcile_terminal_state|1|0 / 0|1362.125 / 1362.125 / 1362.125|1362.125|0|

Cached startup maintenance still takes 36.906s wall/16.602s thread CPU: retention 10.139s, migration plus first health 7.782s, projection database 10.953s, transcript database 4.694s and final fresh health 2.401s. Projection full checks take 2.617s/0.842s around 7.404s VACUUM. The transcript full check takes 4.519s without VACUUM. Parent intervals include children; wall minus CPU does not partition disk, SQLite busy wait or GIL delay. First/final freshness, the missing object and running migration remain distinct. Passive host counters have only collection endpoints, a cumulative first row and buffered interval estimates; they do not establish PA physical I/O or causal pairing.

The candidate adds a two-line partial covering index for **event-derived** cold-reference discovery. A disposable 441,790,464-byte database contains 544,320 events, 12,096 cold rows and 6,048 distinct hashes. Thirty alternating pairs return the exact same complete hash set, including a reference with missing metadata and file. The plan changes from a table scan plus temporary DISTINCT to a covering index. Progress-handler groups fall from 1,675 to 48 (1,000 VM steps per group); query p95 falls from 646.839ms to 25.008ms and maximum from 2002.672ms to 25.048ms. All samples remain. Diagnostic results, both fresh health checks, integrity, transactions, audit, reference counts, GC, pins, backup, retention policy, durability, defaults and thresholds are preserved.

The costs remain visible: one initial index build takes 1823.978ms wall/814.097ms CPU and adds 223 pages (913,408 logical bytes). Fifteen 100-event/20-cold append pairs have p95 69.408/59.298ms. A separate five-pair-per-case WAL probe has identical hot-only frame counts, but cold batches add 3–11 frames and mixed batches add 9–12 frames per 100 events. These are logical frame/file measurements, not device writes or fsync attribution. Existing-database upgrade, idempotent reopening, pinned WAL readers, new publication, replacement, prune, missing metadata and retained quick_check pass.

**Full health is not fixed.** Six pairs have baseline/candidate p50 2052.454/2611.231ms (worse) and p95 5128.379/4565.713ms. Every wall/CPU array and alternating position is retained; no overall health speedup or live causal share is claimed. Full integrity and file checks remain slow, and the initial index upgrade can itself exceed 500ms.

Validation retains one baseline failure/two passes, then 54 storage/retention/maintenance passes, 65 backup passes plus 16 subtests, and two negative constructor-root escape passes. Both PA roots are temporary and guarded. Earlier mixed-run notes incorrectly claimed a constructor guard absent from the inspected runner/script; explicit temporary settings were present, with no evidence of live writes. The correction preserves all older evidence. The benchmark now checks roots throughout boot/lifespan and returns its audit. The initial new helper failed before importing PA because `/var` was compared with resolved `/private/var`; that failure is archived, then the path was resolved and the same wheel reverified.

Both completed mixed stages retain 9,600 ordered active events, expire 5,000 synthetic events under unchanged policy with 16 audit proofs, and have zero operation failures/timeouts/rejections. Their scale differs from the large native query fixture. The first stage takes 92.687s with three worker warnings and six violations: binding 1/max 508.004ms, push 1/max 549.508ms and object collection 4/max 1172.401ms. All 7,221 records remain. Its synchronous journal took 2086.815ms total, with a 168.302ms loop-blocking flush. That observed harness stall justified one replay with serialized, awaited off-loop persistence. The first benchmark source is reconstructed and verified against its recorded SHA `83f662bf`; original custody is not invented.

The final stage uses the same 437-file wheel, 29 matching runtime module hashes, 38 templates and workload. Each latency/error/status/decoded-byte record is hash-chained and flushed before its producer continues. Persistence adds work and has no power-loss/fsync guarantee; these are not controlled whole-system before/after comparisons. Final warnings and every threshold violation remain:

`{"warnings": {}, "violations": {"maintenance.run": 1, "/api/agent/history/session-0?limit=64&message_boundaries=true": 1, "sync.object_collect": 3}}`

| Final mixed residual | n | p50 / p95 / p99 / max ms | Violations |
|---|---:|---|---:|
|maintenance.run|1|547.201 / 547.201 / 547.201 / 547.201|1|
|/api/agent/history/session-0?limit=64&message_boundaries=true|100|185.235 / 318.032 / 490.982 / 528.934|1|
|sync.object_collect|10|279.003 / 1799.271 / 1799.271 / 1799.271|3|

The final journal verifies 7221 records. Writer wall total/max is 2257.749/27.778ms off-loop, not loop-block time. Premeasurement boot/seeding retains 2 slow calls and schema 1327.277ms; loop maximum 216.553ms is separate. Full queue/service/CPU/SQL/filesystem/GC/byte evidence remains.

Every original family remains open: shared locks/queues and native SQLite busy/WAL/fsync/disk/GIL/GC; full health, VACUUM and freshness; Kernel/OpenAPI/MCP/provider startup; transcript history/drain; sync/object collection and call amplification; Fleet/pages/payloads/nonempty active watches; placement/workspace failures and real external APIs; peer identity/build/log/retention gaps; 5.64MB dropped logs and 2,396 status-file failures; receipt/attachment/lost-raw/test-root concerns; and original 505 DOM, 507 native Tab and 508 CI contradictory-signal causes. The matrix retains all 140 slow names, 181 baseline operations, 115 baseline routes, six page shells and 14 startup records. The new row is a query concern, not an invented live operation name.

Backup card0331/session18fc/dispatchc5/fence308 retains missing cold object5cbb (one reference,819bytes,Sep20), its pending source question, failed backup355, eight archives,207failures/zeroverified, paused automation and unaccepted requirementb527. No backup retry/recovery/restore/delete/ref drop/placeholder/admission or competing worker. Database card89957's accepted originalPR471scope stays closed. Ten attachments, eight preserved empty locks and unknown receipt identities remain. Systemic requirement17d034a6/independent_agent_v1/userlocal/macbook is unchanged; the normal PA scheduler owns independent acceptance. CardActive, integrated-only, acceptedfalse. The new index is source-only and still requires protected review, exact activation and retest. No Done follows from a PR, merge, install, activation or small window.

---

# Loaded PR512 retest and remaining mirror inventory calls — 2026-10-18

**All 544 rows and 59 concerns remain open.** [Full matrix](performance-coverage.json); [every new sample, runtime family, maintenance phase, paired comparison and test result](benchmarks/systemic-mirror-inventory-20261018.json). Earlier checkpoints below remain historical evidence.

PR512 is protected-integrated as `5fb32b7a` from exact `d439dca0` / watch `43ef5d07`; head and main CI each have five terminal successes and matching source/merge trees. Immutable installation verifies 437 source files, 38 compiled templates and 52 unchanged dependency versions/configuration. Server capture independently proves the exact source/package, new PID31674/startup411d, unchanged configuration/lease290 and handoff8a with no error. Its own continuation remains queued while running; no duplicate restart or repairer acceptance.

All 95 ordinary sequential reads returned HTTP200, but status593.225ms and Fleet675.973ms exceed500ms. Fleet context is478.0ms wall/462.1ms CPU, template46.5/42.6 and server530.0/510.2; helper time includes more than server processing. Status has no Server-Timing. Every status/error/correlation/decoded-byte/raw timing remains. Decoded bytes are not wire bytes, five-page p95/p99 equal maximum, and the first request is not guaranteed cold. Small samples are not acceptance.

| Ordinary request | n | p50 / p95 / p99 / max ms | >=500ms | max decoded bytes |
|---|---:|---|---:|---:|
|/|5|40.685/129.917/129.917/129.917|0|25464|
|/agent|5|110.598/276.458/276.458/276.458|0|117450|
|/api/fleet/overview|5|169.355/270.104/270.104/270.104|0|936113|
|/api/health|20|20.083/58.523/74.182/74.182|0|15|
|/api/runtime|20|80.923/137.664/409.093/409.093|0|324678|
|/api/status|20|75.907/175.598/593.225/593.225|1|2824|
|/fleet|5|367.734/675.973/675.973/675.973|1|1096577|
|/pull-requests|5|154.897/410.634/410.634/410.634|0|25121|
|/settings?section=agent|5|49.524/66.223/66.223/66.223|0|100559|
|/work|5|40.639/55.097/55.097/55.097|0|31323|

The separate runtime inventory retains every one of81operations,24routes and40slow records(capacity64). Twenty-nine process maxima exceed500ms; workspace provisioning failed1of7 times with cause unknown. Process counters/maxima differ from bounded rolling percentiles and the95request window; slow-call budgets vary. All previously observed names and failures remain, including names absent from this particular process window. No exhaustive peer/log/retention audit is claimed.

| Operation | submitted | failed / timed out | window p50 / p95 / p99 ms | process max ms | budget slow calls |
|---|---:|---|---|---:|---:|
|agent.startup_state_read|1|0/0|7451.087/7451.087/7451.087|7451.087|1|
|dispatch.completion_pending_read|41|0/0|2.024/70.699/1208.746|1208.746|1|
|dispatch.record_read|586|0/0|19.581/37.117/130.199|574.838|3|
|fleet.overview.reachability|13|0/0|17.643/1199.017/1199.017|1199.017|1|
|fleet.overview.status|13|0/0|16.026/1212.77/1212.77|1212.77|1|
|fleet.overview.sync|27|0/0|29.471/183.998/1267.919|1267.919|1|
|fleet.overview_cache_read|564|0/0|1.639/34.042/59.461|2292.71|5|
|fleet.overview_cache_write|170|0/0|14.225/78.01/108.977|2295.209|1|
|goal.dispatch_lifecycle_index|8|0/0|0.421/1179.091/1179.091|1179.091|1|
|health.canonical_obligation|35|0/0|34.402/346.968/1053.501|1053.501|1|
|http.fleet_overview|117|0/0|202.663/3843.898/3902.186|3941.028|0|
|http.github|1|0/0|772.497/772.497/772.497|772.497|0|
|http.pr_supervisor_peer|568|0/0|49.197/145.489/369.222|708.175|0|
|maintenance.run|1|0/0|34030.099/34030.099/34030.099|34030.099|1|
|notifications.present_page|4|0/0|245.178/967.959/967.959|967.959|2|
|progress.checkpoint_write|3|0/0|302.178/609.836/609.836|609.836|1|
|progress.pending_read|68|0/0|10.18/45.313/644.132|644.132|1|
|reconciliation.completion_evidence_read|117|0/0|4.312/87.416/110.411|2300.799|1|
|reconciliation.dispatch_read|196|0/0|2.189/26.941/1173.742|3407.086|2|
|sqlite.agent_session_save|38|0/0|16.314/133.308/4542.67|4542.67|1|
|sqlite.card_read|871|0/0|1.814/22.527/68.798|505.978|1|
|sqlite.pr_supervisor_watch_write|284|0/0|9.109/92.472/157.368|636.188|1|
|startup.openapi|1|0/0|1129.183/1129.183/1129.183|1129.183|1|
|sync.gc_resume|1|0/0|1188.701/1188.701/1188.701|1188.701|1|
|sync.peer_http|15|0/0|58.53/2698.159/2698.159|2698.159|0|
|sync.reconcile_head|15|0/0|759.668/2686.671/2686.671|2686.671|10|
|workspace.collect_garbage|1|0/0|6534.679/6534.679/6534.679|6534.679|1|
|workspace.project_provision|7|1/0|2102.32/2550.52/2550.52|2550.52|0|
|workspace.reconcile_terminal_state|1|0/0|1419.415/1419.415/1419.415|1419.415|0|

Cached startup maintenance remains34.027s wall/15.941s threadCPU: retention5.885s, migration/first health8.543s, projection10.347s, transcript database4.504s and final fresh health3.472s. Projection full checks2.447s/0.891s surround6.979s VACUUM; final checkpoint returns busy/deferred. Transcript full check4.498s runs without VACUUM. Parent intervals include children; wall minus CPU is not a disk/busy/GIL partition. These independent build snapshots do not prove a controlled retention speedup. Integrity, freshness, reclamation and error/stop/default semantics remain required.

A passive whole-host iostat capture overlapped the already planned reads:26 raw rows, first cumulative and later interval estimates. Later rows show48–85%CPU idle and0.59–60.46MB/s disk activity. Only collection endpoints have timestamps; buffering prevents exact row pairing. These aggregates neither identify PA physicalI/O/fsync/busy/GIL costs nor prove a cause for either request outlier. No extra PA workload was injected.

The next source change discovers nonempty compatibility mirrors once **after** canonical pruning. Repeated empty sessions avoid canonical hydration and per-session reads. Selected sessions retain the original fresh canonical sequence read, per-session empty check and exact deletion. Existing canonical transaction/audit/reference/GC/pin behavior, health, backup and retention policy are unchanged. Publication after an empty mirror snapshot survives; this does not invent cross-database atomicity. No new writer transaction or lock is added.

Two new contracts fail on the original source while49tests pass. All51candidate retention/mirror/storage/maintenance contracts then pass, including prior rollback, audit, publication and empty-mirror passivity. All65isolated backup tests and16subtests pass209.98s; exact-wheel mixed evidence is retained in full below. Both PA roots are temporary and workspace construction is guarded. Initial probe setup and generator failures occurred before measurement and remain recorded. Direct-call and actual-worker connection-scope comparisons are separate; connection entries are logical and scoped workers reuse handles. Probe SQL traces start after connection acquisition and exclude setup PRAGMAs before that boundary. A one-second native sample of only the disposable direct-caller process retains six stack samples in SQLite preparation/schema/WAL open-close paths; it perturbs that comparison and neither quantifies their total share nor attributes live PA stalls. Host tests/probes overlap, so these are not controlled whole-system speed estimates or sole-cause attribution of live5.885s retention.

| Direct caller: empty IDs | baseline p50/p95/p99/max ms | candidate p50/p95/p99/max ms |
|---|---|---|
|1|4.514/5.219/6.138/6.138|2.659/3.222/3.288/3.288|
|1000|2362.367/2773.401/3261.173/3261.173|9.887/13.129/14.119/14.119|
|6000|15333.555/22794.811/24234.189/24234.189|46.37/91.898/99.422/99.422|

| Actual worker connection scope: empty IDs | baseline p50/p95/p99/max ms | candidate p50/p95/p99/max ms |
|---|---|---|
|1|4.375/7.019/7.645/7.645|4.112/6.239/9.945/9.945|
|1000|140.497/168.55/173.525/173.525|12.624/17.214/18.588/18.588|
|6000|788.757/993.387/1112.725/1112.725|52.45/66.516/132.429/132.429|

The first exact-wheel mixed scenario overlapped our disposable tests/probes:130.398s,18worker warnings,27threshold violations. Every raw sample and log remains. After all our competing test/probe processes finished, one repeat of the unchanged exact wheel/workload was run; ordinary live PA/host activity remained uncontrolled. This is a separate scenario, not a causal speed comparison.

The final exact-wheel mixed scenario keeps9600ordered active events and applies the unchanged policy to5000expired events with16audit proofs, with zero operation failures/timeouts/rejections. It has one expired session; the6000empty-ID benefit is measured separately. Warnings and all threshold violations remain acceptance failures: `{"warnings": {"Slow off-loop operation": 1}, "threshold_violations": {"maintenance.run": 1, "/api/agent/history/session-0?limit=64&message_boundaries=true": 2, "sync.object_collect": 3}}`. Full raw distributions/bytes/SQL/CPU/loop/GC/locks/logical filesystem counts, bootstrap delays and source hashes remain in the artifact; no outlier is dropped. This candidate is not integrated, installed or accepted.

| Remaining mixed path | n | p50 / p95 / p99 / max ms | violations |
|---|---:|---|---:|
|maintenance.run|1|602.057/602.057/602.057/602.057|1|
|/api/agent/history/session-0?limit=64&message_boundaries=true|100|175.499/384.88/514.154/593.76|2|
|sync.object_collect|10|311.059/1202.495/1202.495/1202.495|3|

First-stage premeasurement retains schema1379.516ms wall/1047.195ms threadCPU and one slow call; loopmax433.508ms is separate. Final-stage bootstrap/loop details are preserved independently in the artifact. All earlier failures/outliers remain.

All140slow names/181baseline operations/115baseline routes/6shells/14startup records retain owners and next actions. Full Kernel/OpenAPI/MCP/provider startup, history/drain/sync/objectGC, native busy/WAL/fsync/disk/GIL/GC/full health/VACUUM, placement/workspace/active-watch/peer paths, dropped logs/status failures/older retention/receipts/lost raw/test-root gaps and original CI causes remain. No two-cause blanket closure.

Backup0331/session18fc owns capture/verification, missingcold5cbb/pending source question, failed355/eight archives/207failures0verified/paused/b527 acceptance. No backup retry/recovery/restore/delete/refdrop/placeholder/new worker/admission. DB89957's accepted originalPR471scope stays closed. All10attachments remain. Systemic17d034a6/independent_agent_v1 is unchanged, Active/integrated-only/acceptedfalse; the normal PA scheduler owns independent acceptance. No closure follows from PR, merge, installation, activation or these samples.

---

# Loaded maintenance phases and retention inventory repair — 2026-10-18

**All 544 evidence rows / 59 concerns remain open.** [Full matrix](performance-coverage.json); [all new samples, runtime/maintenance phases, source comparison, mixed fixture and test evidence](benchmarks/systemic-prune-inventory-20261018.json). Earlier checkpoints below remain retained history.

PR511 is protected-integrated as e6b88f02 from exact e76c340a/watch04868197, with matching trees and five successful head and main checks each. Exact immutable installation verifies437sourcefiles/VCS,38compiledtemplates and52unchangedversions/config. Server capture now proves sourcee6b8/package66e89/PID28400/startupddfb. Installed and loaded proofs remain distinct; no duplicate restart or production fault.

The 95 ordinary sequential GETs all return200, but **three exceed500ms**: Fleet twice(max1631.240ms) and PullRequests once(530.939ms). Every sample/byte/Server-Timing/correlation is retained. Incorrect shorthand flags failed before requests; corrected observer socket-sandbox failure is separately archived; native-approved identical rerun completed. With five page samples, p95 and p99 equal the maximum. Payload sizes are decoded bytes, not wire bytes; the first request is not guaranteed cold. This is not whole-system acceptance.

| Ordinary request | n | p50/p95/p99/max ms | >=500ms | decoded max bytes |
|---|---:|---|---:|---:|
|/|5|33.567/35.117/35.117/35.117|0|25464|
|/agent|5|137.908/372.821/372.821/372.821|0|117270|
|/api/fleet/overview|5|171.05/361.149/361.149/361.149|0|936114|
|/api/health|20|20.786/47.043/73.441/73.441|0|15|
|/api/runtime|20|80.72/217.849/224.781/224.781|0|329735|
|/api/status|20|88.036/173.445/242.653/242.653|0|2824|
|/fleet|5|478.079/1631.24/1631.24/1631.24|2|1096586|
|/pull-requests|5|74.035/530.939/530.939/530.939|1|25121|
|/settings?section=agent|5|35.95/37.142/37.142/37.142|0|100559|
|/work|5|34.784/79.148/79.148/79.148|0|31323|

The separate runtime snapshot retains all 81 operations, 25 route groups and 47 slow worker records (capacity 64):28 operation process maxima exceed500ms and workspace provision failed 1 of 7 times (cause unknown). Processmaxima differ from bounded rolling percentiles and the95GET sample; budgets differ byoperation. All returned names/counters/bytes/phases and the new cachedmaintenance route are retained. No peer/log/retention audit or fleetcausalclaim.

| Operation | submissions | failures/timeouts | window p50/p95/p99 ms | processmax ms | budget slow calls |
|---|---:|---|---|---:|---:|
|agent.startup_state_read|1|0/0|7571.921/7571.921/7571.921|7571.921|1|
|dispatch.completion_pending_read|43|0/0|1.84/71.149/1189.167|1189.167|1|
|filesystem.github_credentials_read|81|0/0|1.931/76.679/575.648|575.648|0|
|fleet.overview.reachability|14|0/0|8.13/1188.01/1188.01|1188.01|1|
|fleet.overview.sync|30|0/0|33.493/617.161/698.138|698.138|2|
|fleet.overview_cache_read|606|0/0|1.615/16.802/35.368|1902.92|7|
|fleet.overview_cache_write|184|0/0|12.393/131.888/636.088|1224.114|3|
|fleet.overview_response_json|126|0/0|4.883/224.722/1876.953|1879.535|3|
|goal.dispatch_lifecycle_index|9|0/0|0.502/1187.757/1187.757|1187.757|1|
|health.canonical_obligation|35|0/0|56.09/993.583/1810.539|1810.539|3|
|http.fleet_overview|126|0/0|137.348/1907.032/2645.502|3698.01|0|
|http.github|1|0/0|717.587/717.587/717.587|717.587|0|
|http.pr_supervisor_peer|616|0/0|48.42/141.374/499.75|1805.017|0|
|maintenance.run|1|0/0|36411.21/36411.21/36411.21|36411.21|1|
|notifications.present_page|4|0/0|202.582/916.142/916.142|916.142|1|
|progress.dispatch_read|87|0/0|49.248/81.841/539.195|539.195|1|
|progress.sequence_allocate|26|0/0|126.188/561.298/577.048|577.048|2|
|reconciliation.completion_evidence_read|123|0/0|3.967/90.131/170.838|592.319|1|
|reconciliation.dispatch_read|206|0/0|2.093/49.912/1041.953|2354.865|3|
|sqlite.agent_session_save|49|0/0|21.324/247.829/6372.777|6372.777|1|
|sqlite.card_read|946|0/0|1.604/5.857/51.997|999.963|1|
|sqlite.pr_supervisor_watch_write|308|0/0|9.574/98.503/218.333|572.367|1|
|startup.openapi|1|0/0|1225.396/1225.396/1225.396|1225.396|1|
|sync.peer_http|15|0/0|81.674/2622.361/2622.361|2622.361|0|
|sync.reconcile_head|15|0/0|776.186/4152.063/4152.063|4152.063|11|
|workspace.collect_garbage|1|0/0|9180.012/9180.012/9180.012|9180.012|1|
|workspace.project_provision|7|1/0|1945.044/2463.011/2463.011|2463.011|0|
|workspace.reconcile_terminal_state|1|0/0|1586.514/1586.514/1586.514|1586.514|0|

Cached startup maintenance exposes36.396s wall/15.842s threadCPU: transcript retention9.271s, migration-and-first-health8.601s, projection maintenance9.923s, transcript database4.462s, finalfreshhealth2.969s. Projection fullchecks2.480s/0.922s surround6.490s VACUUM; finalcheckpoint returnsbusy/deferred aftercompaction. Transcript fullcheck4.450s withoutVACUUM. Parent phases include children; wall-minusCPU does not identify disk/busy/GIL time. Integrity/freshness/busy/error/stop/default semantics remain unchanged. Instrumentation clarifies costs and does not meet500ms.

A scoped candidate targets confirmed retention call amplification: repeated empty expired IDs each query existence under the publication lock. One indexed discovery with a boundJSON identity array and first-occurrence order retains the same publication fence, explicitBEGIN for bothauditmodes, audit multiplicity, positive-only grouped reference release, rollback and unchanged GC invocation/reader pins. Compatibility-mirror cleanup, health freshness, backup capture/verification and retention policy are unchanged.

Thirty alternating direct-method samples pervariant/count preserve an unaffected live event. At6000emptyexpiredIDs, trace statements6002→3 andp95wall118.636→22.463ms; candidate max126.373ms remains an outlier. At1000IDs,p9522.676→7.125ms; at1ID, JSON overheadp953.989→4.589ms. Initial unchanged-versus-unchanged calibration is retained. These isolated results do not explain the full9.271s live retention phase or establish physicalI/O/whole-workload improvement. Host activity is uncontrolled.

Validation: 49 retention, mirror, maintenance and storage tests pass; 14 final contracts pass after strengthening the deterministic cross-thread publication-lock assertion (product unchanged). All 65 backup copy and surface tests plus 16 subtests pass. Both audit modes and earlier-session rollback, duplicate/shared references, deferred GC, empty-input behavior, publication after discovery and mirror/audit remain covered. All test/probe roots are temporary and guarded; no live backup, fault, provider or peer workload. Source repair is not installed or accepted.

The exact 437-file wheel (29 matching module hashes, 38 compiled templates) completed 100 mixed iterations in88.083s: 9600 active events remain ordered, 5000 policy-expired events are deleted with16 independent audit proofs, and operation failures/timeouts/rejections are zero. **Five worker warnings and ten sampled threshold violations remain:** sync push1/max677.818ms, transcript drain3/max609.016ms, maintenance1/500.635ms, object collection5/max1969.437ms. Three premeasurement slow records remain separate, including schema1062.740ms/1033.653ms CPU and sync-ref completion delivery1146.394ms with0.470ms worker service. Loopmax202.556ms is a separate metric. This synthetic scenario is not whole-PA acceptance or a controlled before/after speed comparison; the many-empty-session input is exercised by the separate6000-ID fixture. Every earlier stage/outlier remains.

Backup0331/session18fc/c5/fence308 retains capture/verification/missingcold5cbb/failed355/8archives/207failures0verified/paused/pendingquestion/b527 obligations. Existing coordination key systemic-backup-e6b8-loaded-prune-inventory-window-20261018-v1 acceptedQUEUED promptcf318a13, notclaimedconsumed. No competing worker/admission/retry/recovery/restore/delete/refdrop/placeholder. DB89957originalaccepted scopeclosed. Normal PA scheduler owns systemic independent acceptance under unchanged17d034a6. Every original operation/route/page/startup concern, peer/logloss/statusfailure/olderretention/receipt/unexercisedbranch gap and all10attachments remain. No blanketclosure from source/merge/install/activation/smallsamples.

---

# Corrected build activation and bounded maintenance phase evidence — 2026-10-18

**All543 evidence rows /59 concerns remain open.** [Full matrix](performance-coverage.json); [all new samples, phase results, parity and retained failures](benchmarks/systemic-maintenance-phases-20261018.json). Older checkpoints below remain historical; this stage supersedes their current-build labels.

PR510 is protected-integrated asaa58 from exact5ec/watch27587faf with five head and five main checks successful and matching trees. Server-capturedaa58/packagec0f2/PID24980/startup08bea/config86968/lease290 proves actual activation; installed437files/38templates/52unchangedversions was verified separately. The original rollback counterexample was independently repeated by the backup owner against exactaa58: both audit modes now preserve events/refcounts/evidence/bytes after failure. This is narrow source verification, not production fault injection or backup/performance acceptance. It does not explain the older missing cold object or failed backup.

The95 ordinary sequential GETs from06:35:56.702757 to06:36:26.928613Z return95HTTP200, zero errors and zero>=500ms violations. Every sample is retained, including decoded response bytes and Server-Timing when present. The initial socket-sandbox failure is archived separately and the native-approved identical rerun passed. Five page samples givep95/p99=max; these are not guaranteed cold or whole-system acceptance.

| Ordinary request | n | p50/p95/p99/max ms | decoded payload max bytes |
|---|---:|---|---:|
|/|5|28.442/65.746/65.746/65.746|25464|
|/agent|5|205.963/440.868/440.868/440.868|117088|
|/api/fleet/overview|5|127.61/279.279/279.279/279.279|993308|
|/api/health|20|17.977/61.737/156.418/156.418|15|
|/api/runtime|20|64.693/153.735/223.867/223.867|307014|
|/api/status|20|69.057/206.154/400.832/400.832|2824|
|/fleet|5|199.045/372.792/372.792/372.792|1156303|
|/pull-requests|5|110.155/127.967/127.967/127.967|25121|
|/settings?section=agent|5|29.795/43.697/43.697/43.697|100559|
|/work|5|35.014/41.825/41.825/41.825|31323|

The separate current-process snapshot at06:36:47.402094Z retains all77operations,22routegroups and39slow worker records(capacity64). **29operations still have processmax>=500ms**, and workspace provision failed1of7 times (underlyingcauseunknown). Earlier806b and all source fixture outliers remain. Budgets differ by operation; slow_calls is not a universal500ms count. Maxima and bounded-window percentiles are distinct and cannot be added or paired by assumption.

| Operation | submissions | failed/timeouts | window p50/p95/p99 ms | processmax ms | budget slow calls |
|---|---:|---|---|---:|---:|
|agent.startup_state_read|1|0/0|6378.905/6378.905/6378.905|6378.905|1|
|dispatch.completion_pending_read|23|0/0|1.604/55.119/1182.275|1182.275|1|
|dispatch.followup_pending_read|23|0/0|2.325/68.872/519.667|519.667|1|
|fleet.overview.reachability|7|0/0|18.948/1183.024/1183.024|1183.024|1|
|fleet.overview.status|7|0/0|14.625/1164.889/1164.889|1164.889|1|
|fleet.overview.sync|18|0/0|27.91/528.804/528.804|528.804|1|
|fleet.overview_cache_read|309|0/0|1.929/21.584/53.303|1466.433|5|
|fleet.overview_cache_write|95|0/0|15.19/79.81/940.639|940.639|3|
|goal.dispatch_lifecycle_index|5|0/0|0.511/1181.377/1181.377|1181.377|1|
|health.canonical_obligation|29|0/0|38.427/292.82/1182.266|1182.266|1|
|http.fleet_overview|63|0/0|208.231/2210.981/3263.874|3263.874|0|
|http.github|1|0/0|533.689/533.689/533.689|533.689|0|
|http.pr_supervisor_peer|320|0/0|50.879/154.376/463.176|709.781|0|
|maintenance.run|1|0/0|31301.278/31301.278/31301.278|31301.278|1|
|notifications.present_page|2|0/0|154.627/740.612/740.612|740.612|1|
|progress.checkpoint_write|7|0/0|197.824/602.656/602.656|602.656|1|
|progress.heartbeat_write|8|0/0|141.453/537.607/537.607|537.607|1|
|progress.pending_read|40|0/0|9.908/111.317/1193.19|1193.19|1|
|reconciliation.completion_evidence_read|66|0/0|4.365/92.878/1482.483|1482.483|1|
|reconciliation.dispatch_read|111|0/0|2.443/53.994/1182.923|1969.732|3|
|sqlite.agent_session_save|36|0/0|12.93/507.059/3067.029|3067.029|2|
|sqlite.pr_supervisor_watch_write|160|0/0|8.116/89.943/221.447|1008.597|1|
|sqlite.transcript_append|39|0/0|14.573/376.582/5344.012|5344.012|1|
|startup.openapi|1|0/0|929.72/929.72/929.72|929.72|1|
|sync.peer_http|12|0/0|61.061/2149.111/2149.111|2149.111|0|
|sync.reconcile_head|12|0/0|613.144/2498.134/2498.134|2498.134|8|
|workspace.collect_garbage|1|0/0|7305.764/7305.764/7305.764|7305.764|1|
|workspace.project_provision|7|1/0|1449.159/2707.979/2707.979|2707.979|0|
|workspace.reconcile_terminal_state|1|0/0|898.757/898.757/898.757|898.757|0|

Startup-state reading is6.379s total/5.246s worker/1.131s completion delivery/501ms CPU/4.923s observed SQL,365rows/3calls. Maintenance is31.301s total/12.829s CPU/25.492s observedSQL/4.798s outerhold. Transcript append processmax5.344s remains. These phases are not a physical disk/busy/GIL partition and maxima are independent windows. The artifact retains every route/count/byte distribution, including four current route groups with>=500ms window maxima, beyond the95request retest.

The candidate source adds fixed internal phase call/error counts and wall/threadCPU to maintenance diagnostics: connection/close, before/after integrity, optimize/checkpoint, size reads, disk-space check, VACUUM, inclusive total, and sweep retention/migration-with-health/database/final-health work. It adds no SQL or health reads and preserves the two distinct freshness points. No busy timeout, stop/error/retry, integrity/reclamation, warning/deadline/default or backup policy change. Escaping fatal exceptions still propagate; their private map is not a durable failure journal. Native returned-busy rows need not increment exception counts.

Validation:23initial contracts pass; broad run111tests+16subtests pass with one new fixture setup failure before product code (missing temporarydata parent), preserved in full. Corrected fixture then passes all24focused contracts, including publication between first/final health reads. Six disposable native cases preserve exact prior SQL traces, results (excluding added diagnostics) and retained row hashes across compaction, no-free-pages, busy writer, low disk space, stopping and interrupted VACUUM.

Four declared529311-row307/394MB scale cases retain full integrity/status/counts and expose remaining cost: no-compaction quick checks602/788ms; small-freelist VACUUM9382/9753ms and total11991/13143ms. They overlap broad-test host activity, are sequential/nonrandomized, and do not establish a speed regression, controlled before/after improvement, exact live layout or physicalI/O. Instrumentation adds overhead and does not fix full-check/rewrite complexity. All earlier scale/mixed/page failures remain. Native busy/GIL/disk and fullforeground contention attribution remain open.

This source instrumentation is unmerged and uninstalled at publication. The already accepted e7dd managed handoff continuation is queued behind this old509notice; no duplicate restart and no next activation until its terminal delivery is verified. Every provider/placement/workspace/peer/log-loss/older-retention/receipt/unexercised-branch criterion remains open. Backup0331/18fc/b527 and its missing-source question, eight archives,207failures/0verified and paused automatic attempts retain their separate ownership. Normal PA scheduler owns independent systemic acceptance under unchanged requirement17d034a6.

---

# Non-audit transcript rollback correction — 2026-10-18

**All543 evidence rows /59 concerns remain open.** [Full matrix](performance-coverage.json); [correction, original failures, tests and maintenance overlay](benchmarks/systemic-prune-rollback-20261018.json). This checkpoint supersedes older current-stage and blanket rollback claims below; all original artifacts remain intact.

The backup owner reported a real regression in PR508/806b. We independently reproduced it against the inherited c6be source: with `keep_audit=False`, a failing event deletion leaves all events but changes the shared cold reference count2→0 (or3→2 after an earlier session). The original rollback test covered only the default audit-enabled branch. Python sqlite legacy transaction control does not implicitly begin a transaction for the leading `WITH … UPDATE`; rollback therefore could not undo the first decrement. Two non-audit tests fail before correction; nine tests pass. No live fault or subsequent garbage collection was performed. Possible later collection of still-referenced bytes is an inference, not an observed deletion, and this regression does not establish the cause of the older missing cold object or failed backup.

The source correction explicitly begins a deferred transaction before the first mutation, preserving the existing commit/rollback wrapper and all retention/reader/GC semantics. Four rollback combinations cover both audit modes and failure after an earlier session, then verify retry counts, audit evidence and immutable bytes. The broader isolated storage/connection/mirror/backup copy/pin/retention/deadline/maintenance suite passes110 tests and16 subtests in89.09s, including all four rollback cases. This is a correctness repair; no latency pass follows from it.

PR509 is already integrated as c6be with exact head/main CI5/5 success and matching trees, but remains unactivated. The prior806b handoff780c is delivered. Live source806b/packagef72/PID19594/config86968/lease290 remain verified. The unrequested c6be activation is held; a protected corrected integration must become the next target. No production rollback, backup, restore, deletion, recovery, default or permission change.

The previously durable maintenance overlay is now included: distinct first/final health freshness must remain; synthetic native529311-row307/394MB cases retain full VACUUM5.46/9.62s and no-compaction quick-check609/749ms. These are declared disposable scale observations, not exact live attribution or physical I/O. Deployed Fleet524ms, startup3.84s, maintenance24.8s, source mixed drain552/push640/object1469 and all older failures/coverage gaps remain open. Backup0331/b527 and independent systemic requirement17d034a6 retain their owners and acceptance gates.

---

# Activated806b retest, complete current runtime and startup diagnostic inventory — 2026-10-18

**All540 evidence rows /56 concerns remain open.** [Full matrix](performance-coverage.json); [all raw numeric samples, phases, source identities and retained failures](benchmarks/systemic-startup-inventory-20261018.json). This checkpoint supersedes older current-status labels below; every earlier finding, owner and outlier remains.

PR508 is protected-integrated as806b from exactc4f8/watch977033e5. Head/main CI each have five terminal successes and matching trees; the original85be workflow/job discrepancy later converged but its cause and both earlier browser failures remain unresolved. Normal immutable installation preserved52 dependency versions/configuration. The managed restart resumed this exact session/workspace; server-captured806b/packagef72/PID19594/startup70723695/config86968/lease290 prove activation. Installed metadata was checked separately. No duplicate merge/restart, backup action or repairer acceptance.

The95 ordinary sequential authenticated requests from06:03:23.046482 to06:03:54.283755Z return95HTTP200/zero errors but retain **one Fleet524.197ms violation**. Fleet context436.4ms wall/156.6ms threadCPU, template35.5/30.1, shell3.1/3.1, total477.3/192.1; full helper time524.197 includes transport/read/decode. These phases do not partition all waiting/nativeI/O. Correlation32a44450-a5be-4434-adcb-9025ae5faa77. Every sample, including this outlier, is retained. Five-sample tails equalmax; firstafteractivation is not guaranteed cold; decoded bytes are notwirebytes. Earlier passing5024 and failing134044/a90/b808 windows remain separate.

| Ordinary request | n | p50/p95/p99/max ms | >=500ms |
|---|---:|---|---:|
|/|5|28.114/34.358/34.358/34.358|0|
|/agent|5|116.251/150.238/150.238/150.238|0|
|/api/fleet/overview|5|156.963/191.452/191.452/191.452|0|
|/api/health|20|20.835/58.83/340.56/340.56|0|
|/api/runtime|20|72.068/134.786/165.027/165.027|0|
|/api/status|20|66.751/99.486/421.806/421.806|0|
|/fleet|5|370.704/524.197/524.197/524.197|1|
|/pull-requests|5|41.413/263.437/263.437/263.437|0|
|/settings?section=agent|5|37.369/64.341/64.341/64.341|0|
|/work|5|68.92/304.854/304.854/304.854|0|

The complete owner runtime snapshot retains82operations and25registeredroutegroups, all counts/response bytes, process maxima, bounded distributions and28retained slow worker records. It has25operations with processmax>=500ms and one failed workspace provision/7submissions (underlyingcauseunverified). Budget-specific warning counts are not universal500ms counts. Live/background activity and disposable work shared the host. No peer-log, older retention, exhaustive fleet or controlled acceptance claim. The table lists every current operation with a process tail; the artifact retains all82operations and all25routes. Window percentiles and lifetime process maxima differ.

| Current runtime operation | submitted | failures/timeouts | window p50/p95/p99 ms | processmax ms | budget slow calls |
|---|---:|---|---|---:|---:|
|acp.wire_append|1762|0/0|0.994/51.277/100.836|1172.478|1|
|agent.startup_state_read|1|0/0|3837.558/3837.558/3837.558|3837.558|1|
|fleet.overview_cache_read|818|0/0|1.916/42.17/56.916|731.27|1|
|health.canonical_obligation|35|0/0|20.347/498.796/659.404|659.404|1|
|http.fleet_overview|171|0/0|121.365/1833.967/2221.585|2282.217|0|
|http.github|1|0/0|607.373/607.373/607.373|607.373|0|
|http.pr_supervisor_peer|854|0/0|45.512/129.933/617.735|1262.798|0|
|maintenance.run|1|0/0|24800.683/24800.683/24800.683|24800.683|0|
|progress.checkpoint_write|24|0/0|141.197/645.341/722.975|722.975|2|
|progress.pending_read|106|0/0|9.901/49.939/250.502|517.364|1|
|reconciliation.completion_evidence_read|168|0/0|3.998/44.434/151.175|732.628|1|
|reconciliation.dispatch_read|284|0/0|1.865/18.36/82.471|2882.784|2|
|reconciliation.session_read|168|0/0|2.141/39.222/442.674|1017.715|1|
|sqlite.agent_session_save|74|0/0|19.882/137.059/6122.182|6122.182|2|
|sqlite.pr_supervisor_completion_due_read|110|0/0|15.359/130.232/173.934|600.881|1|
|sqlite.pr_supervisor_watch_read|427|0/0|4.319/64.739/123.492|590.487|1|
|sqlite.pr_supervisor_watch_write|427|0/0|8.196/97.925/267.031|509.874|1|
|sqlite.restart_handoff_completion|204|0/0|4.249/38.611/126.448|1274.119|1|
|sqlite.transcript_append|219|0/0|14.903/213.322/378.107|1350.011|2|
|startup.openapi|1|0/0|892.194/892.194/892.194|892.194|1|
|sync.peer_http|15|0/0|58.237/1866.97/1866.97|1866.97|0|
|sync.reconcile_head|15|0/0|497.211/2154.146/2154.146|2154.146|7|
|workspace.collect_garbage|1|0/0|6284.095/6284.095/6284.095|6284.095|1|
|workspace.project_provision|7|1/0|2357.137/2858.3/2858.3|2858.3|0|
|workspace.reconcile_terminal_state|1|0/0|1693.906/1693.906/1693.906|1693.906|0|

All current registered routes with bounded-window tails:

| Route | count | p50/p95/p99/max ms | process response bytes |
|---|---:|---|---:|
|/api/fleet/overview/local|58|49.688/175.429/889.898/889.898|581300|
|/api/pr-supervisor/replicas|869|32.939/136.391/356.275/508.657|4812130|
|/api/status|85|60.881/277.385/751.544/751.544|238510|
|/api/notifications|5|199.674/964.439/964.439/964.439|55545|
|/api/fleet/dispatch-jobs/{dispatch_id}/checkpoint|3|465.231/576.081/576.081/576.081|599|

The startup read is3837.558ms/365returned rows, worker3822.641ms, threadCPU454.665ms, observedSQL3495.486ms and no measured Pythonlock wait/hold. Native fetch/wait/GIL/disk share is unresolved. Maintenance remains24800.683ms, worker24799.725/threadCPU11920.267/outerhold3858.931ms. Statement wall, lock windows and CPU are not additive or a physicalI/O partition. Some completion-delivery stalls are distinct from worker service; all phases remain in the artifact.

Confirmed next amplification: startup loads unused diagnostic metrics for every persisted session. The candidate explicitly selects every existing nonmetrics session column, only for startup inventory; normal list/get remains complete. It keeps every retained identity/status/order/config/provenance—including closed sessions with durable queued prompts, in-flight work, permissions or lifecycle obligations. Existing recovery re-fetches the complete current session before constructing/saving its runtime, preserving metrics and concurrent external state. No data, durability, recovery policy, warning/deadline threshold or execution default changes.

Thirty alternating actual reads of359synthetic sessions with64KiBmetrics each avoid23,533,527 diagnosticJSONbytes/read while preserving all359models and every other field/order. Wallp50/p95/p99/max full259.177/312.259/320.007/320.007ms versus inventory51.918/73.385/117.318/117.318ms; threadCPUp95259.583→59.134ms. Rawarrays retained. The earlier probe466.118→68.501ms and540.022ms fullread outlier remain. This isolates avoidable hydration, not its full share of deployed startup3.84s or historical7.65s.

175tests plus2subtests pass42.46s across recovery/durability/admission/restart/receipt/CAS and newcontracts; final stalequiesce enhancement has5targetedpasses. Contracts deny metrics column reads, compare allotherfields/filters/order/snapshots/closedobligations, prevent resurrection from stale shutdown files, and prove a complete fresh session is fetched before runtime mutation. The initial five expected failures, a test expectation corrected from one to two deferred sessions, initial probe SyntaxError before work and native sandbox cache denial are preserved. All testprocesses use temporary data/workspace roots with a workspace guard.

The actual437file/29hash/38template wheel mixed run keeps100iterations/29requests/5000policyexpired events/16auditproofs/9600orderedactiveevents. Agentdisabled means this scenario does not measure recoveryinventorybenefit; the separate leaf/recoverycontracts do. Final mixed result: {"warnings": {"Slow off-loop operation": 2}, "threshold_violations": {"transcript.drain": 1, "sync.push_transaction": 1, "sync.object_collect": 3}, "violating_distributions": {"transcript.drain": {"samples": 300, "max_ms": 552.076, "p50_ms": 82.799, "p95_ms": 170.012, "p99_ms": 287.428}, "sync.push_transaction": {"samples": 100, "max_ms": 640.392, "p50_ms": 60.449, "p95_ms": 141.517, "p99_ms": 249.142}, "sync.object_collect": {"samples": 10, "max_ms": 1469.407, "p50_ms": 198.244, "p95_ms": 1469.407, "p99_ms": 1469.407}}}. Zero operation failures/timeouts/rejections. Separate premeasurement retains two slow calls, including OpenAPI693.808ms wall/665.164ms threadCPU. All raw timings/bytes/CPU/GC/SQL/filesystemcounts and startup observations remain. This is not acceptance.

Remaining work still includes full Kernel/OpenAPI/MCP/provider/workspace coldstart; history/transcriptdrain/objectcollection; duplicate health scans preserving freshness/integrity; native busy/WAL/fsync/disk/GIL/GC; livecontrol/active-watch/peerpaths; droppedlogs/statusfailures/olderretention/receiptambiguity/lostraw/testroot/CIcauses and every valid unexercised branch. The539old rows are preserved; the new startupmetricsconcern brings540. Existing140slownames/181baselineops/115baselineroutes/6shells/14startuprecords remain required.

The card remains Active, independent_agent_v1/17d034a6 unchanged; normal PA scheduler owns independent acceptance. This new startup patch is source-only until protectedintegration/exactactivation/retest. Backup0331/18fc has one queued changed-build coordination, no backup action: missingcold5cbb/ref1/819bytes/unknowncapturecause/source, failed35575957/all8archives/207failures0verified/automaticpaused/b527independentacceptance unchanged. DB89957 accepted original scope stays closed. Original10attachments retained; no newworker/verifier/admission or defaults/config change.

---

# Native maintenance SQL coverage and retained CI ambiguity — 2026-10-18

**All539 evidence rows /55 concerns remain open.** [Full matrix](performance-coverage.json); [new phase measurements, original contracts, mixed sample arrays and CI observations](benchmarks/systemic-maintenance-sql-observations-20261018.json). Every earlier stage below remains historical evidence, including all140 slow names,181 baseline operations,115 baseline routes, six shells,14 startup records and every previous owner/outlier/gap.

The source now uses the existing numeric `ObservedConnection` for native maintenance integrity checks, planner optimization, WAL checkpoints and VACUUM. Previously this connection was plain SQLite, so its calls were missing from worker SQL counters. The100 ms busy timeout, progress interruption, integrity checks, SQL, rollback, returned status, warning thresholds and deadlines are unchanged. Existing bounded observation buffers retain numbers, not SQL text, parameters or result payloads. Connection opening/closing, successful native busy waits versus CPU/GIL/disk, physical I/O and complete writer attribution remain unresolved; statement wall time includes waiting and lazy row production. An exception counter does not count every busy checkpoint result.

Ten alternating disposable comparisons preserve compaction, integrity and exactly2000 remaining rows after7000 seeded/5000 removed before measurement. Observed calls change0→28. This is instrumentation, not a speedup: p95 wall73.517→75.375 ms and CPU65.611→70.295 ms, with host variation/overhead retained. A bounded disposable writer lock still defers safely; this case records one busy exception/nine calls/7.889 ms. No live fault, database mutation, provider or backup traffic.

Both new real observation contracts fail against the exact original85be function because it records zero SQL calls, then pass with the product change. The actual maintenance/database suite has33 passes in6.96 s, including existing integrity, busy, live-reader, interruption, rollback and shutdown behavior. An earlier AST-only candidate passes20 maintenance tests. Suites overlap. The first runner referenced a nonexistent test filename and ran zero tests; that configuration error and corrected run remain archived. Every test process has temporary data/workspace roots plus a workspace guard.

The fifth sequential mixed stage uses the actual437-file wheel,29 loaded-module hashes (now including maintenance),38 compiled templates,100 iterations/29 request variants, three ordered transcript families and the real5000-expired-event retention sweep. Its measured workload and thresholds are unchanged; only the extra maintenance attestation changes the script hash. It preserves9600 ordered active events and16 audit proofs. The final stage retains **0 worker warnings /4 sampled threshold violations**. Violating families and p50/p95/p99/max are:

`{"maintenance.run": {"max_ms": 527.028, "p50_ms": 527.028, "p95_ms": 527.028, "p99_ms": 527.028, "samples": 1}, "sync.object_collect": {"max_ms": 1701.981, "p50_ms": 191.288, "p95_ms": 1701.981, "p99_ms": 1701.981, "samples": 10}}`

This73.168 s stage has zero operation failures/timeouts/rejections. Maintenance is527.028 ms and object collection reaches1701.981 ms. The maintenance runtime window now records up to107SQLcalls/44returnedrows,404.689 ms statement time,514.995 ms worker service,128.631 ms threadCPU and45.063 ms outer writer hold. These are independent three-sample windows including startup; they cannot be added or treated as paired causal samples. Separate startup OpenAPI remains849.069 ms wall/713.219 ms threadCPU with two premeasurement slow operations.

All raw timings/decoded-byte samples, logical filesystem counters, CPU/GC, runtime bounded windows, process maxima and premeasurement schema/startup warnings are retained. Decoded bytes are not wire bytes; logical calls/returned rows are not total scanned rows or physical I/O. Increasing SQL counter coverage changes counts without increasing logical database work. This is a sequential run on this host, not randomized causal evidence or whole-system acceptance. The four earlier stages remain intact, especially the mirror-only maintenance2973.385 ms/drain2191.260 ms regression and the subsequent six-violation combined batch.

PR508's original85be head has a separate unresolved integration observation: at05:40/05:41Z fresh workflow39101015686 reads say completed/success, while its test0 job and exact-head check remain in progress with no conclusion; four other checks succeed. Sanitized endpoint SHAs, response dates and request IDs are archived under report7f38b6de revision1. PA correctly blocks integration. This independently prepared native instrumentation advances the authorized card scope; it is not a retry, cancellation, check-status repair or means of erasing the old observation. Every revised head still requires a fresh stable supervisor gate and independent terminal checks/reviews/rules/pagination/authorship/CLEAN validation. Passing a later head cannot establish the cause of the old discrepancy.

The queued PR507 failure notice was independently revalidated: exact da414 head is already merged as1705a665 and all five current checks succeed. Its original native-Tab browser failure and one same-head retry remain open cause evidence, distinct from PR505's DOM replacement failure. No duplicate merge or rerun occurred.

Actual runtime remains **5024/packageb538/PID7531/startupa3a9/config86968/lease290**. Handoff da59 finished delivery05:42:54Z, enabling a future distinct-target handoff only after protected integration, exact main CI, frozen installation and source/package proof. No new install/restart was performed here. The prior95 ordinary successful GETs are a small deployed5024 window, not acceptance of this source.

Remaining owned work includes duplicate full transcript health scans with freshness/integrity semantics, startup-state hydration with durable recovery obligations, full Kernel/OpenAPI and MCP model/import CPU, real providers/placement/workspaces/active-watch details/peers, object collection/history/drain tails, native busy/WAL/fsync/disk/GIL/GC, receipt ambiguity/log loss/status failures/older retention/lost raw evidence, valid unexercised branches, test-root isolation and all CI causes. Neither SQL batching nor instrumentation closes these findings.

The canonical card remains Active under17d034a6 / independent_agent_v1; PA's normal scheduler owns independent acceptance. Backup0331/b527 remains separate and unaccepted: missing current cold5cbb18fdcfb4/ref1/819 bytes, unknown capture-time/cause/no intact source, failed35575957/207 failures/zero verified/automatic paused/all eight archives preserved. No backup capture, retain_objects, GC, verification, configuration, defaults, production backup/restore/deletion/recovery or new worker changed. DB89957's accepted original scope stays closed. Original ten attachments remain preserved.

---

# Retention batching, additional startup evidence and open coverage — 2026-10-18

**All 538 evidence rows / 54 concerns remain open.** [Complete matrix](performance-coverage.json); [all numeric stages, source identities, original failures and evidence hashes](benchmarks/systemic-retention-batching-20261018.json). These status labels supersede older checkpoint labels below. Every earlier finding and owner remains required.

PR507 is integrated as **1705a665** from watched da41473e / watch174c955b. The supervisor's current exact-head gate was green; independent checks verified five terminal successes, zero reviews/threads with complete pagination, attributed commits, current protection and CLEAN merge state before normal head-CAS merge. Head/merge trees match; actual headCI39098821473 and mainCI39099338638 each have five terminal successes. An initial unchanged browser test failed at native Tab focus/line117; its original log SHAa4b1bc41…, isolated unmodified candidate41.01 s/baseline502435.88 s passes and one same-head failed-shard retry remain. The cause is unresolved. This differs from PR505's DOM.resolveNode reload/line172 failure, which also remains open. Green reruns do not erase either failure.

The actual runtime remains **5024f789/packageb538/PID7531/startupa3a9/config86968**, verified through authenticated owner proof in the same leased workspace. Installation and activation of 507 or this new candidate have not occurred. The prior 95 ordinary GETs with zero errors/500 ms violations are a small deployed 5024 window, not whole-system acceptance. The current card is Active with requirement17d034a6 / independent_agent_v1 unchanged; the normal scheduler owns independent acceptance.

The next confirmed amplification is retention cleanup. The compatibility mirror fetched every row and issued one DELETE per expired sequence. Canonical prune also fetched each event, released its cold reference and deleted it separately under the existing transcript writer lock. The candidate batches the mirror delete against the exact post-prune canonical sequence set, and batches canonical cold-reference decrements by hash and event deletion by session. Indexed empty checks avoid needless per-session mutation. Latest16 independent audit proofs, shared live references, positive-only count decrements, transaction rollback/retry and duplicate session inputs remain. An empty input still avoids GC; a nonempty followup with zero event rows still performs the original GC so bytes deferred by a backup reader can be collected safely.

Existing backup owner18fc explicitly cleared a **prune-only** source window in durable progress558. No retain_objects, GC, backup capture/service, verification, configuration, execution default, warning threshold or deadline changed. No production retention mutation, backup, restore, object recovery, deletion, new worker or new verifier was performed.

Validation retains the initial red contracts: mirror2000DELETEs versus1, an empty-mirror authorizer denial, and canonical2000DELETEs versus1. The final six mirror contracts and eight canonical contracts pass. The broader retention/maintenance/storage suite has43 passes; the backup suite has65 passes and16 subtests. These overlap rather than summing to unique coverage. All subprocesses use disposable data/workspace roots and a WorkspaceManager guard. A final COUNT-to-indexed-SELECT1 existence-query refinement was followed by eight canonical passes; the65-test backup suite preceded that query-only refinement.

Twenty alternating compatibility-leaf pairs at10,000 rows preserve exact counts and remaining-row hashes: p95137.750→34.765 ms, max163.926→34.962 ms, observed execute/fetch calls10003→6 and returned rows10001→2. The canonical prune/list reads are stubs in that leaf, so this is not end-to-end retention or backup proof. All earlier leaf stages and host limitations remain.

Four real mixed scenarios are retained separately. Each uses100 iterations/29 foreground request variants plus sync/binding/progress/cache/wire/three transcript families and a real maintenance retention sweep. Each applies the unchanged policy to5000 explicitly expired synthetic events, preserves16 audit proofs and9600 ordered active events, and retains every warning/outlier/failure. Each wheel has437 matching source files,28 runtime module hashes and38 compiled templates. These are sequential single scenarios on this host, not randomized causal comparisons. The final script adds raw latency/decoded-byte arrays and explicit unchanged threshold policy after measurement; its script hash differs without changing measured work. Decoded bytes are not wire bytes. Logical SQLite calls/returned rows are not total rows scanned or physical I/O.

| Stage | Worker warnings | Sampled threshold violations | Maintenance latency distribution ms |
|---|---|---:|---|
|baseline|{'Slow off-loop operation': 3}|8|{'samples': 1, 'max_ms': 1263.12, 'p50_ms': 1263.12, 'p95_ms': 1263.12, 'p99_ms': 1263.12}|
|candidate|{'Slow off-loop operation': 3}|6|{'samples': 1, 'max_ms': 1330.567, 'p50_ms': 1330.567, 'p95_ms': 1330.567, 'p99_ms': 1330.567}|
|candidate-final|{'Slow off-loop operation': 3}|8|{'samples': 1, 'max_ms': 2973.385, 'p50_ms': 2973.385, 'p95_ms': 2973.385, 'p99_ms': 2973.385}|
|batch-final|{'Slow off-loop operation': 1}|6|{'samples': 1, 'max_ms': 572.077, 'p50_ms': 572.077, 'p95_ms': 572.077, 'p99_ms': 572.077}|

The final mirror-only run was **worse** despite lower SQL call counts: maintenance2973.385 ms, three drains up to2191.260 ms, four object collections up to1080.647 ms and eight violations. This regression remains in the artifact. The final combined batch's violating families and p50/p95/p99/max are:

`{"/api/agent/history/session-0?limit=64&message_boundaries=true": {"max_ms": 641.494, "p50_ms": 148.655, "p95_ms": 345.304, "p99_ms": 419.29, "samples": 100}, "/api/pr-supervisor/watches": {"max_ms": 1015.677, "p50_ms": 7.383, "p95_ms": 19.864, "p99_ms": 59.968, "samples": 100}, "maintenance.run": {"max_ms": 572.077, "p50_ms": 572.077, "p95_ms": 572.077, "p99_ms": 572.077, "samples": 1}, "sync.object_collect": {"max_ms": 1716.283, "p50_ms": 179.51, "p95_ms": 1716.283, "p99_ms": 1716.283, "samples": 10}}`

The final combined run completes in83.432 s with zero operation failures/timeouts/rejections, one worker warning and six violations. Observed maintenance SQL calls fall10048→51 and returned rows10016→18 versus the sequential baseline; writer outer-hold maximum808.479→77.300 ms and maintenance1263.120→572.077 ms. Those are bounded windows from separate runs, not paired causal estimates;5000 rows still undergo logical deletion. Object collection worsens to1716.283 ms, PR-watch listing reaches1015.677 ms and message history641.494 ms. Maintenance itself remains over500 ms.

Premeasurement boot/schema warnings remain separately recorded; no source or deployed performance acceptance follows from the combined run. Existing dispatch.runnable_read has a2000 ms budget while other explicit sampled families use500 ms; the final artifact additionally reports every >=500 ms observation and the exact policy, without changing either budget.

Additional findings are owned and open:

- Maintenance requests full transcript metrics through migration and again after database maintenance. Both include health work; their timing share is unmeasured. Any future coalescing must preserve integrity checks and final freshness.
- Maintenance opens a plain SQLite connection for quick_check/optimize/VACUUM/checkpoint. Existing worker SQL timings omit those native calls. Zero observed busy failures is not zero busy wait; independent window maxima cannot be added or sample-correlated. Native phases require bounded instrumentation; report7aeeac15 revision1 preserves this gap.
- Startup-state reads hydrate persisted sessions before recovery. Filtering closed status alone would be unsafe: recovery also checks durable in-flight, queued, permission and lifecycle obligations. The live5.414 s maximum is not attributed solely to hydration; no startup-state repair is claimed.
- Disposable real MCP stdio connects221 tools: initialize3114.845 ms, tools list41.883 ms, end-to-end4143.200 ms. Parent CPU is separate from child3640.941 ms user/287.456 ms system. The initial missing-principal fixture error occurred before spawning and is retained. The child build was not immutably attested. The existing17.413 s failure and earlier6.384 s success remain.
- The `pa mcp` entrypoint is already thin. A separate cProfile import/registration run costs7066.911 ms wall/6639.613 ms CPU with profiling overhead. Importlib4937.1, Pydantic model construction3762.9, MCP import2602.5 and221 tool registrations2029.5 ms are overlapping cumulative times, not additive unprofiled latency. Tool/schema/auth/stdio parity must be preserved in any future repair.

The matrix still covers all140 retained slow names,181 baseline operations,115 baseline route groups and six page families; three actual MCP phases extend11 startup records to14. Named observations do not cover every branch. Full Kernel/schema/cold session/drain, real providers/MCP/placement/workspace/active-watch/peer paths, native busy/WAL/fsync/disk/GIL/GC/object collection, receipt ambiguity, dropped logs/status failures/older retention, missing raw evidence, test-root isolation and both CI failures remain open.

Backup0331/b527 retains its separate acceptance. Missing cold5cbb18fdcfb4 remains a data blocker with absent-current/ref1/819-byte evidence, unknown capture-time/cause and no intact source; failed35575957 is terminal, eight archives preserved,207 failures/zero verified/automatic paused. Existing source-information question remains pending. No unchanged retry, restore, reference drop or fabricated bytes. DB89957's accepted original scope stays closed. Original ten attachments remain preserved at quota.

---

# Combined 5024 activation, deployed retest and bounded ancestry membership — 2026-10-18

**All 530 evidence rows / 49 concerns remain open.** [Complete matrix](performance-coverage.json); [all new raw numeric samples, phases, counters and evidence hashes](benchmarks/systemic-ancestry-membership-20261018.json). This checkpoint supersedes historical current-status labels below. Every previous finding, owner, outlier and gap remains.

PR505 merged as **62551f44** from watched 0c244 after protected stable-green revalidation. The existing backup owner integrated PR506 as **5024f789**, with 62551 as its first parent; all five backup files match their reviewed head. Combined main CI39097201323 has five terminal successful checks. The owner's 98 tests/16 subtests are separate source evidence. Exact immutable 5024 was installed with frozen, unchanged dependencies and configuration; 437 source files, 38 compiled templates and package b538 were verified. Managed handoff da59 resumed this exact session/workspace. Immutable owner proof confirms **loaded 5024/package b538/PID7531/startup a3a94866/config86968/unique lease290**. No reinstall, repeated restart, production backup or repairer acceptance occurred in this stage.

The 95 ordinary authenticated sequential GETs at **04:43:28.963–04:44:00.820Z** returned 95 HTTP200, zero errors and zero 500 ms violations with a stable build. All six page families were sampled; the first request after activation is not guaranteed to be the first-ever cold render. Helper wall time includes authentication, transport, reading and decoding. Decoded payload bytes are not compressed wire bytes. Every request's status, wall/thread CPU Server-Timing, decoded bytes, correlation/error and SHA partial archive remains. Five-sample tails equal the maximum; this small window is not whole-path or mixed-load acceptance.

| Request | Samples | p50 / p95 / p99 / max ms | >=500 ms |
|---|---:|---|---:|
|/|5|28.070/43.913/43.913/43.913|0|
|/agent|5|146.950/158.270/158.270/158.270|0|
|/api/fleet/overview|5|201.808/285.677/285.677/285.677|0|
|/api/health|20|16.605/54.532/366.737/366.737|0|
|/api/runtime|20|89.710/113.348/147.062/147.062|0|
|/api/status|20|68.391/266.705/448.152/448.152|0|
|/fleet|5|255.532/314.258/314.258/314.258|0|
|/pull-requests|5|53.753/431.866/431.866/431.866|0|
|/settings?section=agent|5|30.174/34.371/34.371/34.371|0|
|/work|5|28.827/98.698/98.698/98.698|0|

Previous 134044 Agent668.973/overview634.979 ms, a90 Fleet500.349/overview623.252 ms and b808 Fleet2468.743 ms remain separate windows. New Fleet HTML is 1,098,368 decoded bytes, overview937,778; no payload-size or whole-fleet acceptance follows. The 431.866 ms PR request retains shell345.7 ms wall/4.8 ms CPU, context40.5/19.6 and template1.5/1.5. Waiting/native scheduling remains unpartitioned.

The fresh runtime snapshot retains **all 96 operations and 37 route groups**, every count, byte measurement, bounded percentile window and process maximum separately. It still contains maintenance30.133 s, workspace GC6.637 s, startup-state5.414 s, transcript append3.598 s and reconciliation3.379 s. A passing page sample does not erase those tails. No new exhaustive fleet, peer or log audit is claimed.

Confirmed additional amplification: loaded reconciliation records materialize **60,249–88,563 ancestry hashes /4.04–5.93 MB**. The new source answers a ready tip's ancestry membership within a pinned WAL read view while retaining the existing bounded canonical novel-suffix walk. After 64 distinct probes, one exact bulk read prevents a large suffix from amplifying SQL calls. Older, incomplete or missing-tip indexes retain the original fallback. No source-byte cache, durability/order/CAS/admission/lease/receipt/acceptance/deadline change or backup/transcript_storage edit.

Twenty alternating synthetic-index comparisons at 30,000 retained commits preserve the same false/true answers for two imported-fork queries. Wall p50/p95/p99/max improves **80.663/89.285/89.665/89.665 →3.945/6.826/6.894/6.894 ms**; transferred hashes60,000→0 and logical JSON4,020,002 bytes→0. Observed SQL execute/fetch calls increase15→25. This isolates amplification, not its share of live latency or physical disk IO. The initial baseline p95/max134.568/150.606 ms remains. At5,000 retained, final p9517.837→5.389 ms. The public reproducer seeds only a disposable derived index; canonical fork, missing-parent, realm, reset, WAL snapshot and fallback behavior are tested separately.

**67 tests plus five subtests passed**, followed by six final contracts after the JOIN/probe refinement; these suites overlap. One initial test incorrectly counted SQL execute/fetch as a single observed call: the original136-versus75 assertion failure is retained. The corrected contract verifies no additional SQL after one bulk fallback and bounds the observed call count. No 500 ms or production warning budget changed.

The actual combined candidate wheel verifies **437 source files, 28 loaded module hashes and 38 compatible template entries**. Its 100-iteration/29-request/71.819 s mixed run retains all9,600 ordered events, with zero operation failures/timeouts/rejections, **but one worker warning and four 500 ms violations** remain:

| Path | Samples | p50 / p95 / p99 / max ms | Violations |
|---|---:|---|---:|
| Complete-message history HTTP |100|153.334/320.127/438.348/573.153|1|
| Full object inventory |10|220.254/1205.261/1205.261/1205.261|3|

Two premeasurement slow calls remain separate, including schema913.030 ms/720.880 ms CPU and sync-ref completion936.9 ms. Full SQL/lock/CPU/loop/GC/bytes/logical-write observations remain in the artifact. The new ancestry candidate is **unmerged and uninstalled**. The previous PR505 mixed3-warning/5-violation stage is preserved and is not falsely labeled as containing PR506.

An isolated warm512-file/512-byte read probe demonstrates a possible GIL mechanism:27.933 ms wall/27.778 ms CPU alone versus2755.542 ms wall/26.578 ms CPU with a temporary pure-Python competitor in that disposable process. Open/read/close phases remain. The requested2 s stop timer is not a hard timing guarantee; timer, host scheduling, GIL and native IO remain unpartitioned. This does **not** establish the production cause. An initial six-pair contention probe was interrupted after an observed6m29s; only12 idle samples were durably saved. The missing contended progress is an explicit observability gap, not reconstructed evidence or a PA server timeout.

Actual PR505 main CI39096904163 attempt1 failed the unchanged visualization-browser reload at DOM.resolveNode backend1831/line172. Original log SHA213510c1…/report7d7b524d revision2, unmodified isolated candidate36.89 s/baseline35.17 s passes, and **one** same-head failed-shard retry with all five checks green remain separate. Cause unresolved; passing retry/combined green does not close it. No assertion, deadline or product change was used.

Backup owner0331/18fc retains the narrow506 repair and b527 independent acceptance. Its matched large capture reports hold16.758074→0.185218 s/write16.788190→0.219425 s/138 after samples; that matched-capture raw is still an owner hash reference here. Two HTTP fixture attachments were independently downloaded through the supported API and SHA-verified. Our numeric reanalysis retains2,710 foreground requests, zero HTTP errors and zero2 s violations, **but seven500 ms violations**: card writes4/max1171.978 ms, status2/max921.309 ms and settings1/max527.198 ms. Backup POST123.335 s and verification44.800 s remain separate long control responses; no invented budget or discarded control request. Per-request bytes were not recorded. Three pre-admission startup warnings remain. Backup's2 s scope and the systemic500 ms failure are preserved separately.

Missing live cold object5cbb18fdcfb4/ref1/819 compressed bytes/Sep20 remains a **data and acceptance blocker**, with original capture presence/time/cause unknown. All eight archives are preserved;35575957 remains terminal failed,207 failures/zero verified/automatic backups paused. The existing intact-source information request remains pending. No production backup/restore/deletion/ref drop/unchanged retry/new verifier was authorized or performed.

Every remaining family remains owned and open in the matrix:

- All140 retained slow names,181 baseline operations,115 baseline route groups, six page families and11 startup records. Current observed names do not cover every branch.
- Full Kernel/schema/MCP/provider startup; placement/workspace/active-watch/external calls; cold session start and transcript ordering/drain correctness.
- Native SQL busy/lock/WAL/checkpoint, disk/fsync/write amplification, GIL/CPU/scheduling/GC, maintenance and object collection. Older indexed-head/compatible-history full ancestry consumers remain.
- Peer identity/build/log coverage, historical peer failures,5.64 MB dropped logs,2,396 status failures, older retention/rotation unknowns and lost raw snapshots.
- Receipt ambiguity/recovery, attachment observability, completion/admission/lease fences, general test-root isolation and original CI failures.
- Existing backup0331/b527 acceptance and systemic17d034a6 independent-agent acceptance. The scheduler owns verification; DB89957's accepted original scope stays closed. Original ten attachments are preserved at quota.

**No whole-card closure follows from integration, activation, small samples or isolated speedups.**


---

# Exact134044 retest and build-time template compilation — 2026-10-18

**All527 evidence rows /47 concerns remain open.** [Complete matrix](performance-coverage.json); [every new numeric stage, raw sample and evidence hash](benchmarks/systemic-template-cache-20261018.json). This checkpoint supersedes historical current-status labels below while preserving every prior finding, owner, outlier and gap.

PR504 is merged134044 with matching source/merge trees and allfive head/main CI checks successful. Normal frozen installation verified436files/VCS, zero dependency version changes and unchanged configuration. Immutable owner proof now confirms **source134044/packagef29/PID1621/startupc5ec**, uniquelease290. Its80ordinary sequential GETs returned80HTTP200 but **Agent1/5max668.973ms and overview1/5max634.979ms** remain threshold failures. Fleet0/5slow/max452.699 is a small window, not complete acceptance. The slowAgent correlationddd4040b has shell293.7/template224.1/context97.9/server616.1/helper668.973ms. Complete raw timing/decodedbytes/correlations/errors/buildfences and reportd82ea21b revision7 are retained. Fresh bounded runtime snapshot contains **96operations/31routes**, with all counts/percentiles/processmaxima and bytes retained separately; fixtures overlapped, no fullfleet/log audit claimed.

Disposable real Kernel profiling confirms cold Jinja compilation CPU: Agent compile368.341ms inside430.048ms profiled render; warm23.88ms. Profiler overhead is included and live shell/nativewait causes remain unverified. New code compiles38package templates at wheel build, then retains normal source lookup/checksum, plugin precedence, escaping and custom environment behavior at runtime. IncompatiblePython/Jinja/environment/malformed or missingartifact falls back to normal compilation. No runtime writes. Bounded threadCPU phases supplement existing wall timings without changing warning/deadline thresholds. No speculative Fleet JSON repair: installed FastAPI already uses its typed directJSON fastpath.

Twenty alternating first-render pairs on each actual frozen page context preserve complete HTML every time and reduce candidate runtime compilation to zero. Actual wheel artifact wall p50/p95/p99/max in ms:

| Page | Normal compiler | Packaged bytecode |
|---|---|---|
| Agent |163.117/182.258/188.522/188.522|7.779/12.581/80.596/80.596|
| Fleet |217.428/277.984/283.177/283.177|10.087/14.867/16.391/16.391|
| Home |68.540/90.706/93.351/93.351|5.685/8.349/8.528/8.528|
| Work |109.211/130.844/135.268/135.268|6.234/8.143/8.418/8.418|
| PullRequests |154.683/189.460/222.872/222.872|7.511/13.311/14.449/14.449|
| Settings |195.481/247.388/250.479/250.479|9.659/13.249/14.615/14.615|

Agent80.596ms outlier remains, CPUmax11.024ms; nativewait/scheduling unknown. Initial wrong-autoescape prototype correctly fell back and showed no improvement; its raw results and limited source provenance remain. Build compilation1213ms in a prior disposable stage is separate from readiness; actual wheel manifestload2.812ms/1,102,141bytes. Sourceinstalls without artifact remain correct but receive no claimed speedup.

Source and sdist wheel builds succeed, uv.lock unchanged, lockcheck passes. **51regressions+2subtests** passed with temporary data/workspace roots, then17final cache contracts and18includingCPU timing passed (overlapping suites). Sourceedit/plugin/customfilter/escaping/deterministicbuildroot/malformedartifact contracts preserve semantics. Final actual extractedwheel verifies437sourcefiles,28loadedmodulehashes and38compatiblecacheentries. Its100iteration/29request/73.816s mixed workload retains9600ordered events and zero operation failures/timeouts/rejections, BUT **3worker warnings and5threshold violations** remain: drain533.876ms, messagehistory671.739ms, objectcollect3max2099.407ms. Separate bootstrap3slowcalls/OpenAPI813.393ms/719.543CPU remain. Full raw phases/SQL/locks/GC/CPU/bytes/logicalwrites are retained. This is not performance acceptance; new candidate is unmerged/uninstalled.

Previous testambientworkspace root risk is confirmed;158+2guarded prior rerun and new guarded suites used onlytemporaryroots. Eight old emptylocks were reversibly renamed under exactmetadata/flock checks, zerodeletions; evidence/report754e7d9a revision3 preserved. General reusable harness/otherfixture audit remains open.

Backup35575957 terminally failed280.856222s/transcript_object_missing, all8archives preserved/0published/0pruned/207failures/0verified/automaticpaused. Report5000d93f revision16 and17.658370875s sharedwriterlock capture concern remain open; actualwrite delay/internalcause unmeasured. Verifier explicitly released its coordination hold and closed. Supported lifecycle/lease reads allowed one changed-evidence handoff to the existing repair owner, acceptedqueued/c754742b, consumptionunverified. Old failed7a2292b1 was not replayed; no secondbackup/newworker/competingverification.py edits.

All140retainedslow names/181baselineoperations/115routes/unexercisedbranches, fullstartup/schema/MCPCPU, nativebusy/WAL/GIL/disk/GC/sharedlocks/objectcollection, realprovider/placement/activewatch/externalpaths, peers/unknownidentity/build/logretention, droppedlogs/statusfailures and receipt/lostraw gaps remain required. Schema813ms still fails; one compile speedup does not resolve them. Original10attachments and all historical stages remain. Requirement17d034a6/independent_agent_v1 and backupb5271340 remain schedulerowned and unaccepted; DB89957 original accepted scope stays closed. **No card closure from PR, merge, install or bounded speedup.**

---

# Loaded a90 retest and one Fleet watch snapshot — 2026-10-18

The systemic card remains **open:526 evidence rows /46 concerns**. [Complete matrix](performance-coverage.json); [every new raw observation, profile and workload stage](benchmarks/systemic-fleet-watch-snapshot-20261018.json). All historical findings, failures, owners, unmeasured branches and retention gaps remain. This checkpoint supersedes historical current-status labels below.

PR503 merged exacta90 with equal head/merge trees after stable supervisor evidence and independent checks; allfive actualmainCI39093652500 checks passed. Normal frozen-dependency installation verified all436sourcefiles/VCS identity, zero dependencyversionchanges and unchanged config86968. Managed handoff15bc75d8 resumed this exact session/worktree. Owner API verifies **sourcea90/PID96628/startup6a40/packageb20** and uniquelease290. This proves local activation, not performance acceptance.

The80ordinary sequential owner GETs returned80HTTP200/zeroerrors, but **Fleet1/5 exceeded500ms (p50/p95/p99/max238.177/500.349/500.349/500.349ms) and overview1/5 exceeded500ms (157.463/623.252/623.252/623.252ms)**. Previousb808Fleetp50/max2138.385/2468.743ms remain separate observations. Fleetdecodedpayload is1,136,999bytes in each new sample, overview974,503bytes; these are not compressedwirebytes. The failingFleet correlation38a547b5 has context307.6/template100.3/shell37.7/server448.0/helper500.349ms. Status/runtime/health20each,Agent/Home5each had zero sampledviolations. Every raw status/timing/byte/correlation/error and exactbuildbefore/after is retained withSHAarchives. Smallsequential samples and successfulHTTP do not establish comprehensive acceptance. Reportd82ea21b revision6 records both residuals.

Confirmed next amplification: every overview queried and hydrated the same complete activePRwatches twice, once for the localdimension and again for topology. The new requestscoped snapshot supplies both views, which still independently serialize all fields/proofs and do not share mutable public data. Standalone dimension reads remain fresh. No durable data, ownership, filter, admission, lease, receipt, sync, warning/deadline or executiondefault changes.

Twenty alternating disposable complete overview comparisons with immutablea90/100active-or-blockedwatches/64KBproofeach retain every output field: baseline/sourcep50/p95/p99/max175.344/192.535/193.163/193.163→90.345/106.534/171.113/171.113ms. Hydratedmodels4000→2000, parsedstate-policybytes257,448,000→128,724,000; these are logical parsing counts, notphysicaldiskIO. The171.113ms candidateoutlier remains. Unrelated localdimensions are stubbed; this is an isolated leaf comparison, not solelivecause or deployedcandidateacceptance. **158regressions and2subtests passed**, including detachedcompletepublicvalues, missinglocalnode andstandalonefreshness. Baseline2reads-versus1 regression and corrected initialtestimport error are retained.

The final100iteration/29request/86.603second mixedsource stage verifies26currentmodulehashes and9600ordered retainedevents, zerooperationfailures/timeouts/rejections, BUT2steadyworkerwarnings and7sampledviolations: push1(max558.723ms), reconcile1(508.957), objectcollect3(1231.013), Fleet1(561.633), PullRequests1(969.142). Separatepremeasurement schema1272.885ms andfastworker/slowcompletionref725.5ms remainstartupfailures. Allrawphases/CPU/SQL/lock/GC/loop/byte/warning/failure measurements are in the artifact; residuals remain acceptance failures. Earlier9600ordered-event source stages and every oldoutlier remain linked. No new fullfleet/log audit is claimed from this retest. Current isolatedschema cProfile2400.728mswall/2344.329msCPU includes profiler overhead and still shows CPU cost;481paths/792795bytes/SHA623e83b9 are a newsnapshot, not parity with the older792712-byte contract. Earlierrejectedserialization experiment remains rejected; no unmeasured schema repair is included.

Fullstartup/schema/MCP, nativebusy/WAL/GIL/disk/scheduling/GC/sharedlock partition, objectcollection, validnonemptyprovider/placement/workspace/activewatch/externalbranches, peers/build/logretention and receiptgaps remain required. The new candidate is **unmerged/uninstalled**. Requirement17d034a6/independent_agent_v1 and backupb5271340 remain schedulerowned and independently unaccepted; DB89957 accepted original scope stays closed. Backup12.778613s/2.265sfailures, failedbase501CI,10attachments and originalobservability gaps remain. No newworker/card/productionbackup/fault injection. **No wholecard closure from one speedup, PR, integration or activation.**

The broader158test run left eight empty checkout admissionlockfiles with mtimes during that run. Noopenhandles were observed; exactcreatingfixture remainsunverified. Files are preserved/excludedfromcommit, notdeleted/ignored to claimcleanliness. Report754e7d9a andthe artifact retain metadata; furtherbroadtests paused pendingfixtureisolation. This extends the earlier disclosedtransient checkoutSQLitefootprint, not evidence oflivePAstorewrites. Addedtests/benchmarks specifyexplicittemporaryworkspace roots.

---

# Loaded b808 retest and Fleet dispatch-copy repair — 2026-10-18

The systemic card remains **open: 524 evidence rows / 44 concerns**. [Full matrix](performance-coverage.json); [all numeric stages and raw samples](benchmarks/systemic-fleet-dispatch-20261018.json). All prior owners, failures, unmeasured branches and retention gaps remain. This checkpoint supersedes earlier current-status labels, which remain historical below.

Protected PR500/502 and concurrent501 are integrated in **b808b039**; exact merge CI39091636456 passed all five checks and70focused merged tests passed. A normal frozen-dependency installation changed only PA's Git source, with zero dependency version changes and unchanged config86968d. All436installed source files matched immutableGit. Managed handoff689e2c99 restarted the same session: owner API verifies **PID92567 / startup185b37c9 / sourceb808 / package89ae**, with the same unique lease290. This proves local activation, not performance acceptance. Native PA tools were absent in this resumed turn; the supported authenticated owner API was used without fallbackcredential lookup.

The actual loaded-build retest made80ordinary sequential GETs: allHTTP200, zeroerrors, but allfive Fleet samples exceeded500ms (p50/p95/p99/max2138.385/2468.743/2468.743/2468.743ms, decodedresponsebytes1094324–1115793). One of five Home reads also exceeded500ms(max549.047ms). Status/runtime/health20each andAgent/overview5each had zero sampled violations. A separate three-request phase observation records Fleet context1429.8–2208.9ms versus template39.4–55.7ms. ExistingServer-Timing headers provide this split; no process instrumentation was injected. Raw timings/status/bytes/correlations and buildbefore/after are retained with SHAarchives. These small windows are not process maxima, wirebytes, or wholePA acceptance. Reportd82ea21b revision4 preserves residuals and explicitly corrects revision3's erroneous byte minimum.

A separate timestamped actualb808 runtime inventory retains **all98operations across21name-prefix families and34registered routegroups**, including two newly added route rows. Every count, reported bounded-window p50/p95/p99/max, process maximum, phase and byte distribution is in the artifact and matching row. Five routegroups have windowp95≥500ms (some smallsamples); native operation tails include maintenance32.755s, workspaceGC6.557s, startupstate4.651s and transcriptappend3.796s. These are currentprocess maxima, not rolling-window percentiles or distinct incident counts. Ordinary observations overlapped some disposable tests/benchmarks on the same uncontrolled host; causal partition remains open. The source mixed stage has **four workerwarnings and six sampled violations** (sessionlist1/reconcile1/Fleet1/objectcollect3), with three premeasurement warnings retained separately. None of those families is closed by the Fleet copy repair.

Confirmed source amplification: the indexed latest_by_card call deep-copies every selected full dispatch, including terminal histories Fleet immediately discards. The old docstring and an earlier local checkpoint incorrectly claimed nohistorycopy; both are corrected. The candidate filters terminal rows under the existing index lock, preserving active-preferred selection and realm fencing, then returns every active public field fully detached before canonicalizing current instance names. Existing full-record APIs, writes, durability/order/admission/lease/receipt/sync semantics and all deadlines/defaults stay unchanged.

In20alternating disposable reads of100terminal+5active records, each with500events and256placement candidates (27.18MBserialized input), p50/p95/p99/max improve from 1249.085/1397.601/1433.915/1433.915ms to 40.287/43.525/46.206/46.206ms. Full-model snapshots fall2100→0; the five active public projections remain exact and deeply detached. This isolates copy CPU; it is not proof of the sole live cause. **37scoped regressions and101Fleet regressions/2subtests pass**. New source is unmerged/uninstalled. Earlier prototype outliers, first fixturevalidationfailure, three expected preimplementation contractfailures and unchanged prior test/CI failures remain in the artifact.

The100iteration/29request synthetic mixed stage is retained in full in the artifact, including all warnings, threshold violations, operation distributions, bytes/logicalwrite counts, loop/GC/lock/SQL phases and ordered transcript verification. Its26module hashes now includeFleet; its terminaldispatch card IDs are absent from selector rows, so this scenario does not replace the separate retained-history scale fixture. Fullschema startup, nativeSQL/WAL/GIL/GC/scheduling/disk attribution, unexercised valid branches, realprovider/MCP/external paths, peer/build/logretention/receipt concerns and backup independent acceptance all remain required. No threshold increase, warning suppression, data drop, productionbackup/deletion, competing worker or new card was used.

Systemic requirement17d034a6 independent_agent_v1 and backupb5271340 remain owned by the normal PA scheduler; no repairer acceptance receipt. Original12.778613s/2.265s backup failures, base501CI39091348922 failures and the ten original attachments remain preserved. DB89957's accepted original scope stays closed. **Neither integration, activation nor an isolated speedup closes this card.**

---

# Fleet selector phase repair — 2026-10-18

The systemic card remains **open: 520 evidence rows / 42 concerns**. [Full matrix](performance-coverage.json); [new phase evidence](benchmarks/systemic-fleet-selector-20261018.json). Every prior measured family, owner, gap and failed stage remains in the preceding immutable artifacts and sections below.

Fleet hydrates full card bodies for a selector that renders only identity, title, project, lane and active-dispatch hints. The source reuses the existing compact card-options query, adding the exact prior recent-first SQL ordering while preserving alphabetical ordering for other selectors. No card content, dispatch/admission logic, limits, warning/deadline threshold or execution default changes. **92 scoped regressions pass**, including field/order/realm parity, exclusion of body hydration, active-dispatch hints, existing page rendering, remote admission, membership and UI state.

In 30 alternating reads of 331 disposable cards with 64 KiB bodies, full reads p50/p95/p99/max were 236.748/293.538/360.775/360.775 ms; compact reads 44.584/81.448/83.618/83.618 ms. Materialized body characters fall from 21,692,416 to zero with exact selector field and ordering parity. Normal GC remains enabled. This isolates one phase, is not a whole-page causal comparison, and is not deployed acceptance. Current loaded309 Fleet still has five HTTP200 samples at 1.11 MB each, p50 2.201 s/max 2.946 s. Numeric overview sizes are 734 KB in node dimensions and 212 KB in edge details. Report d82ea21b revision2 retains the correlations and uncertainty; the full context/template/serialization work remains open.

PR500 merged **e3ac8ae1** from watched0794810d after seven stable supervisor observations and independent five successful checks, complete reviews/threads/rules/authorship validation, and protected head-CAS merge. Source and merge trees match8b343246. Integration CI39091247489 is tracked separately; no deployment is inferred. The server still independently fingerprints **309751f5/packagecad7**, which includes backupPR498 and schemaPR499. The compact selector and object-read repair are not installed. Backup actual-scope independent acceptance and systemic requirement17d034a6 remain scheduler-owned; no repairer acceptance receipt exists.

Previous object-read mixed candidate retains9,600ordered events/zero worker warnings/failures/timeouts/rejections but four500ms inventory violations(max1.478s), plus separate startup warnings. That result remains failing acceptance under the unchanged criteria. All startup/MCP CPU, nativeSQL/WAL/GIL/scheduling/disk/GC, valid nonempty branches, peers/builds/logretention and receipt gaps remain required. Original10attachments are preserved; no new worker/card, productionbackup/deletion/fault, or competing backup scope was introduced.

---

# Loaded309 and immutable-object read checkpoint — 2026-10-18

The systemic card remains **open:520rows/42concerns**, all140 retained slow names,181 baseline operations,115routes and every prior owner/outlier preserved. [Full matrix](performance-coverage.json); [complete numeric stages/activation/coverage](benchmarks/systemic-object-read-20261018.json). This section supersedes older current-status labels; historical failures remain below.

Protected PR499 is merged309751f5 from watched67425383; head/main CI5success each and95merged integration tests/22subtests passed. The merge includes backupPR498/8045, so its tree differs from PR499head; this is explicitly retained. Exact immutable309 was installed with frozen unchanged dependency versions/config86968, all436source files/VCS/hash verified. Owner API verifies newPID81657/startup3a55a396/loadedsource309/packagecad7. This proves local activation, not whole-family acceptance. Existing backupdispatchc5a697e5 received the requested authority/loaded-build reply as acceptedqueued prompta8e9cb73; consumption not asserted. Backup independentrequirementb5271340 remains scheduler-owned, including the prior12.78scardwrite and2.265ssource failure. No productionbackup/deletion/newworker was performed.

Confirmed read amplification:512successful immutable object reads caused512 extra Python metadata probes. Source reads bytes first, preserving missingNone, original fallback for other read failures, uncached repair visibility, all hash/version/traversal/fsync/durability guards and counters.44targeted regressions pass. Alternating30batches/512objects eliminate15360Pythonos.stat calls per payloadsize. At512B, p50/p95/p99/max baseline50.472/75.847/85.119/85.119ms versus source39.603/72.127/74.756/74.756ms. At16KiB, p95/max109.784/149.565→66.766/76.833ms. Exact bytes/counters match. These are warm-filesystem samples; native syscalls/physical disk latency are not measured by Python stat counts. Earlier candidate161.892ms tail remains preserved. New candidate is **not installed**.

Integrated309 mixed replay retained9600ordered events/0operation failures/timeouts/rejections, but5steadyworker warnings/7sampled violations: complete-message HTTP3,Fleet1,fullobjectcollect3. Complete-message max1146.729ms, inventory2085.686ms; startup schema1092.112ms and premeasurement warnings remain separately visible. Three message-worker warnings, low-CPU PRcompletionSQL1398.588ms and inventory1978.026ms spent in512gets do not establish a single common native cause. Host is uncontrolled; late targeted tests overlapped this before stage. Numeric monotonic duration is82.43s; calendar execution gaps are not attributed to a specific mechanism.

Candidate 100-iteration/29-request replay retains 9,600 ordered events with zero worker warnings, failures, timeouts or rejections, but four full-inventory acceptance violations remain (max 1,477.686 ms). The existing inventory warning threshold is 2,000 ms; the declared acceptance threshold remains 500 ms. Neither threshold changed. Queue max 0.071 ms, service max 1,475.629 ms and CPU max 328.368 ms leave native filesystem/scheduling/GIL attribution open. Every operation's distributions/counts/bytes/phases and all failures remain in the artifact. This is not a paired causal comparison or deployed acceptance; all startup/provider/peer/branch/native attribution gaps remain.

Live ordinary20sequential owner JSON GET samples each: statusp95/max150.557/226.057ms, runtime133.415/133.870ms, health64.097/246.840ms;0errors in those samples. Re-encoded JSON sizes are not wire bytes. An initial JSON-only observer incorrectly handled15HTML attempts and omitted original status/correlation; those errors are preserved as an observer gap, not proven HTTP failures. Corrected ordinary HTML reads returned HTTP 200: Agent max 275.523 ms and Home max 89.120 ms, but all five Fleet samples exceeded 500 ms (p50/p95/p99/max 2201.105/2945.675/2945.675/2945.675 ms; 1,114,058 bytes each). Report d82ea21b retains exact correlations; server/template/serialization/transport/shared contention attribution remains open. These five-sample tails equal the maximum and do not establish broader page acceptance. No body/credentials are retained; no force-provider/MCP/peer probe occurred. NativePA tools were absent from this turn's available catalog; supported owner API fallback was used, cause unverified.

Independent systemicrequirement17d034a6 remains unchanged: integratedonly, active/verified/acceptancemissing, acceptedfalse. No repairer receipt. Remaining schema/rendering/Kernelroute/MCPregistrationCPU, SQL/nativebusy/WAL/GIL/scheduling/disk/GC, all uncovered validbranches, peers/build/logretention/receipts and backup acceptance still require work. Original10attachments and all historical stage artifacts are preserved.

---

# Startup schema repair and complete coverage checkpoint — 2026-10-17

The systemic card remains **open: 520 evidence rows/42 concerns**, retaining all 140 slow names,181 baseline runtime operations,115 baseline routes,6 page families and every prior failure/owner. These are evidence records, not distinct incidents or independent causes. [Complete matrix](performance-coverage.json); [new numeric evidence and all phase/coverage details](benchmarks/systemic-startup-schema-20261017.json). Earlier sections below remain historical; this checkpoint supersedes their current-status labels.

Confirmed startup amplification: schema discovery constructed3,952 Pydantic adapters, including adapters immediately discarded before rendering. The candidate uses call-local FastAPI 0.136.3 bindings to read metadata during discovery and skip those discarded adapters; original validation/serialization/rendering remains intact. Other framework versions use the public generator. The complete481-path, 792,712-byte schema has identical canonical SHA d4e0c89c. Three cold samples per mode: baseline p50/p95/p99/max3442.814/3781.848/3781.848/3781.848ms; candidate1175.659/1452.912/1452.912/1452.912ms,3952→142adapters. First pair overlapped end of scoped tests; host load is uncontrolled and three-sample tails equalmax. A serialization experiment showed no clear benefit and was removed, with evidence retained.

Full schema generation now uses the existing bounded executor and remains required before readiness. A per-app cache lock prevents concurrent duplicate generation. Ready flags publish on the loop only after actual completion; failed/cancelled startup cannot be made ready by a late worker.37final regressions and6subtests pass, including complete PA and variant schema parity, callbacks/webhooks, concurrent isolation/cache, unknown-version fallback and failure/cancellation fencing. No numeric warning/deadline threshold, execution default, validation, durable state or admission rule changed. Initial head CI39076626460 later failed the new immediate-failure counter observation and an unchanged snapshot200ms deadline test. The readiness test now yields once to its already-queued accounting callback;33corrective tests/2subtests pass, no timeout/threshold or product change. Original failures remain in the numeric artifact and reportf697bc31. Full corrected-head CI is required.

**Startup acceptance still fails.** The final offloaded cold sample records schema 1450.171 ms, capability write 1340.734 ms warnings and startup 1658.313 ms after build; schema worker completion delivery3.667ms. Prior inline sample retained schema1407.822ms and two fast-worker completion deliveries1479.650/1476.818ms. Native/GIL/disk/GC attribution remains incomplete. Kernel boot plus app construction8958.477 ms is a separate open phase. All schema samples still exceed500ms; source improvement/offloading is not acceptance.

Final25module-hash-matched100iteration/29request mixed source replay:9600ordered retained events in90.066s,0operation failures/timeouts/rejections, **3worker warnings and7sampled threshold violations** (reconcile1,Fleet2,fullobjectcollect4). Startup schema1592.018ms and other premeasurement warnings are separate, not excluded from acceptance. Statusp50/p95/p99/max50.727/72.982/113.188/169.306ms; runtime34.678/44.433/64.525/80.111ms; drain104.698/227.555/355.701/434.143ms; complete-message HTTP189.576/367.206/410.528/421.781ms. Fullobjectcollect402.205/2218.833/2218.833/2218.833ms (10samples), loopmax242.610ms. These are declared local fixture results, not real-provider/network/deployed acceptance.

Prior integrated source175ccb4f is independently **loaded locally**: PID69926/startup6c897a27/packagee708 matches435installed source files, unchanged configuration86968. This new schema candidate is uninstalled. These observations are not fleet/deployed-family acceptance. The new artifact preserves all 135 runtime names/38 routes from the22:37Z owner snapshot, their exact counters/bounded windows/maxima, and three newly observed normal retry operations plus one route. No new broad log/peer audit was performed.

The overwritten uncommitted cb33raw snapshot remains an explicit observability gap: derived counts/top windows/three rows and durable progress survive; complete original per-name raw windows do not. New observers retain SHA-addressed archives. No current raw file is substituted as cb33. Original 10 attachments remain at quota. Historical dropped logs/status-file failures, peer failures/unknown identity/build/logs and older retention gaps stay open.

Every row has an owner and next action. Group counts assign all 520 rows once; ownership is sequential in this same leased executor, not a claim all branches have been exercised or that parallel workers were created.

| Remaining work group | Rows | Owner / next action |
| --- | ---: | --- |
| Shared SQL, sync and transcript |153|This session: separate successful native waits, WAL/checkpoint, GC/CPU/disk and lock convoys; repair measured causes without weakening durability/order/CAS/receipts.|
| Startup and MCP admission |16|This session: remaining schema rendering and boot/app-build CPU; full provider/MCP/workspace cold starts and bounded stage attribution. UX batch retains its saved admission identities.|
| Endpoint and background branches |335|This session: valid nonempty paths, payload/serialization/call amplification and realistic page/API responsiveness, including the normal saved retry path.|
| Peer/log coverage |2|This session: supported peer identity/build/runtime/log evidence; retain unavailable peer-log/older-retention exclusions.|
| Receipt and recovery |7|This session: preserve ambiguous outcomes, original keys/correlations and bounded supported recovery; never infer non-commit from missing receipts.|
| Exact activation and acceptance |4|This session for protected integration/activation/retest; PA scheduler for final independent acceptance under unchanged17d034a6.|
| Backup-linked scope |3|Existing repair dispatchc5a697e5/session18fc3aac; shared-cause coordination only. Failed12.78scardwrite verification and actual-scope independent acceptance remain required.|

Database89957d94's accepted originalPR471 scope stays closed. Backup0331d21a has its own actual-scope independent requirement. Systemic card staysActive with integrated evidence only; active/verified/acceptance remain missing. No merged PR, source fixture, startup notification or activation closes the whole card.

---

# Transcript/backend continuation checkpoint — 2026-10-17

The systemic card remains open: **510inventory rows/41concerns**. These are path/evidence records, not independent incidents or root causes. All140retained log slow names,181baseline runtime entries,115baseline route groups and prior evidence/owners remain. [Full matrix](performance-coverage.json); [numeric stage history](benchmarks/systemic-transcript-backend-20261017.json).

PR496 merged646aff54 from exactwatched4c35fb15 after stable6observations and independent5terminalsuccess head checks, complete review/rules/attribution and CLEAN/head-CAS merge. Head/merge trees matchca16cc02. **Subsequent mainCI39053124584 failed** the unchanged40ms loop assertion at301.933ms; no retry hides it. Source is uninstalled. Review caught effective ref-budget phase selection; new ref metrics are now observation-only, preserving previous deadline behavior before activation.

Message completion reuses transcript handles only within one actual worker invocation:30identical3200chunk pages use180→30connections and720→420SQLcalls, same complete2560000characters/SHA. Public replay p95148.125→127.866ms. Direct calls still close on exit; failed borrows roll back/close; active nested borrows never alias; callbacks follow the current budget; a timed-out worker retains its handle until actual completion. Busy timeout/durability/order are unchanged.

Explicit lag reset now separates pre-boundary timer lateness without hiding a40ms post-reset stall. Pre-window delay remains separately visible. The performance fixture now declares production-default loop debugFalse while retaining every20/40msassertion and12loader requests plus readiness/probe. Functional tests retain debugTrue and real stalls are tested in both modes. Local debug gaps were CPU-bound with thread starts<0.3ms and no long import/GC; the same production-profile workload passed. This corrects validation context, not production flags or an established diagnosis of the exact LinuxCI callback.

Public AnyIO backend preparation now happens once during runtime startup before requests need it. Cost/counters/loop-threadCPU and unchanged-threshold warnings remain visible; no workers/capacity defaults are added.47latest tests pass, including unchanged responsiveness bounds, both debug modes, startup close/concurrency, transcript order/timeout/rollback and ref deadline guards; earlier failed local/CI cases remain in the numeric artifact.

Final100iteration/29request source replay matches21module hashes and retains9600ordered events,0operation failures/timeouts/rejections. **Acceptance still fails:8worker warnings/7sampled violations.** Six summary-write warnings form a convoy; firstSQL675ms/43msthreadCPU then followers wait743–1117ms. Card history/reconciliation/Fleet/full inventory tails remain. Message page max460.192ms and drain max293.521ms in this sample do not close all branches or installed behavior. Backend preparation6.095ms is retained separately; full schema startup still blocks callbacks~2.8s in premeasurement.

Bounded SQL-call instrumentation retained37selected invocations; only2of40slow SQLcalls half-overlapped long GC. One190msfetchall overlapped184msGC; several low-CPU193msPRAGMA/INSERT/SELECT calls did not. Native busy/filesystem/scheduling and database-role attribution remain open. Reverse/JSONaggregate transcript query variants worsened p95 and were rejected. Object probe removes15360Pythonos.stat calls, with modest p95 improvement but161.892mscandidate tail; no source change. A private FastAPI flatten prototype removes3640adapters and cuts schema2516.682→1008.710ms with identical481path canonical schema, but remains over500ms and is not committed/deployed.

Existinghandofff6c4e9e1 continuation remains queued at last read; the normal session queue/controller owns delivery, with no competing restart. Backup verifier8d373b2d remains scheduler-owned; original DB89957scope staysaccepted. Original real-provider/peer/log/leaf and independent-agent acceptance requirements all remain. The prior sections below are retained historical stages; current follow-up fields supersede old source-stage labels without erasing failures.

---

# Ref-phase and parent-proof checkpoint — 2026-10-17

The systemic card remains open: **501findings/34concerns**, including all140retained slow names. Every earlier row, outlier and owner remains. [Full matrix](performance-coverage.json); [new numeric evidence](benchmarks/systemic-ref-phases-20261017.json).

PR495 mergedf36d4b38 from exactwatchedf4efd94c after stable10observations and independent5terminalsuccess checks,0requiredapprovals/threads, complete review pagination, attributed commits and CLEAN state. Head/merge trees match62b48efe. Both head and mainCI are5/5green. Activation remains unverified.

The next repair avoids full-history materialization for a canonical direct child of the exact ready index head. A paired10000commit fixture removed300000hashes/20.1MB across30checks; p50/p95/p99/max17.455/20.961/31.852/31.852→2.927/5.308/5.919/5.919ms. The smaller separate1000commit runs are retained, including p50 variation rather than a blanket speedup claim. Forks, additional/missing parents, corrupt tips and non-ready indexes keep authoritative traversal.

Bounded observations now distinguish ref RLock versus advisory file lock, file and directory fsync, rename, and logical object reads/new writes. File fsync→replace→directory fsync and failure cleanup remain unchanged. Byte counts are logical content/attempts, not physical device writes or commit proof; wall intervals include scheduling/GIL delay. No paths, SQL or payloads enter metrics, and no warning/deadline changes.

65broad tests passed;27final focused tests passed in an overlapping run after object counters were added. A further23focused tests pass, including270writes proving lifetime totals remain exact beyond256percentile samples. New source is uninstalled. Managed handoff f6c4e9e1 still has continuation_queued/delivered_atnull; no competing restart. Backup verifier8d373b2d remains scheduler-owned/dispatched with another pending intent; no acceptance asserted. Systemic completion still lacks active/verified/independent acceptance; no repairer receipt created.


Final100iteration/29request stage: all21module hashes match;9600ordered events;0operation failures/timeouts/rejections, but5worker warnings/11explicitthreshold violations remain. Completed-worker logical totals:442atomic attempts/114371intended UTF8bytes,442file+442directory fsync calls,7107object reads/3360600returned bytes,274new object files/146907bytes. No helper fsync failures observed; SQLite/repair/native device IO is outside this census. A725.949ms sync push spent523.854ms waiting for Python/projection locks versus0.131msfilefsync; card history1049.585ms had1037.065msSQLwall but30.102msthreadCPU. The3089.536ms full inventory invocation spent2952.993ms in512object gets/259220bytes,329.248msthreadCPU; filesystem/scheduling attribution remains open. Existence-probe amplification, per-SQL-call attribution and transcript row production are next named investigations; no new fix is claimed for them. These specific samples leave native busy/scheduling and other shared causes unresolved. Earlier phase stage5warnings/12violations and every prior outlier remain.

Mixed stages and exact hashes are retained in the numeric artifact as they finish. Their outcome is never assumed. All preceding checkpoint sections below are historical evidence; current follow-up fields supersede old source-stage labels without deleting failures.

---

# Current continuation checkpoint — 2026-10-17

The systemic card remains open. The matrix now retains **499 findings**, including **140 slow names** and **32 concerns**. All prior rows, stage failures and owners remain. Detailed measurements: [read-repair artifact](benchmarks/systemic-read-repair-20261017.json); [full matrix](performance-coverage.json).

PR492 merged as4811cca after stable-green and independent head checks; head/main CI each passed5checks. Installed startup source4297 is independently fingerprinted. The merge also contains concurrent main changes, so its entire tree is not deployed. The unchanged asset build ID is not proof of loaded Python source. The new follow-up source below is not installed.

Confirmed sync amplification: ten complete1000commit collections fell from3000 to2000file reads each, with identical canonical bytes (p95533.820→246.233ms). Twenty protocol-v3 exchanges with a nearly current synthetic peer fell from512 to8reads each (p9583.867→5.110ms), retaining2objects and3HTTPcalls per exchange. Public scripts replay both fixtures. Cold catch-up and shared-ancestry/corruption/spill correctness are covered by60passing sync regressions. These local fixtures do not establish real-network acceptance.

The expanded100iteration/29request fixture retained9600ordered transcript events with no operation failures/timeouts/rejections, but **34worker warnings and54threshold violations**. It adds Home/Work/runtime and bounded512object inventory builders. Full inventory pages reached5192.888ms and transcript drain2257.018ms. The short-prefix optimization does not close this hard-page workload. Earlier corrected4297 replay had6warnings/11violations; both stages and every prior failure remain in the numeric artifacts. An exact final-source replay is recorded separately when complete; no passing result is assumed.

Additional scoped changes reserve bounded read-only runtime diagnostic capacity under the unchanged5second deadline, separate immutable loaded source from mutable installation metadata, and explain stale negative receipt proofs while preserving unknown effects and explicit recovery. Specific historical504/fork timeout causes remain unverified; root retains the original fork key and recovery. No new worker/request was launched.

Fresh macbook logs cover16:13:50Z–16:34:42Z:5842records,2104279bytes,60slow names,704raw emissions including352structured phase records and copies. The window spans old/new processes and overlapped fixtures; all per-name count/p50/p95/p99/max and numeric phases are retained. Exact reported correlations were absent in this bounded window, leaving a coverage gap. No peer-log or exhaustive retention claim is made.

Managed handoff quiescence took254.656s and restart-to-resume114.210s; continuation remained queued at the last read. These are active-work lifecycle intervals, not a cold-start causal attribution. No competing restart was requested. Backup's existing scheduler verifier is now dispatched8d373b2d with another intent pending; no acceptance or competing dispatch is claimed. DB89957's accepted original scope stays closed. Independent final acceptance remains scheduler-owned under unchanged requirement17d034a6.


The pre-rebase follow-up source100iteration replay completed with all18module hashes matching:9600ordered events,3worker warnings/17threshold violations,0operation failures/timeouts/rejections. It lasted89.934s; runtime GETp95/max28.727/39.415ms, status76.714/136.509ms, full512-object builder1393.967ms max, drain513.647ms max. This remains a failing acceptance scenario; host variability and remaining shared causes prevent causal attribution of the change between stages. All prior outliers are retained.

The branch then incorporated concurrent PR494 Work discovery changes. The mixed replay predates that rebase;17of18attested modules remain identical (items.py also includes the unrelated Work change). 25Work/receipt/runtime/status regressions pass after rebase and validate integration separately; full head CI is required.

The following sections retain the preceding checkpoint verbatim as historical evidence. Current follow-up fields in JSON and the artifact above supersede stage labels such as “not merged/deployed yet”; no historical failure is erased.

---

# Systemic PA performance coverage — 2026-10-17

The card remains open. This consolidates the prior449-row repository matrix and both durable card deltas (452 rows), then accounts for every additional live operation/route and control-plane concern. Every unresolved row retains its owner and next action. Source, merge, loaded build and acceptance are distinct.

Full matrix: [performance-coverage.json](performance-coverage.json), 492 rows. Fresh numeric source: [resume artifact](benchmarks/systemic-resume-20261017.json). Original log/fixture stage evidence: [systemic-inventory-v4.json](benchmarks/systemic-inventory-v4.json).

Fresh owner API confirms macbook0c7d8ecb, PID51003, loaded1.5.9+5caf83e4325a. Status3990.910ms/runtime104.714ms are individual client reads. All154 runtime operations and all observed registered routes are retained, including failures and outliers. Runtime percentiles use bounded windows; counters/maxima can span process lifetime. Loop maximum1064316ms is not attributed to CPU: host suspension remains possible. No peer log coverage or fleet-wide acceptance is claimed.

The status-count fixture retains331cards/359sessions with64KiB synthetic body/metrics each and10active knowledge entries.30reads:20850hydratedmodels, p50/p95/p99/max1179.402/3519.475/3865.728/3865.728ms. Aggregate:0models, identicalcounts,0.060/0.707/3.123/3.123ms.32status/count/CLI/database regressions pass;24lock/runtime/database regressions pass in a separate overlapping suite. New source is not merged/deployed yet. Bounded lock/long-GC attribution does not itself repair a tail.

PR484–488 are integrated, with exact head/merge/watch evidence retained in JSON/card. Their earlier mixed residuals remain:2worker warnings and4sampled violations. Current100iteration mixed replay retained9600ordered events but has18worker warnings/31explicit500msviolations. Statusp50/p95/p99/max59.301/146.942/165.040/198.334ms.41GCpauses>=10ms retained;3slow invocations have overlapping long GC spans (timing overlap only, not sole attribution). Bootstrap2callbacks~13s with<6msservice remain separate. No failure is discarded. Original log inventory covers16retained files,86440227sourcebytes,570983physical lines,2026-10-08T03:25:05Z–2026-10-16T07:10:18Z;110slow names/22prefixfamilies. Active-file growth, duplicate emissions, unknown timestamps, earlier retention and uninspected peers remain exclusions.

Database89957d94 is accepted/Done for original PR471 scope. Backup0331d21a has scheduler-owned independent verification on its actual scope; this session does not dispatch or edit for it. Systemic final acceptance is now scheduler-owned independent_agent_v1 revision17d034a6 with unchanged criteria and0receipts. Every unresolved family remains this systemic card's responsibility.

## Concerns and next action

| Finding | Evidence/cause | Next action |
|---|---|---|
| Sync/global mutation lock | Confirmed shared critical-section and ancestry-query amplification in isolated Kernel fixtures; not every historical outlier attributed. | Exercise remaining sync reads/object collection and every affected waiter under mixed load; inspect supported peer evidence. |
| Payload and copy amplification | Shared lock and hydration candidates; fixture cold start is not real-provider/network acceptance. | Measure recovery/history detail paging, actual MCP/provider/workspace phases and normal prompt admission. |
| Event loop and executor phases | Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Transcript/wire durability and drain | Transcript/wire serialization and shared contention are candidates; isolated batching/order verified, historical external provider tails not fully attributed. | Verify wire read/write, cancellation, reconnect and real-provider startup/drain separately from fixture provider. |
| Progress/dispatch/reconciliation amplification | Durable checkpoint/sequence work and repeated hydration can amplify contention; enqueue remains a source-inspected synchronous path. | Measure normal enqueue, heartbeat, sequence and checkpoints under mixed load; preserve fences and durable acknowledgement. |
| Fleet/provider/selection | Some local status/copy amplification confirmed; peer failures and provider/probe service time not causally attributed. | Measure placement/provider/catalog/overview branches; separate external probes and local CPU; avoid changing admission semantics. |
| PR supervision and external dependencies | External GitHub/peer time and local ledger cost must be separated; historical maxima alone do not prove cause. | Measure external snapshot vs local lease/read/write/effects; verify bounded repeated-failure/backoff behavior. |
| Pages and API responsiveness | Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Backup status and manifest failures | Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Workspace maintenance and leases | Git/filesystem/maintenance costs vs lease-store contention unresolved. | Profile isolated representative provision/renew/GC/maintenance with live-worktree fencing assertions. |
| Every remaining operation family | Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Peer stalls and repeated failures | Some local status/copy amplification confirmed; peer failures and provider/probe service time not causally attributed. | Measure placement/provider/catalog/overview branches; separate external probes and local CPU; avoid changing admission semantics. |
| Log loss, provenance and coverage | Shared contention/CPU/copy amplification is a hypothesis for this leaf; exact cause unconfirmed. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Attachment timeout and receipt recovery | Confirmed attachment canonical events omitted request-key/fingerprint/result recovery metadata. Historical transport timeout and missing recovery-key exposure remain separate unverified causes; future receipt repair does not invent historical receipts. | Reproduce this exact path in an isolated representative mixed workload, split phases, repair measured cost and retain correctness assertions. |
| Incoming fork publication and summary writes | Confirmed incoming fork published ref before primary admission; a concurrent canonical writer then saw a stale fenced parent. Deterministic CAS-retry regression verifies the corrected publication boundary. | Exercise remaining sync reads/object collection and every affected waiter under mixed load; inspect supported peer evidence. |
| Quadratic complete-message compaction | Confirmed repeated text prefix concatenation:3199copies/4097279200characters for3200chunks. Linear grouping preserves exact output with1copy/2560000characters. | Verify wire read/write, cancellation, reconnect and real-provider startup/drain separately from fixture provider. |
| Startup classification and same-request recovery | Project metadata was not settled before materialization inputs froze. Existing PR479 resolves canonical catalog/repository context or blocks uncertain context on the same request/card. | Verify actual deployed build and same-request behavior through supported authority; preserve defaults and receipt identity. |
| Known disabled summary metadata races card version | Confirmed optional disabled-summary metadata was a second canonical write after creation, advancing the caller's card version. Initial-event metadata removes that write without weakening CAS. Missing optional-service registration is separately covered by degraded-kernel regressions. | Run each uncovered SQL operation with concurrent sync/transcript/control traffic; measure native busy and physical writes without changing durability. |
| Integration-only automatic completion exceeds card scope | Canonical card had no machine-readable requirement; completion_state treats that legacy state as accepted after integration. Five automatic Done transitions observed. | Finish all existing repair/activation/verification requirements and let normal PA scheduler own independent final acceptance; never impersonate receipt. |
| Duplicate durable human enqueue checkpoint | Confirmed source and same-host measurement: human activity and cursor are already included in the first checkpoint; synchronous transcript flush schedules the same events without changing recoverable state. Second checkpoint duplicates that durable write. | Integrated source verified; measure normal admission under larger mixed queues and installed workload. |
| Full Kernel schema warm-up blocks completion delivery | Timer around full-schema generation confirms loop-thread CPU and simultaneous completion delivery delay. It does not attribute every live historical loop stall. | Reduce or isolate schema CPU while preserving readiness/admission and verify full cold boot; retain remaining GIL/completion delays. |
| PR metrics and health hydrate proofs to count lifecycle | metrics() and authority_health() decoded all retained watch policy/state to calculate counts, ownership and maximum active fence; source and3000row/193MB probe confirm unnecessary hydration. | Complete supervised integration and exact-head checks; exercise nonempty active-watch API and real external poll/effect paths, then installed acceptance. |
| CLI status writer-capable bootstrap, full count hydration and provenance | status calls Kernel.boot(defaultclaim_writerFalse), which constructs Store/CardProjection with schema/migrations; snapshot fullyhydrates items/sessions for counts and reports its ownprocess/version. No live CLI execution or causal attribution to historical latency/logloss. | Integrate stable-green exact head, activate exact build only through supported lifecycle if needed, retest actual status/CLI and mixed workload; retain other causes. |
| Delayed obsolete and post-merge PR notifications | Repeated queued notifications for PR484–488 arrived after watched heads changed or retired. Fresh GitHub/watch revalidation prevented repeat repairs/merges; delivery cause unverified. | Diagnose observed delivery/receipt behavior with exact IDs; preserve pending versus non-commit, bounded retries and sole-writer semantics. |
| Checkpoint timeout and passive receipt recovery | Two native progress writes returned unknown outcome; first reconciliation queued. Policy edit validation was separately recovered and corrected with explicit fieldintent, not a latency fix. Recovery job later completed but receipt lookup remained pending; success is not inferred. | Diagnose observed delivery/receipt behavior with exact IDs; preserve pending versus non-commit, bounded retries and sole-writer semantics. |
| Independent verification watch owner capability omitted | Current loaded supervisor advertises completion-requirements:v1 but omits implemented completion-agent-verification:v1, so protected watch492 remains unowned. Source uses existing runtime capability derivation; exact positive/negative lease regression passed. | Integrate/activate exact source through protected lifecycle and verify deployed behavior; do not relax independent acceptance or unknown receipt replay. |
| Checkpoint authorization dispatch read blocks serving loop | Initial synchronous DispatchStore.get may yield to writer and deep-copy full history before bounded ProgressService read. Read-only full native dispatch measured7413ms/403214JSONcharacters; exact historical timeout causality unproved. Initial read now uses existing bounded progress.dispatch_read; latch regression preserves access/protocol checks and proves event-loop progress. | Integrate/activate exact source through protected lifecycle and verify deployed behavior; do not relax independent acceptance or unknown receipt replay. |

## Every original retained slow operation

Raw threshold-selected emissions may duplicate incidents. Milliseconds; distributions cover the declared original retained log window, not a fresh deployed replay.

| Operation | Count | p50 | p95 | p99 | Max | Disposition |
|---|---:|---:|---:|---:|---:|---|
| `sync.reconcile_head` | 194 | 1768.0 | 78827.0 | 305836.8 | 305836.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.push_transaction` | 346 | 1083.9 | 37486.4 | 219936.0 | 242337.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.execution_binding_materialize` | 14 | 73205.6 | 197942.1 | 197942.1 | 197942.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.card_write` | 26 | 16745.0 | 159863.4 | 159863.4 | 159863.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.transcript_append` | 2823 | 974.0 | 3561.9 | 6756.8 | 40779.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.object_collect` | 165 | 3692.1 | 21191.6 | 29969.8 | 29969.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `intake.web_prompt` | 18 | 1694.7 | 24703.5 | 24703.5 | 24703.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `workspace.collect_garbage` | 70 | 2336.0 | 11321.1 | 20621.3 | 20621.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.mcp_bootstrap` | 80 | 5008.8 | 7243.3 | 18547.3 | 18547.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.sync` | 888 | 860.4 | 2317.5 | 4399.3 | 15992.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.expire_capacity_reservations` | 99 | 1350.9 | 4939.2 | 15260.6 | 15260.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.recovery_sessions` | 1329 | 768.3 | 3427.0 | 5487.0 | 12250.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.heartbeat_write` | 286 | 873.0 | 3201.8 | 4764.1 | 11847.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_due_read` | 108 | 731.3 | 3186.9 | 10979.9 | 10979.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.transcript_read` | 618 | 847.4 | 3581.6 | 4698.5 | 10929.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.dispatch_read` | 600 | 726.8 | 1919.8 | 3359.2 | 10889.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.commit_read` | 77 | 811.3 | 1722.0 | 10817.9 | 10817.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview_cache_write` | 1257 | 745.8 | 2886.6 | 5721.1 | 10795.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.transcript_message_page` | 726 | 2715.9 | 6492.7 | 8217.9 | 10120.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_completion_due_read` | 486 | 764.9 | 5910.5 | 7618.0 | 9406.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `acp.wire_append` | 1558 | 747.4 | 2095.7 | 3608.5 | 8740.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.agent_session_save` | 340 | 876.3 | 3667.5 | 7294.1 | 8255.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.sequence_allocate` | 372 | 810.3 | 3349.5 | 6649.1 | 8185.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `workspace.project_provision` | 6 | 6536.4 | 7903.7 | 7903.7 | 7903.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `reconciliation.completion_evidence_read` | 335 | 693.4 | 2140.5 | 4472.9 | 7887.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.project_recovery_availability` | 140 | 811.2 | 1891.9 | 7885.1 | 7885.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.startup_state_read` | 12 | 6216.3 | 7652.2 | 7652.2 | 7652.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.history_payload` | 388 | 1701.4 | 3995.1 | 5482.8 | 6741.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `workspace.lease_renew` | 54 | 725.4 | 2829.8 | 6713.0 | 6713.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.runnable_read` | 26 | 2585.1 | 5541.2 | 5541.2 | 5541.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.ref_read` | 111 | 721.5 | 2416.9 | 5395.5 | 5395.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.agent_runtime_checkpoint` | 6 | 909.3 | 5356.8 | 5356.8 | 5356.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `reconciliation.dispatch_read` | 359 | 756.7 | 2757.2 | 3795.3 | 5069.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.completion_pending_read` | 102 | 807.5 | 2676.4 | 4968.7 | 4968.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_capability_write` | 18 | 1069.2 | 4943.0 | 4943.0 | 4943.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.pending_read` | 136 | 799.5 | 2258.6 | 4615.0 | 4615.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.checkpoint_write` | 122 | 789.9 | 3034.8 | 4471.0 | 4471.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.followup_pending_read` | 58 | 815.5 | 2760.2 | 4368.2 | 4368.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.waiting_read` | 56 | 1631.4 | 3539.6 | 4150.4 | 4150.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.placement_resolve` | 30 | 1090.3 | 4017.6 | 4017.6 | 4017.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `reconciliation.supervisor_health` | 16 | 812.3 | 3916.7 | 3916.7 | 3916.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.card_read` | 44 | 952.6 | 2839.3 | 3834.5 | 3834.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.json_decode` | 12 | 741.8 | 3773.6 | 3773.6 | 3773.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `provider.status.cursor` | 122 | 2360.5 | 3340.0 | 3733.0 | 3733.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.restart_handoff_completion` | 408 | 761.6 | 1723.0 | 2485.9 | 3724.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_lease_write` | 26 | 990.0 | 3683.3 | 3683.3 | 3683.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `reconciliation.session_read` | 126 | 700.1 | 2557.9 | 3611.9 | 3611.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview_cache_read` | 1297 | 741.1 | 2275.9 | 3179.3 | 3382.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.repository_read` | 46 | 781.9 | 3336.0 | 3370.1 | 3370.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview_response_json` | 62 | 766.9 | 2782.4 | 3254.7 | 3254.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.status` | 85 | 695.4 | 1657.8 | 3201.5 | 3201.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `provider.status.codex` | 122 | 946.5 | 2334.5 | 3108.4 | 3108.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.record_write` | 12 | 982.6 | 3107.7 | 3107.7 | 3107.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.reconcile_interrupted` | 12 | 2693.5 | 3038.0 | 3038.0 | 3038.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `notifications.list_page` | 48 | 938.8 | 1922.8 | 3001.3 | 3001.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_watch_write` | 42 | 1408.5 | 2806.4 | 2973.7 | 2973.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.restart_handoffs_pending` | 124 | 699.7 | 1466.6 | 2965.2 | 2965.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `preferences.agent_auto_approve_read` | 32 | 720.7 | 2950.5 | 2950.5 | 2950.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_metric` | 46 | 911.1 | 2642.5 | 2914.7 | 2914.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `health.canonical_obligation` | 16 | 884.9 | 2906.8 | 2906.8 | 2906.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_event_write` | 38 | 789.1 | 2880.5 | 2880.5 | 2880.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `runtime.status_snapshot` | 6 | 2560.3 | 2801.9 | 2801.9 | 2801.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_watch_read` | 172 | 805.3 | 1811.6 | 2735.1 | 2735.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.restart_handoffs_watchdog` | 146 | 688.4 | 1825.8 | 2686.5 | 2686.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `filesystem.github_credentials_read` | 8 | 2135.6 | 2575.2 | 2575.2 | 2575.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.project_read` | 30 | 731.1 | 2540.1 | 2540.1 | 2540.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `goal.dispatch_lifecycle_index` | 24 | 926.4 | 2436.1 | 2436.1 | 2436.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.restart_handoff_session` | 74 | 783.4 | 2126.8 | 2290.5 | 2290.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_observation_write` | 8 | 820.5 | 2223.9 | 2223.9 | 2223.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.prompt_acceptance` | 6 | 1164.3 | 2178.6 | 2178.6 | 2178.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `pr_supervisor.response_json` | 8 | 646.2 | 2169.6 | 2169.6 | 2169.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.record_read` | 14 | 644.9 | 2087.4 | 2087.4 | 2087.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.agent_session_read` | 30 | 748.5 | 2082.8 | 2082.8 | 2082.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `pr_supervisor.github_response_json` | 26 | 655.5 | 2082.1 | 2082.1 | 2082.1 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.repositories` | 8 | 736.1 | 2003.5 | 2003.5 | 2003.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.resolve` | 16 | 810.4 | 1969.6 | 1969.6 | 1969.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.object_has` | 62 | 736.3 | 1786.4 | 1792.2 | 1792.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.reachability` | 94 | 711.6 | 1185.6 | 1651.3 | 1651.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.prompt_acceptance_read` | 12 | 725.3 | 1608.7 | 1608.7 | 1608.7 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.target_revalidation` | 44 | 729.6 | 1258.9 | 1360.2 | 1360.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_capability_read` | 16 | 695.0 | 1218.5 | 1218.5 | 1218.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `progress.final_report_write` | 6 | 949.9 | 1159.3 | 1159.3 | 1159.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.prompt_outcome` | 6 | 822.0 | 1091.2 | 1091.2 | 1091.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `provider.status.openinterpreter` | 24 | 630.2 | 1065.5 | 1065.5 | 1065.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.provider_resolve` | 2 | 1042.3 | 1042.3 | 1042.3 | 1042.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `workspace.session_leases` | 4 | 511.2 | 1014.9 | 1014.9 | 1014.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `files.browse_render` | 6 | 803.0 | 1012.6 | 1012.6 | 1012.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.snapshot_payload` | 4 | 739.0 | 987.4 | 987.4 | 987.4 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.startup_reconcile` | 8 | 853.4 | 983.8 | 983.8 | 983.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.prompt_identity` | 8 | 658.0 | 982.6 | 982.6 | 982.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `operation.outcome_read` | 2 | 973.2 | 973.2 | 973.2 | 973.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.followup_ack` | 2 | 954.6 | 954.6 | 954.6 | 954.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `conversation.session` | 6 | 699.9 | 939.5 | 939.5 | 939.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `pr_supervisor.metric` | 2 | 903.0 | 903.0 | 903.0 | 903.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_supervisor_effect_prepare` | 4 | 734.0 | 892.8 | 892.8 | 892.8 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.prompt_compose` | 2 | 865.3 | 865.3 | 865.3 | 865.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sqlite.pr_merge_evidence_read` | 4 | 732.5 | 861.3 | 861.3 | 861.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `notifications.present_page` | 22 | 650.0 | 850.3 | 850.3 | 850.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.settings_resolve` | 2 | 746.3 | 746.3 | 746.3 | 746.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.session_snapshot` | 2 | 745.6 | 745.6 | 745.6 | 745.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.cancel_check` | 2 | 657.2 | 657.2 | 657.2 | 657.2 | Open; per-name cause/repair/tests/status/owner in JSON |
| `reconciliation.dispatch_write` | 2 | 628.6 | 628.6 | 628.6 | 628.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.catalog_enrichment` | 2 | 627.3 | 627.3 | 627.3 | 627.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.configuration_receipt` | 2 | 622.0 | 622.0 | 622.0 | 622.0 | Open; per-name cause/repair/tests/status/owner in JSON |
| `selection.target_receipt` | 2 | 596.9 | 596.9 | 596.9 | 596.9 | Open; per-name cause/repair/tests/status/owner in JSON |
| `sync.json_encode` | 2 | 574.6 | 575.3 | 575.3 | 575.3 | Open; per-name cause/repair/tests/status/owner in JSON |
| `workspace.scratch_provision` | 2 | 566.6 | 566.6 | 566.6 | 566.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `agent.recovery_snapshot` | 2 | 559.5 | 559.5 | 559.5 | 559.5 | Open; per-name cause/repair/tests/status/owner in JSON |
| `fleet.overview.activity` | 2 | 542.6 | 542.6 | 542.6 | 542.6 | Open; per-name cause/repair/tests/status/owner in JSON |
| `dispatch.session_link` | 2 | 528.8 | 528.8 | 528.8 | 528.8 | Open; per-name cause/repair/tests/status/owner in JSON |

All181baseline runtime names,115baseline routes,6page families,9startup phases and every added observation remain individually retained in JSON.78slow names/150baseline runtime names/94baseline routes lacked current controlled fixture coverage at the preceding checkpoint; live observations alone do not close those branch/acceptance gaps.


## Fresh macbook log inventory

Supported logs read:2026-10-17T15:41:26Z–16:11:45Z;12543records/6781652bytes, cap100000 not reached.109slow names/6004raw emissions comprise3002structured numeric phase records and matching stderr copies; no distinct-incident claim. Some isolated fixtures/tests ran concurrently on this host. Full per-name numeric phases, source identity/hash and exclusions are retained in the resume JSON. Status-file failures2396/droppedbytes5640760 are unchanged historical counters, not CLI status command failures.

| Operation | Raw count | p50ms | p95ms | p99ms | Maxms |
|---|---:|---:|---:|---:|---:|
| `acp.pa_mcp_owner_probe` | 2 | 764.5 | 764.5 | 764.5 | 764.5 |
| `acp.wire_append` | 910 | 866.2 | 2776.1 | 6326.9 | 6398.5 |
| `agent.history_payload` | 2 | 1599.8 | 1599.8 | 1599.8 | 1599.8 |
| `agent.project_recovery_availability` | 18 | 1083.4 | 1888.6 | 1888.6 | 1888.6 |
| `agent.recovery_sessions` | 82 | 1397.7 | 3478.0 | 5217.5 | 5217.5 |
| `agent.session_snapshot` | 2 | 3060.3 | 3060.3 | 3060.3 | 3060.3 |
| `browser.free_port` | 2 | 515.9 | 515.9 | 515.9 | 515.9 |
| `cards.attachments.publish` | 2 | 632.8 | 632.8 | 632.8 | 632.8 |
| `conversation.issues` | 110 | 1051.6 | 2764.1 | 4151.8 | 4151.8 |
| `conversation.session` | 54 | 1232.4 | 2148.8 | 3773.5 | 3773.5 |
| `dispatch.block_waiting` | 12 | 1125.8 | 2496.1 | 2496.1 | 2496.1 |
| `dispatch.cancel_check` | 2 | 1583.6 | 1583.6 | 1583.6 | 1583.6 |
| `dispatch.completion_pending_read` | 26 | 1203.3 | 3135.1 | 3135.1 | 3135.1 |
| `dispatch.expire_capacity_reservations` | 16 | 2020.2 | 5819.7 | 5819.7 | 5819.7 |
| `dispatch.followup_ack` | 6 | 633.7 | 696.6 | 696.6 | 696.6 |
| `dispatch.followup_bind` | 2 | 940.2 | 940.2 | 940.2 | 940.2 |
| `dispatch.followup_pending_read` | 16 | 724.9 | 5851.2 | 5851.2 | 5851.2 |
| `dispatch.followup_started` | 4 | 728.8 | 888.4 | 888.4 | 888.4 |
| `dispatch.idempotency_read` | 2 | 2963.5 | 2963.5 | 2963.5 | 2963.5 |
| `dispatch.prompt_bind` | 2 | 676.6 | 676.6 | 676.6 | 676.6 |
| `dispatch.record_fail` | 2 | 1072.3 | 1072.3 | 1072.3 | 1072.3 |
| `dispatch.record_read` | 14 | 837.6 | 1548.5 | 1548.5 | 1548.5 |
| `dispatch.record_write` | 6 | 1676.5 | 1757.9 | 1757.9 | 1757.9 |
| `dispatch.waiting_read` | 8 | 1346.1 | 1620.9 | 1620.9 | 1620.9 |
| `filesystem.github_credentials_read` | 2 | 5070.7 | 5070.7 | 5070.7 | 5070.7 |
| `fleet.overview.activity` | 44 | 993.1 | 6611.2 | 10102.4 | 10102.4 |
| `fleet.overview.mcp_bootstrap` | 50 | 15888.9 | 21576.2 | 22857.8 | 22857.8 |
| `fleet.overview.reachability` | 8 | 944.3 | 2289.5 | 2289.5 | 2289.5 |
| `fleet.overview.repositories` | 42 | 2107.0 | 9870.2 | 10230.8 | 10230.8 |
| `fleet.overview.status` | 4 | 635.5 | 1115.9 | 1115.9 | 1115.9 |
| `fleet.overview.sync` | 52 | 1128.9 | 3868.8 | 4747.6 | 4747.6 |
| `fleet.overview_cache_read` | 600 | 946.1 | 3437.0 | 5235.9 | 6384.5 |
| `fleet.overview_cache_write` | 336 | 1125.6 | 3393.8 | 6014.7 | 8736.7 |
| `fleet.overview_response_json` | 14 | 898.2 | 942.0 | 942.0 | 942.0 |
| `fleet.placement_resolve` | 30 | 1620.3 | 2943.7 | 2943.7 | 2943.7 |
| `fleet.queued_readiness_resolve` | 6 | 839.2 | 1904.1 | 1904.1 | 1904.1 |
| `goal.dispatch_admission_trace` | 4 | 544.6 | 939.7 | 939.7 | 939.7 |
| `goal.dispatch_execution_identity_restore` | 2 | 2706.3 | 2706.3 | 2706.3 | 2706.3 |
| `goal.dispatch_lifecycle_index` | 4 | 682.7 | 1205.8 | 1205.8 | 1205.8 |
| `goal.dispatch_reservation_validate` | 8 | 1336.1 | 2361.4 | 2361.4 | 2361.4 |
| `health.canonical_obligation` | 44 | 714.6 | 2459.1 | 3029.0 | 3029.0 |
| `history.card_page` | 4 | 625.3 | 1639.2 | 1639.2 | 1639.2 |
| `http.session_list` | 18 | 1001.9 | 3416.8 | 3416.8 | 3416.8 |
| `intake.web_prompt` | 12 | 829.7 | 3564.5 | 3564.5 | 3564.5 |
| `notifications.list_page` | 14 | 1722.2 | 2430.5 | 2430.5 | 2430.5 |
| `notifications.present_page` | 12 | 694.7 | 2904.4 | 2904.4 | 2904.4 |
| `operation.canonical_repair` | 8 | 857.4 | 4388.8 | 4388.8 | 4388.8 |
| `operation.outcome_read` | 50 | 1460.3 | 2822.0 | 3293.7 | 3293.7 |
| `operation.repair_admission` | 10 | 1788.8 | 2706.5 | 2706.5 | 2706.5 |
| `operation.repair_finish` | 4 | 927.5 | 1654.9 | 1654.9 | 1654.9 |
| `operation.repair_start` | 8 | 719.9 | 1646.2 | 1646.2 | 1646.2 |
| `pr_supervisor.response_json` | 16 | 761.3 | 1937.3 | 1937.3 | 1937.3 |
| `preferences.agent_auto_approve_read` | 32 | 847.9 | 5119.8 | 5119.8 | 5119.8 |
| `preferences.agent_surface_read` | 2 | 752.7 | 752.7 | 752.7 | 752.7 |
| `progress.checkpoint_write` | 68 | 1048.4 | 4400.3 | 7105.4 | 7105.4 |
| `progress.dispatch_read` | 164 | 836.0 | 2643.1 | 3071.4 | 3071.4 |
| `progress.heartbeat_write` | 140 | 1361.4 | 3674.4 | 6451.7 | 6451.7 |
| `progress.pending_read` | 18 | 583.3 | 2921.3 | 2921.3 | 2921.3 |
| `progress.sequence_allocate` | 188 | 880.4 | 3269.6 | 6314.2 | 6314.2 |
| `provider.status.codex` | 58 | 3227.5 | 6983.9 | 11776.6 | 11776.6 |
| `provider.status.cursor` | 58 | 3787.9 | 7261.2 | 10132.5 | 10132.5 |
| `provider.status.openinterpreter` | 56 | 1219.2 | 5351.0 | 8776.5 | 8776.5 |
| `reconciliation.completion_evidence_read` | 50 | 805.0 | 2254.1 | 3160.6 | 3160.6 |
| `reconciliation.dispatch_read` | 74 | 803.0 | 5229.2 | 5668.6 | 5668.6 |
| `reconciliation.session_read` | 36 | 982.5 | 2827.6 | 2827.6 | 2827.6 |
| `selection.catalog_enrichment` | 12 | 587.8 | 2362.2 | 2362.2 | 2362.2 |
| `selection.receipt_write` | 2 | 3043.3 | 3043.3 | 3043.3 | 3043.3 |
| `selection.target_receipt` | 2 | 698.4 | 698.4 | 698.4 | 698.4 |
| `selection.target_revalidation` | 4 | 684.0 | 1700.1 | 1700.1 | 1700.1 |
| `sqlite.agent_runtime_checkpoint` | 14 | 1129.5 | 7292.5 | 7292.5 | 7292.5 |
| `sqlite.agent_session_read` | 2 | 2482.3 | 2482.3 | 2482.3 | 2482.3 |
| `sqlite.agent_session_save` | 408 | 1292.7 | 4361.2 | 6543.9 | 6950.0 |
| `sqlite.card_read` | 30 | 1148.3 | 2792.4 | 2792.4 | 2792.4 |
| `sqlite.card_summary_read` | 2 | 953.6 | 953.6 | 953.6 | 953.6 |
| `sqlite.dispatch_queue_admit` | 6 | 1272.0 | 2418.9 | 2418.9 | 2418.9 |
| `sqlite.execution_binding_materialize` | 2 | 506.5 | 506.5 | 506.5 | 506.5 |
| `sqlite.notification_create` | 2 | 620.2 | 620.2 | 620.2 | 620.2 |
| `sqlite.pr_supervisor_capability_read` | 12 | 671.2 | 2293.2 | 2293.2 | 2293.2 |
| `sqlite.pr_supervisor_capability_write` | 8 | 1357.4 | 3356.1 | 3356.1 | 3356.1 |
| `sqlite.pr_supervisor_completion_due_read` | 58 | 778.6 | 2506.5 | 5213.9 | 5213.9 |
| `sqlite.pr_supervisor_due_read` | 24 | 974.1 | 2279.6 | 2279.6 | 2279.6 |
| `sqlite.pr_supervisor_effect_finish` | 2 | 835.2 | 835.2 | 835.2 | 835.2 |
| `sqlite.pr_supervisor_event_write` | 10 | 1563.2 | 1920.3 | 1920.3 | 1920.3 |
| `sqlite.pr_supervisor_lease_write` | 8 | 702.6 | 1336.0 | 1336.0 | 1336.0 |
| `sqlite.pr_supervisor_metric` | 20 | 820.3 | 2297.5 | 2297.5 | 2297.5 |
| `sqlite.pr_supervisor_watch_read` | 46 | 922.2 | 2397.1 | 2487.9 | 2487.9 |
| `sqlite.pr_supervisor_watch_write` | 22 | 916.2 | 3347.2 | 3347.2 | 3347.2 |
| `sqlite.project_read` | 46 | 909.0 | 1634.6 | 6430.9 | 6430.9 |
| `sqlite.prompt_acceptance_read` | 26 | 5940.0 | 14827.6 | 14827.6 | 14827.6 |
| `sqlite.repository_read` | 46 | 899.5 | 1922.2 | 5651.5 | 5651.5 |
| `sqlite.restart_handoff_completion` | 38 | 1075.4 | 2645.7 | 2645.7 | 2645.7 |
| `sqlite.restart_handoff_create` | 2 | 3819.5 | 3819.5 | 3819.5 | 3819.5 |
| `sqlite.restart_handoff_session` | 16 | 872.2 | 3204.4 | 3204.4 | 3204.4 |
| `sqlite.restart_handoff_waiting` | 2 | 2831.4 | 2831.4 | 2831.4 | 2831.4 |
| `sqlite.restart_handoffs_pending` | 20 | 762.8 | 1678.0 | 1678.0 | 1678.0 |
| `sqlite.restart_handoffs_watchdog` | 16 | 567.2 | 1650.2 | 1650.2 | 1650.2 |
| `sqlite.transcript_append` | 1154 | 1766.8 | 8504.6 | 11219.5 | 15832.3 |
| `sqlite.transcript_message_page` | 2 | 1280.0 | 1280.0 | 1280.0 | 1280.0 |
| `sqlite.transcript_read` | 2 | 553.4 | 553.4 | 553.4 | 553.4 |
| `sync.commit_read` | 34 | 754.5 | 1825.4 | 1825.4 | 1825.4 |
| `sync.json_decode` | 16 | 745.4 | 2278.2 | 2278.2 | 2278.2 |
| `sync.json_encode` | 10 | 1153.4 | 1671.6 | 1671.6 | 1671.6 |
| `sync.object_collect` | 64 | 11318.6 | 32779.7 | 33483.6 | 33483.6 |
| `sync.object_has` | 16 | 657.9 | 1117.1 | 1117.1 | 1117.1 |
| `sync.push_transaction` | 78 | 1003.1 | 5963.0 | 8125.4 | 8125.4 |
| `sync.reconcile_head` | 44 | 1369.8 | 7917.8 | 12053.9 | 12053.9 |
| `sync.ref_read` | 14 | 736.6 | 2271.4 | 2271.4 | 2271.4 |
| `workspace.lease_renew` | 4 | 618.5 | 1586.8 | 1586.8 | 1586.8 |
| `workspace.project_provision` | 6 | 5284.9 | 13819.4 | 13819.4 | 13819.4 |
