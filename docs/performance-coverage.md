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