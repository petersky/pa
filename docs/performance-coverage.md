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