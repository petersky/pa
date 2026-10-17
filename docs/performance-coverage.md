# Systemic PA performance coverage — 2026-10-17

The card remains open. This consolidates the prior449-row repository matrix and both durable card deltas (452 rows), then accounts for every additional live operation/route and control-plane concern. Every unresolved row retains its owner and next action. Source, merge, loaded build and acceptance are distinct.

Full matrix: [performance-coverage.json](performance-coverage.json), 462 rows. Fresh numeric source: [resume artifact](benchmarks/systemic-resume-20261017.json). Original log/fixture stage evidence: [systemic-inventory-v4.json](benchmarks/systemic-inventory-v4.json).

Fresh owner API confirms macbook0c7d8ecb, PID51003, loaded1.5.9+5caf83e4325a. Status3990.910ms/runtime104.714ms are individual client reads. All154 runtime operations and all observed registered routes are retained, including failures and outliers. Runtime percentiles use bounded windows; counters/maxima can span process lifetime. Loop maximum1064316ms is not attributed to CPU: host suspension remains possible. No peer log coverage or fleet-wide acceptance is claimed.

The status-count fixture retains331cards/359sessions with64KiB synthetic body/metrics each and10active knowledge entries.30reads:20850hydratedmodels, p50/p95/p99/max1179.402/3519.475/3865.728/3865.728ms. Aggregate:0models, identicalcounts,0.060/0.707/3.123/3.123ms.32status/count/CLI/database regressions pass;24lock/runtime/database regressions pass in a separate overlapping suite. New source is not merged/deployed yet. Bounded lock/long-GC attribution does not itself repair a tail.

PR484–488 are integrated, with exact head/merge/watch evidence retained in JSON/card. Their earlier mixed residuals remain:2worker warnings and4sampled violations. Current mixed replay is recorded separately when complete; no failure is discarded. Original log inventory covers16retained files,86440227sourcebytes,570983physical lines,2026-10-08T03:25:05Z–2026-10-16T07:10:18Z;110slow names/22prefixfamilies. Active-file growth, duplicate emissions, unknown timestamps, earlier retention and uninspected peers remain exclusions.

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
| Checkpoint timeout and passive receipt recovery | Two native progress writes returned unknown outcome; first reconciliation queued. Policy edit validation was separately recovered and corrected with explicit fieldintent, not a latency fix. | Diagnose observed delivery/receipt behavior with exact IDs; preserve pending versus non-commit, bounded retries and sole-writer semantics. |

## Every retained slow operation

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
