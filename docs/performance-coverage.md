# Systemic performance coverage

This is an open work ledger, not a claim that all PA performance issues are fixed.

Every finding has a cause assessment, repair/owner, exact measurement status, rollout state, residual and next actor in [the JSON matrix](performance-coverage.json). All rows remain open pending sufficient verification and acceptance.

The original independent attachments contain 233,892 redacted records (2026-10-15T05:48:13Z–2026-10-16T05:48:23Z), 105 slow names, 181 runtime operations and 115 route groups. The additional retained-file inventory covers 16 files, 86,440,227 source bytes and 570,983 physical lines, Oct8–16, with 110 slow names in22families. Cross-sink duplicates, dropped logs, 3,200 unattributable lines, uninspected peer logs and older retention gaps prevent an exhaustive fleet claim.

PR484 and PR485 are merged. At exact merged-tree761a4c6, 100push/100reconcile/300drain mixed iterations retained9600events with zero slow warnings/failures/timeouts; ten tool-free ACP fixture cold starts passed. Running1.5.6 is not verified to contain these repairs. Real provider, peer, unmeasured leaf/route and physical-write acceptance remains open.

PR486 is merged as687436f from watched stable-green682561f with all5head checks successful. The expanded100-iteration source-matched scenario has0worker warnings/failures/timeouts,9600ordered events, but2HTTP500ms violations (message history518.524ms;PullRequests675.397ms) and loopmax508.158ms. This remains failed performance acceptance. All earlier outliers remain in [the numeric artifact](benchmarks/systemic-inventory-v4.json). PR487 enqueue reduction is also integrated, exactmerge8fcc3ce with5/5head+mainCI; original CI failures/retry retained. These earlier scenario results are not superseded by source integration alone.

Latest PR-inventory source patch:99new/existing supervisor tests plus5final inventory contracts pass.100retained-watch/30metrics measurement reduces proof hydration3000rows/193MB to0 and p9594.061to8.667ms. The same100-iteration mixed scenario retains9600events, but2worker warnings and4measured violations remain (drain507.378ms;push548.788ms;reconcile534.643ms;message597.144ms), loopmax180.803ms. This source patch is pending integration; no deployed acceptance. Nonempty active-watch API and real external polling remain explicit gaps.

## Concern dispositions

| Finding | Evidence and remaining work | Owner |
|---|---|---|
| Sync/global mutation lock | Historical push219.9s, binding wait197.9s vs own hold<=21.3ms; sync lock-hold p99~73.95s. Exact merged-tree 100push/100reconcile mixed fixture passes zero slow calls. Remaining lock waiters stay open. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Payload and copy amplification | Session~1.19MBp95; dispatch22.4MB; history~860KBp95; recovery2.83MB. Dispatch list reduction measured; history/recovery detail contracts still need bounded paging/measurement. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Event loop and executor phases | Historical loop10.6s. Numeric queue/handoff/service/threadCPU/completion phases implemented; remaining normal synchronous calls and native busy/disk partition unresolved. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Transcript/wire durability and drain | Integrated fixture retains exactly9600ordered events;300drains plus10tool-free cold starts, zero failures/slow/timeouts. Real-provider/network and unexercised wire/read paths remain open. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Progress/dispatch/reconciliation amplification | Compact candidate/status fixes merged; normal enqueue synchronous checkpoints and leaf/control-path coverage remain open. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Fleet/provider/selection | MCP bootstrap18.5s, placementp9527.1s, dispatchp9517.5s observed. Cache/catalog fixes do not establish all placement/probe acceptance. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| PR supervision and external dependencies | GitHub snapshot25.7s, refreshp9514.8s, effects-dispatchp9535.6s(13samples). Separate network latency, local hydration and repeat-failure behavior. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Pages and API responsiveness | All115route groups and6historical shell families retained below;59routep95>=500ms include small samples. Normal83SSE observations excluded from ordinary request SLO. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Backup status and manifest failures | Status53.7s and duplicate compressed archive scans; PR483 merged83c4ba7. Existing sibling owns large-manifest integrity/scalability and rollout acceptance; no competing backup edits. | 0331d21a-f014-44c8-9176-bc1e955a9ad2 |
| Workspace maintenance and leases | Historical GC11.3s, renew6.7s, provision6.5s, maintenance13.4s; current causes unverified; retain fencing. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Every remaining operation family | Prompt intake, preferences, filesystem, notification, conversation, goal, health, receipt, runtime, selection and provider exact names remain explicit rows with next action; no shared-cause closure by assumption. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Peer stalls and repeated failures | macmini02dbcd47:reconcile92.9s/push79s; original fleet4762,PR62,sync173failures; later snapshot worsened. Other configured peer identity unknown after bounded read timeout. Peer logs/build uninspected. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Log loss, provenance and coverage | CLI5.64MBdropped/2396status failures; PR485 fixes UUID provenance and WARNING normalization, not all pump losses. Own16-file retained scan covers Oct8–16; unknown timestamps3200 and missing older/peer logs remain gaps. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Attachment timeout and receipt recovery | Independent reportd6f9f96d correlation89e1728a-ce91-49c1-a036-5bc8bc7cfd66 operation9fce2c73-175e-4495-aca2-aafb3c5bcc2e; own report0e918df2 fullID in ledger references. Exact-hash effects exist despite unknown transport/absent receipt; never retry ambiguously. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Incoming fork publication and summary writes | Expanded25-request mixed fixture exposed one unhandled DagIndexStaleError during summary publication, despite zero measured latency violations. Confirmed incoming merge advanced ref before application primary admission; repair routes prepared immutable merge through projection/CAS publisher and offloads summary work with fresh-input/manual-edit fences. Report19c44107-99dc-4b64-92ea-fda8d22a3418; regression/retest status below. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Quadratic complete-message compaction | Expanded100-iteration fixture exposed transcript message-page tails as a single message grew to3200chunks/2.56million characters. Repeated prefix concatenation materialized about4.1billion characters. Linear grouping joins once while preserving replacement snapshots, source sequence spans, interleaved tools and all durable input chunks; exact reconstruction and copy-volume regression passed. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Startup classification and same-request recovery | Report e2bed017-d546-49f5-910e-3d8326d80e40: start classified as research, same request recovered after metadata correction. The workaround is not a repair or new authorization. Preserve request/receipt identity and user model/permission defaults; root classification and regression coverage remain open. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Known disabled summary metadata races card version | PR486 CI38883010060 exposed a disabled-summary metadata write about10ms after a caller read the card version, rejecting its following execution-preference edit. Correction settles known unconfigured metadata in the initial canonical creation event and exact receipt. Configured/manual summary behavior and strict stale-version rejection remain intact; PR486 merged687436f after all5checks passed; optional-service absence remains compatible with degraded local operations (9targeted tests). | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Integration-only automatic completion exceeds card scope | Supervisor marked this expanded performance card Done after PR484, PR485, PR486 and PR487 despite open acceptance/peer/coverage findings; card restored Active four times. Report861aef1f-3d04-461d-8594-fc5b4260ccf2 revision4 preserves evidence. Integration is not whole-PA acceptance; completion-policy/requirement coordination remains open, with no invented acceptance receipt or weakened gate. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Duplicate durable human enqueue checkpoint | 100 real paused human enqueue calls produced200checkpoints (p95enqueue13.882ms/max18.055ms). Removing the redundant second checkpoint yields100checkpoints, p959.980ms/max11.701ms, with all100prompts durable. Existing queue/recovery/acceptance suites:87passed. PR487 merged8fcc3ce after supervised stable-green head and5/5head+mainCI. Ordinary synchronous checkpoint cost remains; no claim of whole-PA acceptance. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| Full Kernel schema warm-up blocks completion delivery | Disposable full boot: OpenAPI2919.984ms wall/2878.777ms CPU on loop; concurrent sync-ref service1.57ms/delivery3015.695ms. Exact offload alternative preserves readiness paths but still records schema1443.870ms and ref520.821ms warnings. No source readiness repair committed; real full-boot and GIL/CPU limits remain open. Reportd2db90ed-30f7-4cca-ae3a-af18d4fa056a. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |
| PR metrics and health hydrate proofs to count lifecycle | 100retained watches/30metrics calls hydrate3000records/193086000state-policy bytes; baselinep9594.061ms. SQLaggregate replacement:0proof hydrations, same counts, clean sequentialp958.667ms.99new/existing PR tests plus5final inventory contracts passed. Final mixed run still2worker warnings/four measured violations; no blanket root-cause or deployment closure. | 7894e44b-eef9-4da8-9f7f-7d3ae82c0b54 |

## Exact observed slow operations

Counts are raw retained slow emissions, not distinct incidents. Milliseconds; p50/p95/p99/max are threshold-selected. Fixture evidence refers only to the named operation in the declared integrated scenario.

| Operation | Count | p50 | p95 | p99 | Max | Earlier PR485 fixture; latest follow-up in JSON |
|---|---:|---:|---:|---:|---:|---|
| `sync.reconcile_head` | 194 | 1768.0 | 78827.0 | 305836.8 | 305836.8 | 100 calls; 0 slow; full metrics in JSON |
| `sync.push_transaction` | 346 | 1083.9 | 37486.4 | 219936.0 | 242337.6 | 100 calls; 0 slow; full metrics in JSON |
| `sqlite.execution_binding_materialize` | 14 | 73205.6 | 197942.1 | 197942.1 | 197942.1 | 100 calls; 0 slow; full metrics in JSON |
| `sqlite.card_write` | 26 | 16745.0 | 159863.4 | 159863.4 | 159863.4 | 10 calls; 0 slow; full metrics in JSON |
| `sqlite.transcript_append` | 2823 | 974.0 | 3561.9 | 6756.8 | 40779.8 | 300 calls; 0 slow; full metrics in JSON |
| `sync.object_collect` | 165 | 3692.1 | 21191.6 | 29969.8 | 29969.8 | Open: exact path not exercised |
| `intake.web_prompt` | 18 | 1694.7 | 24703.5 | 24703.5 | 24703.5 | Open: exact path not exercised |
| `workspace.collect_garbage` | 70 | 2336.0 | 11321.1 | 20621.3 | 20621.3 | Open: exact path not exercised |
| `fleet.overview.mcp_bootstrap` | 80 | 5008.8 | 7243.3 | 18547.3 | 18547.3 | Open: exact path not exercised |
| `fleet.overview.sync` | 888 | 860.4 | 2317.5 | 4399.3 | 15992.3 | 2 calls; 0 slow; full metrics in JSON |
| `dispatch.expire_capacity_reservations` | 99 | 1350.9 | 4939.2 | 15260.6 | 15260.6 | Open: exact path not exercised |
| `agent.recovery_sessions` | 1329 | 768.3 | 3427.0 | 5487.0 | 12250.5 | Open: exact path not exercised |
| `progress.heartbeat_write` | 286 | 873.0 | 3201.8 | 4764.1 | 11847.9 | Open: exact path not exercised |
| `sqlite.pr_supervisor_due_read` | 108 | 731.3 | 3186.9 | 10979.9 | 10979.9 | 14 calls; 0 slow; full metrics in JSON |
| `sqlite.transcript_read` | 618 | 847.4 | 3581.6 | 4698.5 | 10929.5 | Open: exact path not exercised |
| `progress.dispatch_read` | 600 | 726.8 | 1919.8 | 3359.2 | 10889.4 | Open: exact path not exercised |
| `sync.commit_read` | 77 | 811.3 | 1722.0 | 10817.9 | 10817.9 | 100 calls; 0 slow; full metrics in JSON |
| `fleet.overview_cache_write` | 1257 | 745.8 | 2886.6 | 5721.1 | 10795.2 | 106 calls; 0 slow; full metrics in JSON |
| `sqlite.transcript_message_page` | 726 | 2715.9 | 6492.7 | 8217.9 | 10120.6 | Open: exact path not exercised |
| `sqlite.pr_supervisor_completion_due_read` | 486 | 764.9 | 5910.5 | 7618.0 | 9406.6 | 14 calls; 0 slow; full metrics in JSON |
| `acp.wire_append` | 1558 | 747.4 | 2095.7 | 3608.5 | 8740.4 | 100 calls; 0 slow; full metrics in JSON |
| `sqlite.agent_session_save` | 340 | 876.3 | 3667.5 | 7294.1 | 8255.4 | Open: exact path not exercised |
| `progress.sequence_allocate` | 372 | 810.3 | 3349.5 | 6649.1 | 8185.3 | 100 calls; 0 slow; full metrics in JSON |
| `workspace.project_provision` | 6 | 6536.4 | 7903.7 | 7903.7 | 7903.7 | Open: exact path not exercised |
| `reconciliation.completion_evidence_read` | 335 | 693.4 | 2140.5 | 4472.9 | 7887.4 | Open: exact path not exercised |
| `agent.project_recovery_availability` | 140 | 811.2 | 1891.9 | 7885.1 | 7885.1 | Open: exact path not exercised |
| `agent.startup_state_read` | 12 | 6216.3 | 7652.2 | 7652.2 | 7652.2 | Open: exact path not exercised |
| `agent.history_payload` | 388 | 1701.4 | 3995.1 | 5482.8 | 6741.5 | Open: exact path not exercised |
| `workspace.lease_renew` | 54 | 725.4 | 2829.8 | 6713.0 | 6713.0 | Open: exact path not exercised |
| `dispatch.runnable_read` | 26 | 2585.1 | 5541.2 | 5541.2 | 5541.2 | 100 calls; 0 slow; full metrics in JSON |
| `sync.ref_read` | 111 | 721.5 | 2416.9 | 5395.5 | 5395.5 | 6 calls; 0 slow; full metrics in JSON |
| `sqlite.agent_runtime_checkpoint` | 6 | 909.3 | 5356.8 | 5356.8 | 5356.8 | Open: exact path not exercised |
| `reconciliation.dispatch_read` | 359 | 756.7 | 2757.2 | 3795.3 | 5069.1 | 106 calls; 0 slow; full metrics in JSON |
| `dispatch.completion_pending_read` | 102 | 807.5 | 2676.4 | 4968.7 | 4968.7 | 6 calls; 0 slow; full metrics in JSON |
| `sqlite.pr_supervisor_capability_write` | 18 | 1069.2 | 4943.0 | 4943.0 | 4943.0 | 1 calls; 0 slow; full metrics in JSON |
| `progress.pending_read` | 136 | 799.5 | 2258.6 | 4615.0 | 4615.0 | 10 calls; 0 slow; full metrics in JSON |
| `progress.checkpoint_write` | 122 | 789.9 | 3034.8 | 4471.0 | 4471.0 | 100 calls; 0 slow; full metrics in JSON |
| `dispatch.followup_pending_read` | 58 | 815.5 | 2760.2 | 4368.2 | 4368.2 | 6 calls; 0 slow; full metrics in JSON |
| `dispatch.waiting_read` | 56 | 1631.4 | 3539.6 | 4150.4 | 4150.4 | Open: exact path not exercised |
| `fleet.placement_resolve` | 30 | 1090.3 | 4017.6 | 4017.6 | 4017.6 | Open: exact path not exercised |
| `reconciliation.supervisor_health` | 16 | 812.3 | 3916.7 | 3916.7 | 3916.7 | Open: exact path not exercised |
| `sqlite.card_read` | 44 | 952.6 | 2839.3 | 3834.5 | 3834.5 | Open: exact path not exercised |
| `sync.json_decode` | 12 | 741.8 | 3773.6 | 3773.6 | 3773.6 | Open: exact path not exercised |
| `provider.status.cursor` | 122 | 2360.5 | 3340.0 | 3733.0 | 3733.0 | Open: exact path not exercised |
| `sqlite.restart_handoff_completion` | 408 | 761.6 | 1723.0 | 2485.9 | 3724.9 | Open: exact path not exercised |
| `sqlite.pr_supervisor_lease_write` | 26 | 990.0 | 3683.3 | 3683.3 | 3683.3 | Open: exact path not exercised |
| `reconciliation.session_read` | 126 | 700.1 | 2557.9 | 3611.9 | 3611.9 | Open: exact path not exercised |
| `fleet.overview_cache_read` | 1297 | 741.1 | 2275.9 | 3179.3 | 3382.6 | 118 calls; 0 slow; full metrics in JSON |
| `sqlite.repository_read` | 46 | 781.9 | 3336.0 | 3370.1 | 3370.1 | Open: exact path not exercised |
| `fleet.overview_response_json` | 62 | 766.9 | 2782.4 | 3254.7 | 3254.7 | Open: exact path not exercised |
| `fleet.overview.status` | 85 | 695.4 | 1657.8 | 3201.5 | 3201.5 | 2 calls; 0 slow; full metrics in JSON |
| `provider.status.codex` | 122 | 946.5 | 2334.5 | 3108.4 | 3108.4 | Open: exact path not exercised |
| `dispatch.record_write` | 12 | 982.6 | 3107.7 | 3107.7 | 3107.7 | Open: exact path not exercised |
| `dispatch.reconcile_interrupted` | 12 | 2693.5 | 3038.0 | 3038.0 | 3038.0 | Open: exact path not exercised |
| `notifications.list_page` | 48 | 938.8 | 1922.8 | 3001.3 | 3001.3 | Open: exact path not exercised |
| `sqlite.pr_supervisor_watch_write` | 42 | 1408.5 | 2806.4 | 2973.7 | 2973.7 | Open: exact path not exercised |
| `sqlite.restart_handoffs_pending` | 124 | 699.7 | 1466.6 | 2965.2 | 2965.2 | Open: exact path not exercised |
| `preferences.agent_auto_approve_read` | 32 | 720.7 | 2950.5 | 2950.5 | 2950.5 | Open: exact path not exercised |
| `sqlite.pr_supervisor_metric` | 46 | 911.1 | 2642.5 | 2914.7 | 2914.7 | Open: exact path not exercised |
| `health.canonical_obligation` | 16 | 884.9 | 2906.8 | 2906.8 | 2906.8 | 12 calls; 0 slow; full metrics in JSON |
| `sqlite.pr_supervisor_event_write` | 38 | 789.1 | 2880.5 | 2880.5 | 2880.5 | Open: exact path not exercised |
| `runtime.status_snapshot` | 6 | 2560.3 | 2801.9 | 2801.9 | 2801.9 | Open: exact path not exercised |
| `sqlite.pr_supervisor_watch_read` | 172 | 805.3 | 1811.6 | 2735.1 | 2735.1 | Open: exact path not exercised |
| `sqlite.restart_handoffs_watchdog` | 146 | 688.4 | 1825.8 | 2686.5 | 2686.5 | Open: exact path not exercised |
| `filesystem.github_credentials_read` | 8 | 2135.6 | 2575.2 | 2575.2 | 2575.2 | 14 calls; 0 slow; full metrics in JSON |
| `sqlite.project_read` | 30 | 731.1 | 2540.1 | 2540.1 | 2540.1 | Open: exact path not exercised |
| `goal.dispatch_lifecycle_index` | 24 | 926.4 | 2436.1 | 2436.1 | 2436.1 | 1 calls; 0 slow; full metrics in JSON |
| `sqlite.restart_handoff_session` | 74 | 783.4 | 2126.8 | 2290.5 | 2290.5 | Open: exact path not exercised |
| `sqlite.pr_supervisor_observation_write` | 8 | 820.5 | 2223.9 | 2223.9 | 2223.9 | Open: exact path not exercised |
| `sqlite.prompt_acceptance` | 6 | 1164.3 | 2178.6 | 2178.6 | 2178.6 | Open: exact path not exercised |
| `pr_supervisor.response_json` | 8 | 646.2 | 2169.6 | 2169.6 | 2169.6 | Open: exact path not exercised |
| `dispatch.record_read` | 14 | 644.9 | 2087.4 | 2087.4 | 2087.4 | Open: exact path not exercised |
| `sqlite.agent_session_read` | 30 | 748.5 | 2082.8 | 2082.8 | 2082.8 | Open: exact path not exercised |
| `pr_supervisor.github_response_json` | 26 | 655.5 | 2082.1 | 2082.1 | 2082.1 | Open: exact path not exercised |
| `fleet.overview.repositories` | 8 | 736.1 | 2003.5 | 2003.5 | 2003.5 | Open: exact path not exercised |
| `selection.resolve` | 16 | 810.4 | 1969.6 | 1969.6 | 1969.6 | Open: exact path not exercised |
| `sync.object_has` | 62 | 736.3 | 1786.4 | 1792.2 | 1792.2 | Open: exact path not exercised |
| `fleet.overview.reachability` | 94 | 711.6 | 1185.6 | 1651.3 | 1651.3 | 2 calls; 0 slow; full metrics in JSON |
| `sqlite.prompt_acceptance_read` | 12 | 725.3 | 1608.7 | 1608.7 | 1608.7 | Open: exact path not exercised |
| `selection.target_revalidation` | 44 | 729.6 | 1258.9 | 1360.2 | 1360.2 | Open: exact path not exercised |
| `sqlite.pr_supervisor_capability_read` | 16 | 695.0 | 1218.5 | 1218.5 | 1218.5 | Open: exact path not exercised |
| `progress.final_report_write` | 6 | 949.9 | 1159.3 | 1159.3 | 1159.3 | Open: exact path not exercised |
| `selection.prompt_outcome` | 6 | 822.0 | 1091.2 | 1091.2 | 1091.2 | Open: exact path not exercised |
| `provider.status.openinterpreter` | 24 | 630.2 | 1065.5 | 1065.5 | 1065.5 | Open: exact path not exercised |
| `agent.provider_resolve` | 2 | 1042.3 | 1042.3 | 1042.3 | 1042.3 | Open: exact path not exercised |
| `workspace.session_leases` | 4 | 511.2 | 1014.9 | 1014.9 | 1014.9 | Open: exact path not exercised |
| `files.browse_render` | 6 | 803.0 | 1012.6 | 1012.6 | 1012.6 | Open: exact path not exercised |
| `agent.snapshot_payload` | 4 | 739.0 | 987.4 | 987.4 | 987.4 | Open: exact path not exercised |
| `sync.startup_reconcile` | 8 | 853.4 | 983.8 | 983.8 | 983.8 | Open: exact path not exercised |
| `selection.prompt_identity` | 8 | 658.0 | 982.6 | 982.6 | 982.6 | Open: exact path not exercised |
| `operation.outcome_read` | 2 | 973.2 | 973.2 | 973.2 | 973.2 | Open: exact path not exercised |
| `dispatch.followup_ack` | 2 | 954.6 | 954.6 | 954.6 | 954.6 | Open: exact path not exercised |
| `conversation.session` | 6 | 699.9 | 939.5 | 939.5 | 939.5 | Open: exact path not exercised |
| `pr_supervisor.metric` | 2 | 903.0 | 903.0 | 903.0 | 903.0 | Open: exact path not exercised |
| `sqlite.pr_supervisor_effect_prepare` | 4 | 734.0 | 892.8 | 892.8 | 892.8 | Open: exact path not exercised |
| `agent.prompt_compose` | 2 | 865.3 | 865.3 | 865.3 | 865.3 | Open: exact path not exercised |
| `sqlite.pr_merge_evidence_read` | 4 | 732.5 | 861.3 | 861.3 | 861.3 | Open: exact path not exercised |
| `notifications.present_page` | 22 | 650.0 | 850.3 | 850.3 | 850.3 | Open: exact path not exercised |
| `selection.settings_resolve` | 2 | 746.3 | 746.3 | 746.3 | 746.3 | Open: exact path not exercised |
| `agent.session_snapshot` | 2 | 745.6 | 745.6 | 745.6 | 745.6 | Open: exact path not exercised |
| `dispatch.cancel_check` | 2 | 657.2 | 657.2 | 657.2 | 657.2 | Open: exact path not exercised |
| `reconciliation.dispatch_write` | 2 | 628.6 | 628.6 | 628.6 | 628.6 | Open: exact path not exercised |
| `selection.catalog_enrichment` | 2 | 627.3 | 627.3 | 627.3 | 627.3 | Open: exact path not exercised |
| `selection.configuration_receipt` | 2 | 622.0 | 622.0 | 622.0 | 622.0 | Open: exact path not exercised |
| `selection.target_receipt` | 2 | 596.9 | 596.9 | 596.9 | 596.9 | Open: exact path not exercised |
| `sync.json_encode` | 2 | 574.6 | 575.3 | 575.3 | 575.3 | Open: exact path not exercised |
| `workspace.scratch_provision` | 2 | 566.6 | 566.6 | 566.6 | 566.6 | Open: exact path not exercised |
| `agent.recovery_snapshot` | 2 | 559.5 | 559.5 | 559.5 | 559.5 | Open: exact path not exercised |
| `fleet.overview.activity` | 2 | 542.6 | 542.6 | 542.6 | 542.6 | Open: exact path not exercised |
| `dispatch.session_link` | 2 | 528.8 | 528.8 | 528.8 | 528.8 | Open: exact path not exercised |

## Runtime and routes

All 181 operation records and 115 route groups are retained individually in the JSON, including queue/lock samples and payload bytes where present. These dated runtime windows are not contemporaneous production retests. The 83 normal SSE observations excluded by the source remain excluded.

## Next verification

Investigate normal enqueue/checkpoint behavior and remaining control-plane/leaf paths in isolated mixed fixtures; fix confirmed synchronous work. Separate external dependency service time from local overhead. Coordinate backup acceptance on0331d21a and database/API acceptance on89957d94. Obtain supported peer evidence and repair bounded repeated-failure behavior. Address mutation timeout/receipt observability without ambiguous replay. Publish updated measured outcomes and exact rollout identity before closing any deployed acceptance gap.
