# Actual loaded9e5 inventory

Host macbook0c7d8ecb; PID40587/startupc24bcd97; source9e5c7c55/package583f. Runtime owner API observation 2026-10-18T08:26:39.451438+00:00. Raw systemic-retention-phases-20261018.json (raw runtime entry) SHA25689b7a1dade11e97e33ec2d164dc5f068cdb8292ef12d8be58d3f9cc8bf19ae08.

All returned operations/routes and retained slow records follow. Operation percentile windows are last256 observations; process maxima/counters are separate. Route counts/decoded bytes are process totals; route percentiles and max below are bounded windows. Retained slow record percentiles are only this bounded record set, not lifetime incident counts. Some metadata collector requests may overlap after the completed95sequentialGET performance window; no causal paired or exhaustive fleet claim. Native PA MCP tools absent in current catalog; supported authenticated owner API used without fallback credentials.

{"baseline_route_groups_current": 20, "baseline_routes_missing_current": 95, "baseline_runtime_names_current_positive": 76, "baseline_runtime_names_missing_current_positive": 105, "families": {"acp": 4, "agent": 8, "dispatch": 4, "filesystem": 1, "fleet": 7, "goal": 1, "health": 1, "http": 3, "maintenance": 1, "notifications": 2, "pr_supervisor": 2, "preferences": 1, "progress": 5, "reconciliation": 3, "selection": 3, "sqlite": 21, "startup": 3, "sync": 7, "workspace": 4}, "operation_failures": {"workspace.project_provision": {"cancelled": 0, "failed": 1, "rejected": 0, "submitted": 7, "timed_out": 0}}, "operations": 81, "process_maxima_ge500": 24, "routes": 24, "slow_calls_process_sum": 38, "slow_record_capacity": 64, "slow_records_retained": 38}

## Every runtime operation

| Operation | Submitted | Slow | Failed/timeouts | Window samples | p50 | p95 | p99 | Window max | Process max ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| acp.pa_mcp_owner_probe | 1 | 0 | 0/0 | 1 | 44.174 | 44.174 | 44.174 | 44.174 | 44.174 |
| acp.provider_resolve | 1 | 0 | 0/0 | 1 | 0.452 | 0.452 | 0.452 | 0.452 | 0.452 |
| acp.wire_append | 173 | 0 | 0/0 | 173 | 1.334 | 71.596 | 130.081 | 207.522 | 207.522 |
| acp.wire_init | 1 | 0 | 0/0 | 1 | 0.524 | 0.524 | 0.524 | 0.524 | 0.524 |
| agent.project_recovery_availability | 34 | 0 | 0/0 | 34 | 2.924 | 42.715 | 51.715 | 51.715 | 51.715 |
| agent.prompt_compose | 1 | 0 | 0/0 | 1 | 30.177 | 30.177 | 30.177 | 30.177 | 30.177 |
| agent.provider_resolve | 1 | 0 | 0/0 | 1 | 1.983 | 1.983 | 1.983 | 1.983 | 1.983 |
| agent.quiesce_snapshot_clear | 1 | 0 | 0/0 | 1 | 1.357 | 1.357 | 1.357 | 1.357 | 1.357 |
| agent.recovery_sessions | 16 | 1 | 0/0 | 16 | 144.074 | 664.677 | 664.677 | 664.677 | 664.677 |
| agent.recovery_snapshot | 1 | 0 | 0/0 | 1 | 48.585 | 48.585 | 48.585 | 48.585 | 48.585 |
| agent.session_mkdir | 1 | 0 | 0/0 | 1 | 0.887 | 0.887 | 0.887 | 0.887 | 0.887 |
| agent.startup_state_read | 1 | 1 | 0/0 | 1 | 6220.325 | 6220.325 | 6220.325 | 6220.325 | 6220.325 |
| dispatch.completion_pending_read | 23 | 1 | 0/0 | 23 | 1.755 | 137.492 | 1364.712 | 1364.712 | 1364.712 |
| dispatch.followup_pending_read | 23 | 2 | 0/0 | 23 | 2.059 | 711.957 | 1133.354 | 1133.354 | 1133.354 |
| dispatch.record_read | 340 | 0 | 0/0 | 256 | 19.742 | 40.905 | 133.519 | 347.787 | 347.787 |
| dispatch.workspace_admission_read | 7 | 0 | 0/0 | 7 | 3.12 | 49.842 | 49.842 | 49.842 | 49.842 |
| filesystem.github_credentials_read | 44 | 0 | 0/0 | 44 | 3.575 | 84.229 | 104.292 | 104.292 | 104.292 |
| fleet.overview.reachability | 8 | 1 | 0/0 | 8 | 16.219 | 1377.139 | 1377.139 | 1377.139 | 1377.139 |
| fleet.overview.status | 8 | 1 | 0/0 | 8 | 10.795 | 1371.58 | 1371.58 | 1371.58 | 1371.58 |
| fleet.overview.sync | 9 | 1 | 0/0 | 9 | 70.706 | 1424.963 | 1424.963 | 1424.963 | 1424.963 |
| fleet.overview_cache_read | 337 | 5 | 0/0 | 256 | 4.775 | 73.504 | 115.447 | 189.024 | 1405.083 |
| fleet.overview_cache_write | 97 | 3 | 0/0 | 97 | 20.092 | 176.533 | 1434.359 | 1434.359 | 1434.359 |
| fleet.overview_response_json | 72 | 0 | 0/0 | 72 | 5.48 | 37.342 | 46.249 | 46.249 | 46.249 |
| fleet.update_recovery | 1 | 0 | 0/0 | 1 | 12.108 | 12.108 | 12.108 | 12.108 | 12.108 |
| goal.dispatch_lifecycle_index | 5 | 0 | 0/0 | 5 | 0.59 | 13.562 | 13.562 | 13.562 | 13.562 |
| health.canonical_obligation | 29 | 2 | 0/0 | 29 | 27.428 | 535.093 | 1494.664 | 1494.664 | 1494.664 |
| http.fleet_overview | 72 | 0 | 0/0 | 72 | 193.429 | 3989.956 | 4054.816 | 4054.816 | 4054.816 |
| http.github | 1 | 0 | 0/0 | 1 | 477.972 | 477.972 | 477.972 | 477.972 | 477.972 |
| http.pr_supervisor_peer | 312 | 0 | 0/0 | 256 | 51.269 | 202.77 | 375.151 | 418.06 | 477.156 |
| maintenance.run | 1 | 1 | 0/0 | 1 | 34243.154 | 34243.154 | 34243.154 | 34243.154 | 34243.154 |
| notifications.list_page | 2 | 0 | 0/0 | 2 | 2.178 | 27.235 | 27.235 | 27.235 | 27.235 |
| notifications.present_page | 2 | 0 | 0/0 | 2 | 158.858 | 290.902 | 290.902 | 290.902 | 290.902 |
| pr_supervisor.github_response_json | 1 | 0 | 0/0 | 1 | 131.017 | 131.017 | 131.017 | 131.017 | 131.017 |
| pr_supervisor.response_json | 312 | 0 | 0/0 | 256 | 0.912 | 18.619 | 47.819 | 57.79 | 82.284 |
| preferences.agent_auto_approve_read | 4 | 0 | 0/0 | 4 | 3.261 | 12.904 | 12.904 | 12.904 | 12.904 |
| progress.checkpoint_write | 2 | 1 | 0/0 | 2 | 182.023 | 601.012 | 601.012 | 601.012 | 601.012 |
| progress.dispatch_read | 40 | 0 | 0/0 | 40 | 53.685 | 111.052 | 391.821 | 391.821 | 391.821 |
| progress.heartbeat_write | 7 | 0 | 0/0 | 7 | 184.996 | 287.453 | 287.453 | 287.453 | 287.453 |
| progress.pending_read | 39 | 0 | 0/0 | 39 | 12.065 | 55.612 | 94.936 | 94.936 | 94.936 |
| progress.sequence_allocate | 9 | 1 | 0/0 | 9 | 143.842 | 553.636 | 553.636 | 553.636 | 553.636 |
| reconciliation.completion_evidence_read | 66 | 1 | 0/0 | 66 | 5.203 | 133.173 | 1423.154 | 1423.154 | 1423.154 |
| reconciliation.dispatch_read | 111 | 3 | 0/0 | 111 | 2.492 | 87.567 | 1370.472 | 3435.117 | 3435.117 |
| reconciliation.session_read | 66 | 0 | 0/0 | 66 | 3.91 | 49.971 | 72.998 | 72.998 | 72.998 |
| selection.attempt_authority | 1 | 0 | 0/0 | 1 | 11.265 | 11.265 | 11.265 | 11.265 | 11.265 |
| selection.configuration_receipt | 1 | 0 | 0/0 | 1 | 55.605 | 55.605 | 55.605 | 55.605 | 55.605 |
| selection.prompt_identity | 1 | 0 | 0/0 | 1 | 157.162 | 157.162 | 157.162 | 157.162 | 157.162 |
| sqlite.agent_runtime_checkpoint | 2 | 0 | 0/0 | 2 | 28.423 | 68.121 | 68.121 | 68.121 | 68.121 |
| sqlite.agent_session_read | 21 | 0 | 0/0 | 21 | 5.281 | 24.596 | 33.215 | 33.215 | 33.215 |
| sqlite.agent_session_save | 35 | 1 | 0/0 | 35 | 21.136 | 248.91 | 6210.06 | 6210.06 | 6210.06 |
| sqlite.card_read | 497 | 1 | 0/0 | 256 | 1.711 | 15.983 | 122.763 | 125.883 | 606.929 |
| sqlite.pr_supervisor_capability_write | 2 | 0 | 0/0 | 2 | 4.993 | 132.815 | 132.815 | 132.815 | 132.815 |
| sqlite.pr_supervisor_completion_due_read | 42 | 0 | 0/0 | 42 | 21.593 | 197.673 | 256.517 | 256.517 | 256.517 |
| sqlite.pr_supervisor_due_read | 42 | 0 | 0/0 | 42 | 3.783 | 91.63 | 208.357 | 208.357 | 208.357 |
| sqlite.pr_supervisor_event_write | 156 | 0 | 0/0 | 156 | 3.55 | 64.738 | 152.541 | 191.0 | 191.0 |
| sqlite.pr_supervisor_watch_read | 156 | 0 | 0/0 | 156 | 5.376 | 96.749 | 114.584 | 307.306 | 307.306 |
| sqlite.pr_supervisor_watch_write | 156 | 0 | 0/0 | 156 | 9.912 | 115.499 | 187.837 | 210.642 | 210.642 |
| sqlite.project_read | 347 | 0 | 0/0 | 256 | 0.852 | 23.882 | 96.719 | 99.88 | 221.53 |
| sqlite.repository_read | 340 | 0 | 0/0 | 256 | 1.207 | 23.282 | 76.37 | 190.612 | 441.419 |
| sqlite.restart_continuation_authorization | 2 | 0 | 0/0 | 2 | 2.756 | 6.616 | 6.616 | 6.616 | 6.616 |
| sqlite.restart_handoff_completion | 68 | 0 | 0/0 | 68 | 6.03 | 84.346 | 251.627 | 251.627 | 251.627 |
| sqlite.restart_handoff_queued | 1 | 0 | 0/0 | 1 | 6.653 | 6.653 | 6.653 | 6.653 | 6.653 |
| sqlite.restart_handoff_resuming | 1 | 0 | 0/0 | 1 | 2.088 | 2.088 | 2.088 | 2.088 | 2.088 |
| sqlite.restart_handoff_session | 34 | 0 | 0/0 | 34 | 5.911 | 15.784 | 50.454 | 50.454 | 50.454 |
| sqlite.restart_handoffs_pending | 17 | 0 | 0/0 | 17 | 3.539 | 9.397 | 9.397 | 9.397 | 9.397 |
| sqlite.restart_handoffs_watchdog | 16 | 0 | 0/0 | 16 | 3.448 | 64.842 | 64.842 | 64.842 | 64.842 |
| sqlite.transcript_append | 23 | 0 | 0/0 | 23 | 38.262 | 192.274 | 205.642 | 205.642 | 205.642 |
| sqlite.transcript_sequence | 1 | 0 | 0/0 | 1 | 6.848 | 6.848 | 6.848 | 6.848 | 6.848 |
| startup.agent_manager | 1 | 0 | 0/0 | 1 | 4.544 | 4.544 | 4.544 | 4.544 | 4.544 |
| startup.async_backend_prepare | 1 | 0 | 0/0 | 1 | 8.078 | 8.078 | 8.078 | 8.078 | 8.078 |
| startup.openapi | 1 | 1 | 0/0 | 1 | 1131.788 | 1131.788 | 1131.788 | 1131.788 | 1131.788 |
| sync.commit_read | 12 | 0 | 0/0 | 12 | 1.751 | 14.882 | 14.882 | 14.882 | 14.882 |
| sync.gc_resume | 1 | 0 | 0/0 | 1 | 51.427 | 51.427 | 51.427 | 51.427 | 51.427 |
| sync.json_decode | 12 | 0 | 0/0 | 12 | 1.319 | 39.799 | 39.799 | 39.799 | 39.799 |
| sync.peer_http | 12 | 0 | 0/0 | 12 | 56.061 | 2724.245 | 2724.245 | 2724.245 | 2724.245 |
| sync.reconcile_head | 12 | 9 | 0/0 | 12 | 657.445 | 3985.631 | 3985.631 | 3985.631 | 3985.631 |
| sync.ref_read | 40 | 0 | 0/0 | 40 | 1.081 | 29.161 | 36.209 | 36.209 | 36.209 |
| sync.startup_reconcile | 1 | 0 | 0/0 | 1 | 81.281 | 81.281 | 81.281 | 81.281 | 81.281 |
| workspace.collect_garbage | 1 | 1 | 0/0 | 1 | 7910.723 | 7910.723 | 7910.723 | 7910.723 | 7910.723 |
| workspace.lease_renew | 1 | 0 | 0/0 | 1 | 147.943 | 147.943 | 147.943 | 147.943 | 147.943 |
| workspace.project_provision | 7 | 0 | 1/0 | 7 | 2292.296 | 3014.164 | 3014.164 | 3014.164 | 3014.164 |
| workspace.reconcile_terminal_state | 1 | 0 | 0/0 | 1 | 1400.375 | 1400.375 | 1400.375 | 1400.375 | 1400.375 |

## Every returned route group

| Route | Count | Decoded bytes | Window samples | p50 | p95 | p99 | Window max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| / | 5 | 127320 | 5 | 14.559 | 105.01 | 105.01 | 105.01 |
| /api/agent/observability/v1/sessions/{session_id} | 1 | 3834 | 1 | 74.656 | 74.656 | 74.656 | 74.656 |
| /api/agent/sessions/{session_id}/restart-handoffs | 1 | 237042 | 1 | 30.972 | 30.972 | 30.972 | 30.972 |
| /api/configuration | 1 | 169949 | 1 | 32.012 | 32.012 | 32.012 | 32.012 |
| /api/fleet/dispatch-jobs/{dispatch_id} | 2 | 358948 | 2 | 17.288 | 33.592 | 33.592 | 33.592 |
| /api/fleet/overview | 5 | 4680585 | 5 | 235.469 | 358.39 | 358.39 | 358.39 |
| /api/fleet/overview/local | 24 | 480800 | 24 | 77.756 | 314.278 | 508.506 | 508.506 |
| /api/health | 20 | 300 | 20 | 1.993 | 2.545 | 3.588 | 3.588 |
| /api/instance | 1 | 560 | 1 | 4.607 | 4.607 | 4.607 | 4.607 |
| /api/instance/maintenance | 1 | 5534 | 1 | 5.769 | 5.769 | 5.769 | 5.769 |
| /api/notifications | 2 | 22218 | 2 | 208.141 | 304.846 | 304.846 | 304.846 |
| /api/owner-ready | 1 | 71 | 1 | 3.875 | 3.875 | 3.875 | 3.875 |
| /api/pr-supervisor/instances/heartbeat | 4 | 68 | 4 | 22.314 | 890.485 | 890.485 | 890.485 |
| /api/pr-supervisor/replicas | 340 | 1883971 | 256 | 41.339 | 188.193 | 480.914 | 588.309 |
| /api/ready | 24 | 2778 | 24 | 3.617 | 9.184 | 14.152 | 14.152 |
| /api/runtime | 20 | 6391068 | 20 | 41.505 | 66.0 | 425.582 | 425.582 |
| /api/status | 50 | 139670 | 50 | 64.079 | 466.457 | 511.282 | 511.282 |
| /api/sync/get | 4 | 3588 | 4 | 13.51 | 1152.632 | 1152.632 | 1152.632 |
| /api/sync/refs | 2 | 518 | 2 | 3.887 | 203.4 | 203.4 | 203.4 |
| /api/ui/assets | 2 | 158 | 2 | 4.067 | 8.425 | 8.425 | 8.425 |
| /api/workspaces | 1 | 1439 | 1 | 8.404 | 8.404 | 8.404 | 8.404 |
| /fleet | 5 | 5756983 | 5 | 337.381 | 745.065 | 745.065 | 745.065 |
| /pull-requests | 5 | 125605 | 5 | 167.854 | 243.263 | 243.263 | 243.263 |
| /{page_path:path} | 15 | 1248460 | 15 | 63.793 | 313.614 | 313.614 | 313.614 |

## Every operation in the retained slow-record window

| Operation | Records | p50 | p95 | p99 | Max ms |
|---|---:|---:|---:|---:|---:|
| agent.recovery_sessions | 1 | 664.677 | 664.677 | 664.677 | 664.677 |
| agent.startup_state_read | 1 | 6220.325 | 6220.325 | 6220.325 | 6220.325 |
| dispatch.completion_pending_read | 1 | 1364.712 | 1364.712 | 1364.712 | 1364.712 |
| dispatch.followup_pending_read | 2 | 711.957 | 1133.354 | 1133.354 | 1133.354 |
| fleet.overview.reachability | 1 | 1377.139 | 1377.139 | 1377.139 | 1377.139 |
| fleet.overview.status | 1 | 1371.58 | 1371.58 | 1371.58 | 1371.58 |
| fleet.overview.sync | 1 | 1424.963 | 1424.963 | 1424.963 | 1424.963 |
| fleet.overview_cache_read | 5 | 1373.396 | 1405.083 | 1405.083 | 1405.083 |
| fleet.overview_cache_write | 3 | 1140.698 | 1434.359 | 1434.359 | 1434.359 |
| health.canonical_obligation | 2 | 535.093 | 1494.664 | 1494.664 | 1494.664 |
| maintenance.run | 1 | 34243.154 | 34243.154 | 34243.154 | 34243.154 |
| progress.checkpoint_write | 1 | 601.012 | 601.012 | 601.012 | 601.012 |
| progress.sequence_allocate | 1 | 553.636 | 553.636 | 553.636 | 553.636 |
| reconciliation.completion_evidence_read | 1 | 1423.154 | 1423.154 | 1423.154 | 1423.154 |
| reconciliation.dispatch_read | 3 | 1370.472 | 3435.117 | 3435.117 | 3435.117 |
| sqlite.agent_session_save | 1 | 6210.06 | 6210.06 | 6210.06 | 6210.06 |
| sqlite.card_read | 1 | 606.929 | 606.929 | 606.929 | 606.929 |
| startup.openapi | 1 | 1131.788 | 1131.788 | 1131.788 | 1131.788 |
| sync.reconcile_head | 9 | 864.606 | 3985.631 | 3985.631 | 3985.631 |
| workspace.collect_garbage | 1 | 7910.723 | 7910.723 | 7910.723 | 7910.723 |

All545rows60concerns remain open. Unobserved names/branches/providers/peers/log history and backup-owned acceptance remain required. Full phases/SQL/fsync/lock/file counters, sizes, errors and before-after build fences remain in raw JSON, not inferred from wall-minus-CPU.
