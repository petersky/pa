# Inspect canonical continuation evidence without recovery

`GET /api/agent/observability/v1/sessions/{session_id}/restart-handoffs/{handoff_id}/canonical-evidence`
and ordinary MCP `get_restart_canonical_evidence` read canonical transcript-index
metadata. Unlike the existing reconcile POST, they write no audit and never
transition a receipt, reopen a session, spawn/load a provider or send queued work.

Supply exact serving `expected_authority_instance_id`, receipt `expected_version`
and `expected_attempt`, saved `expected_session_version` (timezone required), and
`expected_native_id` (empty string explicitly asserts no saved native identity).
The authenticated owner or administrator must own read access. Saved session and
receipt authority, version, attempt and native identity must match. The original
continuation prompt ID is obtained from the receipt, never supplied as replacement
text. No config, continuation text, error, private answer or raw transcript body is
loaded by the metadata identity query. The session/receipt snapshot is rechecked
after evidence is read. This is observation, not admission or a workspace lease.

Each page examines at most128 canonical sequence rows, default64. Initial
`after_seq=-1` includes sequence zero. Continue with returned `next_after_seq` and
the same returned `snapshot` as `expected_snapshot`; a changed population rejects
the read. Page rows are read twice, and sequence population/count metadata plus
retention-evidence population are checked before/after. These are bounded
read-consistency checks, not a claim of complete historical retention. Snapshot
exhaustion means only that the currently retained sequence population is exhausted.

Only exact prompt matches in allowlisted lifecycle types are emitted as event ID,
sequence, type, creation time and payload hash. A `turn_completed` match requires
`queued_prompt_id`, not an unrelated generic `id`. Prompt completion metadata and
receipt-attempt attribution are separate: explicit attribution requires both exact
`handoff_id` and an integer `attempt` in that event. Do not attribute an old prompt
completion to the receipt's current attempt merely because the prompt ID agrees.
The response always retains `historical_effect=unknown`: even an attributed
completion does not certify task effects or resolve a deliberate close/dismissal.

Cold objects are not opened. Invalid or larger-than32KiB hot payloads are not
decoded, and raw payloads never leave SQLite. Such rows are counted as
uninspectable. A missing match, cold identity, pruned row or unfinished page can
never prove non-submission or non-delivery. `retention_complete` remains unknown;
the existing wire reader and receipt inspection remain separate evidence sources.

Responses are private/no-store and limited to128KiB. Foreign ownership yields403;
missing exact identity404; stale/unsupported snapshots409; unavailable storage or
deadline503 with unknown outcome. No exception payload is echoed. Assigned-service
tool access is unchanged. Inspecting a closed/dismissed original receipt is allowed;
this does not release its fence, restore its native history or authorize replay.

For retained be010d09 and c1b1b358, the next step after reviewed integration is one
coordinated runtime capability check and, only where loaded, this passive query
with freshly observed exact preconditions. Preserve both original receipts and
their outcomes. If the reader is absent, arrange one supported activation with
the lifecycle/cleanup owner rather than another historical restart or duplicate
installation. Source integration, successful metadata reads and saved result
records do not satisfy runtime recovery acceptance.
