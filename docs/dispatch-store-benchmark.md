# Dispatch store benchmark

Recorded on 2026-08-04 in the card-scoped macmini worktree without touching or
restarting a running PA instance.

Command:

```console
uv run python scripts/benchmark_dispatch_store.py
```

The isolated temporary fixture contained 200 dispatches and 25,000 preexisting
idempotency receipts. The benchmark then committed 500 alternating progress and
heartbeat updates as quickly as possible, which is a stronger load than the
acceptance rate of 50 writes per second.

```json
{
  "commit_latency_ms": {
    "max": 235.929,
    "p50": 0.261,
    "p99": 4.611
  },
  "elapsed_seconds": 0.297,
  "fixture": {
    "dispatches": 200,
    "mixed_writes": 500,
    "prior_receipts": 25000
  },
  "legacy_bytes_unchanged": 1383631,
  "row_counts": {
    "dispatches": 200,
    "final_reports": 0,
    "heartbeats": 100,
    "progress_events": 250,
    "receipts": 25500
  },
  "store_bytes": 12552928,
  "wal_checkpoint": {
    "busy": 0,
    "checkpointed_pages": 146,
    "duration_ms": 1.376,
    "truncated": false,
    "wal_pages": 146
  },
  "writes_per_second": 1682.4
}
```

The maximum includes the one-time fully synchronous legacy migration transaction;
steady incremental writes remained below the 25 ms p99 criterion. The companion
test `test_one_heartbeat_is_delta_only_and_never_rewrites_legacy_or_history`
instruments SQL and serialization: one heartbeat executes exactly three row
mutations (heartbeat, receipt, dispatch watermark), serializes one bounded dispatch
core, performs no progress-history delete/rewrite, and leaves the legacy source
byte-for-byte and mtime unchanged.
