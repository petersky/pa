# Home, fleet, and session latency remediation

The September 13 investigation measured Home partials at 11.7–18.4 seconds,
session creation at 14–40 seconds, and a 5.1 MB Fleet response. Those are the
reported production baseline, not results from this worktree.

The changes address the shared background workload and each page's extra work:

1. Fleet activity carries bounded dispatch summaries. Full materialization plans,
   admission proofs, and historical evidence remain on dispatch detail endpoints.
   Local probes, older peer responses, and legacy disk caches all pass through
   the same summary boundary. Cache persistence writes only the changed dimension
   under `fleet_overview_cache.d`; tombstones preserve invalidation across restarts.
   Slow fsync does not hold the lock used by cache readers. The old monolithic
   file is read for compatibility and is no longer rewritten.
2. Completion reconciliation filters for pending, blocked, or prompted work and
   due retry times before limiting results or copying Pydantic records. Periodic
   sync backs off each failed peer from 10 seconds up to 5 minutes, while healthy
   peers continue and explicit convergence can retry immediately. Quarantined
   peers do not enter the push path. Object catalog count and byte totals are
   maintained transactionally by SQLite triggers, with one initial backfill;
   status no longer scans all objects or publishes a write on every read.
3. CardProjection keeps one SQLite connection per thread, preserving nested
   commit/rollback semantics and per-call lock timeouts. Only budgeted operations
   install the Python progress callback. Connections detect an offline restore's
   replacement database. Restore checkpoints SQLite WALs before swapping files
   so old WAL pages cannot overwrite restored state.
4. Home uses SQL lane counts plus body-free, batched lifecycle evidence. It
   neither builds fleet topology nor constructs a Workshop snapshot, and it no
   longer walks every ordinary card or gets an execution session for every card.
   Cards with dispatch, session, or actionable review evidence still use the
   canonical presenter so operator input, queued work, and historical outcomes
   keep their existing meaning. GET activity projections no longer expire
   reservations; the dispatch worker owns expiration.
5. The execution catalog warms in the background during the server lifecycle.
   Admission reuses still-valid evidence while a refresh runs. Cold or expired
   evidence still requires discovery; the five-minute evidence validity check
   and connection-revision checks remain in force. Codex startup consumes
   already-reported MCP failures without a fixed two-second wait. Its existing
   session-specific observer continues to report pending, usable, or failed
   state. Actual provider initialization and required workspace provisioning
   remain necessary; no fixed improvement for those external operations is claimed.
6. Session list and Agent page queries filter purpose/archive state in SQLite,
   backed by an index. The page's card selector does not read bodies. Snapshot
   and history metadata assembly and JSON encoding run off the event loop, and
   HTTP responses reuse those encoded bytes. With a known owner, history fetch
   overlaps route resolution; snapshot application and SSE connection do not
   wait for history. Owner/selection checks still reject obsolete responses.
7. Maintenance runs in a background worker at every server startup and then
   every 24 hours by default (`maintenance_interval_seconds=86400`). After
   retention cleanup it maintains both `pa.db` and `pa.transcripts.db`: SQLite
   integrity checks, planner statistics, WAL checkpoints, and transactional
   VACUUM when free pages can be reclaimed. Disk-space checks prevent starting
   a rewrite without enough room. A busy database is reported as deferred and
   retried on the next sweep. VACUUM holds SQLite's write lock while it runs, so
   writes can wait for compaction to finish. Files are never swapped beneath
   live connections.

`pa maintain status` reports the last sweep, including per-database allocated
and free bytes before/after compaction and any deferral reason. `pa maintain run`
triggers the same sweep manually; concurrent requests share the active worker,
including when a request disconnects. Server shutdown signals SQLite to interrupt
unfinished maintenance. The interval remains configurable; explicit existing
configuration overrides are preserved.

`pa maintain compact` also remains available for offline primary-database
compaction under PA's exclusive data-directory writer lock. Stop PA before using
that command. Deploying this worktree is required for the new automatic behavior;
the installed service and its database have not been changed.

Regression coverage includes thread isolation and rollback, dimension write
amplification and legacy cache migration, filtering before dispatch copies,
transactional catalog totals, per-peer backoff, concurrent catalog refresh,
sidebar SQL filtering, Home counts and prohibited expensive reads, offline
compaction, startup/daily scheduling, busy-reader/writer deferral, disk-space
checks, cancellation and coalescing, retained-projection restore, and delayed
route/history/SSE behavior.

Run `uv run python scripts/benchmark_investigation_performance.py` for a disposable
fixture containing 300 cards, 302 sessions, and seven HTTP samples per route.
Set `PYTHONPATH` to another checkout's `src` directory to measure that source with
the same fixture. This benchmark does not use production data or spawn providers.

Measured on the same host against baseline commit `09432c0`, after a warmup:

| Route | Baseline median | Worktree median |
| --- | ---: | ---: |
| Home sections | 38.059 ms | 1.594 ms |
| Agent page | 31.884 ms | 6.962 ms |

These synthetic timings isolate retained-data amplification. They do not
represent the reported production workload, external provider initialization,
or future deployed latency.

Final validation: 3,038 pytest tests and 876 subtests passed, with three warnings,
using four workers and the CI exclusions for header layout, fleet topology layout,
and browser E2E. The focused restore/compaction checks and the Node owner/history
overlap harness passed. Boot smoke and wheel/source distribution builds passed.
A further interruption test passed separately: cancelling an active VACUUM
retained all 50,000 fixture rows and passed SQLite's integrity check.
