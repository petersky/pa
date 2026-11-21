# PR supervisor worker health and recovery

`GET /api/pr-supervisor/health` exposes `worker` health for both the polling
worker and its supervising task. `alive` describes the worker;
`supervisor_alive` and `supervisor_state` describe its controller. Controller
states distinguish `not_started`, `running`, `exited`, `failed`, `cancelled`,
and `closed`. `supervisor_failure_kind` is a fixed category, without exception
text. `orphaned_worker` means a worker is still live without a live controller;
it is not evidence that the worker's effects have finished.

Progress ages and stall detection use the same monotonic clock. Persisted
timestamps are historical diagnostics, not a current heartbeat. A diagnostics
write failure increments `diagnostics_failure_count`, sets
`diagnostics_state=write_failed`, and records a bounded failure category in
memory while supervision continues. Successful later persistence restores
`diagnostics_state=ready`. These files are diagnostic snapshots, not watch
ownership or effect-authority records.

Creating an actionable watch or refreshing an existing one may restart an
already-started controller that has finished. It does not start the service
before normal startup, run during shutdown, or create a second worker around
a live orphan. The same watch ID, history, and policy remain in use. The
normal capability probe, repository scope, authority lease/fence, repeated
observations, stable-head interval, review checks and effect authorization all
still apply. Refresh success means scheduling succeeded; inspect subsequent
observations to establish recovery. Missing observations do not authorize a
duplicate watch or a merge.

This behavior requires the source revision to be loaded. An older service's
accepted refresh cannot activate new code. Operational activation belongs to
the designated lifecycle executor; this reader/recovery behavior never
installs packages, restarts PA or replays managed continuations.
