# Derived DAG index

PA's immutable content-addressed objects and durable realm refs remain canonical.
`dag_index.db` is a server-owned, WAL-mode SQLite derivative beside the refs. It
contains commit provenance, ordered parent edges, entity/event provenance,
idempotency keys, decoded event metadata, and a verified published head. It is
safe to delete and rebuild through `POST /api/sync/index/maintenance` (or the
matching MCP tool); operators must not edit it directly.

## Production traversal inventory and budgets

| Caller | Class | Indexed behavior | Authoritative budget |
|---|---|---|---|
| `append_event` duplicate/version preflight | mutation critical | entity-only rows; zero object reads; repair before locks | no traversal under event/ref locks; retry if the parent fence changes |
| `entity_history_page`, `entity_history`, item history API/MCP | request | matching rows plus a SQLite parent-edge CTE; canonical objects are not reopened | no synchronous object-DAG fallback for a real durable head; page limit 500 |
| `recent_entity_events` activity UI | request | newest matching rows, limit 80 | legacy fallback capped at 2,000 commits only for explicit/synthetic verifier contexts |
| `entity_snapshot`, recovery hydration, merge conflict snapshots | request/mutation recovery | matching entity events only | authoritative replay is offline/reconcile only, capped at 100,000 commits |
| `find_operation_event` mutation recovery and sync receipts | mutation critical | covering idempotency lookup plus ancestry; validates the few matching canonical commits | no whole-realm request scan |
| `is_ancestor`, sync direction and receipt revisions | sync/request | parent-edge recursive CTE; zero object reads | authoritative cap 100,000 only when the descendant is not indexed |
| `compatible_histories`, `_ancestors`, merge audit | sync/reconcile/diagnostics | partially indexed; explicit convergence work remains bounded/offloaded | 100,000 commits, 30–120 second worker deadline |
| `apply_commit_chain`, projection `rebuild_from_log` | startup/reconcile | canonical verifier by design | 100,000 commits, off event loop, one worker per realm |
| `_iter_commits_parent_first` | authoritative primitive | never the default indexed request implementation | 100,000 commits; cycle/missing/corrupt objects fail closed |
| sync engine object `get` / HTTP `get_many` | sync transfer/validation | hashes are requested explicitly | request-provided object set and transport body limits; no reachability discovery per object |

Request telemetry includes `scanned_commits=0`, `index_result=hit`, requested
head, and indexed head for indexed history. Sync status exposes schema version,
state, generation, indexed/durable heads, counts, last success/failure, and build
duration. The index publishes `ready` only after one transaction has verified and
inserted every object reachable from the requested head. Missing, corrupt, or
future-schema objects abort publication and leave an actionable failed state.

## Storage and scaling policy

The index stores normalized provenance and compact decoded event JSON so matching
queries do not reopen thousands of tiny files. It does not move refs or canonical
objects into SQLite. Parent edges are linear in commit count; ancestry is computed
inside SQLite rather than materializing a quadratic transitive-closure table.

Rebuilds are checkpointed, cancellable (`action=cancel`), and resumable. When the
published indexed head is an ancestor of the durable head, `ensure_indexed`
prefers an incremental advance before a full rebuild.

See [SYNC_HISTORY_SCALE.md](SYNC_HISTORY_SCALE.md) for object-store retention,
snapshot epochs, GC planning, and protocol-v3 anti-entropy at large history
sizes.

Do not add packfiles merely for commit-count latency. Reconsider an append-only
pack only when measurements show at least one million objects or object filesystem
metadata exceeds 25% of retained object bytes, and only with dual-read migration,
hash verification, crash-safe rollback to loose objects, and independent ref
authority.

## Compatibility and PR #314

This change is independent of unmerged PR #314 (`ce11fa4`). That PR's lock-free
succeeded-receipt lookup can compose on top of the indexed
`find_operation_event`; neither change depends on the other's operational-table
layout. Rebase and conflict resolution should therefore preserve #314's receipt
serialization changes and this change's EventLog index boundary.
