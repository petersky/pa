# Sync object store, retention, and scale

PA's multi-instance sync history is an immutable, content-addressed DAG. This
document defines what those objects are, which indexes are derived, how retention
and garbage collection work, and how operators maintain a fleet whose history
grows into hundreds of thousands of objects.

## Authoritative versus derived

| Artifact | Role | Safe to delete? |
|---|---|---|
| `objects/<ab>/<hash>` | Authoritative content-addressed bytes (commits, events, epochs, snapshots) | Only after an acknowledged snapshot epoch permits reclaim |
| `sync_refs.json` | Authoritative realm/instance heads | Never by hand |
| SQLite projection (`pa.db`) | Derived card/project view | Rebuild from the event log |
| `dag_index.db` | Derived provenance index | Delete and rebuild via `/api/sync/index/maintenance` |
| `object_catalog.db` | Derived object presence/size/class index for status/GC | Rebuild via `catalog_rebuild` |
| `sync_epochs.json` | Local epoch registry and ACK state | Recreated from epoch objects |
| `sync_gc_journal.json` | GC audit / crash journal | Retain for audit |

## Object classes

Each ordinary mutation produces:

1. One or more immutable **CardEvent** objects.
2. One immutable **SyncCommit** object referencing those events and parent
   commit(s). Merge commits preserve both parents.

Additional classes:

- **snapshot_epoch** — versioned checkpoint root with parent-epoch provenance,
  fencing token, projection digest, and an embedded card snapshot used for
  rebootstrap after reclaim.
- **snapshot** — legacy unreferenced compaction object (still written only when
  epoch advancement is unavailable).
- **snapshot_epoch_ack** — peer acknowledgement evidence.

Unreachable objects may remain as diagnostic or conflict ancestry. They are not
deleted by normal sync.

## Status (indexed, hot-path safe)

`GET /api/sync/status` reports:

- store-global totals (count, bytes, oldest/newest mtime, growth samples)
- per-realm reachable commit/event/auxiliary counts and unreachable count
- retention reasons for each class
- DAG index, snapshot epoch, recovery, and quarantine diagnostics

Counts come from `object_catalog.db` and `dag_index.db`. The status handler does
**not** scandir the object store. After upgrade, the server compares catalog
population to DAG-reachable commit+event counts. When coverage is below 95%,
status reports `history.catalog.stale=true` / `ready=false`, refuses to invent
unreachable counts, and schedules a **background checkpointable catalog
backfill** on startup. Operators can also force:

```http
POST /api/sync/index/maintenance
{"action":"catalog_rebuild","realm_id":"default"}
```

Rebuilds are resumable (`resume=true` by default) and cancellable via
`action=catalog_cancel`.

## Snapshot epoch protocol (v1)

1. Authority creates an epoch via `POST /api/sync/epoch` (or `compact_realm` with
   an `EpochRegistry`). The epoch object is content-addressed and fenced.
2. Ordinary sync replicates the epoch object fleet-wide.
3. Each subscribed instance acknowledges with `POST /api/sync/epoch/ack`.
4. Quorum is evaluated against the explicit `required_instance_ids` set.
5. Only after quorum is `reclaimable=true` may GC delete pre-epoch unreachable
   ancestry. Offline peers that never ACK keep history pinned and must
   **rebootstrap from the epoch root** after reclaim.

Epochs never silently rewrite refs. Merge and audit correctness for retained
history are unchanged; deleted ancestry is explicitly unrecoverable without the
epoch snapshot.

## Garbage collection

```http
POST /api/sync/gc/plan      # dry-run by default
POST /api/sync/gc/execute   # requires confirm=true and a non-dry plan
```

Planning includes:

- reachability from the current realm head
- safety window (default 14 days) for recently written unreachable objects
- active peer-head pins from convergence state
- operator pins, backup/recovery pins
- epoch + parent-epoch pins
- missing ACK list and explicit rebootstrap requirement

Deletes are journalled (rename-to-tombstone then unlink) so a crash can resume
without leaving half-applied reclaim.

## Anti-entropy (protocol v3)

Normal peer exchange is head-first and O(delta) for converged peers:

1. Fetch peer refs.
2. If the peer tip is already local, skip object transfer.
3. Otherwise pull missing objects along the peer tip with bounded `/api/sync/get`
   pages; push with `/api/sync/need` + batched `/api/sync/push`.

Inventory pages never abort when a single commit's event fanout exceeds the
bounded inventory size: remaining event hashes spill into later pages. Push and
reconcile catch up the SQLite projection **before** publishing a durable tip so
a failed rebuild cannot leave `head` ahead of `projection_head`. Converge also
repairs local projection lag before peer exchange.

Legacy full-history preparation is **not** used on the normal path. Peers that
only accept legacy bundles and whose reachable history exceeds the soft limit
(`LEGACY_BUNDLE_SOFT_LIMIT`, 2000 objects) are quarantined as
`protocol_incompatible` without encoding the whole DAG. There is no global 20k
ceiling on retained history; transfer batches remain bounded.

`POST /api/sync/have` accepts at most 512 hashes and reads the catalog. Prefer
`/need` for inventory.

## DAG index rebuild

Rebuilds are checkpointed every 250 commits, cancellable
(`action=cancel`), and resumable. Incremental advance is preferred when the
indexed head is an ancestor of the durable head. The index never materializes a
quadratic transitive-closure table for publication; ancestry uses parent edges.

## Retention defaults

| Class | Default retention | Why |
|---|---|---|
| Reachable commits/events | Indefinite | Verify hashes, rebuild projection, merge, audit |
| Snapshot epochs | Indefinite (pinned) | Rebootstrap + GC floor |
| Unreachable (no epoch quorum) | Indefinite | Offline peer / conflict evidence |
| Unreachable (quorum + outside safety window) | Reclaimable via audited GC | Operator-approved reclaim |
| Object catalog / DAG index | Disposable | Derived |

## Operator maintenance

| Goal | Action |
|---|---|
| Inspect scale | `GET /api/sync/status` / MCP `sync_status` |
| Backfill catalog after upgrade | Automatic on startup when stale; or `POST /api/sync/index/maintenance` `catalog_rebuild` |
| Cancel catalog backfill | `action=catalog_cancel` |
| Rebuild DAG index | `action=rebuild` (cancellable) |
| Open a retention checkpoint | `POST /api/sync/epoch` then peer ACKs |
| Plan reclaim | `POST /api/sync/gc/plan` |
| Execute reclaim | `POST /api/sync/gc/execute` with `confirm=true` |
| Repair projection | MCP `sync_reconcile` |

## Upgrade and rebootstrap

1. Upgrade all peers to a release that includes protocol v3 and epoch/GC APIs.
2. Run `catalog_rebuild` on each instance.
3. Verify `/api/sync/status` shows indexed realm counts without multi-second stalls.
4. Before reclaiming, create an epoch, replicate, and collect ACKs from every
   subscribed instance you still need to support without rebootstrap.
5. Instances that miss the epoch after GC must rebootstrap from the epoch
   snapshot (embedded card state + post-epoch commits), not from deleted
   ancestry.

## Performance targets

- Ordinary local Home/Work/card/session/API health: p95 &lt; 2s while sync runs
- Sync status from maintained indexes: &lt; 500ms
- Converged anti-entropy cost proportional to head delta
- Bounded inventory/transfer batches; no global retained-history ceiling at 20k
