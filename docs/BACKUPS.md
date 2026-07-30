# Metadata backups and restore

PA creates verified local metadata recovery points independently on every
instance. The default interval is six hours, with bounded deterministic jitter
so a fleet does not start storage I/O at the same instant. A process-local
single-flight lock and durable idempotency keys prevent overlapping or duplicate
jobs after restarts.

## Recovery set

The `pa.metadata-backup/v1` archive is the minimal coherent PA metadata recovery
set:

- `projection.sqlite3`: created with SQLite's online backup API while PA's
  event/projection mutation boundary is held;
- `sync_refs.json`: the exact durable realm refs at that boundary;
- `objects/`: verified content-addressed event and commit objects;
- `manifest.json`: instance identity, PA/schema version and fingerprint,
  durable and projection heads, verification policy, exclusions, sizes, and
  SHA-256 checksums.

The archive excludes `config.json`, session and sync secrets, attachment blobs,
repositories and worktrees, PR-supervisor state, logs, telemetry, caches, and
runtime state. Attachment manifests remain metadata, but attachment bytes use
normal PA attachment sync/recovery. The manifest records these exclusions.
Backups are never ordinary realm-sync objects.

WAL and SHM sidecars are never copied. The online SQLite snapshot is normalized
to a standalone rollback-journal database, checked with `quick_check` or
`integrity_check`, and compared with the durable refs. PA writes a private
temporary archive and atomically renames it only after verification. The
destination directory must be owner-only (`0700`); archives and state are
`0600`.

The local backend boundary is intentionally small (`health`, `publish`, `list`,
and `delete`) so object-storage backends can preserve the same manifest,
verification, atomic-publication, retention, and restore semantics later.

## Configuration

Defaults:

| Setting | Default |
| --- | --- |
| enabled | `true` |
| interval | `21600` seconds (six hours) |
| retained copies | `8` |
| destination | sibling `<PA_DATA_DIR>-backups` |
| run on startup | `false` (configurable minimum age when enabled) |
| verification | `full` |
| compression | `true` |
| concurrency | `1` |
| alert threshold | `3` consecutive failures |
| jitter | up to `300` seconds, bounded to 10% of the interval |

Count retention is required. Maximum age and maximum total size are optional.
Pruning is oldest-first with backup ID as the deterministic tie-break. PA
verifies candidates before pruning, audits each decision, skips corrupt
archives, and never deletes the last known-good recovery point. A failed new
attempt does not prune an older good archive.

Use Settings → Backups, the `/api/backups` endpoints, MCP `backup_*` tools, or:

```bash
pa backup status
pa backup config
pa backup config --interval-seconds 21600 --retention-count 8
pa backup run --idempotency-key operator-ticket-123
pa backup list --verify
pa backup inspect BACKUP_ID
pa backup verify BACKUP_ID
pa backup export BACKUP_ID --output ./recovery.pa-backup.tgz
```

The MCP `backup_export` operation returns verified size/SHA-256 metadata and an
administrator-authorized local download URL rather than placing a potentially
large binary archive into the MCP response.

`pa backup status --json` reports configured and effective values, the source
of each value (`default`, `config.json`, or a backup environment variable such
as `PA_BACKUP_INTERVAL_SECONDS`), destination health and pressure,
next run, last attempt/success,
duration/size/failure history, metrics, and the repeated-failure alert state.
A missing, disconnected, read-only, exposed, or full destination fails the
backup visibly without affecting PA runtime.

## Guarded restore

Restore is intentionally split into online validation and offline mutation:

1. While PA is running, inspect and verify the archive.
2. Create a restore request:

   ```bash
   pa backup restore-initiate BACKUP_ID
   ```

3. Record the request ID and stop the owning writer with `pa stop`.
4. Run the exact command returned by the request:

   ```bash
   pa backup restore BACKUP_ID --request-id RESTORE_ID
   ```

5. Start PA and run `pa sync status` for every subscribed realm.

The offline command takes the same exclusive `server-writer.lock` used by the
server and refuses to proceed while a writer is running. It verifies archive
checksums, SQLite integrity, instance identity, and schema compatibility before
changing live state. It first creates and verifies a protected `pre_restore`
backup. Replacement is staged; projection, refs, and event objects are swapped
under the writer lock, with rollback copies retained on failure. The result and
recovery instructions are appended to the owner-only backup audit log.

Restore preserves the archived durable refs exactly. It does not fast-forward,
force, or choose a realm head. If post-restore durable and projection heads
differ, the result is `reconciliation_required`; keep PA writes paused and use
`pa sync reconcile` or PA's supported sync conflict-resolution workflow. A
divergent history must be resolved as a two-parent merge with explicit values,
never by editing `sync_refs.json`.

API and MCP restore initiation only create the guarded maintenance request.
They cannot overwrite the running server's database.

## Observability and recovery

Structured `backup.event` logs and `backup_audit.jsonl` records cover job
start/success/failure, overlap skips, verification, pruning, configuration,
missed schedules, destination pressure, and restore start/result. They include
IDs and bounded reasons, never secrets or database contents.

If a restore reports failure:

- leave PA stopped;
- preserve the reported rollback staging directory;
- verify the pre-restore backup ID;
- inspect `backup_audit.jsonl`;
- correct destination/schema/storage problems before retrying;
- do not edit SQLite or sync refs directly.
