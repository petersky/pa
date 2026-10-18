# Metadata backups and restore

PA captures local metadata independently on every instance and distinguishes
fully intact recovery points from captures with disclosed historical data missing.
The default interval is six hours, with bounded deterministic jitter
so a fleet does not start storage I/O at the same instant. A process-local
single-flight lock and durable idempotency keys prevent overlapping or duplicate
jobs after restarts.

## Recovery set

The `pa.metadata-backup/v1` archive is the minimal coherent PA metadata recovery
set:

- `projection.sqlite3`: created with SQLite's online backup API from a read view
  anchored at PA's event/projection mutation boundary;
- `sync_refs.json`: the exact durable realm refs at that boundary;
- `objects/`: verified content-addressed event and commit objects;
- `transcripts.sqlite3` and `transcript_objects/`: the transcript index and
  content-addressed cold payloads, when the source has a transcript store;
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

The v1 manifest supports up to 250,000 files and 64 MiB of JSON. Both the
writer and reader enforce these limits; an oversized snapshot fails before
archive publication and leaves existing recovery points intact. New archives
put the manifest first so reading it does not decompress all payloads. Legacy
v1 archives remain compatible. Verification still checks paths, duplicate
members, file sizes and hashes, SQLite/schema integrity, durable refs and
reachable event/transcript objects.

The local backend boundary is intentionally small (`health`, `publish`, `list`,
and `delete`) so object-storage backends can preserve the same manifest,
verification, atomic-publication, retention, and restore semantics later.

## Historical data missing

A cold transcript payload may already be absent from the source. PA preserves
the captured index, its live references and all available data, and records the
exact absent SHA-256, decoded/stored byte counts and reference count in
`missing_transcript_objects`. Such a capture uses **`pa.metadata-backup/v2`**.
Complete captures remain v1; older readers reject v2 rather than overlook the
limitation. The existing file-count and manifest-size limits still apply.

Verification checks that declarations match exactly the absent payloads in the
captured index. It still rejects corruption, undeclared missing files, changed
hashes or reference metadata, event-graph damage, unsafe paths, schema damage
and inconsistent durable heads. An existing source file that was not captured
is an error, not permission to declare it missing. No missing bytes or replacement
references are created.

A limited archive has `integrity_verified=true`, `data_complete=false`, and
`verification_status=verified_with_missing_data`. **`verified=false`** still
means it is not a fully intact recovery point. The UI, CLI and API disclose the
limitation and its size; PA creates one durable warning per limitation set.
`last_success` and `last_known_good` retain their fully intact meanings.
`last_completed` and `last_completed_backup` separately identify captures that
preserve the available state. Legacy run history keeps `status=success` for
publication but `verification=failed` and an explicit reason for a limited
capture, so an older state reader cannot treat it as intact.

After an integrity-checked limited publication, future scheduled captures can
continue at the unchanged configured cadence. The operational failure streak
clears, while the historical warning and earlier failed runs remain. Limited
captures follow retention bounds among themselves; while the newest capture
is limited, every fully intact recovery point is protected. Unknown or changed
archives remain excluded from automatic pruning. A preserving manual capture
(`preserve_existing=true`) protects every current archive for that run.

Limited archives can be inspected, verified and exported with their v2
declarations. Full restore refuses them before changing live state. A limited
pre-restore capture also cannot satisfy the intact rollback-backup requirement.
This supports future capture of available data; it does not recover the missing
historical payload or provide a fully intact restore.

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
uses verification records tied to the archive's file identity before pruning,
audits each decision, skips corrupt or unverified
archives, and never deletes the last known-good recovery point. A failed new
attempt does not prune an older good archive.

Use Settings → Backups, the `/api/backups` endpoints, MCP `backup_*` tools, or:

```bash
pa backup status
pa backup config
pa backup config --interval-seconds 21600 --retention-count 8
pa backup run --idempotency-key operator-ticket-123
pa backup list --verify
pa backup list --offset 50 --limit 50
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

Status and ordinary inventory reads never open archives or trigger verification.
The page computes status and its recovery-point page from one inventory scan.
Inventory uses cached verification metadata plus filesystem identity (device,
inode, size, modification and change timestamps). Replacing or changing an
archive invalidates its cached result. Legacy records without that identity and
newly discovered archives appear as `unverified`, not `failed`; select Verify
to check them. Verification in progress and recorded failures have distinct
states and reasons. The last successful run remains historical; the last known
good retained recovery point requires a current verification record.

`GET /api/backups`, MCP `backup_list`, and CLI `backup list` accept `offset`
and `limit` (default 50, maximum 200). `verify=true` verifies only that page.
List responses omit manifests; inspect/verify responses include them. Status
omits the potentially large per-archive verification and idempotency maps.
Destination health on reads checks permissions without creating probe files;
backup execution still performs a real write probe.

At `alert_after_failures`, automatic startup, scheduled backups and periodic
scrubs pause. PA creates one durable service-health notification per failure
streak with a link to Backups. The page shows the reason, last recovery point,
and Administrator as the next actor. Correct the cause and explicitly run a
manual backup; an integrity-checked publication clears the operational streak
and resumes scheduling, with any historical limitation still disclosed as above.
The pause survives restart. Raising the configured threshold can
also make automatic attempts eligible again. Temporary attempts are recorded
as runs, not retained archives; successful maintenance removes obsolete
verification entries left by earlier versions.

For acceptance of a repair build, first use isolated fixtures to verify a
manifest above 2 MiB in compressed and uncompressed archives, changed/damaged
archives, concurrent verification, pagination, and failure-pause recovery.
Live-instance acceptance must record the exact build, affected instance and
scenario through the card's independent acceptance workflow. A merge or passing
fixture suite alone does not establish live recovery. Live backup runs,
verification sweeps, configuration changes, restore and deployment require their
own authorization.

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
