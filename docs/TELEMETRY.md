# Resource telemetry

PA collects low-overhead host and PA-owned agent resource samples for the live
header, agent chat, and Reports page. Telemetry is operational data: it is kept
in its own SQLite database and is never written to `pa.db`, the event log, or
ordinary realm sync.

## Collection and quality

The sampler runs outside request handling, has one worker thread and a bounded
write queue, and drops samples instead of delaying dispatch or agent execution.
Collection and storage errors are reported through telemetry health without
making the PA service unhealthy.

Every value includes its unit, source, collection timestamp, collection
duration, freshness, and one of these quality states:

- `measured`: obtained directly from an operating-system counter;
- `estimated`: derived from measured counters, such as interval latency;
- `unavailable`: supported in principle but absent or unreadable now;
- `unsupported`: the platform cannot attribute the value safely.

Linux and macOS collectors normalize rates to bytes/second, CPU and capacity to
percent, memory and storage to bytes, and latency to milliseconds. The first
sample for cumulative I/O and network counters is unavailable because there is
no prior counter from which to calculate a rate.

Session attribution starts at the exact PID and process creation time of a
PA-launched provider. Descendants are accepted only while that ownership chain
remains valid. Creation-time checks prevent PID reuse, and recently detached
descendants are retained briefly as explicitly orphaned PA-owned processes.
Exited processes disappear from the next sample.

Portable per-process network attribution is not reliable on Linux or macOS, so
session network activity is reported as `unsupported`; host network counters
remain measured. Process I/O counters are summed where the OS exposes them.
Errors reading an owned process become partial or unavailable data and never
cause inspection of unrelated processes.

## Privacy and privileges

The collector records numeric counters and PA identifiers only. It does not
collect command arguments, environment variables, prompts, file names or
contents, credentials, socket payloads, peer addresses, or metadata about
unrelated processes. Public responses and exports omit root PIDs and internal
principal identifiers.

No root access is required. PA uses the permissions of its service account.
Linux `/proc` restrictions, container namespaces, hardened macOS process
controls, or another-user processes may make individual dimensions unavailable.
Do not grant broad process-inspection or network-capture privileges solely for
telemetry.

Session-linked reads are filtered to the authenticated principal when auth is
enabled. Fleet instance bearer credentials may read instance summaries and
series, but cannot query session dimensions or export session data.
Configuration and maintenance operations require an administrator.

## Storage and retention

The default database is `<PA_DATA_DIR>/telemetry.db`. It has independent schema
migrations, WAL locking, integrity checks, corruption quarantine, pruning, and
compaction. It must not point at `pa.db`. Five-minute rollups support long
ranges without retaining every raw point.

Pruning is deterministic:

1. remove raw samples older than the raw-retention boundary;
2. remove rollups older than the rollup-retention boundary;
3. if the maximum database size is still exceeded, remove the oldest raw
   samples in bounded batches, then the oldest rollups;
4. checkpoint the WAL and record the result.

Storage status reports the database and WAL size, oldest and newest retained
timestamps, raw and rollup counts, dropped samples, and the last pruning result.
If storage is slow, the bounded in-memory queue applies backpressure by dropping
new persistence batches while live sampling continues.

## Configuration

Settings can be supplied with `PA_` environment variables, persisted instance
configuration, or the Settings UI. The effective configuration endpoint reports
values and their sources.

| Setting | Default | Constraint |
| --- | --- | --- |
| `PA_TELEMETRY_ENABLED` | `true` | Boolean |
| `PA_TELEMETRY_LIVE_INTERVAL_SECONDS` | `5` | 1–300 seconds |
| `PA_TELEMETRY_PERSISTENCE_INTERVAL_SECONDS` | `30` | 5–3600 seconds and not below live interval |
| `PA_TELEMETRY_RAW_RETENTION_HOURS` | `168` | 1–8760 hours |
| `PA_TELEMETRY_ROLLUP_RETENTION_HOURS` | `2160` | 1–43800 hours and not below raw retention |
| `PA_TELEMETRY_MAX_DATABASE_BYTES` | `536870912` | 16 MiB–64 GiB |
| `PA_TELEMETRY_DATABASE_PATH` | `<PA_DATA_DIR>/telemetry.db` | Must be outside `pa.db`, sync refs, and the object store; restart after changing |
| `PA_TELEMETRY_PER_SESSION_ENABLED` | `true` | Boolean |
| `PA_TELEMETRY_UI_REFRESH_SECONDS` | `5` | 2–300 seconds |
| `PA_TELEMETRY_DEFAULT_REPORT_RANGE` | `1h` | `15m`, `1h`, `6h`, `24h`, `7d`, or `30d` |

The agent resource header preference is `hidden`, `compact`, or `expanded` and
defaults to `compact`.

## UI, API, MCP, and CLI

Open `/reports` for synchronized CPU, memory, disk, network, process/task, and
agent-concurrency charts. Reports support fleet or local scope, comparison
filters, preset ranges, a custom range of at most 31 days, keyboard cursor
movement, gaps, restart markers, and explicit unavailable dimensions. Header
sparklines link to their corresponding report group.

Supported local endpoints are:

- `GET /api/telemetry/live`, `/health`, `/storage`, `/config`, and `/dimensions`;
- `GET /api/telemetry/series` and `POST /api/telemetry/query`;
- `GET /api/telemetry/export` for a bounded redacted JSON diagnostic slice;
- `PATCH /api/telemetry/config` and `POST /api/telemetry/maintenance`;
- `GET /api/fleet/telemetry/live` and `POST /api/fleet/telemetry/query`.

The equivalent MCP tools are `telemetry_live`, `telemetry_health`,
`telemetry_query`, `telemetry_storage_status`, `telemetry_configure`,
`telemetry_maintenance`, and `telemetry_export`. CLI commands are under
`pa telemetry`: `live`, `status`, `query`, `configure`, `prune`, and `export`.
