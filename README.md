# PA — Personal Agent, or Personal Assistant, or Primary Agent, or Probably Alive...

PA is an **agent-native** orchestration platform: agents and humans are co-equal operators. Agents direct PA (create cards, manage projects, run work across a fleet) and PA directs agents (leases, project context, routing, per-user credentials). The web UI, CLI, MCP, and ACP chat are peer interfaces — not a hierarchy with UI on top.

**v0.0.1** — this begins now.

## Usage vs development

| Goal | Command | URL |
|------|---------|-----|
| **Daily use** (host) | `pa install` then `pa start` | http://127.0.0.1:8080 |
| **Develop** (container) | `./scripts/dev.sh` | http://127.0.0.1:8081 |

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for install, updates, launchd, and Dev Container details.

For multi-host fleets (Tailscale, SSH push-install, join tokens), see [docs/MULTI_MACHINE.md](docs/MULTI_MACHINE.md) or the **Fleet** page in the UI.

For durable, fleet-wide GitHub PR lifecycle monitoring and agent-driven merge,
see [docs/PR_SUPERVISOR.md](docs/PR_SUPERVISOR.md).

## Architecture

PA uses a **modular kernel**: built-in features and external plugins implement the same `Module` contract, communicate via a hook bus, and register through setuptools entry points (`pa.modules`). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full guide.

## What PA provides

- **Backend server** (`pa serve`) — FastAPI REST API + HTMX web UI
- **CLI** (`pa ...`) — terminal interactions
- **MCP server** (`pa mcp`) — primary agent API; exposes cards, projects, fleet, and more
- **ACP client** — agent session transport; PA tools injected via MCP stdio bridge
- **Knowledge capture** — summarizes and stores learnings from agent interactions
- **Distributed foundations** — instance identity, peer registry, cross-instance awareness

## Quick start

### Using PA (host)

**One-line install from GitHub:**

```bash
curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Or from a local clone:

```bash
./scripts/install.sh
# or: pa install --from-source .
pa status
# → http://127.0.0.1:8080
```

### Developing PA (container)

```bash
./scripts/dev.sh
# → http://127.0.0.1:8081
```

### From source (no container)

```bash
uv sync
uv run pa init --name local
uv run pa serve
```

## Commands

| Command | Description |
|---------|-------------|
| `pa install` | Install on host (uv tool + launchd) |
| `pa start` / `pa stop` / `pa restart` | Manage launchd service (macOS) |
| `pa logs` | Merged timestamped access/application logs (`-f` to follow; `--stdout` preserves the legacy access-only view) |
| `pa update` | Check/install updates (`--check`, `--channel`, `--restart`) |
| `pa channel list` | Show release tracks and latest versions |
| `pa release patch\|minor\|major\|beta\|alpha` | Bump version and create git tag (maintainers) |
| `pa init` | Initialize data directory and instance config |
| `pa config` | Schema-driven configuration (`list`/`get`/`set`/`unset`/`validate`/`diff`/`apply`, or `-i` interactive) |
| `pa serve` | Start the FastAPI + HTMX server (foreground) |
| `pa status` | Show instance status |
| `pa mcp` | Run PA's MCP server (stdio, for agent sessions) |
| `pa card dispatch-wait <dispatch-id>...` | Wait for durable dispatches, optionally keeping a Mac awake |
| `pa plugins list` | List loaded modules and entry points |
| `pa version` | Show version |

### Waiting for durable dispatches

`pa card dispatch-wait` observes the public PA API; it does not consume an agent
slot or mutate dispatch state. It follows queued and running work until every
dispatch is terminal, prints state transitions and a final summary, and tolerates
temporary server or network loss until the configured deadline.

```bash
pa card dispatch-wait <dispatch-id> [<dispatch-id> ...]
pa card dispatch-wait <dispatch-id> --timeout 7200 --json
pa card dispatch <card-id> --instance macmini --wait --keep-awake
```

`--keep-awake` uses macOS `caffeinate` only for the lifetime of the wait and
releases it on success, failure, timeout, Ctrl-C, or SIGTERM. It is intentionally
rejected on other platforms; omit it there. Use `--quiet` for no output or
`--json` for one machine-readable result. Exit status is `0` only when every
dispatch succeeds, `1` for failed/cancelled/unavailable work, `124` for a wait
deadline, `130` for Ctrl-C, and `128 + signal` for other handled termination
signals. Queued dispatches remain watched without occupying execution capacity.

## Configuration

Persistent instance settings live in `~/.pa/config.json`. Manage them with:

```bash
pa config show
pa config list --json
pa config schema --json
pa config set host 0.0.0.0
pa config add peers http://macbook:8080
pa config remove peers http://macbook:8080
pa config apply patch.json --dry-run
pa config edit          # interactive TUI
```

Settings → Configuration and every CLI/API/MCP configuration operation use the
same registry, including configured/effective values, source precedence,
validation, redaction, and restart/reload metadata. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

To run a fully isolated development or secondary instance, set `PA_DATA_DIR`.
PA loads and updates `config.json`, the database, identity, peer and agent settings,
and all other instance state from that directory without reading or modifying
`~/.pa`:

```bash
PA_DATA_DIR=.dev/pa-data pa init --name development
PA_DATA_DIR=.dev/pa-data pa serve --port 8081
```

Keep `PA_DATA_DIR` set to the same path for every command targeting that instance.

Environment variables (prefix `PA_`):

| Variable | Default | Description |
|----------|---------|-------------|
| `PA_INSTANCE_NAME` | `local` | Instance display name |
| `PA_DATA_DIR` | `~/.pa` | Data storage directory |
| `PA_HOST` | `127.0.0.1` | Server bind host |
| `PA_PORT` | `8080` | Server bind port |
| `PA_PEERS` | — | Comma-separated peer URLs |
| `PA_AGENT_ENABLED` | `true` | Connect to ACP agent on startup |
| `PA_AGENT_GITHUB_TOKEN_ENABLED` | `false` | Opt in to mapping the instance GitHub credential into ACP children as `GH_TOKEN` |
| `PA_AGENT_PROVIDER` | `cursor` | Default ACP provider (`cursor`, `codex`, or `openinterpreter`) |
| `PA_AGENT_COMMAND` | _(provider default)_ | Optional spawn command override |
| `PA_AGENT_ARGS` | _(provider default)_ | Optional spawn args override (JSON array or comma-separated) |
| `PA_AGENT_RECOVERY_CONCURRENCY` | `2` | Maximum provider runtimes recovered concurrently at startup |
| `PA_DEBUG` | `false` | Debug logging, hook history, dev tools |
| `PA_DEV_TOOLS` | `false` | In-browser developer panel (auto-on with debug) |
| `PA_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, …) |
| `PA_LOG_ROTATION_MAX_BYTES` | `26214400` | Rotate each service stdout/stderr log after this many active bytes |
| `PA_LOG_ROTATION_INTERVAL_SECONDS` | `86400` | Rotate active service logs after this age |
| `PA_LOG_RETENTION_COUNT` | `7` | Maximum retained compressed service-log archives |
| `PA_LOG_RETENTION_MAX_AGE_SECONDS` | `1209600` | Maximum service-log archive age |
| `PA_LOG_RETENTION_MAX_TOTAL_BYTES` | `268435456` | Total service-log archive byte budget |
| `PA_LOG_DISK_PRESSURE_FREE_BYTES` | `536870912` | Stop retaining output below this free-space reserve while continuing to drain pipes |
| `PA_GITHUB_TOKEN` | — | Instance-local GitHub token used by PA service code and, when explicitly enabled, mapped to agent `GH_TOKEN` |
| `PA_GITHUB_WEBHOOK_SECRET` | — | Instance-local secret for HMAC-SHA256 webhook verification |
| `PA_TELEMETRY_ENABLED` | `true` | Collect bounded instance resource telemetry |
| `PA_TELEMETRY_PER_SESSION_ENABLED` | `true` | Attribute metrics to PA-owned agent process trees when supported |
| `PA_TELEMETRY_DATABASE_PATH` | `<PA_DATA_DIR>/telemetry.db` | Independent, local-only telemetry database |
| `PA_RELEASE_TRACK` | `release` | Update track: `release`, `beta`, `alpha`, `dev`, or `pypi` |
| `PA_UPDATE_CHANNEL` | *(alias)* | Legacy alias for `PA_RELEASE_TRACK` |
| `PA_UPDATE_REPO` | `petersky/pa` | GitHub repo for release checks |
| `PA_UV_BIN` | *(auto-detected)* | Absolute `uv` path override for install/update in sparse service or SSH environments |

See [Telemetry](docs/TELEMETRY.md) for collection quality, retention, privacy,
platform limitations, and the API/CLI surface.

## Theming

The web UI supports **system**, **light**, and **dark** appearance. Use the header dropdown or API:

```bash
curl -X PUT http://127.0.0.1:8080/api/ui/theme \
  -H 'Content-Type: application/json' \
  -d '{"appearance":"dark"}'
```

Preferences persist in `~/.pa/preferences.json`. Custom themes add a directory under `static/themes/{id}/` with `manifest.json` and variant CSS — no core changes needed.

## Static assets & cache busting

Static files (CSS, JS, themes) are served with a version query string derived at startup from the app version and static file mtimes (`pa/core/assets.py`):

```
/static/style.css?v=a1b2c3d4e5f6
```

- **HTML** responses: `Cache-Control: no-cache` — always revalidate the shell
- **Versioned static** (`?v=…`): `Cache-Control: public, max-age=31536000, immutable`
- **API** responses: `Cache-Control: no-store`

After changing static files, restart `pa serve` (or use `--reload`) to refresh the asset fingerprint. Check current version via `GET /api/ui/assets`.

## System diagram

```
┌─────────────────────────────────────────────────────────┐
│  PA Instance                                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │
│  │ Web UI   │  │ REST API │  │ Instance Agent (ACP) │ │
│  │ (HTMX)   │  │ (FastAPI)│  │ → agent acp          │ │
│  └────┬─────┘  └────┬─────┘  └──────────┬───────────┘ │
│       │             │                    │             │
│       └─────────────┼────────────────────┘             │
│                     ▼                                  │
│              ┌─────────────┐    ┌─────────────┐        │
│              │ Domain Store│    │ MCP Server  │        │
│              │ goals/tasks │    │ (pa mcp)    │        │
│              │ knowledge   │    └─────────────┘        │
│              └─────────────┘                           │
│                     │                                  │
│              ┌──────┴──────┐                           │
│              │ Peer Registry│ ←→ other PA instances   │
│              └─────────────┘                           │
└─────────────────────────────────────────────────────────┘
```

## Ideas

1. Automatic recursive self-improvement
2. Enable agent autonomy
3. Always-on awareness
4. Communicate intent, not instructions
5. Be a builder

More to come as things iterate and evolve.
