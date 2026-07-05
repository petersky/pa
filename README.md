# PA — Personal Agent, or Personal Assistant, or Primary Agent, or Probably Alive...

PA is an orchestration interface for humans and agents to operate together — capturing goals, tasks, projects, and concerns, engaging with agents through multiple channels, and evolving with each interaction.

**v0.0.1** — this begins now.

## Usage vs development

| Goal | Command | URL |
|------|---------|-----|
| **Daily use** (host) | `pa install` then `pa start` | http://127.0.0.1:8080 |
| **Develop** (container) | `./scripts/dev.sh` | http://127.0.0.1:8081 |

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for install, updates, launchd, and Dev Container details.

## Architecture

PA uses a **modular kernel**: built-in features and external plugins implement the same `Module` contract, communicate via a hook bus, and register through setuptools entry points (`pa.modules`). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full guide.

## What PA provides

- **Backend server** (`pa serve`) — FastAPI REST API + HTMX web UI
- **CLI** (`pa ...`) — terminal interactions
- **ACP client** — communicates with agent servers via [Agent Client Protocol](https://agentclientprotocol.com/protocol/overview) (starting with Cursor's `agent acp`)
- **MCP server** (`pa mcp`) — exposes PA tools to agent sessions
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
| `pa logs` | Tail server logs (`-f` to follow) |
| `pa update` | Check/install updates (`--check`, `--channel`, `--restart`) |
| `pa channel list` | Show release tracks and latest versions |
| `pa release patch\|minor\|major\|beta\|alpha` | Bump version and create git tag (maintainers) |
| `pa init` | Initialize data directory and instance config |
| `pa serve` | Start the FastAPI + HTMX server (foreground) |
| `pa status` | Show instance status |
| `pa mcp` | Run PA's MCP server (stdio, for agent sessions) |
| `pa plugins list` | List loaded modules and entry points |
| `pa version` | Show version |

## Configuration

Environment variables (prefix `PA_`):

| Variable | Default | Description |
|----------|---------|-------------|
| `PA_INSTANCE_NAME` | `local` | Instance display name |
| `PA_DATA_DIR` | `~/.pa` | Data storage directory |
| `PA_HOST` | `127.0.0.1` | Server bind host |
| `PA_PORT` | `8080` | Server bind port |
| `PA_PEERS` | — | Comma-separated peer URLs |
| `PA_AGENT_ENABLED` | `true` | Connect to ACP agent on startup |
| `PA_AGENT_COMMAND` | `agent` | ACP agent command |
| `PA_AGENT_ARGS` | `acp` | ACP agent arguments (JSON array or space-separated) |
| `PA_DEBUG` | `false` | Debug logging, hook history, dev tools |
| `PA_DEV_TOOLS` | `false` | In-browser developer panel (auto-on with debug) |
| `PA_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, …) |
| `PA_RELEASE_TRACK` | `release` | Update track: `release`, `beta`, `alpha`, `dev`, or `pypi` |
| `PA_UPDATE_CHANNEL` | *(alias)* | Legacy alias for `PA_RELEASE_TRACK` |
| `PA_UPDATE_REPO` | `petersky/pa` | GitHub repo for release checks |

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
