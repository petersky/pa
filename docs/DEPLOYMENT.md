# Deployment: Host Install vs Containerized Development

PA separates **daily usage** (host install) from **development** (container). Both can run on the same machine without conflict.

## Overview

| | Production (host) | Development (container) |
|---|---|---|
| Install | `pa install` or `scripts/install.sh` | `scripts/dev.sh` or Dev Container |
| Data | `~/.pa` | `.dev/pa-data` |
| Port | `8080` | `8081` |
| Instance | `local` | `dev` |
| Agent (ACP) | enabled (host `agent acp`) | disabled |
| Process | launchd (`com.pa.server`) | Docker Compose |

## Using PA (host install)

### Prerequisites

- [uv](https://docs.astral.sh/uv/)
- macOS (for launchd service management)

### Install

**From GitHub (recommended):**

```bash
curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Install a specific tag:

```bash
PA_GIT_REF=v0.0.1 curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Skip launchd registration:

```bash
PA_SKIP_SERVICE=1 curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

**From a local clone:**

```bash
./scripts/install.sh
# or
pa install --from-source .
```

### Daily commands

```bash
pa status          # instance + service state
pa start           # start launchd service
pa stop            # stop service
pa restart         # restart service
pa logs            # recent logs
pa logs -f         # follow logs
pa update --check  # check for updates
pa update          # install latest
pa update --restart
```

### Service files

- Plist: `~/Library/LaunchAgents/com.pa.server.plist`
- Logs: `~/.pa/logs/server.log`, `~/.pa/logs/server.err.log`
- Install metadata: `~/.pa/install.json`

### Configuration

Set environment variables in the launchd plist (re-run `pa install --service-only` after changing), or use `~/.pa` defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `PA_DATA_DIR` | `~/.pa` | Data directory |
| `PA_PORT` | `8080` | Server port |
| `PA_UPDATE_CHANNEL` | `github` | `github` or `pypi` |
| `PA_UPDATE_REPO` | `petersky/pa` | GitHub repo for releases |

## Developing PA (container)

### Quick start

```bash
./scripts/dev.sh
# Open http://127.0.0.1:8081
```

### Docker Compose

```bash
docker compose -f docker-compose.dev.yml up --build
```

Environment defaults (see [`.env.dev.example`](../.env.dev.example)):

- `PA_DATA_DIR=/data` → mounted at `.dev/pa-data`
- `PA_PORT=8081`
- `PA_AGENT_ENABLED=false`
- `PA_DEBUG=true`

### Cursor Dev Container

1. Open the repo in Cursor
2. **Reopen in Container** (uses [`.devcontainer/devcontainer.json`](../.devcontainer/devcontainer.json))
3. Port `8081` is forwarded automatically

The dev container bind-mounts the repo for live code changes with `--reload`.

### Why agent is disabled in dev

The instance agent spawns `agent acp` on the **host**. Running that inside a container is unreliable. Use the host install (`:8080`) when you need Cursor agent integration; use the dev instance (`:8081`) for UI and API work.

## Updates

`pa update` supports pluggable channels:

- **github** (default): GitHub Releases API; falls back to latest tag with `git+https` install
- **pypi**: `uv tool install pa=={version}`

```bash
pa update --check
pa update --channel github
pa update --restart
```

Until CI publishes wheels to GitHub Releases, updates from git tags use:

```
uv tool install git+https://github.com/petersky/pa.git@v0.0.1
```

## Moving to another machine (e.g. Mac mini)

Same host install flow:

```bash
uv tool install pa   # or ./scripts/install.sh from repo
pa init --name mini
pa start
```

Link instances with `PA_PEERS`:

```bash
PA_PEERS=http://macbook.local:8080 pa serve
```

## Troubleshooting

**Port in use**

- Production: `8080` — check `pa status`, stop conflicting process
- Dev: `8081` — check `docker compose -f docker-compose.dev.yml ps`

**Service won't start (macOS)**

```bash
launchctl print gui/$UID/com.pa.server
pa logs -n 100
```

**Dev container rebuild**

```bash
docker compose -f docker-compose.dev.yml down -v
./scripts/dev.sh
```
