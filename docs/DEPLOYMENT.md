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

Install a specific release track:

```bash
PA_CHANNEL=release curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
PA_CHANNEL=beta curl -fsSL .../install-remote.sh | bash
```

Install a specific tag (overrides channel):

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
pa update          # install latest on configured track
pa update --channel beta
pa update --restart
pa channel list    # show tracks and latest versions
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
| `PA_RELEASE_TRACK` | `release` | Release track: `release`, `beta`, `alpha`, `dev`, or `pypi` |
| `PA_UPDATE_CHANNEL` | *(alias)* | Legacy alias for `PA_RELEASE_TRACK` |
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

## Agent development with worktrees

Use this workflow when Cursor agents (or other tooling) develop PA in a container while you keep your **host install** running for daily use.

### Three layers

| Layer | Purpose | Port | Data |
|-------|---------|------|------|
| **Host PA** | Daily driver, ACP agent, launchd | `8080` | `~/.pa` |
| **Dev PA** | Test code changes from a worktree | `8081` (or `8082+`) | `<worktree>/.dev/pa-data` |
| **Agent editor** | Cursor in Dev Container | — | worktree mounted at `/app` |

Host and dev do not share state. Dev uses a separate instance name (`dev`), port, and data directory.

### Setup (one-time)

Keep host PA running and leave it alone while agents work:

```bash
pa status    # expect :8080, data dir ~/.pa
pa start     # if not already running
```

### Per agent task

**1. Create a git worktree** (keeps your main checkout clean):

```bash
# from your primary clone
git worktree add ../pa-wt/my-feature -b feat/my-feature
cd ../pa-wt/my-feature
```

Each worktree gets its own working tree, `.dev/pa-data/`, and Docker Compose project name (derived from the folder).

**2. Open the worktree in Cursor → Reopen in Container**

Or start dev manually:

```bash
./scripts/dev.sh
```

**3. Verify separation**

| URL | What it is |
|-----|------------|
| http://127.0.0.1:8080 | Your real PA (`~/.pa`) |
| http://127.0.0.1:8081 | Code from this worktree |

```bash
pa status    # run on the host — still shows :8080
```

**4. Agent rules**

Inside the Dev Container, agents should:

- edit and test under `/app` (the worktree on disk)
- validate UI/API at `http://127.0.0.1:8081` (or the overridden port)
- run tests with `uv run …`
- **not** write to `~/.pa` or run `pa install` (that affects the host binary and launchd)

**5. Ship and update host**

```bash
git push -u origin feat/my-feature
gh pr create ...
# after merge and release:
pa update --restart
```

When the worktree is done:

```bash
git worktree remove ../pa-wt/my-feature
```

### Multiple agents in parallel

Run one worktree + one dev container per task. If more than one container runs at once, give each a unique **host** port.

Copy the example override in each worktree that needs a non-default port:

```bash
cp docker-compose.override.example.yml docker-compose.override.yml
# edit the host port (e.g. 8082, 8083, …)
./scripts/dev.sh
```

See [docker-compose.override.example.yml](../docker-compose.override.example.yml). Docker Compose loads `docker-compose.override.yml` automatically alongside `docker-compose.dev.yml`.

Example: map host `8082` → container `8081`, then open http://127.0.0.1:8082.

### Do / don't

| Do | Don't |
|----|-------|
| `git worktree add …` per task | `pa install` from a worktree |
| Dev Container or `./scripts/dev.sh` | Set `PA_DATA_DIR=~/.pa` in dev |
| Test on `:8081` (or overridden port) | Enable `PA_AGENT_ENABLED` in the container |
| `pa update` on the host after release | Restart host PA on every dev save |

### Optional: host-side dev (no Docker)

Quick check without a container — still keep host data separate:

```bash
cd ../pa-wt/my-feature
mkdir -p .dev/pa-data
PA_DATA_DIR=.dev/pa-data PA_PORT=8081 uv run pa serve --reload --debug
```

Prefer the Dev Container for agent sessions so the environment matches CI.

## Updates and release tracks

PA supports multiple **release tracks** for install and update:

| Track | Description |
|-------|-------------|
| `release` | Latest stable (non-prerelease) GitHub release or semver tag |
| `beta` | Latest beta prerelease |
| `alpha` | Latest alpha prerelease |
| `dev` | `main` branch (bleeding edge) |
| `pypi` | Latest on PyPI (when published) |

```bash
pa channel list
pa update --check
pa update --channel beta
pa update --restart
```

Curl install uses `PA_CHANNEL` (default: `release`), resolved via `channels.json` on `main`:

```bash
PA_CHANNEL=beta curl -fsSL .../install-remote.sh | bash
```

### Creating releases (maintainers)

From a git checkout with a clean working tree:

```bash
pa release patch          # 0.0.1 → 0.0.2, tag v0.0.2
pa release minor
pa release major
pa release beta           # 0.0.2-beta.1
pa release patch --push   # commit, tag, push to origin
```

Pushing a `v*` tag triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml) to build a wheel and publish a GitHub Release (prerelease for alpha/beta/rc tags).

Until CI publishes wheels, updates from git tags use:

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
- Multiple worktrees: use [docker-compose.override.example.yml](../docker-compose.override.example.yml) with a unique host port per worktree

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
