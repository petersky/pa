# Multi-Machine Deployment (Tailscale)

This guide covers a minimal four-machine PA setup using **Tailscale** for connectivity.

| Machine | Role | Install | Track | Realm | Peers |
|---------|------|---------|-------|-------|-------|
| MacBook | Interactive owner | `install-remote.sh` | `release` | `personal` | Mac mini |
| Mac mini | Always-on personal cards | `install-remote.sh` + fleet join | `release` | `personal` | MacBook |
| Linux dev | PA development | `./scripts/dev.sh` / Docker | n/a (source) | `dev` | none |
| Linux staging | Validate in-dev builds | `install-remote.sh` | `beta` / `dev` | `staging` | none |

All inter-instance URLs use Tailscale hostnames (e.g. `http://macbook:8080`), not `127.0.0.1`.

See also: [DEPLOYMENT.md](DEPLOYMENT.md) for host vs dev separation.

---

## Prerequisites

- [Tailscale](https://tailscale.com/) on all machines that sync
- [uv](https://docs.astral.sh/uv/) (installer bootstraps if missing)
- Python 3.14+ (uv can install: `uv python install 3.14`)
- Choose a shared **sync token** for home fleet machines: `openssl rand -hex 32`

---

## 1. MacBook (fleet owner)

Install PA on the release track with the `personal` realm:

```bash
export PA_SYNC_TOKEN="<your-secret>"
export PA_INSTANCE_NAME=macbook
export PA_REALM=personal
export PA_INSTANCE_URL=http://macbook:8080   # Tailscale hostname
export PA_HOST=0.0.0.0

curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Verify:

```bash
pa doctor
pa status
```

Generate a fleet join token for the Mac mini:

```bash
pa fleet join-token
```

Note the token and your owner URL (`http://macbook:8080`).

After Mac mini is installed, add it as a peer on MacBook. Either set in `~/.pa/config.json`:

```json
"peers": ["http://mini:8080"]
```

Or export `PA_PEERS=http://mini:8080` and re-run `pa install --service-only`.

---

## 2. Mac mini (fleet joiner)

Install and join the MacBook fleet:

```bash
export PA_SYNC_TOKEN="<same-secret-as-macbook>"
export PA_INSTANCE_NAME=mini
export PA_REALM=personal
export PA_INSTANCE_URL=http://mini:8080
export PA_FLEET_OWNER_URL=http://macbook:8080
export PA_FLEET_TOKEN="<token-from-macbook>"
export PA_PEERS=http://macbook:8080
export PA_HOST=0.0.0.0

curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Verify sync:

```bash
pa doctor
pa peers
pa sync status --realm personal
```

Create a card on MacBook → confirm it appears on Mac mini (may take a few seconds).

Manual join if installer join failed:

```bash
PA_FLEET_OWNER_URL=http://macbook:8080 pa fleet join <token> \
  --url http://mini:8080 --name mini
pa install --service-only
```

---

## 3. Linux dev (PA development)

Keep development isolated from your personal fleet. Use the dev container or local worktree data:

```bash
./scripts/dev.sh
# UI at http://127.0.0.1:8081
```

- Data: `.dev/pa-data` (not `~/.pa`)
- Port: `8081`
- Agent disabled in container (use host MacBook for ACP)
- No `PA_PEERS` to home fleet

---

## 4. Linux staging (validate in-dev PA)

Install from the **beta** or **dev** track to test pre-release builds:

```bash
export PA_CHANNEL=beta
export PA_INSTANCE_NAME=staging
export PA_REALM=staging
export PA_INSTANCE_URL=http://staging:8082
export PA_PORT=8082
export PA_HOST=0.0.0.0

curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Verify:

```bash
pa doctor
pa update --check
pa channel list
```

Use realm `staging` only — do not peer to `personal`.

Linux uses a **systemd user unit** (`~/.config/systemd/user/pa-server.service`). Enable lingering if needed:

```bash
loginctl enable-linger "$USER"
```

---

## Release tracks

| Track | Purpose | Install | Update |
|-------|---------|---------|--------|
| `release` | Stable (MacBook, Mac mini) | `PA_CHANNEL=release` | `pa update` |
| `beta` | Pre-release (staging) | `PA_CHANNEL=beta` | `pa update --channel beta` |
| `dev` | Latest main | `PA_CHANNEL=dev` | `pa update --channel dev` |

Track is stored in `~/.pa/config.json` (`release_track`) and embedded in the service unit.

Channel pointers live in [`channels.json`](../channels.json) on `main`; CI updates them on tag push.

---

## Configuration reference

| Variable | Purpose |
|----------|---------|
| `PA_INSTANCE_URL` | Tailscale URL for this instance |
| `PA_FLEET_OWNER_URL` | Owner URL when joining fleet |
| `PA_FLEET_TOKEN` | One-time join token |
| `PA_SYNC_TOKEN` | Shared secret for inter-instance API |
| `PA_PEERS` | Comma-separated peer URLs |
| `PA_REALM` / `PA_SUBSCRIBED_REALMS` | Sync namespace |
| `PA_HOST` | Bind address (`0.0.0.0` for Tailscale) |
| `PA_CHANNEL` / `PA_RELEASE_TRACK` | Release track |

Persistent config: `~/.pa/config.json`  
Install metadata: `~/.pa/install.json`

---

## Troubleshooting

```bash
pa doctor          # health, peers, sync status
pa logs -f         # follow server logs
pa peers           # configured + discovered peers
pa sync status --realm personal
```

| Problem | Fix |
|---------|-----|
| Fleet join fails | Ensure owner is running; retry `pa fleet join` with `PA_FLEET_OWNER_URL` |
| Peers unreachable | Confirm Tailscale; use `http://hostname:port` not localhost |
| Sync not working | Same `PA_SYNC_TOKEN`, same realm, bidirectional `PA_PEERS` |
| Linux service won't start | `systemctl --user status pa-server`; check `loginctl enable-linger` |
| Wrong update track | `pa update --channel beta`; check `config.json` `release_track` |

---

## CI/CD (maintainers)

- Push `v*` tag → GitHub Release with wheel + `channels.json` update
- `pa release patch|minor|major` for local releases (pushes by default; use `--no-push` to skip)
- CI smoke tests on PR/push to `main`
