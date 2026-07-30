# Multi-Machine Deployment (Tailscale)

This guide covers a minimal four-machine PA setup using **Tailscale** for connectivity.

| Machine | Role | Install | Track | Realm | Peers |
|---------|------|---------|-------|-------|-------|
| MacBook | Interactive owner | local or wizard | `release` | `personal` | Mac mini |
| Mac mini | Always-on personal cards | SSH push-install or join | `release` | `personal` | MacBook |
| Linux dev | PA development | `./scripts/dev.sh` / Docker | n/a (source) | `dev` | none |
| Linux staging | Validate in-dev builds | curl installer | `beta` / `dev` | `staging` | none |

All inter-instance URLs use Tailscale hostnames (e.g. `http://macbook:8080`), not `127.0.0.1`.

Each row in this topology is a separate PA instance with a separate
`PA_DATA_DIR`. One server process owns each directory; distribution happens by
exchanging immutable commits between those servers. Never point two services,
containers, or hosts at the same directory, including through a network mount.

See also: [DEPLOYMENT.md](DEPLOYMENT.md) for host vs dev separation.

---

## Prerequisites

- [Tailscale](https://tailscale.com/) on all machines that sync
- [uv](https://docs.astral.sh/uv/) (installer bootstraps if missing)
- Python 3.14+ (uv can install: `uv python install 3.14`)
- Owner has a reachable **`PA_INSTANCE_URL`** (Tailscale hostname) and binds with **`PA_HOST=0.0.0.0`**
- A shared sync token is created automatically on first join/install if missing (or set `PA_SYNC_TOKEN` yourself)

---

## Preferred: Fleet wizard (owner UI)

On the owner instance open **Fleet** in the web UI (`/fleet`):

1. **Readiness** — fix any warnings (instance URL, bind host). Generate a sync token if prompted.
2. **Install via SSH** — enter an OpenSSH target or alias and discover it first.
   Review the resolved hostname, user, port, jump host, and exact host-key
   fingerprint before creating the plan. Select the worker profile, providers,
   GitHub/repositories, capacity, and optional smoke card.
3. **Create plan & start** — PA records a durable 13-phase job, quarantines the
   new member from placement, and pauses explicitly for short-lived SSH,
   provider, or GitHub input. The job resumes from checkpoints after a restart.
4. **Add existing** — mint a join token and run `pa fleet join` on the remote, or use “Join over SSH” when PA is already installed.
5. Confirm the new instance shows **up** and peer routes appear for your realm.

SSH passwords, key passphrases, and sudo input are memory-only and are not
persisted in config, job files, API responses, or logs. Strict `known_hosts`
verification is the default. An unknown key requires exact fingerprint
confirmation; a changed known key blocks onboarding.

---

## Preferred: CLI push-install from the owner

On the owner (MacBook):

```bash
pa fleet setup-machine peter@mini \
  --name mini \
  --url http://mini:8080 \
  --idempotency-key setup-mini-2026-07 \
  --profile code \
  --providers codex \
  --github-transport ssh
```

This creates a reviewable plan by default. Add `--start` to begin immediately,
or control it later with `pa fleet bootstrap-job JOB_ID --action start|resume|retry|cancel`.
Use an immutable `--release-ref` when the target cannot receive the local
installer. The final readiness report separates ready, partial, blocked, and
awaiting-input states with recovery actions.

The older compatibility command remains available:

```bash
# Ensure owner is reachable to peers
# config.json should have instance_url like http://macbook:8080 and host 0.0.0.0

pa fleet install-remote peter@mini \
  --name mini \
  --url http://mini:8080 \
  --ask-password   # optional one-shot; omit if agent/keys work
```

This creates a join token on the live owner, SSHs to the remote, runs the install script with fleet/sync env, and verifies `/api/health`.

Join an already-installed remote without reinstalling:

```bash
pa fleet install-remote peter@mini \
  --name mini \
  --url http://mini:8080 \
  --join-only
```

Other CLI:

```bash
pa fleet join-token          # works while the server is running
pa fleet list                # re-probes health
pa fleet remove <instance-id>
pa fleet register --url http://mini:8080 --name mini
```

---

## 1. MacBook (fleet owner) — first install

If the owner is not installed yet:

```bash
export PA_SYNC_TOKEN="$(openssl rand -hex 32)"   # optional; auto-created on join otherwise
export PA_INSTANCE_NAME=macbook
export PA_REALM=personal
export PA_INSTANCE_URL=http://macbook:8080
export PA_HOST=0.0.0.0

curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

Verify:

```bash
pa doctor
pa status
```

Join tokens (server-aware):

```bash
pa fleet join-token
```

After a remote joins, the owner automatically adds peer routes, persists the remote URL in `peers`, and ensures a sync token exists — no manual `PA_PEERS` step is required for sync. You can still set peers explicitly if you prefer.

---

## 2. Mac mini (fleet joiner)

### Option A — from the owner (recommended)

Use the Fleet wizard **Install via SSH** or `pa fleet install-remote` above.

### Option B — curl installer on the remote

```bash
export PA_SYNC_TOKEN="<same-as-owner-or-omit-if-joining>"
export PA_INSTANCE_NAME=mini
export PA_REALM=personal
export PA_INSTANCE_URL=http://mini:8080
export PA_FLEET_OWNER_URL=http://macbook:8080
export PA_FLEET_TOKEN="<token-from-owner>"
export PA_PEERS=http://macbook:8080
export PA_HOST=0.0.0.0

curl -fsSL https://raw.githubusercontent.com/petersky/pa/main/scripts/install-remote.sh | bash
```

### Option C — PA already running on the remote

On the owner: `pa fleet join-token`

On the remote:

```bash
PA_FLEET_OWNER_URL=http://macbook:8080 pa fleet join <token> \
  --url http://mini:8080 --name mini
```

Join persists `fleet_id`, owner URL, peers, realms, and sync token, then refreshes the service env automatically.

Verify sync:

```bash
pa doctor
pa peers
pa sync status --realm personal
```

`Head`, `Projection`, and `Consistent: yes` should agree. If the projection was
interrupted or an older utility changed refs, repair it through the live server:

```bash
pa sync reconcile --realm personal
```

Agents should use the PA MCP `sync_status` and `sync_reconcile` tools. Do not
edit `pa.db`/`sync_refs.json`, invoke PA store internals from a side script, or
restart merely to make an in-memory head notice a disk change. A true divergent
history is resolved with `resolve_sync_conflicts`, which preserves both heads in
an auditable merge commit.

Create a card on MacBook → confirm it appears on Mac mini (may take a few seconds).

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
| `PA_SYNC_TOKEN` | Shared secret for inter-instance API (auto-created on join if missing) |
| `PA_PEERS` | Comma-separated peer URLs (also updated automatically on join) |
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
| Fleet join fails | Ensure owner is running; retry `pa fleet join` / wizard. Tokens from CLI work with a live server. |
| SSH install auth failed | Use agent/keys, `--identity`, or `--ask-password` (one-shot, not stored) |
| Peers unreachable | Confirm Tailscale; use `http://hostname:port` not localhost; `pa config set host 0.0.0.0` then `pa restart` |
| Sync not working | Same sync token (join sets this), same realm, check Fleet readiness warnings |
| Head and projection differ | Run MCP `sync_reconcile` or `POST /api/sync/reconcile`; inspect missing-object errors instead of restarting |
| “data directory already has a running writer” | Stop the duplicate service or give it a distinct `PA_DATA_DIR`; never share one directory between instances |
| Sync returns 409 conflict | Supply explicit per-field resolutions through MCP `resolve_sync_conflicts` / the resolution API; do not force a ref |
| Linux service won't start | `systemctl --user status pa-server`; check `loginctl enable-linger` |
| Wrong update track | `pa update --channel beta`; check `config.json` `release_track` |

---

## CI/CD (maintainers)

- Push `v*` tag → GitHub Release with wheel + `channels.json` update
- `pa release patch|minor|major` for local releases (pushes by default; use `--no-push` to skip)
- CI smoke tests on PR/push to `main`
