# Cursor ACP (`agent acp`)

**Provider id:** `cursor`  
**Last verified:** 2026-07-25 (from public Cursor docs + PA integration)

**Docs:** https://cursor.com/docs/cli/acp

## Spawn

| Field | Default |
|-------|---------|
| Command | `agent` |
| Args | `["acp"]` |

PA resolves `agent` via PATH / service PATH. Install/update: Cursor CLI itself (`agent update` when available). `pa agent-provider install --provider cursor` verifies PATH presence.

## Auth

- ACP `authenticate` uses Cursor login (`cursor_login` in Cursor’s docs).
- PA checks the supported `agent status` command (or `cursor-agent status` for that
  executable name) as the same OS user and environment that launches ACP. A
  target-scoped `CURSOR_API_KEY` is also recognized without returning its value.
- A connected Cursor ACP runtime is corroborating evidence for the profiles of
  those existing sessions. PA keeps the direct CLI result separately when it
  differs, because another service user or profile can still have different auth.
- PA reports `authenticated`, `signed out`, `unavailable`, `probe failed`, `timed
  out`, or `unknown` instead of treating a missing optional token file or
  unrecognized CLI output as a definitive sign-out.
- PA does not copy Cursor credentials between instances. Users authenticate via
  Cursor CLI/account flows on each target host.

## Capabilities (known)

- Transport: stdio, JSON-RPC 2.0, newline-delimited JSON.
- Session flow: `initialize` → `authenticate` → `session/new` (or `session/load`) → `session/prompt`.
- Streaming via `session/update`; tool approvals via `session/request_permission`.
- Modes/models exposed as ACP session configuration (PA renders toolbar selectors when advertised).
- Cancel in-flight turns via `session/cancel`.

## MCP

- Cursor documents project/user `.cursor/mcp.json` for MCP servers.
- PA injects `pa mcp` as an ACP `mcpServers` stdio entry on `session/new` / resume.
- **Limitation (upstream reports):** dynamic `mcpServers` on `session/new` and/or `session/load` have been unreliable in some Cursor ACP builds—confirm with `pa agent-provider probe` and live wire logs before depending on MCP for a release.

## Resume / quiesce

- PA quiesces sessions and attempts ACP resume when the agent advertises resume support.
- Cursor advertises `loadSession: true` and supports `session/list`. On reconnect PA resolves the external id via `session/list`, loads with that session’s persisted `cwd`, and falls back to `session/new` when the id is absent (Cursor returns `Invalid params` / “Session not found” for unknown ids, including brand-new unprompted sessions). PA never performs that `session/new` fallback once process shutdown has begun, so stop/restart cannot orphan fresh Cursor sessions that would be missing from the next `session/list`.

## Client methods

- Cursor may call vendor client methods such as `cursor/update_todos` (and unstable `elicitation/*`) without the ACP `_` extension prefix.
- PA acknowledges those via the client handler wrapper so they do not log as `Method not found`.

## Slash commands / extras

- Prefer Cursor’s interactive CLI docs for slash commands outside ACP.
- ACP is intended for custom clients (PA, editors); interactive `agent` remains the human terminal UX.

## PA ops

```bash
pa agent-provider status --provider cursor
pa agent-provider install --provider cursor
pa agent-provider update --provider cursor
pa agent-provider probe --provider cursor
```

Set instance default:

```bash
export PA_AGENT_PROVIDER=cursor
# or PUT /api/agent/providers/default {"provider":"cursor"}
```
