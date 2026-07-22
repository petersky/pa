# Cursor ACP (`agent acp`)

**Provider id:** `cursor`  
**Last verified:** 2026-07-11 (from public Cursor docs + PA integration)  
**Docs:** https://cursor.com/docs/cli/acp

## Spawn

| Field | Default |
|-------|---------|
| Command | `agent` |
| Args | `["acp"]` |

PA resolves `agent` via PATH / service PATH. Install/update: Cursor CLI itself (`agent update` when available). `pa agent-provider install --provider cursor` verifies PATH presence.

## Auth

- ACP `authenticate` with Cursor login (`cursor_login` in Cursor’s docs).
- PA does not manage Cursor credentials; users authenticate via Cursor CLI / account flows on the host.

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
- **Limitation (upstream):** Cursor advertises `loadSession: true` but `session/load` returns `Invalid params` / session-not-found. PA’s Cursor provider sets `session_load_supported=False` and uses `session/new` after restart instead of attempting load.

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
