# ACP provider capability notes — template

Copy this file to `docs/acp/<provider_id>.md` when adding a new ACP server (e.g. Cortex).

**Provider id:** `<id>`  
**Last verified:** YYYY-MM-DD  
**Upstream:** \<link\>

## Spawn

| Field | Value |
|-------|-------|
| Command | |
| Args | |
| Install method | path / npm / other |

## Auth

- Methods advertised at initialize:
- How PA should configure credentials (env / `~/.pa/integrations/<id>.json`):

## Capabilities

List protocol features that matter to PA:

- [ ] `session/new`
- [ ] `session/load` / resume
- [ ] `session/prompt` + streaming updates
- [ ] `session/request_permission`
- [ ] `session/cancel`
- [ ] Models / modes / config options
- [ ] Client MCP (`mcpServers` on new/load)
- [ ] Slash commands / extras

## MCP with PA

Notes on `pa mcp` injection compatibility.

## Resume / quiesce

How reliably does the agent resume? Document fallbacks.

## Runtime env

| Variable | Purpose |
|----------|---------|
| | |

## Limitations

-

## PA implementation checklist

- [ ] `src/pa/acp/providers/<id>.py` implementing install/status/configure/probe/resolve_spawn
- [ ] Register in `src/pa/acp/providers/registry.py`
- [ ] Add row to [README.md](README.md)
- [ ] Probe on a real host and paste capability JSON summary (redact secrets)
- [ ] Update settings UI provider `<select>` options if exposing in the web UI
