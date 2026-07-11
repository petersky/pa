# Codex ACP (`codex-acp`)

**Provider id:** `codex`  
**Last verified:** 2026-07-11 (from [agentclientprotocol/codex-acp](https://github.com/agentclientprotocol/codex-acp) README)  
**Package:** `@agentclientprotocol/codex-acp`  
**Upstream:** [OpenAI Codex](https://github.com/openai/codex)

## Spawn

| Preference | Command | Args |
|------------|---------|------|
| Global install | `codex-acp` | `[]` |
| Fallback | `npx` | `["-y", "@agentclientprotocol/codex-acp"]` |

Optional: `CODEX_PATH` to use a specific Codex binary instead of the bundled dependency.

```bash
pa agent-provider install --provider codex   # npm install -g @agentclientprotocol/codex-acp
pa agent-provider configure --provider codex --api-key "$CODEX_API_KEY" --no-browser
```

## Auth

Advertised during ACP initialize. Methods (upstream):

- ChatGPT login (browser). Use `NO_BROWSER=1` on headless / remote PA hosts (PA configure `--no-browser` sets this).
- API key: `CODEX_API_KEY` or `OPENAI_API_KEY` (PA stores keys in `~/.pa/integrations/codex.json` on the target host only).
- Custom OpenAI-compatible gateway when the client opts into gateway auth.

## Capabilities (known)

From upstream feature list (confirm with probe):

- Text prompts, embedded context, images, resource links, additional workspace directories.
- Shell command, file change, permission request, MCP tool call, terminal output, reasoning, plan, web search, image generation/view, token usage, review events.
- Client-provided MCP servers (stdio and HTTP).
- Model, reasoning effort, fast mode, approval, and sandbox configuration.
- Slash commands: `/status`, `/mcp`, `/skills`, `/review`, `/review-branch`, `/review-commit`, `/compact`, `/logout`, plus configured skills.
- Modes: `read-only`, `agent`, `agent-full-access` via `INITIAL_AGENT_MODE`.

## Runtime env (common)

| Variable | Purpose |
|----------|---------|
| `CODEX_API_KEY` / `OPENAI_API_KEY` | API-key auth |
| `CODEX_PATH` | Alternate Codex binary |
| `CODEX_CONFIG` | JSON merged into Codex session config |
| `MODEL_PROVIDER` | Model provider for new sessions |
| `INITIAL_AGENT_MODE` | `read-only` \| `agent` \| `agent-full-access` |
| `NO_BROWSER` | Hide ChatGPT browser auth |
| `APP_SERVER_LOGS` | Adapter log directory |

## MCP with PA

PA injects `pa mcp` on session create/resume the same as for Cursor. Codex documents client-provided MCP stdio/HTTP—prefer verifying with a probe + a short live session after install.

## Resume / quiesce

Treat resume as best-effort: PA uses initialize session capabilities; on failure it opens a new session (queued prompts preserved in PA’s quiesce snapshot).

## PA ops / fleet

```bash
pa agent-provider status --provider codex
pa agent-provider install --provider codex --instance <fleet-instance-id>
pa agent-provider configure --provider codex --api-key sk-... --no-browser
pa agent-provider probe --provider codex
```

MCP tools: `agent_provider_install`, `agent_provider_configure`, `agent_provider_probe` with optional `instance_id`.

## Limitations

- Requires Node.js/`npm` or `npx` on the PA host for install/run.
- Browser ChatGPT auth is unsuitable for most fleet/service hosts—prefer API key + `NO_BROWSER`.
- Do not sync API keys via realm sync; configure each host (or use fleet proxy configure which writes only on the peer).
