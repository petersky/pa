# Codex ACP (`codex-acp`)

**Provider id:** `codex`  
**Last verified:** 2026-07-17 (Codex CLI 0.144.5 and official Codex authentication guidance)
**Package:** `@agentclientprotocol/codex-acp`  
**Upstream:** [OpenAI Codex](https://github.com/openai/codex)
**Official auth guidance:** [Codex authentication](https://learn.chatgpt.com/docs/auth)

## Spawn

| Preference | Command | Args |
|------------|---------|------|
| Global install | `codex-acp` | `[]` |
| Fallback | `npx` | `["-y", "@agentclientprotocol/codex-acp"]` |

Optional: `CODEX_PATH` to use a specific Codex binary instead of the bundled dependency.

```bash
pa agent-provider install --provider codex   # npm install -g @agentclientprotocol/codex-acp
pa agent-provider install-codex-cli          # npm install -g @openai/codex
pa agent-provider configure --provider codex --api-key "$CODEX_API_KEY" --no-browser
```

## Auth

`codex-acp` and the official Codex CLI are separate dependencies. The adapter runs ACP;
the CLI owns ChatGPT/device authentication and the target user's Codex credential store.
Installing or probing `codex-acp` never starts a login.

Supported authentication methods:

- ChatGPT OAuth. For headless and remote hosts, explicitly start device auth:

  ```bash
  pa agent-provider login --provider codex --consent [--instance INSTANCE_ID]
  pa agent-provider login-status JOB_ID [--instance INSTANCE_ID]
  pa agent-provider login-cancel JOB_ID [--instance INSTANCE_ID]
  ```

  PA launches `codex login --device-auth` as the same OS user running PA. The
  verification URL and one-time code are safe to show to the controller. Tokens
  remain in that target user's Codex credential store and are never returned or
  copied to the controller. On Unix, PA gives Codex a bounded pseudo-terminal so
  current CLIs flush their interactive instructions immediately; terminal control
  sequences are normalized before the URL and code are parsed. The prompt can be
  completed in any browser—the browser does not need to run on the target.
- API key: `CODEX_API_KEY` or `OPENAI_API_KEY` (PA stores keys in `~/.pa/integrations/codex.json` on the target host only).
- Codex access token, when configured for trusted enterprise automation.
- Custom OpenAI-compatible gateway when the client opts into gateway auth.

`ProviderStatus.auth_method` is one of `none`, `chatgpt_oauth`, `api_key`,
`access_token`, or `unknown`. PA prefers explicit target-process credentials and
otherwise runs the bounded, read-only `codex login status`. Status output and
credential-file contents are never returned. Missing CLI, status timeout, invalid
credentials, and unknown future CLI responses are reported actionably.

Device login jobs last 10 minutes by default (configurable from 1–30 minutes),
can be cancelled, persist only redacted public events, and become `interrupted`
after a PA restart. Refresh/reconnect using the job id; start a new job after an
interruption or timeout. PA also stops login process groups on cancellation,
timeout, and restart recovery. A silent CLI fails after 30 seconds; output that
does not yield both an actionable HTTPS URL and device code fails after 90 seconds.
Captured terminal data, public event counts, event messages, URLs, and codes are
bounded, and raw CLI output is not persisted. Only one active Codex login job is
allowed per instance. The Fleet UI resumes an existing local or proxied job and
refreshes provider authentication after success.

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
pa agent-provider login --provider codex --consent --instance <fleet-instance-id>
pa agent-provider probe --provider codex
```

MCP tools include `agent_provider_install`, `agent_provider_configure`,
`agent_provider_probe`, `agent_provider_login_start`,
`agent_provider_login_status`, and `agent_provider_login_cancel`, with optional
`instance_id`. Login start requires `consent=true`.

## Limitations

- Requires Node.js/`npm` or `npx` on the PA host for install/run.
- Device auth requires the official `@openai/codex` CLI on the target service user's PATH.
- OS keyring availability depends on how the PA service user/session is configured;
  Codex may use `~/.codex/auth.json` when configured for file storage. Treat it as a secret.
- Do not sync API keys via realm sync; configure each host (or use fleet proxy configure which writes only on the peer).
