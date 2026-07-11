# ACP servers in PA

PA is an **ACP client**: it spawns an ACP agent subprocess per session and speaks Agent Client Protocol over stdio. Built-in providers today:

| Provider id | Binary / package | Capability notes |
|-------------|------------------|------------------|
| `cursor` | Cursor CLI `agent acp` | [cursor.md](cursor.md) |
| `codex` | `@agentclientprotocol/codex-acp` (`codex-acp`) | [codex.md](codex.md) |

Additional ACP servers (for example Snowflake Cortex Code `cortex acp serve`) can be added later by registering a provider and copying [_TEMPLATE.md](_TEMPLATE.md).

## How PA selects a provider

Precedence (most specific wins):

1. **Invocation override** — `provider` on session create / explicit API override
2. **Surface preference** — `agent_surfaces.<surface>.provider` (user prefs, then global prefs)
3. **Project** — `Project.tool_config.agent_provider` when working in a project
4. **User preference** — `agent_provider` in user preferences
5. **Instance** — `PA_AGENT_PROVIDER` / `config.json` `agent_provider` / global prefs
6. **Default** — `cursor`

Well-known **surfaces** (string keys, extensible):

- `chat.default` — `/agent` default session
- `chat.card` — card-embedded chat (`label=card:*`)
- `project` — project-scoped work
- `execution` — `ExecutionRouter` prompts

Optional spawn overrides: `PA_AGENT_COMMAND` / `PA_AGENT_ARGS` (when set) replace the selected provider’s command/args.

## Host lifecycle

| Interface | Examples |
|-----------|----------|
| CLI | `pa agent-provider list\|status\|install\|update\|configure\|probe` |
| REST | `/api/agent/providers…` |
| MCP | `agent_providers_list`, `agent_provider_install`, … (optional `instance_id` for fleet peers) |
| Fleet UI | `/fleet` shows per-instance provider availability |
| Doctor | `pa doctor` reports provider availability |

Secrets stay on the target host (`~/.pa/integrations/{provider}.json`); they are never sync’d across the fleet.

## Maintaining capability knowledge

Update these docs whenever PA’s provider integration changes or upstream ACP servers ship material capability changes.

### Sources to check

1. **Upstream docs / READMEs**
   - Cursor ACP: https://cursor.com/docs/cli/acp
   - Codex ACP: https://github.com/agentclientprotocol/codex-acp
   - ACP spec: https://agentclientprotocol.com/
2. **Runtime probe** — on a host with the binary installed:

   ```bash
   pa agent-provider probe --provider cursor
   pa agent-provider probe --provider codex
   ```

   Capture `agent_capabilities` / `auth_methods` from the JSON into the provider doc (with a dated “Last verified” line).
3. **PA wire logs** — `~/.pa/sessions/<id>/wire.jsonl` for real session behavior (resume, MCP inject, permissions).
4. **Community / issue trackers** — known gaps (e.g. MCP inject or `session/load` quirks) should be recorded under Limitations, with links.

### PR checklist when updating docs

- [ ] Update the relevant `docs/acp/<provider>.md` (capabilities, auth, env, limitations).
- [ ] Bump “Last verified” date.
- [ ] If adding a provider: implement `src/pa/acp/providers/<id>.py`, register in `registry.py`, add a doc from `_TEMPLATE.md`, and mention it in this README table.
- [ ] Cross-link from [ARCHITECTURE.md](../ARCHITECTURE.md) if selection semantics change.

### Cadence

Re-verify at least when upgrading `agent-client-protocol`, Cursor CLI, or `@agentclientprotocol/codex-acp`, and before shipping PA features that depend on a specific ACP capability (resume, MCP stdio inject, auth methods, modes).
