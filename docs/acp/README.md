# ACP servers in PA

PA is an **ACP client**: it spawns an ACP agent subprocess per session and speaks Agent Client Protocol over stdio. Built-in providers today:

| Provider id | Binary / package | Capability notes |
|-------------|------------------|------------------|
| `cursor` | Cursor CLI `agent acp` | [cursor.md](cursor.md) |
| `codex` | `@agentclientprotocol/codex-acp` (`codex-acp`) | [codex.md](codex.md) |
| `openinterpreter` | OpenInterpreter `interpreter acp` | [openinterpreter.md](openinterpreter.md) |

Additional ACP servers can be added later by registering a provider and copying [_TEMPLATE.md](_TEMPLATE.md).

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

## Session configuration compatibility and admission

PA treats model, mode, reasoning/thought level, and provider configuration as a
single startup admission step. A session cannot accept its first prompt until all
requested values have been applied and confirmed by the ACP agent.

- A trailing selector such as `model-id[high]` is split generically into model
  `model-id` and reasoning `high`; PA does not maintain provider-specific model-id
  tables. An explicit, conflicting reasoning value is rejected.
- When the session advertises ACP model or mode state and the installed client has
  the corresponding dedicated setter, PA uses that setter. If the runtime method
  is absent, PA falls back to an agent-advertised semantic `model` or `mode`
  configuration option.
- Reasoning is always a separate advertised configuration option. PA recognizes
  semantic category/id/name variants for reasoning, thought, thinking, level, and
  effort, while preserving the provider's actual option id.
- `session/set_config_option` must return the full option state with the requested
  value as `currentValue`. PA records that confirmed value, not merely the request.
- Unsupported, ambiguous, rejected, or unconfirmed settings fail admission with an
  actionable compatibility error. PA terminates the provider, fences repository
  leases, retains the failed attempt in session diagnostics, and does not deliver
  the prompt.
- Retries reuse the stable PA session/worktree identity and reapply the persisted
  requested configuration. Restart recovery does the same before queued prompts
  resume. Requested and effective values are visible in session snapshots, lists,
  history, the Agent UI, and remote dispatch diagnostics.

Providers can therefore expose legacy dedicated setters, modern config options,
both (dedicated setters win), or neither (explicit configuration is rejected before
work is shown as running).

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
   pa agent-provider probe --provider openinterpreter
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
