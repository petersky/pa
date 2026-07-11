# Architecture

PA is built as a **modular kernel** with clear boundaries. Core features are implemented as built-in modules; external packages extend PA through the same contracts and entry points.

## Layers

```
┌─────────────────────────────────────────────────────────┐
│  CLI / Web UI / MCP / ACP                               │
├─────────────────────────────────────────────────────────┤
│  Modules (builtin + entry-point plugins)                │
│  items · instance · theme · debug · …                   │
├─────────────────────────────────────────────────────────┤
│  Kernel — registry, hooks, context, preferences         │
├─────────────────────────────────────────────────────────┤
│  Domain services (store, config, agent session, …)      │
└─────────────────────────────────────────────────────────┘
```

## Module contract

Every module implements `pa.core.contracts.Module`:

| Capability | Method | Purpose |
|------------|--------|---------|
| Lifecycle | `on_load`, `on_startup`, `on_shutdown` | Register services, start/stop resources |
| REST API | `api_routers()` | Mount FastAPI routers under `/api` |
| Web UI | `ui_routers()` | Mount HTMX routes at app root |
| MCP | `register_mcp(mcp, ctx)` | Expose tools to agent sessions |
| CLI | `cli_commands()` | Attach Typer commands to `pa` |
| Assets | `static_mounts()`, `template_dirs()` | Themes, plugin UI |

External plugins register via setuptools entry points:

```toml
[project.entry-points."pa.modules"]
my-plugin = "my_pa_plugin:MyModule"
```

See `examples/plugin_example.py` for a minimal reference implementation.

## Hook bus

Cross-module coordination uses named hooks (`pa.core.hooks.HookBus`):

- `app.startup` / `app.shutdown` — application lifecycle
- `request.start` / `request.end` — HTTP tracing (debug mode)
- Custom hooks — modules emit and subscribe without importing each other

When `PA_DEBUG=true`, hook history is retained and exposed at `/api/debug/hooks`.

## Theming

Themes live in `src/pa/server/static/themes/{theme_id}/`:

- `manifest.json` — metadata and variant list
- `light.css` / `dark.css` — CSS custom properties scoped to `[data-theme][data-appearance]`

User preference (`system` | `light` | `dark`) is stored in `~/.pa/preferences.json` and synced to cookies/localStorage for instant client-side application.

Additional themes are added by dropping a new directory + manifest; no core code changes required.

## Debug & developer mode

Enable with `PA_DEBUG=true`, `pa serve --debug`, or both:

| Feature | Location |
|---------|----------|
| Verbose logging | stderr, `PA_LOG_LEVEL=DEBUG` |
| Hook history | `GET /api/debug/hooks` |
| Module list | `GET /api/debug/modules`, `pa plugins list` |
| Request tracing | Hook events + `X-PA-Debug` header |
| Dev panel | Footer UI when `PA_DEV_TOOLS=true` |

## Adding a plugin (checklist)

1. Create a Python package with a class implementing `Module`
2. Register `[project.entry-points."pa.modules"]` in `pyproject.toml`
3. `pip install` / `uv add` the package
4. Restart PA — the kernel discovers and loads the module at boot

## Web UI (SPA)

The web UI uses an HTMX-driven single-page shell:

- **Top nav** — icon + label buttons; `hx-push-url` for deep links
- **Page layout** — optional left sidebar, main panel, right sidebar per page
- **Chrome** — agent status button, theme cycle icon, settings gear

Pages register via `PageRegistry` (`pa/core/ui/pages.py`). See `UiShellModule` and `ItemsModule` for examples.

### Routes

| Path | Page |
|------|------|
| `/` | Home |
| `/work` | Work board (cards by lane) |
| `/knowledge` | Knowledge |
| `/projects` | Projects (card containers + agent context) |
| `/fleet` | Fleet and realm management |
| `/agent` | Agent chat (via status button when online) |
| `/settings` | Settings (via gear icon) |

## Fleet, realms, and sync

PA separates **who runs instances** from **what card state is shared**:

| Term | Meaning |
|------|---------|
| **Fleet** | Instances a user owns and admins |
| **Realm** | Sync namespace for cards (universe of shared state) |
| **Instance** | One PA install |
| **Membership** | Principal or fleet bound to a realm with a role |
| **Relay** | Instance that forwards sync between network partitions |

Card state is stored as an append-only **event log** (git-inspired content-addressed objects) with a SQLite **projection** for fast reads. Instances sync via `POST /api/sync/*` endpoints.

Configure with:

- `PA_FLEET_ID`, `PA_SUBSCRIBED_REALMS`, `PA_ZONE`, `PA_CAPABILITIES`, `PA_RELAY_ENABLED`
- `PA_SYNC_TOKEN` — bearer token for instance-to-instance auth (T1)
- `PA_PEERS` — comma-separated peer URLs

CLI: `pa fleet list`, `pa realm list`, `pa peers`, `pa sync status`, `pa login`

## Projects

A **Project** is a realm-scoped container for cards with its own metadata:

- Description, tags, memberships
- Associated repositories (`ProjectRepo`)
- Default `agent_prompt` and `tool_config` injected when agents work on project cards

Cards link via `project_id`. Use `CardKind.PROJECT` only for legacy work-item taxonomy — prefer the Project entity for grouping.

## Agent-native design

PA is designed **agent-first**: agents can direct PA and be directed by it, including as the primary interface.

### Principles

1. **MCP is the agent API** — capabilities agents need exist as MCP tools (cards, projects, fleet, sync).
2. **ACP is session transport** — the instance agent connects via ACP; PA MCP is injected as a stdio server in the session.
3. **Bidirectional control**
   - *Agent → PA:* create/move cards, assign projects, query fleet, trigger execution.
   - *PA → Agent:* leases, project context prefix on prompts, per-user env, instance routing.
4. **Project context** — prompts with a `card_id` or `project_id` prepend the project's `agent_prompt` and repo list.
5. **UI is optional** — HTMX web UI, CLI, MCP, and ACP chat are peers.

### Interfaces

| Interface | Role |
|-----------|------|
| `pa mcp` | Tool surface for any agent session |
| ACP (provider subprocess) | Interactive chat; PA tools via MCP bridge. Built-ins: Cursor (`agent acp`), Codex (`codex-acp`). See [acp/](acp/README.md). |
| `pa` CLI | Human/script operator |
| Web UI | Human-friendly views of cards, projects, fleet |

### ACP provider selection

PA resolves which ACP server to spawn per invocation:

**surface → user → instance → default (`cursor`)**

Surfaces are string keys (`chat.default`, `chat.card`, `project`, `execution`, …) so new agent entry points can opt into the same cascade. Manage installs with `pa agent-provider` / MCP `agent_provider_*` tools; fleet admins can target peers by `instance_id`. Capability notes live in [docs/acp/](acp/README.md).

## External integrations

Planned sync with GitHub Issues, Notion, Jira, and others. Scaffold only — see [INTEGRATIONS.md](INTEGRATIONS.md).
