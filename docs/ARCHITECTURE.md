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
| `/work` | Work items |
| `/knowledge` | Knowledge |
| `/agent` | Agent chat (via status button when online) |
| `/settings` | Settings (via gear icon) |
