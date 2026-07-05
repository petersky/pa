# External Integrations

PA can synchronize cards and projects with external systems (GitHub Issues, Notion, Jira, and others). **Live connectors are not implemented yet** — this document describes the extension contract and planned behavior.

## Concepts

| Term | Meaning |
|------|---------|
| **Connector** | Pluggable bridge to one external system |
| **ExternalRef** | Stable link: system + external record ID |
| **SyncBinding** | Maps a PA card or project to an external record |
| **SyncDirection** | `inbound`, `outbound`, or `bidirectional` |

## Connector contract

Connectors implement `pa.integrations.base.Connector`:

- `configure(config)` — load credentials and options
- `pull(binding)` — fetch external state → PA-shaped dict
- `push(binding, pa_snapshot)` — write PA state → external record

Stub connectors live in `src/pa/integrations/stubs/` and return empty results until implemented.

## Bindings

Bindings are stored in `~/.pa/integrations.json` (not in the sync event log). Each binding links:

- `pa_type`: `card` or `project`
- `pa_id`: local UUID
- `realm_id`: realm scope
- `external_ref`: system + external ID + URL
- `direction`: sync flow
- `field_map`: optional per-field mapping config

Create bindings via `POST /api/integrations/bindings`. Sync via `POST /api/integrations/sync/{binding_id}` (returns 501 until implemented).

## Conflict policy (planned)

| Layer | Policy |
|-------|--------|
| PA event log | Source of truth for PA-native fields |
| External systems | Source of truth for fields owned by that system (e.g. GitHub labels) |
| Bidirectional | Field-level last-writer-wins with `updated_at` comparison; conflicts surfaced in UI |

## Authentication

Credentials live in `~/.pa/integrations/{system}.json` — never in the P2P sync log or card events.

## Per-system notes

### GitHub Issues

- Map: title, body, state (open/closed), labels
- Inbound: import issues as cards in a project
- Outbound: create/update issues from cards

### Notion

- Map: database properties → card lanes and tags
- Requires database ID and property mapping in binding `field_map`

### Jira

- Map: summary, description, status, project key
- Bidirectional status transitions need workflow awareness

## Hooks

Future sync workers subscribe to:

- `integration.binding.created`
- `integration.sync.requested` / `integration.sync.completed`
- `card.updated` / `project.updated` (outbound triggers)

See [ARCHITECTURE.md](ARCHITECTURE.md) for agent-native design and [DEPLOYMENT.md](DEPLOYMENT.md) for instance setup.
