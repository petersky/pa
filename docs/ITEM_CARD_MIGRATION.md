# Item-to-card API migration

Cards are PA's canonical work domain model. New code should use `Card`,
`CardCreate`, `CardUpdate`, and the `lane` lifecycle field everywhere:

| Canonical lane | Legacy item status |
| --- | --- |
| `inbox` | `open` |
| `active` | `active` |
| `waiting` | `blocked` |
| `done` | `done` |

The former `archived` item status maps to `done`. It was never distinguishable
in card persistence, so a later legacy read returns `done`. Archival of a
container remains represented by project status and is not a card lifecycle.

## Compatibility policy

- `/api/cards` and the MCP `list_cards`, `create_card`, and `update_card` tools
  are canonical.
- Canonical create and update schemas accept a legacy `status` input during the
  transition. Supplying both `status` and `lane` is allowed only when they map
  to the same lifecycle; conflicts return validation error `422`.
- `/api/items` and the MCP item tools remain compatibility adapters. Their
  response shape and status vocabulary are preserved. HTTP responses include
  `Deprecation: true` and a migration-document `Link` header.
- `ItemKind` is now an import-compatible alias of `CardKind`. `Item`,
  `ItemCreate`, and `ItemUpdate` remain DTO adapters, not a second persistent
  domain model.
- Existing `card_created` and `card_updated` histories containing legacy
  `status` values are translated while projecting. Durable events are not
  rewritten, and replicas converge on canonical lanes.

## Migration examples

Change item creation:

```json
{"kind": "task", "title": "Investigate", "status": "blocked"}
```

to card creation:

```json
{"kind": "task", "title": "Investigate", "lane": "waiting"}
```

Change item updates from `status: "open"` to `lane: "inbox"`, and from
`status: "done"` to `lane: "done"`. Clients may migrate one operation at a time
because item and card IDs refer to the same durable card.
