# Configuration

PA's authoritative configuration registry is exposed by:

```console
pa config schema --json
pa config list --json
```

The registry records canonical keys and aliases, types/defaults, validation and
normalization, operational/security impact, scope, source precedence,
restart/reload behavior, surface access, applicability, and migration metadata.
Settings → Configuration, the terminal editor, CLI, HTTP API, and MCP tools use
the same registry.

## Precedence and unset values

The authoritative order is:

1. runtime/operator override;
2. command-specific CLI override;
3. persisted `config.json`;
4. process environment or `.env`;
5. registry default.

`PA_DATA_DIR` is bootstrap-only because PA must locate `config.json` before it
can load persisted settings. The configuration views show configured value,
effective value, source, and precedence separately. An unset value inherits from
the next available source; it is not the same as an explicitly configured empty
list, empty mapping, `false`, or zero.

## Scriptable changes

Single values can be set or reset directly:

```console
pa config set log_level INFO
printf '%s' "$NEW_SYNC_TOKEN" | pa config set sync_token --stdin
pa config unset sync_token
```

Multi-setting changes use a JSON patch and are validated atomically:

```json
{
  "changes": {
    "host": "127.0.0.1",
    "port": 8080,
    "log_level": "INFO"
  },
  "clear": ["agent_command"]
}
```

```console
pa config validate patch.json --json
pa config diff patch.json
pa config apply patch.json --dry-run
pa config apply patch.json --json
```

Every update uses an optimistic revision and idempotency key in the HTTP/MCP
layer. The write is validated as a complete candidate and atomically replaces
the managed JSON document. Audit events record principal, timestamp, interface,
changed keys, and revisions without recording secret values.

PA never restarts itself merely to refresh configuration. Live settings take
effect immediately; reload and restart requirements are returned explicitly.
When a remote fleet member does not advertise the schema-driven configuration
contract, targeting it fails actionably instead of claiming success.

## Secrets and internal controls

Secret values are never returned by web, CLI JSON, API, MCP, diffs, or audit
events. Replacing and clearing are explicit operations. Provider credentials,
one-time fleet tokens, owner-channel credentials, browser attachment data, and
execution fencing values remain hidden and are owned by their dedicated
workflows. The registry still inventories these environment-only controls with
the reason they are not generally editable.

Unknown persisted keys are retained and reported. Deprecated aliases are
reported with their canonical replacement and migration behavior.

## Coverage artifact

Generate the human-readable surface audit with:

```console
uv run python scripts/generate-configuration-coverage.py > configuration-coverage.md
```

The configuration coverage tests fail when a `Settings` or persisted
`InstanceConfig` field lacks registry metadata, an editable setting lacks any
required surface, a documented environment variable is unknown, or a
deprecated key lacks migration metadata.
