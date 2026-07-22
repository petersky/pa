# OpenInterpreter ACP (`interpreter acp`)

**Provider id:** `openinterpreter`

**Last verified:** 2026-07-22

**Upstream:** [OpenInterpreter](https://github.com/openinterpreter/openinterpreter)

## Spawn

| Field | Value |
|-------|-------|
| Command | `interpreter` |
| Args | `["acp"]` |
| Install method | Official standalone installer |

PA resolves `interpreter` using the service PATH, including `~/.local/bin`.
`pa agent-provider install --provider openinterpreter` runs the official
platform installer when the binary is absent. Updates use `interpreter update`.

## Configuration and model providers

By default PA gives this provider an isolated home under:

```text
<PA_DATA_DIR>/agent_providers/openinterpreter/home
```

That location is passed as `INTERPRETER_HOME`; its `config.toml` is managed by
PA. The home override is reserved so configuration, sessions, and installer
state cannot escape the provider-owned directory.

Configure a built-in model provider:

```bash
pa agent-provider configure --provider openinterpreter \
  --model-provider openai \
  --model gpt-5.1-codex \
  --api-key "$OPENAI_API_KEY"
```

Configure a custom OpenAI-compatible provider:

```bash
pa agent-provider configure --provider openinterpreter \
  --model-provider acme \
  --model acme-coder \
  --provider-name Acme \
  --provider-base-url https://api.acme.example/v1 \
  --provider-env-key ACME_API_KEY \
  --provider-wire-api chat \
  --api-key "$ACME_API_KEY"
```

The REST and MCP configure operations expose the same `model`,
`model_provider`, `model_provider_name`, `model_provider_base_url`,
`model_provider_env_key`, and `model_provider_wire_api` fields. Arbitrary
provider environment and secret variables remain available through `env` and
`secrets` (CLI: `--env-json` and `--secret-json`).

Non-secret model settings are stored in the managed `config.toml`. API keys are
stored separately in `<PA_DATA_DIR>/integrations/openinterpreter.json` with
mode `0600`, injected only into the spawned process, and never returned by
status APIs.

## Capabilities

The upstream ACP server documents:

- session creation, closing, loading, and listing;
- streaming prompts, reasoning summaries, and tool progress;
- permission requests and cancellation;
- sandbox modes `read-only`, `workspace-write`, and
  `danger-full-access`;
- model and reasoning controls where supported by the configured provider;
- the same MCP, skills, provider, approval, and session state as its terminal
  CLI.

PA injects `pa mcp` through the normal ACP `mcpServers` session field. Use
`pa agent-provider probe --provider openinterpreter` on the target host to
record the exact capability advertisement for the installed release.

## Operations

```bash
pa agent-provider status --provider openinterpreter
pa agent-provider install --provider openinterpreter
pa agent-provider update --provider openinterpreter
pa agent-provider probe --provider openinterpreter
```

All lifecycle and configuration commands accept `--instance <fleet-id>`; the
equivalent MCP tools accept `instance_id`. Installation and secrets stay on
the selected host.

## Limitations

- The official installer and self-update need outbound network access.
- OpenInterpreter can execute generated code. Keep PA workspace leases,
  sandbox, and approval policy enabled for repository work.
- Model availability and authentication depend on the selected upstream model
  provider.
