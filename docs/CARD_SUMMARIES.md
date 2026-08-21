# Card summaries

PA generates semantic card summaries outside card writes, page rendering, sync, and
startup-critical work. The fleet owner is the sole generation authority; peers receive
the resulting card events through normal realm convergence.

## Provider and authentication policy

Choose the provider in Settings → Configure (Card summaries) or with
`PA_CARD_SUMMARY_PROVIDER`: `openai`, `anthropic` (Claude), or `minimax`. The model
field remains editable. When it is empty or still another provider's default, PA uses
the selected provider's default:

| Provider | Default model | Default base URL | HTTP contract |
| --- | --- | --- | --- |
| `openai` | `gpt-5-mini` | `https://api.openai.com/v1` | Bearer `POST {base}/chat/completions` with `json_schema` |
| `anthropic` | `claude-haiku-4-5` | `https://api.anthropic.com` | Native Messages `POST {base}/v1/messages` with `x-api-key` and `anthropic-version: 2023-06-01` |
| `minimax` | `MiniMax-M2.5` | `https://api.minimax.io/v1` | Bearer `POST {base}/chat/completions`; `json_schema` when the host accepts it |

`PA_CARD_SUMMARY_BASE_URL` overrides the selected provider's default. Use it for the
MiniMax China host `https://api.minimaxi.com/v1`. Anthropic is never treated as OpenAI
Chat Completions unless that override is an explicit documented compatibility URL.

Each provider has its own write-only Settings secret. Selecting a provider uses that
provider's key; stored keys for the other providers remain in instance `config.json`
and do not need to be re-entered:

- OpenAI: `card_summary_api_key` / `PA_CARD_SUMMARY_API_KEY`
- Anthropic (Claude): `card_summary_anthropic_api_key` / `PA_CARD_SUMMARY_ANTHROPIC_API_KEY`
- MiniMax: `card_summary_minimax_api_key` / `PA_CARD_SUMMARY_MINIMAX_API_KEY`

These settings apply on restart. After you replace a key or switch providers, restart
PA unless the control is already live-apply. Summarization stays `unconfigured` only
when the *selected* provider has no key.

Set `PA_CARD_SUMMARY_AUTH_SOURCE=codex` to reuse a provider-scoped Codex API key or
access token for **OpenAI only**:

```bash
pa agent-provider configure --provider codex --api-key "$CODEX_API_KEY"
```

Anthropic and MiniMax never fall back to Codex credentials, ChatGPT OAuth, or an
agentic CLI. If Codex has only ChatGPT OAuth, OpenAI diagnostics report
`oauth_not_supported` and give the dedicated-key setup path.

Configuration list, diff, audit, MCP, CLI, and the Settings form never return secret
values. Configured secrets show as `<redacted>` or unset; the web form is password
replace/clear only. There is no `--show-secrets`. Secrets stay in the local instance
`config.json` under `PA_DATA_DIR` and are not synced to peers.

`GET /api/cards/summary/diagnostics` and the card Summary panel expose the effective
provider, model, redaction-safe authentication source, authority, retry policy, setup
guidance, and last classified failure. They never expose credential values.

CARD_DATA isolation is unchanged: card title and body remain untrusted text inside
`CARD_DATA_JSON`. Summaries are still sanitized to 1–3 sentences and at most 600
characters.

## Retry and migration policy

Each provider call consumes one durable attempt. Only timeouts, connection failures,
HTTP 408/425/429, and server errors are retried. Authentication, request/model, and
structured-output failures are terminal until an operator regenerates after correcting
the cause. `PA_CARD_SUMMARY_MAX_RETRIES` is the retry count after the first attempt;
backoff, jitter, and scan cadence use `PA_CARD_SUMMARY_RETRY_BASE_SECONDS`,
`PA_CARD_SUMMARY_RETRY_MAX_SECONDS`, `PA_CARD_SUMMARY_RETRY_JITTER_RATIO`, and
`PA_CARD_SUMMARY_WORKER_INTERVAL_SECONDS`.

The delayed worker scans at most `PA_CARD_SUMMARY_MIGRATION_BATCH` cards per interval.
It incrementally migrates fallback/prefix summaries and resumes due transient attempts.
A completion is accepted only when its content hash and durable attempt timestamp still
match the current card, preventing stale or superseded work from overwriting newer state.
