# Card summaries

PA generates semantic card summaries outside card writes, page rendering, sync, and
startup-critical work. The fleet owner is the sole generation authority; peers receive
the resulting card events through normal realm convergence.

## Provider and authentication policy

The effective provider and model are `PA_CARD_SUMMARY_PROVIDER` (default `openai`)
and `PA_CARD_SUMMARY_MODEL` (default `gpt-5-mini`). The default authentication source
is `dedicated` and requires `PA_CARD_SUMMARY_API_KEY` on the fleet-owner instance.
With no key, summarization is deliberately `disabled`; PA does not create a failed or
pending retry loop.

Set `PA_CARD_SUMMARY_AUTH_SOURCE=codex` to reuse a provider-scoped Codex API key or
access token configured on that same instance:

```bash
pa agent-provider configure --provider codex --api-key "$CODEX_API_KEY"
```

PA does not extract ChatGPT OAuth tokens from Codex's credential store or pass
untrusted card descriptions to an agentic CLI. If Codex has only ChatGPT OAuth,
diagnostics report `oauth_not_supported` and give the dedicated-key setup path rather
than pretending the summary provider is authenticated.

`GET /api/cards/summary/diagnostics` and the card Summary panel expose the effective
provider, model, redaction-safe authentication source, authority, retry policy, setup
guidance, and last classified failure. They never expose credential values.

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
