# PA HTTP API authentication

PA publishes its live OpenAPI document at `/openapi.json` and interactive
documentation at `/docs`. The document includes the authentication and CSRF
requirements below, plus request and response examples for remote dispatch.

Browser clients authenticate with the HttpOnly `pa_session` cookie returned by
`POST /api/auth/login`. For every cookie-authenticated `POST`, `PUT`, `PATCH`, or
`DELETE`, copy the readable `pa_csrf` cookie into the `X-CSRF-Token` header.
Missing or invalid authentication returns `401`; an absent or mismatched CSRF
token returns `403`. CLI user bearer tokens are CSRF-exempt. Fleet-internal and
sync operations use the instance bearer token documented on those operations.

## Supported HTTP automation client

Use `pa.http_client.PAClient` for cookie-authenticated HTTP automation. It owns
the cookie jar, sends only same-origin requests, rotates expired CSRF tokens,
retries an unsafe request only when it has an idempotency key, and routes peers
through an explicit `instance_id` to URL mapping. The helper never returns or
includes CSRF/session cookies in errors. Bearer clients use the same helper but
do not create or send CSRF state.

```python
from pa.http_client import PAClient

with PAClient(
    "https://pa.example.invalid",
    peer_urls={"monica": "https://monica.example.invalid"},
) as pa:
    pa.login("operator", "REDACTED")
    admission = pa.request(
        "POST",
        "/api/fleet/dispatch",
        instance_id="monica",
        idempotency_key="dispatch-2026-07-24-001",
        json={
            "authority_instance_id": "monica",
            "card_id": "9a5e8b7c-2d41-4f84-a32c-9128f97dbe20",
            "placement_policy": "least_busy",
            "message": "Implement the linked task.",
            "provider": "codex",
        },
    ).json()
```

`POST /api/fleet/dispatch` accepts exactly one of `target_instance_id` or
`placement_policy`. Policies are `best_match`, `least_busy`, `round_robin`, and
`random_eligible`. PA probes fresh readiness and workload, rejects ineligible
candidates, resolves the request to a concrete instance before admission, and
stores the explainable decision on the dispatch. The legacy concrete route
`POST /api/fleet/instances/{instance_id}/agent/start` remains supported.

Keep the same idempotency key only when retrying the same logical mutation.
Policy retries return the original dispatch and resolved target instead of
running placement again. For model/agent callers, prefer the `dispatch_card`
MCP tool, which accepts either `instance_id` or `policy`; the compatibility
`dispatch_card_to_instance`, `get_dispatch`, `retry_dispatch`,
`cancel_dispatch`, and `prompt_dispatch_session` tools remain available. They
authenticate to the owning PA server internally and never expose browser
cookies. Set `authority_instance_id` explicitly when an always-on peer must own
the durable dispatch independently of the caller.

Structured live progress is described in
[`DISPATCH_PROGRESS.md`](DISPATCH_PROGRESS.md). Agents may use
`report_dispatch_progress`; ordinary ACP commentary and sanitized tool lifecycle
updates are also derived automatically after version negotiation.

## Security boundaries

- Browser UI requests retain session-cookie plus signed double-submit CSRF and
  same-origin enforcement. A token failure returns a specific code such as
  `csrf_missing`, `csrf_mismatch`, `csrf_expired`, or `invalid_origin`.
- MCP mutations use an instance-local user bearer between the MCP subprocess and
  its PA server. Peer hops use the fleet instance credential only on narrowly
  allowlisted dispatch/session routes.
- HTTP automation uses `PAClient`; direct cookie/token scraping is unsupported.
- Dispatch records and tool results contain identifiers and acknowledgements,
  never cookies, bearer tokens, session cookies, or CSRF values.
