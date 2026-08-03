# PA HTTP API authentication

Durable notification and correlated user-interaction endpoints are documented
in [Interactions and fleet notifications](INTERACTIONS_AND_NOTIFICATIONS.md).

PA publishes its live OpenAPI document at `/openapi.json` and interactive
documentation at `/docs`. The document includes the authentication and CSRF
requirements below, plus request and response examples for remote dispatch.

Browser clients authenticate with the HttpOnly `pa_session` cookie returned by
`POST /api/auth/login`. For every cookie-authenticated `POST`, `PUT`, `PATCH`, or
`DELETE`, copy the readable `pa_csrf` cookie into the `X-CSRF-Token` header.
Missing or invalid authentication returns `401`; an absent or mismatched CSRF
token returns `403`. CLI user bearer tokens are CSRF-exempt. Fleet-internal and
sync operations use the instance bearer token documented on those operations.

## Durable fleet onboarding

`POST /api/fleet/bootstrap/discover` resolves an OpenSSH target and returns its
configuration and untrusted live host-key fingerprint without mutating the
target. `POST /api/fleet/bootstrap-jobs` requires an `idempotency_key` and
creates a versioned 13-phase plan. Set `start=true` only after reviewing the
plan and confirming an unknown key's exact fingerprint.

Read and control jobs through:

- `GET /api/fleet/bootstrap-jobs` and `/incomplete`
- `GET /api/fleet/bootstrap-jobs/{job_id}`
- `POST .../{job_id}/start|resume|retry|cancel`
- `POST .../{job_id}/input` for exact host-key confirmation, a short-lived
  password/passphrase, or explicit provider/GitHub/smoke completion evidence

Secret input is held only in process memory and replaced with a sanitized audit
event. Restarted active jobs become retryable from their durable checkpoint.
New instances receive a disabled quarantine policy before optional setup, a
manual-only policy after capability checks, and automatic placement only after
every requested probe and smoke dispatch passes. MCP exposes the same lifecycle
through the `*_fleet_bootstrap_*` tools; CLI callers use
`pa fleet setup-machine` and `pa fleet bootstrap-job`.

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
Both routes use the capacity contract in
[`FLEET_CAPACITY.md`](FLEET_CAPACITY.md). Rejections include working, queued,
and reserved counts, the effective limit/source, freshness, and consumer links.
An administrator may pass `capacity_override=true` only with a durable
`capacity_override_reason`.

`GET /api/config` returns `dispatch_capacity`,
`dispatch_provider_capacities`, and `effective_dispatch_capacity`. Update the
typed setting with:

```http
PATCH /api/config/capacity
Content-Type: application/json

{
  "dispatch_capacity": 8,
  "dispatch_provider_capacities": {"codex": 3}
}
```

Values must be integers from 1 through 256. The response states that the
change applies immediately to new placement admissions. CLI callers use `pa
config set dispatch_capacity 8`; MCP callers use `get_dispatch_capacity` and
`set_dispatch_capacity`.

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

## Collaboration modes and slash commands

New dispatches accept `collaboration_mode`, `collaboration_risk`,
`collaboration_ambiguous`, and `collaboration_unattended`. These inputs feed the
recorded Plan-first policy decision and do not replace the existing execution
`mode_id`. Session policy, transition, and command endpoints live under
`/api/agent`; MCP and CLI expose the same state and mutations. See
[`COLLABORATION_MODES.md`](COLLABORATION_MODES.md) for endpoint paths, result
states, precedence, recovery, and command-catalog behavior.

## Metadata backups

The authenticated backup surface is instance-local:

- `GET /api/backups/status` — schedule, effective/configured values and sources,
  destination health, storage, metrics, and recent attempts;
- `GET /api/backups?verify=true` — retained archives;
- `GET|PATCH /api/backups/config` — validated policy;
- `POST /api/backups` — immediate backup; send `Idempotency-Key` or
  `{"idempotency_key":"..."}`;
- `GET /api/backups/{backup_id}` and
  `POST /api/backups/{backup_id}/verify` — manifest and integrity;
- `DELETE /api/backups/{backup_id}` — explicit deletion with last-good
  protection;
- `GET /api/backups/{backup_id}/export-info` — verified export metadata,
  checksum, and authorized download URL;
- `GET /api/backups/{backup_id}/download` — authorized archive export;
- `POST /api/backups/restores` and
  `GET /api/backups/restores/{restore_id}` — guarded offline restore request
  and monitoring.

Mutations and downloads require an administrator. Restore initiation verifies
the archive and returns maintenance instructions; it never overwrites the
running writer. The corresponding MCP tools are named `backup_status`,
`backup_list`, `backup_run`, `backup_inspect`, `backup_verify`,
`backup_delete`, `backup_export`, `backup_update_config`,
`backup_restore_initiate`, and `backup_restore_status`. See
[`BACKUPS.md`](BACKUPS.md).

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
