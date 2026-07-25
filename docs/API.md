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

## Remote dispatch with curl

This redacted example first establishes the CSRF cookie, logs in while preserving
both cookies, then safely admits card-linked work. The target is the
`instance_id` path parameter. If `project_id` is omitted, PA inherits the linked
card's project.

```sh
PA_URL=https://pa.example.invalid
COOKIE_JAR=/tmp/pa-api-cookies.txt

curl --silent --show-error --cookie-jar "$COOKIE_JAR" "$PA_URL/api/health"
CSRF_TOKEN="$(awk '$6 == "pa_csrf" { print $7 }' "$COOKIE_JAR")"

curl --fail --silent --show-error \
  --cookie "$COOKIE_JAR" --cookie-jar "$COOKIE_JAR" \
  --header "X-CSRF-Token: $CSRF_TOKEN" \
  --data-urlencode 'username=REDACTED' \
  --data-urlencode 'password=REDACTED' \
  "$PA_URL/api/auth/login"

curl --fail --silent --show-error \
  --cookie "$COOKIE_JAR" \
  --header "Content-Type: application/json" \
  --header "X-CSRF-Token: $CSRF_TOKEN" \
  --header "Idempotency-Key: dispatch-2026-07-24-001" \
  --data '{"card_id":"9a5e8b7c-2d41-4f84-a32c-9128f97dbe20","message":"Implement the linked task.","provider":"codex"}' \
  "$PA_URL/api/fleet/instances/02dbcd47-8f40-44eb-8403-5eb57545afc8/agent/start"
```

Keep the same `Idempotency-Key` when retrying the same request. PA returns the
original dispatch with `duplicate: true`. Reusing that key for a different
request or target returns `409`.
