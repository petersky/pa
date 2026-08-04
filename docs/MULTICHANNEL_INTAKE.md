# Multichannel multimodal intake

PA normalizes browser, Telegram, and Discord input into one versioned,
event-sourced `IntakeEnvelope` before downstream goal or agent processing. The
provider payload is an untrusted source record; it never becomes authority or a
provider-specific domain object.

## Canonical contract

An envelope records:

- a stable intake ID, provider message ID, correlation ID, direction, and kind;
- sender identity, link confidence, realm/project/goal routing, and visibility;
- conversation, thread/topic, parent, and reply relationships;
- text, locale, modality, artifact metadata, source hashes, and derived
  representation provenance;
- reply capabilities, delivery receipts, sensitivity, security disposition, and
  retention boundaries; and
- actor, authority instance, idempotency key, version, and immutable audit event.

The SQLite intake tables are projections. `intake_envelope_upserted` and
`channel_identity_upserted` events in the realm object log are authoritative and
rebuild the same projection on a replacement instance. Concurrent receipt and
verified-conversation additions use deterministic set unions. Conflicting
principal relinks stop sync for an explicit value through the normal conflict
resolution API.

## Channel behavior

The existing agent-chat prompt endpoint records browser text and images through
the canonical service before prompt admission and returns the intake ID and
correlation ID with its acknowledgement. `POST /api/intake/web` is the direct
authenticated web adapter.

Telegram uses `POST /api/intake/webhooks/telegram`. PA validates
`X-Telegram-Bot-Api-Secret-Token` before JSON parsing. When a bot token and an
HTTPS webhook URL are configured, startup registers the callback and requests
messages, edits, channel posts, and reactions. Provider files are resolved with
`getFile`, downloaded without redirects, and streamed under the configured cap.

Discord ordinary messages, edits, reactions, direct messages, channels, and
threads arrive through a managed, reconnecting Gateway consumer. The consumer
requests only the required intents, bounds frames, maintains thread-parent
mapping, sends heartbeats, and never logs the bot token. The signed
`POST /api/intake/webhooks/discord` endpoint validates Ed25519 Event Webhooks and
answers provider PINGs; it is not used as an unsigned message ingress. Discord
artifacts are accepted only from the approved Discord CDN origins.

Both adapters cover text, captions, images, voice/audio, video, files, commands,
replies, edits, threads/topics, and reactions. Bot-authored inbound messages are
rejected to prevent feedback loops.

## Identity and audience control

External senders must be explicitly allowlisted by provider user/conversation ID
or linked to a PA principal. An authenticated PA user creates a short-lived
one-time code with `POST /api/intake/links`, then sends `/link CODE` from the
target Telegram or Discord identity. Codes are hashed at rest, scoped to one
channel and realm, expire, and are consumed once. The resulting binding is a
durable realm event.

Responses use `POST /api/intake/{id}/responses`. PA replies to the initiating
audience by default, retains the source correlation ID, and records pending plus
sent/delivered/failed receipts. A cross-channel target is allowed only for a
private source with the same linked principal and an explicitly verified target
conversation. Provider mentions are disabled for Discord replies.

## Security and retention

Admission applies these controls before downstream use:

- signed webhook authentication, subscribed-realm checks, user/conversation
  allowlists, explicit linking, per-identity and per-conversation rate limits;
- event and artifact size limits, redirect-free bounded streaming, provider URL
  validation, content hashes, type/signature checks, executable and malware-test
  quarantine;
- control-character/secret-shaped text sanitation, prompt-injection appraisal,
  and an explicit `untrusted_content` marker; and
- content-addressed source storage with storage-instance provenance.

Quarantined content remains auditable but is never silently treated as clean.
Derived OCR, transcripts, descriptions, or indexes are appended with processor,
version, source artifact, time, and confidence instead of replacing originals.

Raw payloads and source artifacts default to seven days; canonical envelopes
default to ninety days. `POST /api/intake/retention/run` applies bounded
retention work. Raw expiry clears provider references and removes a local blob
only when no card or intake still references it. Canonical expiry redacts text,
artifacts, and identity details while preserving the immutable audit history.
Legal-hold envelopes do not expire. Operators can invoke scoped redaction with
`POST /api/intake/{id}/redact`.

## Configuration

All fields are available through PA's shared configuration surfaces. Provider
tokens and webhook secrets are explicitly secret and are redacted from reads,
diffs, and audits.

| Setting | Default | Purpose |
|---|---:|---|
| `PA_INTAKE_MAX_EVENT_BYTES` | 2097152 | Maximum signed provider event |
| `PA_INTAKE_MAX_ARTIFACT_BYTES` | 26214400 | Maximum source artifact |
| `PA_INTAKE_RAW_RETENTION_HOURS` | 168 | Raw/source retention |
| `PA_INTAKE_CANONICAL_RETENTION_HOURS` | 2160 | Canonical retention |
| `PA_INTAKE_IDENTITY_RATE_LIMIT` | 30 | Events per identity per minute |
| `PA_INTAKE_CONVERSATION_RATE_LIMIT` | 120 | Events per conversation per minute |
| `PA_INTAKE_CHANNEL_ROUTES` | `{}` | Conversation-to-realm/project/goal mapping |
| `PA_TELEGRAM_BOT_TOKEN` | empty | Telegram Bot API credential |
| `PA_TELEGRAM_WEBHOOK_SECRET` | empty | Signed callback secret |
| `PA_TELEGRAM_WEBHOOK_URL` | empty | Public HTTPS callback URL |
| `PA_TELEGRAM_ALLOWED_USER_IDS` | `[]` | Telegram sender allowlist |
| `PA_TELEGRAM_ALLOWED_CONVERSATION_IDS` | `[]` | Telegram chat allowlist |
| `PA_DISCORD_BOT_TOKEN` | empty | Discord bot/Gateway credential |
| `PA_DISCORD_APPLICATION_PUBLIC_KEY` | empty | Event Webhook verification key |
| `PA_DISCORD_ALLOWED_USER_IDS` | `[]` | Discord sender allowlist |
| `PA_DISCORD_ALLOWED_CONVERSATION_IDS` | `[]` | Discord channel allowlist |

Route keys use `channel:conversation_id`, for example
`telegram:-100123`. A value may contain `realm_id`, `project_id`, and `goal_ids`.
The target realm must be subscribed by the receiving PA instance.

`GET /api/intake/capabilities` reports enabled transports and limits without
returning credentials. `GET /api/intake` and `GET /api/intake/{id}` expose the
bounded canonical read model to authenticated clients. MCP exposes capability,
list, get, and link-code tools over the local PA API rather than opening PA's
data directory as a second writer.

## Operational checks

Before enabling a provider, configure a narrow allowlist or create identity
links, set channel routes, and confirm `GET /api/intake/capabilities`. Telegram
requires a publicly reachable HTTPS callback. Discord requires the Message
Content privileged intent for ordinary message text and the configured Gateway
intents in the application portal.

Delivery failure is durable: the outbound envelope remains correlated and gains
a failed receipt. Retrying with the same idempotency key returns the existing
outbound envelope rather than posting twice.
