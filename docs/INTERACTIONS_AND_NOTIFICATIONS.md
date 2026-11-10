# Interactions and fleet notifications

PA represents notices and requests for user input as durable, realm-scoped
notification events. This keeps a permission prompt, provider elicitation, or
operator question visible across browser reconnects and fleet sync instead of
leaving it only in a transcript.

## Domain contract

`Notification` is the extensible envelope. It carries a type, severity,
priority, title/body/summary, source realm and instance, optional
card/session/dispatch/project/PR/watch provenance, timestamps, actions,
deduplication and mutation-idempotency keys, expiry, and routing authority.
Initial types cover interactions, dispatch failures, sync conflicts, PR/CI and
review events, security, upgrades, service health, and general notices.

An interactive notification contains an `InteractionRequest` with:

- a stable `request_id`, interaction kind, prompt, and optional protocol method
  and request ID;
- provider-defined choices, optional freeform input, optional JSON Schema
  structured fields, cancellation policy, sensitivity, and deadline;
- continuation mode (`protocol`, `prompt`, or `none`); and
- durable response, responder, delivery attempts, delivery error, and delivery
  timestamp.

Interaction states are `outstanding`, `answered`, `cancelled`, `expired`,
`superseded`, `delivery_pending`, `delivered`, and `failed`. Submission first
records the answer, then marks delivery pending, and finally records delivery or
failure. Retrying failed delivery must use the same semantic answer because the
original delivery may have partially succeeded; clients use the explicit
delivery-only `retry` response shape so sensitive or structured answers never
need to be re-entered or exposed. Mutation and response
idempotency keys make reconnect, repeated click, proxy retry, and sync replay
safe. A deduplication key identifies one logical prompt and coalesces repeated
creation.

Notification events use PA's canonical event log and sync histories. The
monotonic notification version is the deterministic conflict winner; equal
versions use canonical event ordering. Projection rows are indexed for realm,
priority, outstanding, unread, resolved, principal, and deduplication. Resolved
records remain available for audit and bounded retention maintenance.

## Visibility, authority, and routing

Visibility is either an entire subscribed realm or one principal within that
realm. APIs return not-found for a notification outside the caller's authorized
realms or principal scope. Sensitive response values remain durable for
correlated delivery but are removed from public API, UI, audit, and diagnostic
representations.

Every actionable notice records its owning instance, advertised owner URL,
whether response proxying is distributable, an exact destination, and an
optional capability. The UI and API use these durable fields:

- local owner: perform the action locally;
- remote, distributable owner: authenticate and proxy the correlated response;
- remote, non-distributable owner: return `remote_authority_required` and the
  exact advertised destination;
- missing or unreachable owner: return an explicit owner-unreachable error and
  preserve the outstanding record for recovery.

Read and action APIs re-check current durable state, so an already answered,
expired, or concurrently resolved request returns a conflict instead of
delivering twice.

## Choosing an interaction mechanism

Agents should choose the narrowest protocol-native mechanism:

- Tool or sandbox permission: ACP `session/request_permission`, preserving all
  provider-defined choices.
- Provider question, choice, confirmation, or structured form: ACP
  `elicitation/*`.
- A bounded question during a PA MCP dispatch: structured
  `report_dispatch_progress(operator_input={...})`. A legacy string is accepted
  as a freeform request for compatibility.
- A question decided by post-turn evaluation: the versioned
  `request_operator_input` action.
- External action, approval, choice, or freeform reply needed for progress: the
  applicable structured operator-input or elicitation mechanism above.
- A blocker that does not need a response: report it as progress/blocker state;
  do not create an interaction.

Agents must not rely only on ordinary final prose when work cannot continue
without a user. PA has a deliberately conservative final-output fallback for
explicit requests such as “run `gh auth login`, then tell me”; it is recovery,
not the primary contract. Completion reports, card-disposition JSON, commentary,
and text that merely contains action-like words are not treated as requests.
When a later operator prompt proves that a fallback is obsolete, PA supersedes
that fallback with an audit event; reading or acknowledging it alone never
answers, approves, cancels, or resolves the request.

Protocol interactions deliver directly to the waiting ACP request. Prompt-mode
interactions enqueue a correlated continuation on the same live or recoverable
PA session. Provider reconnects reuse the deduplication key and can redeliver a
previously recorded answer without prompting again.

### Structured operator input

`operator_input` accepts a legacy string or this version-1 object:

```json
{
  "request_id": "deploy-target-1",
  "prompt": "Choose the deployment target",
  "choices": [
    {"id": "staging", "label": "Staging", "value": "staging"},
    {"id": "production", "label": "Production", "value": "production"}
  ],
  "allow_freeform": false,
  "allow_cancel": true,
  "sensitive": false,
  "deadline": "2026-08-03T00:00:00Z"
}
```

`response_schema` may instead contain a JSON Schema object. The web UI renders
object properties as typed controls, the CLI accepts `--fields-json`, and the
service validates the value before recording or delivering it.

## HTTP and MCP APIs

Stable HTTP endpoints are:

- `GET /api/notifications` with realm, type, priority, unread, outstanding,
  resolved, limit, and offset filters;
- `GET /api/notifications/{id}` including routing and audit information;
- `POST /api/notifications/{id}/read`;
- `POST /api/notifications/{id}/acknowledge`;
- `POST /api/notifications/{id}/resolve`; and
- `POST /api/notifications/{id}/respond` with exactly one of `choice_id`,
  `value`, `fields`, `cancel`, or delivery-only `retry`, plus an
  `idempotency_key`.

PA MCP exposes matching `list_notifications`, `get_notification`,
`acknowledge_notification`, `resolve_notification`, and
`respond_notification` tools. Private response values are never written to
progress strings or ordinary logs.

## CLI and web UI

`pa notifications list` supports the same filters, pagination, automatic TTY
color, `--color`/`--no-color`, and `--json`. Use `view`, `acknowledge`,
`resolve`, or `respond`; responses accept `--choice`, `--value`,
`--fields-json`, or `--cancel`. Invalid input and local failures exit nonzero;
a remote-authority requirement exits 2 and prints its destination.

The global bell badge appears only when the authorized outstanding count is
positive. Its responsive, keyboard-accessible panel resolves project, card, and
session names while retaining exact IDs in technical details. It distinguishes
information from requests, states the required action and the effect of a
response, renders the complete body as sanitized Markdown (raw HTML and embedded
media are disabled), and provides only the choices, freeform input, structured
fields, validation, and cancellation declared by the request contract. Read,
dismiss, cancel, respond, delivery-pending, delivered, failed, expired, and
superseded states are labeled separately. Polling and fleet live-update events
refresh state while non-sensitive drafts are retained by notification and field
ID in session storage; sensitive drafts remain memory-only.

## Stall notifications

Most states where an agent is stuck used to leave no trace outside the card.
`pa.notifications.stall.notify_stall(ctx, kind=..., subject_ids=..., title=...,
body=..., severity=..., dedupe_key=...)` is the one call that creates a
deduplicated notification for them. `severity` maps to priority
(`info`->low, `warning`->normal, `error`->high, `critical`->urgent), and the
`capability` field carries `stall:<kind>` so channels know what replies make
sense. It never raises. A repeat call coalesces into the unresolved notification;
after the owner resolves it the call is a no-op unless `reopen_resolved=True`.

| Kind | Meaning | Default replies |
| --- | --- | --- |
| `prompt_failed` | a prompt/turn ended in an error | retry, take over, dismiss |
| `connection_lost` | provider connection dropped mid-turn | retry, take over, dismiss |
| `recovery_blocked` | session cannot be recovered automatically | retry, take over, dismiss |
| `hung_turn` | no progress for too long | resume, take over, dismiss |
| `verification_failed` | independent verification rejected the work | retry, take over, dismiss |
| `acceptance_needed` | card waits for human acceptance | accept, dismiss |
| `pr_closed` | supervised PR closed unmerged | dismiss |
| `dispatch_failed` | a dispatch failed | retry, dismiss |
| `completion_undeliverable` | finished work could not reach the card's authority | dismiss |
| `schedule_failed` | scheduled prompt failed terminally (8 attempts) | retry, dismiss |
| `manual_review` | post-turn evaluation escalated to a human | take over, dismiss |
| `other` | anything else | dismiss |

Wired today: dispatch worker failure, completion outbox giving up
(`transport_exhausted`/`permanent_failure`), terminal scheduled-prompt failure and
the post-turn `escalate_for_manual_review` action. The session queue, drain and
recovery sites and the PR supervisor adopt the same call.

## Telegram push and replies

High-priority notifications (and every HIGH interaction: permission requests,
elicitation, operator input, final-output fallback, goal approvals) can also be
pushed to the owner's Telegram chat through the intake transport and bot token.
It is off by default.

Settings (all under *Channel intake*): `telegram_push_enabled` (false),
`telegram_owner_chat_id`, `telegram_owner_user_id`, `telegram_owner_principal`
(`user:local`), `telegram_push_min_priority` (`high`), `telegram_renotify_hours`
(4, 0 disables), `telegram_renotify_max` (3), `telegram_push_rate_limit_per_hour`
(20). Push also needs `telegram_bot_token`; replies additionally need the signed
webhook (`telegram_webhook_secret` and `telegram_webhook_url`).

Outbound: one concise message per notification (title, card/session, a redacted
excerpt up to 700 characters, numbered choices and the valid replies, a `[#token]`
and a link when `instance_url` is set). Sensitive interactions show no content.
Sending runs on a background thread, so a Telegram outage never affects creating
the in-app notification; failed first pushes are retried for an hour by the
maintenance sweep, and unanswered ones are re-sent every N hours up to M times.

Inbound: a reply is acted on only when it comes from the owner chat AND owner
user. It binds to the notification whose pushed message it replies to, or whose
`#token` starts the text; anything else is ordinary intake. Each Telegram update
id is claimed once, and the action uses idempotency key `telegram:<update_id>`, so
replays are harmless and a second answer gets "already answered". Replies call the
same services as the web UI as `telegram_owner_principal` (or the principal a
`/link` binding gives the owner user):

* any text answers an operator question, elicitation or final-output fallback;
  a number, option name, `approve` or `reject` picks a choice (`approve` never
  selects an "always" grant); `cancel`; `retry` re-sends a failed answer delivery;
* `take over` sets the session to human control, `resume` resumes its queue,
  `retry` retries the session, the dispatch (fleet retry route) or the schedule
  occurrence, `dismiss` resolves a stall notice;
* `accept` is not performed: PA records a human acceptance only for a signed-in
  browser session, so the reply points at the card unless an `accept_handler` is
  installed on the `telegram_replies` service.

## Extension points

Add a `NotificationType` without changing interaction delivery. Producers call
`NotificationService.create` with provenance, authority, deduplication, action,
and expiry metadata. Interactive producers additionally supply an
`InteractionRequest` and either register a live protocol delivery handler or
use prompt continuation. New delivery channels should update the same delivery
fields and must preserve idempotency, realm authorization, public redaction,
bounded payloads, and deterministic versioning.
