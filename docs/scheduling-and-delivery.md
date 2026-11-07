# Scheduling and delivery operations

The approved contract is [SCHEDULING_AND_DELIVERY_REQUIREMENTS.md](SCHEDULING_AND_DELIVERY_REQUIREMENTS.md).
This implementation adds a server-owned SQLite scheduling ledger, a bounded server
worker, `/schedules` controls and `/api/schedules` endpoints. Stdio MCP proxies the
server through `list_schedules`, `schedule_history`, `preview_schedule` and
`schedule_command`. Registration and CLI boot do not open the ledger.

Payloads accept text or structured JSON and preserve their JSON type in the delivery envelope. UTF-8 JSON payloads are bounded to 16,000 bytes; non-finite numbers are rejected. The UI provides Text/JSON input formats.

Schedule listing uses bounded keyset pages (default 50, maximum 200). API/MCP responses expose `next_cursor`; the MCP accepts that cursor and the UI provides Previous/Next page controls. List summaries select the latest occurrence/status directly without hydrating history envelopes.

Schedules belong to the authenticated local principal. Writes require editor or
administrator access. Mutations use stable command keys; changing a command under
an existing key conflicts. Updates, pause/resume and deletion require the current
version. Creation returns the durable ID, enabled state, destination and next due
Unix timestamp. The UI sends CSRF tokens through the existing middleware.

## Timing and events

Intervals anchor at creation and retain cadence across pause/resume. Default
catch-up creates one occurrence with a missed count. Skip discards a backlog of
multiple due times; bounded replay retains the earliest configured number and
records omitted occurrences in the final replay envelope. Cron catch-up processes
at most 1,000 due times per schedule per cycle and persists its unfinished cursor.
No delivery is created from an unfinished cron catch-up batch.

Cron uses five fields and an IANA timezone through croniter. Upcoming times are
strictly after the preview instant. With the locked parser, a missing local time
at the spring transition advances to the transition boundary (New York 02:30
becomes 03:00); a repeated fall local time fires at both UTC instants (01:30 EDT
and 01:30 EST). Tests pin both behaviors. A once trigger requires an aware time;
a past once time is due on the next worker cycle.

The event reader consumes PA's durable event DAG in bounded batches. It starts at
installation's observed head and retains its frontier across restarts. Stable PA
event IDs deduplicate replay. Event types use PA EventType values; supported
filters are card, session, state and repo. Owners must retain realm read access.
Event-source errors appear in scheduler health and do not stop timed delivery.
Webhook triggers use the authenticated command endpoint with action `webhook` and
a sender-supplied stable idempotency key. Existing PA bearer/session authentication
applies; no unauthenticated webhook route is introduced.

## Delivery and races

Agent targets are exact existing session IDs. Dispatch-linked sessions use the
existing authority follow-up protocol; other sessions use durable normal prompt
input with the occurrence ID as client identity. Busy sessions queue normally.
Only an explicit destination interrupt flag selects interruption. The scheduler
never evaluates responses, completion, reporting quality or task progress.

Inbox targets are the owner's principal ID, such as `user:local`. Messaging targets
are existing intake envelope IDs, preserving their configured channel and audience;
Telegram and Discord use PA's existing transports. Envelopes exceeding provider
limits fail visibly rather than truncate. Discord supports provider nonce dedupe;
Telegram offers at-least-once delivery and may duplicate after a lost reply.
HTTPS webhook targets receive the JSON envelope with `Idempotency-Key`. HTTP 2xx
means acceptance. Redirects, embedded URL credentials, proxies and private-address
connections are excluded; TLS verifies the hostname against the pinned public IP.
A receiver that needs dedupe must persist the occurrence ID before acknowledging.

An occurrence freezes its payload, context and transport envelope before the first
send. Delivery-start time and dedupe/retry policy travel in that envelope; attempts,
actual acknowledgment time, errors and retry deadline remain in history. Frozen
bytes let destinations reject mismatched retries without causing false conflicts.
Update affects future payloads. Correcting a destination updates pending/failed
occurrences while retaining their identity. Pause blocks new claims. Delete cancels
pending/failed occurrences. A delivery already claimed may finish after either
operation; accepted deliveries are never sent again by the ledger. A crash or lost
acknowledgment may cause a retry with the same occurrence ID.

SQLite serializes occurrence creation. An exclusive process lock fences the runner
across recovery, delivery and receipt persistence for the owning ledger. Records
are local to their owning PA server; this does not provide automatic cross-machine
ledger replication or failover. A machine outage catches up after that owner returns.
Up to four deliveries run concurrently on PA's bounded executor infrastructure, so one unavailable channel does not serialize unrelated targets.
Retry backoff doubles to a one-hour cap; eight failures expose exhaustion and manual
retry. Missing targets remain visible. Coalescing replaces only undelivered,
never-attempted occurrences, retaining missed/coalesced totals.

History responses retain the latest 100 entries. Terminal envelope content is
pruned beyond 1,000 entries per schedule; identity tombstones remain for dedupe.
Commands and source-frontier identities remain durable. Pending/error records are
retained for correction. Scheduler health and history remain available when a
recipient channel fails. There is no permanent model process.

## Verification and activation

Compressed-clock tests cover transactional occurrence creation, concurrent ticks,
catch-up policies, long cron outages, retries/exhaustion, lost acknowledgments,
restart recovery, runner fencing, destination correction, event filtering and
replay, authenticated API operations, version races, UI controls and DST. Disposable
adapter tests distinguish mock normal-input routing from real queue acceptance.
The isolated full-server lifecycle test exercises durable PA inbox acceptance.
Existing stdio registration tests freeze all pre-existing tool schemas and explicitly
add only the four scheduling tools.

The separate real-clock fixture under `.dev` waits for two actual 600-second
intervals with no browser. It uses an accepting disposable adapter, not a deployed
monitoring agent. It is not evidence of production agent delivery, cross-machine
failover or managed deployment. Source integration, CI/review, exact-head runtime
acceptance and deployment coordination remain distinct requirements. Do not claim
card completion from source tests or fixture delivery alone.

The opt-in `tests/test_scheduling_real_clock.py` additionally waits for two actual
ten-minute deliveries through PA's real authority/target input paths into a durable
agent queue. Run with `PA_SCHEDULING_REAL_CLOCK=1`; ordinary CI skips it. The
recipient is a disposable quiesced runtime and transport, so this verifies queue
acceptance without starting a model or claiming deployed-recipient acceptance.
