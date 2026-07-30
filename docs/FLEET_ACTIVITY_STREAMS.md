# Fleet activity stream lifecycle

Remote Operations uses a multiplexed activity feed. A selected peer contributes
at most one browser `EventSource`, regardless of its live ACP session count.
The peer adds and removes live runtime subscriptions inside that feed; closed,
quiesced, orphaned, expired, and history-only sessions are never subscribed.
Every activity envelope carries `session_id`, the durable per-session `seq`,
event `type`, `created_at`, and payload. Reconnects resume from the session
cursors supplied when the stream was opened plus `Last-Event-ID`. Consumers
deduplicate by `(session_id, seq)` and reconcile the latest session list after
state-changing events.

The shared page layout emits `pa:section-will-change` before visibility changes
and `pa:section-changed` afterward. Fleet opens Remote Operations only when the
layout controller says `operations` is active. It synchronously releases
view-owned activity before section changes, HTMX navigation, history changes,
page suspension, and teardown.

Tabs coordinate with a `BroadcastChannel` and a five-second renewable
`localStorage` lease per remote instance. Only the lease owner opens the
transport and it broadcasts activity to follower tabs. Ownership is released
on page suspension or teardown; another interested tab normally takes over
within five seconds. Notification mode uses the same owner and transport. Thus
the browser budget is one Fleet activity connection per simultaneously selected
remote instance across cooperating tabs, not one connection per tab or session.
The embedded Remote Operations chat consumes this same feed and does not open a
second per-session `EventSource`.

Peers predating `/api/agent/session-events` return 404 for its capabilities
probe. The controller treats that result as terminal and falls back to one
15-second session-list poll per selected instance. It never falls back to
per-session SSE. Notifications on an older peer are limited to state transitions
visible through that poll until the peer is upgraded.

The Fleet proxy polls downstream disconnect state every 500 ms while an upstream
read is pending. Cancellation, half-close, EOF, peer restart, server shutdown,
and transport exceptions all close the upstream response and client in the
generator's `finally` block. Expected cancellation is recorded without an error
trace. A closed tab or cancelled navigation should therefore release both proxy
legs within one second under normal scheduling.

`GET /api/runtime` exposes `sse_connections`: active streams grouped by endpoint,
direction, downstream tab/client ID, and peer, together with opened, closed,
cancelled, errored, reconnecting, over-age, and paired upstream/downstream
counters. Fleet Operations shows a concise version under **Activity transport
diagnostics**. Active connections older than five minutes are marked over-age
for investigation; they are not automatically terminated solely because of age.
