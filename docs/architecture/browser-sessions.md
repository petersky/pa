# Browser session architecture

## Pre-change audit

Browser control had two different ownership models:

- The agent-chat/UI runtime used one `BrowserManager` entry per canonical ACP
  session and a session-specific Chromium profile.
- The stdio Browser MCP module constructed one mutable
  `McpBrowserController` per MCP process. Its implicit `attachment`,
  process-random `session_key`, and `PA_BROWSER_CDP_URL` fallback were not tied to
  an authenticated PA principal or checked canonical session. Operations also had
  no per-target lock, document revision, retry deduplication, or authorization
  boundary.

Separate MCP processes often avoided collision by accident, but the model did not
make isolation an invariant. Environment CDP/target values acted as authority, and
the MCP path was independent of HTTP browser control. Compound holds could be
interleaved because each event/operation opened its own CDP connection without an
interaction transaction.

## Authoritative model

`BrowserSessionManager` now runs only in the owning PA server. The manager owns:

| Scope | Ownership |
|---|---|
| Chromium process/profile | Agent-owned per isolated PA agent session, or adopted user-owned ACP attachment |
| Browser context | One isolated profile/process per default agent session |
| Page/target | One managed page in the current capability version |
| Current target | Resolved only through a server-minted opaque browser handle |
| Pointer/keyboard holds | Stored per target and protected by its interaction lock |
| Snapshot references | Bound to handle target + document ID + DOM revision |

The key is authenticated principal + full canonical agent session UUID + PA
instance UUID. Server-side session lookup verifies principal ownership. Callers
cannot supply filesystem paths, PIDs, CDP endpoints, or raw target IDs.

MCP and `pa browser` are authenticated clients of
`POST /api/browser/{operation}`. Therefore all routes share one handle registry,
deduplication cache, target lock, quotas, lifecycle, URL policy, and audit path.
The legacy `/api/agent/sessions/{id}/browser` routes remain the user-facing
agent-chat attachment surface; automation can adopt such an attachment only by an
exact server-side canonical-session lookup.

## Isolation and sharing

Default lookup never scans other sessions or selects the first available target.
Explicit `br_...` handles are authorization capabilities only after their binding
is verified. Unknown, forged, cross-principal, cross-session, cross-instance, and
expired handles are rejected.

Sharing is an explicit two-step grant:

1. The owner mints a single-use `bs_...` token for one other full canonical
   session UUID (same authenticated principal and instance).
2. That exact session redeems it through attach before expiry.

Both scopes then resolve the same target and lock. Shared detach removes only the
guest binding. Owner detach revokes grants and shared bindings.

## Ordering and recovery

Every target has one `asyncio.Lock`; an advanced action sequence holds it from the
first input through the last input or cleanup. Different target locks are
independent. Mutation operation IDs retain terminal success or failure so a
transport retry observes the result instead of replaying input.

Disconnect/cancellation releases known held keys/buttons best-effort and records
an ambiguous interrupted result. Navigation during a compound sequence is
reported as ambiguous rather than replayed. Reconnect within the same server
process reuses the scope default. PA restart deliberately invalidates old opaque
handles; caller attach recovers with a new handle and process, while an ACP-owned
browser may be re-adopted after its runtime resumes.

Expired agent-owned sessions are stopped during cleanup. User-owned attachments
are preserved by automation detach/cleanup and remain governed by the ACP
lifecycle. Shutdown closes only agent-owned automation sessions.
