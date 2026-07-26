# PA Browser control

PA exposes Playwright-style semantic input while retaining ownership of Chromium,
profiles, targets, and authorization. Prefer `browser_snapshot`, then a returned
element `ref` (or a known CSS selector), and explicit viewport coordinates only as
a fallback.

Common typed tools are `browser_open`, `browser_snapshot`, `browser_click`,
`browser_hover`, `browser_type`, `browser_press`/`browser_press_key`,
`browser_scroll`, `browser_drag`, `browser_resize`, `browser_back`, and
`browser_screenshot`. `browser_actions` is the bounded escape hatch for holds,
chords, drags, and mixed input. It does not accept raw JavaScript, CDP methods,
process IDs, filesystem paths, endpoints, or target IDs.

## Ownership and isolation

The owning PA server is the sole Browser authority. MCP and `pa browser` use its
authenticated local HTTP API; neither owns an independent control path.

- Each authenticated principal + canonical agent session + fleet instance gets an
  isolated profile, page, focus, input state, cookies, history, and opaque `br_...`
  handle by default.
- A UI-attached browser is adopted only through server-side lookup of that exact
  session. It is `user_owned` and preserved on automation detach.
- Agent-started browsers are `agent_owned`; loopback CDP details remain internal.
- Cross-principal/session/instance handles fail with `ownership_failure`.
- Sharing requires `browser_share` with the full authorized session UUID. Its
  `bs_...` grant is single-use, expires in 30–900 seconds, and must be explicitly
  redeemed with `browser_attach(share_handle=...)`.

There is no cross-client implicit current page. Calls without a handle resolve only
the authenticated caller's default and auto-attach an isolated browser when needed.
Shared callers serialize on the same target lock.

## References, retries, and locking

Snapshot element refs are tied to browser handle, target, document identity, and
DOM revision. Navigation or mutation returns retryable `stale_snapshot_reference`;
take another snapshot. A ref never resolves against a different/later element.

Mutations are non-idempotent. Pass a unique `operation_id` (1–128 letters, numbers,
`.`, `_`, `:`, or `-`) when transport retry is possible. PA caches the terminal
per-target result and returns `deduplicated: true` without replay. Timeout,
cancellation, navigation during a compound sequence, or transport loss can be
ambiguous; PA never blindly replays the same operation ID.

Every operation holds a per-page interaction lock. `browser_actions` holds it for
the entire sequence, while unrelated isolated targets can operate concurrently.

## Input contract

Coordinates are viewport-relative CSS pixels; both `x` and `y` are required and
bounded to ±100,000. Wheel deltas use CSS pixels, are also bounded to ±100,000,
and positive `delta_y` scrolls down.

Buttons accept `left`, `middle`, `right` or integers `0`, `1`, `2` respectively.
`browser_click` accepts `click_count` 1–3. Modifiers accept `Alt`, `Control`,
`Meta`, and `Shift`; `ctrl`, `cmd`, and `command` aliases normalize accordingly.

Keys are one Unicode character or: `Alt`, `ArrowDown`, `ArrowLeft`, `ArrowRight`,
`ArrowUp`, `Backspace`, `Control`, `Delete`, `End`, `Enter`, `Escape`, `F1`–`F12`,
`Home`, `Insert`, `Meta`, `PageDown`, `PageUp`, `Shift`, `Space`, or `Tab`.

`browser_type(selector, text, clear=True)` remains compatible. It also accepts a
snapshot `ref`, `submit`, `delay_ms` (0–1000), and modifiers. Zero-delay text uses
the browser insertion primitive; delayed/modified text emits key events. `submit=true`
submits the focused form with `requestSubmit`, or presses Enter when no form owns it.
`browser_drag` accepts selector/ref endpoints or coordinate pairs, a button, and
1–50 steps.

`browser_actions` accepts 1–100 objects using only:

```json
[
  {"type":"pointer_move","x":100,"y":200},
  {"type":"pointer_down","button":"left"},
  {"type":"key_down","key":"Shift"},
  {"type":"wheel","delta_x":0,"delta_y":120},
  {"type":"pause","duration_ms":50},
  {"type":"key_up","key":"Shift"},
  {"type":"pointer_up","button":"left"}
]
```

`key_press` is also supported. Each pause is 0–2,000 ms and total pause time is at
most 10 seconds. Cancellation/failure attempts to release every held button/key
before unlocking and returns `interrupted_sequence` or the concrete error.

## Interfaces and examples

MCP is portable and discoverable. `browser_capabilities` returns compact schema and
limits. Shell-capable agents can use the same manager and stable JSON:

```console
pa browser capabilities
pa browser attach --session 11111111-1111-4111-8111-111111111111
pa browser open --session 11111111-1111-4111-8111-111111111111 \
  --operation-id nav-1 https://example.com
pa browser snapshot --session 11111111-1111-4111-8111-111111111111
pa browser click --session 11111111-1111-4111-8111-111111111111 \
  --ref 'snap_...:4' --operation-id click-1
pa browser actions --session 11111111-1111-4111-8111-111111111111 \
  --operation-id chord-1 '[{"type":"key_down","key":"Control"},{"type":"key_press","key":"k"},{"type":"key_up","key":"Control"}]'
```

The authenticated API is `GET /api/browser/capabilities` and
`POST /api/browser/{operation}`. Bodies use canonical `agent_session_id`, optional
opaque `browser_handle`, and the same fields as MCP/CLI. Localhost alone grants no
authority.

## Lifecycle, limits, and audit

Handles have a 30-minute sliding idle TTL. Reconnect/resume in one PA process keeps
the default session. After PA restart, old handles are rejected and attach mints a
new one; a resumed UI-owned browser can be adopted by the same session. Owner detach
stops agent-owned Chromium; shared detach removes only that caller. Idle cleanup
stops orphaned agent-owned browsers. PA shutdown does not stop user-owned browsers
through the automation manager.

URLs are limited to `http`, `https`, `about`, and `data`; file, `javascript:`,
privileged schemes, raw evaluation, and unrestricted CDP are rejected. Quotas are
16 contexts per instance, one managed page per context, 100 actions per sequence,
and one concurrent operation per page. Downloads are not exposed. Existing PA
network/origin policy still applies.

PA audits principal, canonical session, instance, opaque handle, target, action
class, operation ID, outcome, and error code. It never logs selector payloads,
typed text, cookies, authorization headers, or credential values.
