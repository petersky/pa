# Agent chat drafts

PA keeps unsent Agent composer text in browser-local storage. Draft storage is a
client convenience, not a server record: draft text is never sent to Knowledge,
card activity, transcripts, logs, audit records, or other PA instances until the
user submits it.

## Scope and privacy

Each record is keyed by the current PA instance ID, authenticated principal ID,
and durable agent session ID. The browser origin and browser profile provide two
additional isolation boundaries. This prevents a draft from being reused by a
different session, card conversation, project, PA instance, signed-in user, or
browser profile.

Draft text is stored as plaintext in the browser's `localStorage`; PA does not
have a browser-held encryption key that would add protection from another script
already executing with the same origin. Deployments handling sensitive prompts
should use encrypted device storage, browser-profile access controls, and a
trusted PA origin. Private/incognito storage follows the browser's retention
rules and may disappear when the private profile closes.

PA never persists attachment bytes or previews in draft storage. It retains at
most four attachment names, MIME types, and sizes so a restored composer can
explain which files must be reselected. Clearing or submitting a draft removes
that metadata.

## Retention and limits

- Drafts expire after 30 days without an edit. Expired and malformed records are
  garbage-collected when an Agent composer starts.
- Text is limited to 65,536 UTF-16 characters (about 128 KiB before record
  overhead). A larger composer remains usable in the current tab but is not
  stored; PA removes the older stored copy so it cannot restore stale text.
- Browser storage quotas are browser- and profile-specific. If storage is full,
  blocked, or unavailable, PA keeps the current composer in memory, reports the
  limitation beside the composer, and does not block typing or navigation.
- "Clear draft", durable prompt acceptance, and session close write an empty
  revision marker. The marker carries no prompt content and prevents an older
  tab revision from silently reappearing.

## Save, restore, and multi-tab behavior

Text, selection/caret, and attachment metadata are saved after a 300 ms debounce.
PA also flushes at `visibilitychange`, `pagehide`, and before HTMX swaps. IME
composition is not submitted by Enter and is saved after composition completes.
Normal paste, Markdown, multiline text, Shift+Enter, and textarea accessibility
remain native browser behavior.

Each update has a monotonic revision, timestamp, and random per-tab writer ID.
Revision wins first, then timestamp, then writer ID. `storage` events apply a
newer record to another tab. A tab with an unsaved local edit first writes a
newer revision, making the conflict rule deterministic without merging prompt
text.

Prompt submission uses a stable client prompt ID stored with the draft. The
browser sends that same value as both `client_prompt_id` and the HTTP
`Idempotency-Key`; the server serializes admission per session and looks up the
ID in the durable transcript. A retry after a timeout, restart, refresh, or lost
response returns the original acceptance instead of enqueueing the prompt twice.
Transient recovery errors retain the ID. After refresh, the composer shows a
reconnecting/checking state and reconciles the saved ID against the durable
transcript before offering a same-ID retry. PA clears the composer draft only
after durable acceptance; validation, network, storage, or recoverable server
failures retain it.
