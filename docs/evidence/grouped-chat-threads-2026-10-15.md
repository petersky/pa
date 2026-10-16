# Parallel threads within a chat

Card: `d2e34b5b-b208-4009-b30e-9c44e589d52e` (`chat-thread-ux`).
Source selection: `561d7362-705b-4678-bff7-f2ea9127c87f`, issue
`grouped-parallel-chat-threads`, source conversation
`904584e9-f1ab-48d4-bb9d-0e9ac8db0554`.

## Behavior

- New chat retains the existing fresh conversation path. Its durable session ID
  identifies a new chat group, including existing standalone chats.
- New thread creates an empty, independently routed session in the selected
  chat. Its creation uses the existing authenticated, idempotent branch admission
  path and switches to the acknowledged thread through ordinary session routing.
- Native fork and Copy recent context create siblings in that same group.
  Native fork still uses latest provider history; no message checkpoint support
  or direct Codex app-server integration is introduced.
- Child provenance records `chat_group_id` and `source_session_id` in the existing
  persisted `initiating_workflow`. Creating from any sibling retains the root.
  Old sessions without group provenance remain independent roots.
- The thread strip shows titles, source/context labels, activity, queued count
  and archive state. It refreshes sibling activity every five seconds, supports
  normal links and session switching, and scrolls horizontally on narrow screens.
  It sits above Resources; the existing detail tabs remain directly below Resources.
- Threads keep their existing per-session drafts, history, queues, questions,
  permission requests and workspace admission. No source workspace, card-worker
  ownership, dispatch identity or execution binding is copied to a new session.
  Reading a group filters both owner and realm.
- Session selection clears the prior permission and queue controls before
  asynchronous routing. Permission actions retain their original session and
  subscription generation, so a stale control cannot answer another thread.

## Verification

- Conversation grouping tests exercise empty/copy/native membership, creation
  from siblings, persistence through store reads, duplicate creation, unrelated
  fresh chats, owner/realm isolation and independent queue state.
- Node regression verifies creation receipt reuse after uncertain responses and
  prevents late creation results from navigating a different selected session.
- Chromium regression uses real PA templates, JavaScript, HTTP endpoints and two
  synthetic active PA runtimes. It checks thread navigation, separate drafts and
  histories, selection/reload, narrow scrolling, retained queues, an unanswered
  permission request visible only in its owning thread, and creating a third
  empty sibling while the original work remains active. It also exercises a
  detached stale permission button during the transition. Provider startup is
  substituted in this isolated fixture; this is not a live-provider concurrency
  or deployment claim.
- Broader conversation, composer-draft, session-action, SSE, stream-join, MCP,
  provider-title and timing regressions pass. Isolated Kernel boot and `uv build`
  pass.

No production restart or deployment forms part of this change.
