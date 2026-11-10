# Handling `@` mentions on the server (ideas)

Status: section 1 is built (below); everything else is still a proposal.

**Built.** When a prompt is accepted the server resolves its `@` tokens (`chat_mentions.resolve_prompt_mentions`,
same access rules as search) and appends a `prompt_mentions` transcript event
(`{prompt_id, mentions: [{token, kind, label, href, ok}]}`). The message text is never changed. The chat
widget turns the resolved tokens into links or chips inside that prompt's bubble and shows unresolved explicit
ones (`@card:nope`) struck through. Bare `@word` is only a mention when it names a file in the working
directory. A prompt retried after a lost acknowledgement is not re-resolved, so its bubble stays plain text.

## Where things stand

The composer completes `@src/app.py`, `@card:<id>`, `@session:<id>` and `@user:<name>` from
`GET /api/agent/sessions/{id}/mentions` (`pa/modules/chat_mentions.py`), but once sent they are
plain text: the server never looks at them, bubbles do not link them, and the agent only sees the
literal token. Images are the one exception; the browser attaches them as normal image uploads.

Because only the browser understands the tokens, a prompt from the CLI, MCP or an API client gets
none of the behaviour. Anything below that matters beyond the web UI belongs on the server.

## 1. Resolve at submission and record structured mentions (built, recorded as a separate event)

In `_session_prompt_owned`, parse the message with one shared rule (the same one as
`composer-triggers.js`: `@` at a word start, so `a@b.com` is not a mention), then resolve each
token with the access rules the search endpoint already applies. Store the result on the
`user_message` payload:

```json
{"message": "compare @card:abc with @src/app.py",
 "mentions": [{"kind": "card", "id": "abc", "label": "Fix release pipeline", "ok": true},
              {"kind": "file", "path": "src/app.py", "ok": true}]}
```

The text is unchanged, so older clients and the agent see exactly what was typed. What it buys:

- Bubbles render chips or links: cards open the card dialog, conversations open the chat modal
  (it already intercepts `/agent?session=` links), files open the raw-file viewer.
- An unresolvable token (deleted file, wrong id, someone else's conversation) is flagged `ok: false`
  and the composer can warn before or after sending, instead of the agent guessing.
- Search and resolution share one authorization path, so they cannot drift apart.

## 2. Give the agent the referenced content (opt-in)

A bare `@card:abc` means little to a model. Add a setting, `chat_mention_context`, with values
`off`, `references` (default) and `inline`, and have the server add a context block when it
forwards the prompt (kept out of the visible bubble, like other injected context):

| Kind | `references` | `inline` |
| --- | --- | --- |
| file | absolute path and size; the agent reads it | path plus the first N lines |
| card | title, lane, acceptance criteria | plus the latest summary |
| conversation | title, id and how to read it (item 3) | plus the last few turns |
| user | nothing | nothing |

Rules: per-kind and total character budgets; files must resolve inside the session's own working
directory (no symlink escapes); a denylist for obvious secret files (`.env`, `*.pem`, key files);
and the same access checks as search, applied at send time rather than at completion time.

## 3. Let the agent read other conversations on demand

Inlining a conversation is heavy and goes stale. A better fit is an MCP tool, for example
`read_conversation(session_id, limit)`, that returns recent turns only if the prompting principal
could have mentioned that conversation. `@session:<id>` plus a one-line hint ("use
read_conversation") is then enough. It also leaves room for the later agent-to-agent work through
the existing collaboration service without changing the mention syntax.

## 4. Cards and users do something

- `@card:` could link the card to the conversation (`POST /sessions/{id}/cards/{card_id}` already
  exists), so the card shows where it was discussed and its context follows the chat.
- `@user:` could raise a notification (the notifications module exists). It needs a rule for
  whether the mentioned user may then open the conversation, so start admin-only or notify-only.

## 5. Smaller items

- Support `@"path with spaces"` in both the parser and the search; today such files are skipped.
- Record the resolved absolute path, so a later rename does not change what the message meant.
- Server-side image mentions: if a non-browser client sends `@docs/shot.png`, attach the image
  from the working directory with the same size caps the composer uses.
- A mention backlink index ("which conversations referenced this card or file") falls out of
  item 1 if the resolved mentions are also written to a small table.

## Suggested order

Item 1 together with chip rendering first (small, no behaviour change for the agent), then item 2
in `references` mode, then item 3. Items 4 and 5 are independent and can follow any time.

## Open questions

- Should `inline` ever read files larger than a few KB, or always point at the path?
- Should a mention the sender cannot access fail the send, or send and flag it?
- Is notifying a mentioned user acceptable on an instance without per-user login?
