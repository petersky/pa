# Substantive completion records (v1)

A completion record is a durable, human-readable deliverable on a card. It is
separate from the dispatch transport `completion_envelope`, administrative card
disposition, task description summary, and independently recorded acceptance.
Multiple records may come from one attempt, different agents, or later revisions.

## Submit and read

Ordinary dispatch agents use `submit_completion_record(card_id, envelope,
idempotency_key, realm)`. `list_completion_records` returns compact metadata and
`get_completion_record` retrieves one full record. PA authenticates a restricted
`SessionResult` capability bound to the ordinary dispatch and session; it cannot
be used to update a card, record acceptance, or access another card. The server
derives the principal, instance, session, dispatch, stable record ID and UTC time.
This remains valid for durable result reads after the worker session closes.

Authenticated API clients can use:

- `POST /api/cards/{card_id}/completion-records?realm=default`, with a required
  `Idempotency-Key` header and the envelope as the JSON body.
- `GET` on the collection, with `limit` (1–50, default 20) and `offset` (0–100000).
  Follow `next_offset`; `total` counts all records, including earlier revisions.
- `GET /api/cards/{card_id}/completion-records/{record_id}?realm=default`.
- The human permalink is `/results?card_id={card_id}&realm={realm}&record={record_id}`;
  omit `record` to open the collection.

Browser mutations require existing CSRF protections. Owner API submissions record
their authenticated principal and instance with absent session/dispatch provenance;
clients cannot claim session identity by putting fields in the envelope. Assigned
Goal sessions retain their existing restricted tool contract. No permissions,
model defaults, completion requirements or PR gates change.

Use one stable key per intended record. Same-key identical retries return the
original record, including its ID, time and provenance. Changed content or actor
with the same key is a conflict (409). Concurrent delivery may return the existing
`operation_in_progress` response; follow its lookup guidance through
`get_operation_outcome` and retry the identical operation with the same key.
Never issue a new key solely because the response was lost. The canonical event
contains the receipt and result so replay/reload recovers the same record.

## Envelope

All common fields reject unknown keys; `schema_version` must be 1. Title and summary
cannot be blank. Require 1–32 sections, each with a nonblank heading (max 200
characters) and nonblank Markdown content (max 32000). Title is at most 200
characters, summary at most 2000, references at most 64, and the normalized JSON
envelope at most 65536 UTF-8 bytes. Oversize submissions fail validation without
truncation. Publish large artifacts as attachments instead.

An audit can use:

```json
{
  "schema_version": 1,
  "title": "Assessment findings",
  "summary": "Two usability gaps found; production deployment was not assessed.",
  "sections": [
    {"heading": "Findings", "content": "Describe observations and their impact."},
    {"heading": "Evidence, validation and limits", "content": "Describe actual checks, evidence and uncertainty."}
  ],
  "references": []
}
```

A design or implementation handoff can use:

```json
{
  "schema_version": 1,
  "title": "Search design handoff",
  "summary": "Requirements and interface behavior are ready for review.",
  "sections": [
    {"heading": "Requirements", "content": "Users can search by title and tag."},
    {"heading": "Design", "content": "Explain behavior and tradeoffs."},
    {"heading": "Handoff and next steps", "content": "Proposed implementation steps; these require their own authorization."}
  ]
}
```

Headings are author-selected. Findings, evidence, designs, next steps and limits
are optional combinations of sections, not a universal checklist. No PR, code,
commit or deployment fields are required. Explicit submission is the supported
contract: administrative messages are never parsed into result records.

## Durable references and retained evidence

Publish a file with `create_card_attachment`, then use its returned immutable
identity: `{"kind":"attachment","label":"Assessment report",
"attachment_id":"RETURNED_ID","sha256":"RETURNED_64_HEX_DIGEST"}`.
The ID and hash must match an attachment on this same card, or evidence already
retained by one of its records. Worker paths and temporary URLs are not references.

Other supported references are `{"kind":"completion_record","label":"Earlier
design","record_id":"EXISTING_SAME_CARD_RECORD_ID"}` and
`{"kind":"external","label":"Public specification","url":"https://example.org/spec"}`.
External URLs cannot contain credentials; PA does not retain or verify their
remote content. Unsupported kinds, schemes and target fields fail validation.

PA snapshots attachment metadata in its result event and pins local evidence using
hard links to the immutable blob. Card attachment removal affects the current
attachment list; original result evidence remains downloadable through its stable
card/attachment URL and fleet retrieval. The viewer labels this retained state.
Result pins have no automatic deletion policy, matching immutable result history.
Failed publication can conservatively retain an unused pin; it cannot manufacture
a result or acceptance. This small integration does not redesign general
attachment editing, intake limits or replacement workflows (tracked separately
by card c786ca14-83b2-46c0-8532-008702f19ae8).

## History, UI and acceptance

Every submission is append-only. Optional `supersedes_record_id` must name an
existing same-card record; the new record points back to it. This construction
prevents cycles, self-revisions and cross-card relationships. Multiple sibling
revisions are retained and shown explicitly. A record without that field is an
additional result, not an implicit replacement. Display order is descending
server creation time, then stable ID; it does not imply trust or acceptance.

The card's prominent Results entry always opens the collection. Session and card
execution views link to it independently of administrative transcript endings.
The viewer renders sanitized Markdown, provenance, dates, revision links and
evidence links. Authors should report actual checks and limits candidly.
Separately recorded card acceptance is displayed with its original subject;
it never automatically verifies every report. Saving a result does not move a
lane, acknowledge a dispatch, deploy, approve claims or authorize next steps.

Legacy cards display an honest empty state and preserve session/output links.
There is no transcript migration, invented provenance or fabricated acceptance.
Records use a dedicated canonical event and replayable SQLite projection, outside
compact card discovery payloads. Realm synchronization must use binaries that
understand `completion_record_created`; older versions fail on unknown event
types rather than silently interpreting records as administrative acceptance.
Keep a compatible version available to read/replay a realm containing these events.
