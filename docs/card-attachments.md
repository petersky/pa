Card details list durable supporting files and resource links above the detail tabs.
Use **Edit details** for names/descriptions and resource URLs, **Edit content** for
UTF-8 source, and **Replace file** for new bytes. Source editing supports text,
Markdown, HTML, JSON, XML, CSS, JavaScript and CSV up to 1 MiB; JSON must validate.
Other formats and larger files have an explicit replacement workflow. Limits are
64 attachments per card, 25 MiB per upload and 100 MiB of current material per card.

HTML previews preserve structural HTML and inline CSS inside an opaque sandbox;
scripts, forms, external resources and navigation are removed or blocked. Text
and JSON previews escape markup. Image, audio and video use browser viewers;
PDF and other formats explain how to download or replace the original. Previewing never
changes saved content. Resource links accept HTTP(S) without embedded credentials;
PA opens their targets without fetching or validating remote availability.

The canonical APIs are shared by browser and MCP clients:

| Action | API | MCP |
| --- | --- | --- |
| Discover | `GET /api/cards/{card}/materials` | `get_card_materials` |
| Upload | `POST /api/cards/{card}/attachments` (multipart `file`) | `create_card_attachment` |
| Add link | `POST /api/cards/{card}/attachment-links` | `create_card_resource_link` |
| Edit details/URL | `PATCH /api/cards/{card}/attachments/{id}` | `update_card_attachment` |
| Read/edit source | `GET/PUT /api/cards/{card}/attachments/{id}/content` | `get_card_attachment_content`, `edit_card_attachment_content` |
| Replace | `PUT /api/cards/{card}/attachments/{id}/replace` (multipart `file`) | `replace_card_attachment` |
| Remove reference | `DELETE /api/cards/{card}/attachments/{id}` | `remove_card_attachment` |

Pass `realm` explicitly for non-default realms. Mutations use `Idempotency-Key`;
browser writes also require CSRF. Edits and replacement require `expected_revision`
and accept card `updated_at` as optional `expected_version`; removal uses the
foundation's optional card version fence. On response loss, retry the identical
request with its original key. Browser forms retain that request while the page
remains open. A reload reads canonical state. Stale writes return 409 and preserve
the saved material. Authenticated realm viewers can read; editors can mutate.
Local no-login access follows PA's existing local access mode.

IDs stay stable across editing and replacement. Every revision records its author,
instance and timestamp in canonical history; prior bytes are retained. Download or
preview a saved revision with `revision=N`; completion evidence URLs select their
immutable `sha256`. Fleet retrieval verifies that same hash. Removing a reference
revokes revision access except for originals retained by completion records. Known
description/evidence references block removal unless the current original is
retained. Material history explains and links saved revisions, subject to those
access rules. Saved material changes remain descriptive and never dispatch work.

Discovery returns metadata and a 240-character description excerpt, without file
bytes or the full card body. Use the full card or content endpoint deliberately
when more detail is needed. Existing file manifests load as file revision 1, and
default editing metadata preserves legacy dispatch manifest digests.
