# Completion records verification — 2026-11-18

Agent-reported implementation evidence for card
`73fe8fa7-793c-4f2d-b133-42e579580d22`. This is not an independent acceptance receipt.

## Verified behavior

- Two substantive shapes (audit and design/handoff) validate without code/PR fields.
- Separate submissions, original/revision/sibling records, and two authenticated
  attempts retain distinct IDs, timestamps and server-derived provenance.
- Identical retries, initial concurrent delivery, and a lost post-commit response
  recover one original result. Fresh projection rebuilds retain content and receipts.
- Unknown versions, blank/oversize input, spoofed provenance, invalid references,
  cross-card relationships and credential misuse are rejected.
- Submitting a result leaves the entire protected card unchanged, including its
  lane, completion requirements, acceptance evidence and execution preferences.
- An attachment remains downloadable with its original hash after removal from
  the current list and after garbage collection; records retain the original metadata.
- Slow evidence retrieval does not hold the canonical mutation lock or block an
  unrelated card write.

The relevant regression run covered `test_completion_records`, browser verification,
attachments/operation receipts, completion actor/requirements/provider transport,
prompts, card sessions, history display/filter, MCP bootstrap/domain schemas/registration,
and the assigned-service allowlist. All passed. The boot smoke test and `uv build`
also passed with isolated settings. Tests were rerun after the final persistence
and lock-boundary changes.

The original first MCP registration (`list_items`) remains first, preserving
duplicate-registration detection behavior as the three new tools are added.

## Actual browser verification

An isolated PA instance built from this worktree served port 8097 with separate
data/workspace directories and its agent disabled. PA MCP browser tools visibly
opened the actual Work card dialog and its prominent Results entry, the three-record
collection, original audit, revision relationship, full Markdown sections,
verification limits and retained evidence link. Screenshots were inspected at
1280×1000 and 390×844. The automated Chromium test also covers the research realm,
stable direct URLs/reload, an administrative `card_disposition` transcript event,
a closed originating worker, keyboard activation, attachment retrieval, revision navigation and the
legacy empty state. The final mobile assertion checks absence of horizontal overflow.

Browser inspection caught and corrected reuse of the existing `card` modal query
parameter. Result permalinks use `card_id`, so the card dialog does not reopen over
the report. Existing compact session summary schemas remain unchanged.

Durable evidence on the card:

- Desktop screenshot: attachment `e13f447ec015474999ca68377ab2fc97`, SHA-256
  `2aad1521bd381dbcb40ebfa1a20078fc3aeee341260b2fff762d945fba8e61a2`.
- Mobile screenshot: attachment `92c213d7798242ef90c913ffef946030`, SHA-256
  `5365bb5ddb0cc8066a667977cd0c892f6cd9e16b7d8eaeeaa2287bc3ad616630`.

## Limits and adjacent scope

Verification used disposable instances, not a production rollout or recreation of
the original Ribbon session. External HTTPS availability is not asserted. Result
evidence pins intentionally have no automatic deletion policy. Old binaries do not
understand the new canonical event; see the compatibility note in
[the contract](../completion-records.md). General attachment maintenance and intake
limit redesign remain on card `c786ca14-83b2-46c0-8532-008702f19ae8`.

An isolated initial test teardown logged an unrelated cross-event-loop sync
shutdown error. It was reported as PA problem
`5c470feb-2271-4761-8d59-cee055f3d5a8`; production impact is unconfirmed and no
out-of-scope repair or restart was attempted.
