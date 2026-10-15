# PR #457 approval evidence

## Confirmed observation and ownership

The attempted merge concerned old head
`34aaab9b2b525769a4b13106530a8a6e8fe959e3`, watch
`18e8258d-1dc4-45ad-a75e-13e0e4e0a017`, and worker session
`e758e94e-894c-4eae-b8c4-9293ce02b526`. It did not merge. The native
Guardian review `b784cb29-66c4-4c1d-a34f-a2968bbff110` completed at
2026-10-15T01:31:30.520051Z (durable session event sequence 1934) with
**denied**, risk **high**, authorization **medium**.

A bounded read through PA's session-history API confirmed exact equality between
the native rationale and the recorded denial: the reviewer considered merging a
shared default branch high impact, with authorization conditional on green checks
and independent revalidation, supported in its context only by untrusted claims.
The 241-character rationale has SHA-256
`b4baf4b31786519fd437534ace79cf6733b5b868d1ff51a7849d240dc498dfbd`.
No raw transcript, credentials, command inputs, or private user answers are
included here. An initial diagnostic command that could print raw content was
denied; the successful read returned only whitelisted labels, event metadata,
and the equality/hash result.

Installed versions: **codex-cli 0.153.4**, **@agentclientprotocol/codex-acp 2.2.2**.
Adapter source is the installed `dist/index.js` at
`/Users/petersky/.npm/_npx/4877722a062902ce/node_modules/@agentclientprotocol/codex-acp/`,
SHA-256 `6b1bb4e7e9caaf4aeca1042ae4aec2dc2433b0b0e4b1614b38a4d41699902aaa`.
Lines 29286–29325 render Guardian decisions as standard ACP tool content;
31407–31410 route Codex autoApprovalReview notifications; 34291–34308 select
auto_review for Agent mode; 35604–35610 pass approvalsReviewer to the Codex turn.

The [version-matched Codex prompt builder](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/core/src/guardian/prompt.rs)
constructs review input from retained conversation evidence and the exact proposed
action. It explicitly treats transcript/tool material as evidence rather than
instructions. [Official auto-review documentation](https://learn.chatgpt.com/docs/sandboxing/auto-review)
also describes the separate reviewer and retained context. PA does not own that
reviewer's policy or retain its complete composed input. **A false rejection or
provider policy defect is not established.**

## Supported PA correction

Previously the supervisor's green executor message supplied a prose gate summary
and failed checks/unresolved comments, omitting passing checks, approval counts,
known-policy flags, observation time, and stable-head observations. The full
`get_pr_watch` response includes these in a large audit history, which is poorly
suited to compact inspection.

`get_pr_merge_evidence(watch_id, expected_head_sha)` now reads a bounded packet
from the sole-writer HTTP API. It accepts no caller-supplied snapshot, green flag,
or authorization assertion. The API re-resolves canonical policy on a copy and
does not mutate the watch, refresh GitHub, create an effect authorization, or
merge. It reports:

- Repository, PR, exact expected/observed/confirmed head and source URLs.
- Individual check states, review counts, unresolved-thread count and known-policy flags.
- Observation time, 120-second freshness limit, stable-head counts/duration,
  watch condition/version/fence and originating session/instance/card context.
- Project policy source/revision and merge-on-green preference, explicitly
  distinguished from user authorization and provider approval.

It recomputes the gate from stored facts. Missing fields/provenance, wrong head,
changed policy, stale/future observations, contradictory gate data, unfinished
checks or truncated check evidence produce `insufficient_evidence`. A consistent
green result is only `supervisor_snapshot_green`; it always says it grants no
merge permission, requires independent revalidation, and cannot override a
provider denial. This is a consistency check, not a signed attestation or proof
against compromise of the PA server itself. External names stay redacted/bounded;
logs and comment bodies are omitted. Supervisor prompts include a stable exact-head
lookup reference inside their existing untrusted-data delimiter. The packet is
read separately: embedding changing timestamps/counts in the notification would
invalidate the content digest of a retry of the same durable executor effect.

Tests exercise valid, stale/future, wrong-head/repository/PR, missing, malformed,
contradictory and forged-green cases; canonical policy changes; truncation and
secret redaction; read-only HTTP behavior; client assertions ignored; and the MCP
proxy/schema. No provider approval was manufactured or retried to validate it.

PA problem `270ca9ed-b448-423e-9ecc-e4b0963b8e58`, occurrence
`pr457-head34aaab9-guardian-denial-20261015T0131`, revision 2 retains this finding.
The old operator interaction `conversations-pr457-merge-approval-v1` remains
preserved but concerns a superseded head. The denial remains binding; this change
is not a workaround for it. Integration and production activation remain separate.
