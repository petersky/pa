# Chat UI state evidence

Card: 16f7d011-d12a-4808-937c-bda81816fa64. Session: 31c821b9-5a77-40f0-8dd4-51aaeff26904.
Instance: macbook / 0c7d8ecb-7e45-4579-8fa0-35159492d3f1. Repository: petersky/pa.
Base: c5cf412098de033e40cff5f0ea2dc0f7d1905e72. Lease fence: 407.
Branch: pa/16f7d011-d12a-48-b9085664-31c821b9-5a77-40-d9796150-266ade2c.
Worktree: /Users/petersky/.pa-workspaces/worktrees/16f7d011-d12a-48-b9085664/31c821b9-5a77-40-d9796150/5ce49cd8-f632-5f6d-a-b2354070266ade2c.

## Findings

The header retained the prior conversation title during asynchronous owner/history/live-state loading. Snapshot application also lacked a common session-identity guard, so an asynchronous recovery action could apply another conversation's snapshot. Selection now clears the header, snapshots reject mismatched session IDs, and rename responses check selection generation and owner as well as title version. Existing event session guards remain in place.

Persisted corruption was not reproduced. Synthetic A/B titles remain distinct after six switches and deliberately delayed cross-session snapshots/events. Existing runtime title tests verify rejection of cross-provider-session metadata, timestamp ordering, and manual-title preservation. No source conversation was reused or modified; this establishes a display failure and does not claim historical production titles are intact.

Older-page requests unconditionally enabled historyPreserveWindow, even when the window retained its tail. Incoming final chunks were then held rather than appended; the recovery notice showed a bounded preview. The notice was a sibling between the transcript and composer. This explains both the opening-only transcript and commentary/tool labels in the prompt-entry vicinity without missing provider output.

On the original base, a synthetic delayed older read leaves actual text `Opening ` instead of `Opening complete ending.`. The regression now passes. Fetching older rows pauses live rendering only if retention actually evicts the tail. Deep event and DOM eviction still enable preservation and forward pagination. The notice now resides inside the transcript, survives transcript reconstruction, and is excluded from session DOM caching. Genuine gaps retain their explicit incomplete-preview recovery.

## Verification

- Focused chat/history/title/SSE/action/shell/memory suite: 103 tests passed.
- Browser reentry integration plus history reconstruction: 9 tests passed. Real Jinja widget, real PA SSE, failed/delayed history, anchors, draft isolation, reconnect, desktop/mobile, genuine deep-window paging. Updated prior preview expectations to require live transcript text when the tail remains available.
- Existing chat-bubble browser tests passed in the broader run; the earlier reentry expectations exposed the intended behavior change and a missing post-eviction notice refresh, both addressed before the final passing run.
- Isolated PA browser: tests/agent_chat_state_browser_harness.html, served with `uv run python -m tests.agent_chat_history_fixture`. Six A/B transitions, stale snapshot/event rejection, complete final, exhausted notice, writable composer/draft and persisted synthetic title identity all PASS/DONE.
- Existing long-history browser harness: all transition checks PASS/DONE, including exact 1,957-character final from 396 chunks, delayed arrivals, cache, forward/backward paging and DOM eviction.
- Isolated Kernel.boot().build_app() smoke passed with separate temporary data/workspace roots; no production service restart or writes.
- git diff --check passed. PR #702 was confirmed merged and its tools-status change is untouched.

Source reports: b612819d-b8c0-400c-ac23-7cd3204b242b and e98e0b2b-ac27-4223-ac55-e31b82bf5478 (updated to revision 3 with bounded reproduction).
Production deployment and user acceptance remain separate from local verification and PR merge.
