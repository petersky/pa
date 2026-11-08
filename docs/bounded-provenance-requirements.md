# Bounded cleanup provenance

The original cleanup owner has an empty index and retained tracked edits. The
legacy whole-scan diagnostic rejects its large tracked content at the 16 MiB
budget (HTTP 409, correlation 4562bdef-8e92-4318-8884-51ebbb1145a8). That guard
must remain intact. A new opt-in read-only page operation will stream content,
retain hash continuation only in bounded expiring process memory, and return
metadata, never file content. Losing process state invalidates a cursor.

Each admitted observation must retain the 16 MiB content and 10,000 distinct
tracked-path limits. A file larger than a page must continue across pages.
Metadata and exported rows also need explicit independent bounds. Cursors must
bind principal, exact owner/card/lease/fence/HEAD, original repair identity,
index and tracked-input identities; stale or changed inputs must fail closed.
Pages must be ordered and replayable without double advancement. A final page
must identify completeness, including counts and export truncation explicitly.

No page may create a preservation receipt, archive, index update, worktree edit,
apply authority, new owner, or collection. Prepare/apply and the legacy diagnostic
remain unchanged. Tests must cover multi-page files and trees, ordering, replay,
scope and stale cursors, file/index/HEAD/fence changes and in-read races, output
and memory bounds, and exact retained bytes. Production observation requires
reviewed integration and separately coordinated eligible activation. The original
0337 owner alone retains collection responsibility. Scheduling source is separate.

The opt-in action is `diagnose_page`, with optional `diagnostic_cursor` copied
exactly from `diagnostic.next_cursor`. Keep the original owner, key, fence and
HEAD arguments identical. Each page returns at most 64 completed-path rows; a
large file can span pages with no completed row until its hash finishes. Combine
rows in response order and trust totals only when `complete` is true. Replaying
the immediately previous cursor returns its same page after checking inputs;
older cursors fail explicitly. Do not submit the initial cursor again after
advancing. Lost/expired cursors require a new observation, never an apply.

The metadata fence remains capped at 10,000 distinct tracked paths, 4 MiB of
bounded record/fence encoding, and a 1 MiB empty index. These are explicit
residual limits, not a claim to support arbitrarily large path inventories.
The content budget remains 16 MiB per page, read in unbuffered chunks of at most
64 KiB. Up to four scoped continuations per workspace manager expire after five
minutes. A complete result is still only observation: no receipt, preservation
revision or apply eligibility is created. Existing mutation guards remain the
sole authority for preservation and repair.

An initial disposable sample exposed repeated Git subprocess amplification:
five pages all exceeded 500 ms. Batching equivalent Git identity queries and
retaining the bounded immutable HEAD manifest reduced driver calls from 60 to
20 across five pages. Current tracked stats and index are still checked before
and after each content page; the durable lease is checked again before return.
The repeat sample was 260–362 ms/page with less than 1 MiB traced Python peak.
Both samples and all test failures must remain in the performance evidence;
neither tiny sample establishes deployed responsiveness or whole-card acceptance.
