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
