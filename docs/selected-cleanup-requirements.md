# Explicit retained-worktree cleanup

The original cleanup card 7938e21e-05fe-4dc9-b85e-98689957dd9d owns fourteen retained targets. Its verified inventory separates eight uncommitted-data blockers, five integration-evidence blockers and one missing-branch blocker. These are distinct outcomes; the original collector's damaged checkout is not a common prerequisite.

The selected operation must:

- Select 1–32 distinct explicit lease IDs and compare their observed fence, updated timestamp and exact HEAD. Unknown or initially stale selections fail before cleanup. Compare again under the existing repository lock after fetch; reject a changed owner, branch, repository association or HEAD without deleting it.
- Use a distinct endpoint and tool, so an older server cannot ignore selectors and execute global collection. Never reconcile or collect an unselected lease. Omission of a selection is invalid in this endpoint; existing global behavior stays separate.
- Preserve the existing completed/merged/expiry, active-owner, card acceptance, pending restart, dirty/untracked and unique-commit guards. Exclude new provider admissions for selected owners until the actual operation completes, including after caller disconnection.
- Allow explicit legacy PR revalidation only as cleanup integration evidence: primary GitHub merged identity, exact head and confirmed head, repository and merge commit. The merge must exist in freshly fetched origin history; existing changed-path/blob equivalence still applies. No dispatch/session rewrite, synthetic watch, historical green, policy or acceptance inference.
- Return every selected target's actual cleaned, retained, blocked or rejected result. Report removed `du` allocation separately from the concurrent filesystem free-space delta; neither establishes exclusive physical APFS block reclamation.

Validate real Git selected/unselected invariance, unknown/stale inputs and fetch races, all inherited guards, exact legacy identities and genuine integration, owner-admission exclusion and MCP forwarding. Integrate through the ordinary ready PR, durable supervisor and independent green-head checks. Coordinate one exact reviewed build activation including scheduling PR648/650, then use supported selected operations and preserve any valuable data before further repair.

Paged provenance product code is deferred. Its complete source remains in pushed commit 11a5c7d802e7aa920e45f75a44b8cc71e052828a and is reversed by 4226df2c. Published evidence and retained-parent chunks remain preserved. The selected cleanup repair does not extend that feature. All 740 systemic findings and independent acceptance requirements remain open.
