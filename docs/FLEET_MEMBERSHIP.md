# Canonical fleet membership

PA's canonical fleet roster is the versioned `FleetRegistry` snapshot. Each
record contains a stable instance ID, display name, authenticated endpoints,
zone, capabilities, typed global/provider dispatch capacity, lifecycle state,
join/update/removal provenance, credential fingerprint, and the generation at
which it changed.

Fleet Overview, instance-name resolution, dispatch placement, authority
participation, update/readiness views, and direct peer routes consume this
projection. Reachability, health, sync heads, repositories, sessions,
dispatches, and PR supervision are observations about members; they never add
or remove membership.

## Consistency and lifecycle

The authority serializes membership changes into monotonically increasing
generations. Signed snapshots are idempotent. A receiver rejects cross-fleet,
future-schema, stale, duplicate-ID, and duplicate-endpoint snapshots. Equal
generation snapshots that differ require operator resolution. Removed members
remain as tombstones and cannot be resurrected by a stale join or route.
Temporary unreachability does not change lifecycle state.

During a join, the authority durably registers the stable joiner ID, returns the
complete roster, and pushes that generation to existing members. Retries for the
same ID are idempotent; an endpoint already bound to another ID is rejected.
Mixed-version peers remain visible but are reported as pending/incompatible
until upgraded.

The shared fleet credential authenticates membership transport. Snapshot
envelopes are HMAC signed and name an issuer that must itself be an active
member. Membership mutations and repairs require an authenticated operator or
fleet instance as appropriate.

## Repair and diagnostics

`POST /api/fleet/membership/reconcile` is the supported repair operation. It:

1. audits the local canonical projection and peer routes;
2. fetches signed snapshots from known authenticated peer endpoints;
3. selects only one unambiguous highest generation;
4. installs it idempotently and rebuilds direct routes; and
5. reports before/after generations, member counts, route changes, and
   unreachable or incompatible peers.

Every canonical mutation and projection install appends an event to the
membership audit log through `FleetRegistry`; operators must not edit PA data
files directly. The operation is safe to restart and retry.

For Monica's legacy self-only registry, its two existing peer routes discover
the authenticated local and Mac mini snapshots. Their identical three-member
legacy roster has a higher migration generation than Monica's one-member
projection, so reconciliation installs all three canonical IDs and derives the
same routes without direct data-file edits.
