# Restart failure evidence and retry versions

The restart worker records `failed_before_host_restart_request` when its own
execution fails while waiting for the initiating turn or quiescing. The host
restart call occurs after those phases. Failure during the host call keeps the
existing reason classification because the request may have taken effect.

This marker describes the failing execution only. It does not settle earlier
attempts, continuation delivery, or the receipt's overall effect. Observations
continue to report UNKNOWN. No existing receipt is relabeled from its stage,
error text, an empty preview, or the currently loaded process identity.

An explicit caller can supply `{"expected_version": 3}` to the existing
`POST /api/agent/sessions/{session_id}/restart-handoffs/{handoff_id}/retry`
route. A stale version is rejected before any transition or scheduling; the
existing CAS also fences a concurrent change after the initial read. Wrong
owners remain rejected. Omission retains compatibility with existing callers.
The version fence is concurrency protection, not evidence or permission to
replay an unknown effect. Retry remains an explicit effect-changing operation;
it is never a passive lookup or an automatic response to this marker.

The historical c1b1 and PR758 activation 7fdc receipts have no such worker marker.
Their effects remain UNKNOWN. Integrating this repair does not activate a
package, retry them, satisfy their continuation obligations, or complete workspace
cleanup. A supported exact historical dispatch/continuation proof or a deliberate
owner disposition remains necessary before any corresponding effect change.
