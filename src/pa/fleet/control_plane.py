"""Read-only fleet control-plane compatibility diagnostics.

This module deliberately does not infer consensus authority from legacy
configuration.  It is the compatibility contract used while the strongly
consistent control plane is introduced in later stages.
"""

from __future__ import annotations

from typing import Any

CONTROL_PLANE_STATUS_VERSION = 1


def build_control_plane_status(
    settings: Any,
    *,
    pr_supervisor_health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return secret-free status without manufacturing leader or quorum state."""
    explicit_seed = (settings.pr_supervisor_authority_url or "").rstrip("/")
    owner_seed = (settings.fleet_owner_url or "").rstrip("/")
    local_url = (settings.instance_url or "").rstrip("/")
    legacy_seed = explicit_seed or owner_seed or local_url or None
    health = pr_supervisor_health or {}

    warnings = [
        "Consensus control plane is not enabled; no leader, term, commit index, "
        "or authority epoch is available.",
        "Legacy routing is not an election result and provides no quorum or "
        "split-brain protection.",
    ]
    if not explicit_seed and not owner_seed:
        warnings.append(
            "This instance currently treats itself as the legacy PR-supervisor "
            "lease authority because no static seed is configured."
        )

    return {
        "status_version": CONTROL_PLANE_STATUS_VERSION,
        "fleet_id": settings.fleet_id,
        "serving_instance_id": settings.instance_id,
        "mode": "legacy_static",
        "consensus": {
            "available": False,
            "protocol_versions": [],
            "leader_instance_id": None,
            "term": None,
            "commit_index": None,
            "quorum_size": None,
            "quorum_healthy": None,
            "membership_source": "node_local_registry",
        },
        "discovery": {
            "routing_mode": "legacy_static",
            "legacy_seed_url": legacy_seed,
            "explicit_pr_supervisor_seed": bool(explicit_seed),
            "seed_is_consensus_authority": False,
        },
        "service_authorities": {
            "pr-supervisor": {
                "authority_instance_id": None,
                "authority_epoch": None,
                "term": None,
                "fenced_by_consensus": False,
                "legacy_role": health.get("role"),
                "legacy_state": health.get("state"),
                "legacy_route_url": health.get("authority_url", legacy_seed),
                "max_observed_resource_fence": health.get("max_fence_token", 0),
            }
        },
        "migration": {
            "legacy_routing_active": True,
            "automatic_failover_enabled": False,
            "downgrade_to_unfenced_writes_blocked": False,
        },
        "warnings": warnings,
    }
