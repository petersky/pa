"""Presentation-only fleet instance identity resolution."""

from __future__ import annotations

from collections import Counter
from typing import Any


def short_instance_id(instance_id: str) -> str:
    value = str(instance_id or "").strip()
    if not value:
        return ""
    return value[:8] if len(value) >= 32 else value[:12]


def canonical_instance_identities(ctx: Any) -> list[dict[str, Any]]:
    """Return the current canonical fleet directory with display disambiguation."""
    by_id: dict[str, str] = {}
    fleet = ctx.services.get("fleet_registry")
    if fleet:
        for instance in fleet.list_instances():
            instance_id = str(instance.instance_id or "").strip()
            if instance_id:
                by_id[instance_id] = str(instance.name or "").strip()

    counts = Counter(name.casefold() for name in by_id.values() if name)
    result = []
    for instance_id, name in by_id.items():
        duplicate = bool(name and counts[name.casefold()] > 1)
        if name:
            display_name = (
                f"{name} · {short_instance_id(instance_id)}" if duplicate else name
            )
        else:
            display_name = f"Unknown instance · {short_instance_id(instance_id)}"
        result.append(
            {
                "id": instance_id,
                "name": name or "Unknown instance",
                "display_name": display_name,
                "duplicate": duplicate,
            }
        )
    return sorted(
        result, key=lambda item: (item["display_name"].casefold(), item["id"])
    )


def resolve_instance_identity(ctx: Any, instance_id: str | None) -> dict[str, Any]:
    value = str(instance_id or "").strip()
    if not value:
        return {
            "id": "",
            "name": "Unknown instance",
            "display_name": "Unknown instance",
            "duplicate": False,
            "known": False,
        }
    for identity in canonical_instance_identities(ctx):
        if identity["id"] == value:
            return {**identity, "known": True}
    return {
        "id": value,
        "name": "Unknown instance",
        "display_name": f"Unknown instance · {short_instance_id(value)}",
        "duplicate": False,
        "known": False,
    }


def current_instance_name(
    ctx: Any, instance_id: str | None, snapshot_name: str | None = None
) -> str:
    """Resolve a UUID to canonical current membership, with snapshot fallback."""
    identity = resolve_instance_identity(ctx, instance_id)
    if identity["known"]:
        return str(identity["name"])
    return str(snapshot_name or identity["display_name"])


def canonicalize_dispatch_public(ctx: Any, record: Any) -> dict[str, Any]:
    """Present current names while retaining explicitly-labelled snapshots."""
    public = record.public_dict()
    for role in ("authority", "target"):
        instance_id = getattr(record, f"{role}_instance_id", None)
        snapshot = getattr(record, f"{role}_instance_name", None)
        public[f"{role}_instance_name"] = current_instance_name(
            ctx, instance_id, snapshot
        )
        public[f"{role}_instance_name_snapshot"] = snapshot
        public[f"{role}_instance_name_at_dispatch"] = (
            snapshot
            if snapshot and snapshot != public[f"{role}_instance_name"]
            else None
        )
    return public


def present_instance_references(
    ctx: Any, text: str, instance_id: str | None, *aliases: str | None
) -> str:
    """Replace durable instance tokens with the current canonical display name."""
    rendered = str(text or "")
    identity = resolve_instance_identity(ctx, instance_id)
    for token in (*aliases, instance_id):
        value = str(token or "").strip()
        if value:
            rendered = rendered.replace(value, identity["display_name"])
    return rendered
