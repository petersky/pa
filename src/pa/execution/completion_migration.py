"""One-time cleanup: drop the `active` milestone from open requirements that only have it by default.

Before ``active`` became opt-in, declarations routinely listed it although nothing ever records an
activation, so cards sat in Waiting behind a checkpoint that could never be met. This plans (and, on
request, applies) the removal through the public card API so every fence, version check and audit
record of an ordinary requirement edit still applies. Dropping a milestone asks for strictly less,
so receipts already recorded keep counting (see ``pa.domain.completion.requirement_covers``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from pa.domain.completion import completion_state, requirement_covers

Request = Callable[..., Any]


def eligible(card: dict) -> bool:
    requirement = card.get("completion_requirement") or {}
    status = card.get("completion_status") or {}
    return bool(
        requirement
        and requirement.get("schema_version", 1) == 1
        and "active" in requirement.get("milestones", [])
        and not requirement.get("include_active")
        and card.get("lane") != "done"
        and "active" not in status.get("satisfied", [])
    )


def plan_card(card: dict) -> dict:
    requirement = dict(card["completion_requirement"])
    milestones = [m for m in requirement["milestones"] if m != "active"]
    replacement = {**requirement, "milestones": milestones}
    covered = requirement_covers(requirement, replacement)
    # The edit mints a new revision; a covering edit inherits the receipts of the old one.
    projected = {**replacement, "revision": "next",
                 "prior_revisions": [*requirement.get("prior_revisions", []), requirement["revision"]] if covered else []}
    after = completion_state(projected, card.get("completion_evidence") or [])
    before = card.get("completion_status") or {}
    return {
        "card_id": card["id"], "realm": card.get("realm_id", "default"), "title": card.get("title", ""),
        "lane": card.get("lane"), "revision": requirement["revision"], "version": card.get("updated_at"),
        "milestones_before": requirement["milestones"], "milestones_after": milestones,
        "status_before": before.get("reason_code"), "status_after": after["reason_code"],
        "completes": bool(after["accepted"]), "replacement": replacement,
    }


def plan(cards: Iterable[dict]) -> list[dict]:
    return [plan_card(card) for card in cards if eligible(card)]


def apply_plan(request: Request, entry: dict) -> dict:
    """Edit one requirement; if that leaves the card complete, finish it (an ordinary lane move)."""
    path = f"/api/cards/{entry['card_id']}"
    params = {"realm": entry["realm"]}
    edited = request("PATCH", path, params=params, body={
        "expected_version": entry["version"], "field_intent": ["completion_requirement"],
        "completion_requirement": entry["replacement"],
    }, headers={"Idempotency-Key": f"drop-active:{entry['card_id']}:{entry['revision']}"})
    result = {"card_id": entry["card_id"], "edited": True, "lane": edited.get("lane"), "finished": False}
    status = edited.get("completion_status") or {}
    if status.get("accepted") and edited.get("lane") != "done":
        done = request("PATCH", path, params=params, body={
            "expected_version": edited["updated_at"], "field_intent": ["lane"], "lane": "done",
        }, headers={"Idempotency-Key": f"drop-active-done:{entry['card_id']}:{edited['completion_requirement']['revision']}"})
        result.update(finished=True, lane=done.get("lane"))
    return result


def run(request: Request, realms: Iterable[str], *, apply: bool = False) -> dict:
    """Plan across ``realms``; apply only when asked. Dry-run is the default and writes nothing."""
    entries: list[dict] = []
    for realm in realms:
        cards = request("GET", "/api/cards", params={"realm": realm})
        entries.extend(plan(cards))
    results = []
    if apply:
        for entry in entries:
            try:
                results.append(apply_plan(request, entry))
            except Exception as exc:  # noqa: BLE001 - report and continue with the other cards
                results.append({"card_id": entry["card_id"], "edited": False, "error": str(exc)[:300]})
    return {"apply": apply, "eligible": len(entries),
            "plan": [{k: v for k, v in entry.items() if k != "replacement"} for entry in entries], "results": results}
