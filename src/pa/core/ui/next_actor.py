"""Small shared next-actor vocabulary for conversation and work surfaces."""
def next_actor(actor_id: str, action: str | None = None) -> dict:
    labels = {"operator": "You", "provider": "Agent", "pa": "PA", "reviewer": "Reviewer", "none": "No action required", "unknown": "Not confirmed"}
    return {"id": actor_id, "label": labels[actor_id], "action": action}
