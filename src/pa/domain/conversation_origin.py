"""Small display projection of existing conversation provenance.

Keep saved excerpts, provider identities and execution authority out of list rows.
Source availability is checked separately when opening the conversation.
"""
from pa.domain.models import AgentSession


def conversation_origin(session: AgentSession) -> dict | None:
    workflow = session.initiating_workflow or {}
    strategy = workflow.get("strategy")
    if session.purpose != "chat" or not isinstance(strategy, str) or strategy not in {"native", "copy", "empty"}:
        return None
    source_id = workflow.get("source_session_id")
    if not isinstance(source_id, str) or not source_id:
        return None
    source_title = workflow.get("source_title")
    if not isinstance(source_title, str) or not source_title:
        source_title = "Conversation " + source_id[:8]
    label = {"native": "Branch", "copy": "Context copy", "empty": "Fresh chat"}[strategy]
    default_title = {
        "native": "Branch of " + source_title,
        "copy": "Context copy of " + source_title,
        "empty": "New chat",
    }[strategy]
    admission = session.config_json.get("branch_admission")
    return {
        "strategy": strategy,
        "label": label,
        "default_title": default_title,
        "source_session_id": source_id,
        "source_instance_id": workflow.get("source_instance_id"),
        "source_title": source_title,
        "native_state": admission.get("state")
        if strategy == "native" and isinstance(admission, dict) else None,
    }
