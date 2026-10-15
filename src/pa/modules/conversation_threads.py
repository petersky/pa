"""Independent conversation branches with explicit native/copy semantics."""
from __future__ import annotations

from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from pa.acp.configuration import SessionConfigurationRequest
from pa.acp.conversation_capabilities import conversation_capabilities
from pa.auth.middleware import get_principal_id
from pa.core.async_runtime import AsyncRuntime
from pa.modules.conversation_issues import _call, _context, _preferences

router = APIRouter(prefix="/agent/sessions")


class BranchConversation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=8, max_length=80, pattern=r"^[a-zA-Z0-9_-]+$")
    strategy: Literal["native", "copy"]


@router.get("/{session_id}/conversation-capabilities")
async def get_conversation_capabilities(request: Request, session_id: str):
    manager, session, _ = await _context(request, session_id)
    runtime = manager.get(session_id)
    connection = runtime.connection if runtime else None
    result = conversation_capabilities(connection._init_response if connection else None)
    result["native_branch_available"] = bool(result["fork"] and session.external_session_id)
    result["copy_available"] = True
    result["branch_point"] = "latest provider history; message-specific fork points are not supported"
    return result


async def _branch(request, source_id, body):
    manager, source, _ = await _context(request, source_id)
    principal = get_principal_id(request)
    target_id = str(uuid5(NAMESPACE_URL, f"pa:conversation-branch:{source.id}:{principal}:{body.request_id}"))
    async with manager.label_lock("branch:" + target_id):
        existing = await _call(manager, manager.store.get_session, target_id)
        if existing:
            if existing.initiating_workflow.get("strategy") != body.strategy:
                raise HTTPException(409, "Request ID belongs to another branch strategy")
            return {"session": existing.model_dump(mode="json"), "duplicate": True, "branch": existing.initiating_workflow}
        if not source.mode_id:
            raise HTTPException(409, "Choose a conversation permission mode before branching")
        if source.project_id:
            project = await _call(manager, manager.store.get_project, source.project_id)
            if not project:
                raise HTTPException(409, "Source project is unavailable")
            user = request.state.user
            if project.memberships and not any(m.principal_id == principal for m in project.memberships) and getattr(user, "role", None) != "admin":
                raise HTTPException(403, "Project membership is required to branch")
        workflow = {"kind": "conversation_branch", "strategy": body.strategy, "request_id": body.request_id,
                    "source_session_id": source.id, "source_card_id": source.card_id,
                    "source_instance_id": source.origin_instance_id, "source_external_id": source.external_session_id}
        workflow["source_connection"] = ((source.config_json or {}).get("execution_selection") or {}).get("selected", {}).get("connection")
        if body.strategy == "native":
            capabilities = await get_conversation_capabilities(request, source_id)
            if not capabilities["native_branch_available"]:
                raise HTTPException(409, "Native fork is unavailable. Choose Copy recent context explicitly to create an independent conversation.")
        else:
            events = await _call(manager, manager.store.list_transcript_events_before, source.id, limit=100)
            texts = []
            for event in events:
                if event.event_type in {"user_message", "agent_message", "agent_message_chunk"}:
                    text = event.payload.get("message") or event.payload.get("text")
                    if isinstance(text, str) and text:
                        texts.append(event.event_type + ": " + text)
            workflow["saved_excerpt"] = "\n\n".join(texts)[-8000:]
            workflow["source_through_seq"] = events[-1].seq if events else 0
            workflow["context_limit"] = "last 100 events, at most 8000 text characters; no hidden provider context"
        # A new PA session gets its own normal workspace admission. Never pass
        # source.cwd, execution_binding, dispatch_id or context_source_session_id:
        # the latter is a replacement operation that would fence the discussion.
        runtime = await manager.create_session(
            session_id=target_id, label=f"branch:{target_id}",
            title=None, principal_id=principal, project_id=source.project_id,
            realm_id=source.realm_id, provider_override=source.agent_name,
            execution_preferences=_preferences(source),
            initial_configuration=SessionConfigurationRequest.from_values(model_id=source.model_id, mode_id=source.mode_id),
            purpose="chat", control_mode="human", initiating_workflow=workflow,
            _defer_drain=True,
        )
        return {"session": runtime.session.model_dump(mode="json"), "duplicate": False, "branch": workflow}


@router.post("/{session_id}/branch", status_code=201)
async def branch_conversation(request: Request, session_id: str, body: BranchConversation):
    if request.headers.get("idempotency-key") != body.request_id:
        raise HTTPException(409, "Idempotency-Key must match request_id")
    # Authenticate before joining an owned operation.
    await _context(request, session_id)
    executor = request.app.state.ctx.services.get("async_runtime")
    if isinstance(executor, AsyncRuntime):
        return await executor.run_owned(f"conversation-branch:{get_principal_id(request)}:{session_id}:{body.request_id}:{body.strategy}",
                                        lambda: _branch(request, session_id, body))
    return await _branch(request, session_id, body)
