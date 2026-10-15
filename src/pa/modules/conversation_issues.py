"""Discussion-only issue lists and explicit, durable work admission."""
from __future__ import annotations

from time import perf_counter

from fastapi import APIRouter, HTTPException, Request

from pa.auth.middleware import get_principal_id, require_user
from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout
from pa.domain.conversation_issues import ConversationIssues, IssueConflict, IssueEdit, StartIssues, fingerprint
from pa.domain.models import CardCreate
from pa.execution.selection import ExecutionPreferences, SelectionConstraints

router = APIRouter(prefix="/agent/sessions")


async def _context(request, session_id):
    from pa.modules.agent_chat import _manager, _offload
    user = require_user(request)
    manager = _manager(request)
    session = await _offload(manager, "conversation.session", manager.store.get_session, session_id)
    if not session:
        raise HTTPException(404, "Conversation not found")
    if session.purpose != "chat":
        raise HTTPException(409, "Issue lists belong to human conversations")
    if session.principal_id not in {None, get_principal_id(request)} and getattr(user, "role", None) != "admin":
        raise HTTPException(403, "Conversation belongs to another principal")
    return manager, session, ConversationIssues(manager.store)


async def _call(manager, call, *args, **kwargs):
    from pa.modules.agent_chat import _offload
    try:
        return await _offload(manager, "conversation.issues", call, *args, **kwargs)
    except IssueConflict as exc:
        raise HTTPException(409, {"code": "conversation_issue_conflict", "message": str(exc)}) from exc


@router.get("/{session_id}/issues")
async def get_issues(request: Request, session_id: str):
    started = perf_counter()
    manager, _, notes = await _context(request, session_id)
    result = notes.public(await _call(manager, notes.read, session_id))
    result["diagnostics"] = {"read_ms": round((perf_counter() - started) * 1000, 2)}
    return result


@router.put("/{session_id}/issues")
async def edit_issues(request: Request, session_id: str, body: IssueEdit):
    manager, _, notes = await _context(request, session_id)
    return notes.public(await _call(manager, notes.edit, session_id, body))


def _preferences(session):
    """Carry explicit preferences and the conjunction of existing constraints."""
    receipt = (session.config_json or {}).get("execution_selection") or {}
    inputs = receipt.get("resolved_inputs") or {}
    values = {k: v for k, v in inputs.items() if not k.startswith("options.")}
    values["options"] = {k[8:]: v for k, v in inputs.items() if k.startswith("options.")}
    constraints = [SelectionConstraints.model_validate(c) for c in receipt.get("constraints", [])]
    merged = SelectionConstraints().model_dump()
    for key in ("instance_ids", "harnesses", "connection_ids", "model_providers", "models"):
        sets = [set(getattr(c, key)) for c in constraints if getattr(c, key) is not None]
        if sets:
            merged[key] = sorted(set.intersection(*sets))
    for key in ("required_tools", "modalities"):
        merged[key] = sorted({v for c in constraints for v in getattr(c, key)})
    merged["min_context_tokens"] = max([0, *(c.min_context_tokens for c in constraints)])
    costs = [c.max_cost_usd for c in constraints if c.max_cost_usd is not None]
    merged["max_cost_usd"] = min(costs) if costs else None
    for key in ("permission_eligible", "ownership_eligible"):
        merged[key] = all(getattr(c, key) for c in constraints)
    values["hard_constraints"] = merged
    return ExecutionPreferences.model_validate(values).model_dump(mode="json")


async def _start_context(request, manager, session):
    settings = request.app.state.ctx.settings
    # The dispatch default may be broader than this conversation. Never invoke
    # that fallback while transferring a human's discussion into bounded work.
    if not session.mode_id:
        raise HTTPException(409, {"code": "conversation_mode_unknown", "message": "Choose a permission mode in conversation settings before starting work."})
    if session.realm_id != settings.primary_realm:
        raise HTTPException(409, "Start work on the conversation's realm authority")
    source = await _call(manager, manager.store.get_card, session.card_id) if session.card_id else None
    project = await _call(manager, manager.store.get_project, session.project_id) if session.project_id else None
    if session.project_id and not project:
        raise HTTPException(409, "The conversation project is unavailable")
    if project and project.memberships and not any(m.principal_id == get_principal_id(request) for m in project.memberships) and getattr(require_user(request), "role", None) != "admin":
        raise HTTPException(403, "Project membership is required to start work")
    return {
        "realm_id": session.realm_id, "project_id": session.project_id,
        "source_session_id": session.id, "source_card_id": session.card_id,
        "principal_id": get_principal_id(request), "instance_id": settings.instance_id,
        "completion_requirement": source.completion_requirement.model_dump(mode="json") if source and source.completion_requirement else None,
        "dispatch": {"provider": session.agent_name, "model_id": session.model_id,
                     "mode_id": session.mode_id, "execution_preferences": _preferences(session),
                     "target_instance_id": settings.instance_id},
    }


def _create_card(store, context, group, key):
    # This is the same canonical event/operation ledger used by POST /cards.
    # Replay recovers a committed event even if the process died before receipt
    # publication. Never retry an ambiguous creation with a different key.
    detail = "\n\n".join(i["title"] + ("\n" + i["detail"] if i["detail"] else "") for i in group["issues"])
    data = CardCreate(
        title=group["title"], body=detail + "\n\nSource conversation: " + context["source_session_id"],
        summary=group["title"], realm_id=context["realm_id"], project_id=context["project_id"],
        parent_id=context["source_card_id"], completion_requirement=context["completion_requirement"],
        execution_preferences=context["dispatch"]["execution_preferences"], auto_enrich=False,
    )
    digest = fingerprint(data.model_dump(mode="json"))
    replay = store.begin_operation(idempotency_key=key, operation="card.create", request_fingerprint=digest, realm_id=context["realm_id"])
    if replay is not None:
        return replay["id"]
    card = store.create_card(data, principal_id=context["principal_id"], instance_id=context["instance_id"],
                             idempotency_key=key, request_fingerprint=digest, direct_human=True)
    store.complete_operation(key, card.model_dump(mode="json"))
    return card.id


def _child_request(request, key):
    # Fleet admission prefers the request header. Give each frozen group its own
    # stable identity while retaining the authenticated request scope.
    scope = dict(request.scope)
    scope["headers"] = [(k, v) for k, v in request.scope.get("headers", []) if k.lower() != b"idempotency-key"] + [(b"idempotency-key", key.encode())]
    return Request(scope)


async def _execute(request, session_id, request_id, manager, notes):
    from pa.modules.fleet import FleetDispatchBody, dispatch_fleet_work
    async with manager.label_lock("conversation-work:" + session_id):
        state = await _call(manager, notes.read, session_id)
        record = next(r for r in state["requests"] if r["request_id"] == request_id)
        if record["state"] == "admitted":
            return notes.public(state)
        context, groups = record["context"], record["groups"]
        started = perf_counter()
        try:
            for index, group in enumerate(groups):
                key = f"conversation:{session_id}:{request_id}:{index}"
                if not group.get("card_id"):
                    group["card_id"] = await _call(manager, _create_card, manager.store, context, group, key + ":card")
                    await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="admitting")
                # A secondary association preserves the discussion's execution
                # binding; workers receive separate sessions/worktree leases.
                await _call(manager, manager.store.link_session_card, session_id, group["card_id"], principal_id=context["principal_id"], make_primary=False)
                if not group.get("dispatch_id"):
                    body = FleetDispatchBody(**context["dispatch"], card_id=group["card_id"], project_id=context["project_id"],
                        message="Implement only the selected scope recorded on this card. Preserve its completion requirements and existing authorization. Source conversation: " + session_id,
                        idempotency_key=key + ":dispatch")
                    result = await dispatch_fleet_work(_child_request(request, key + ":dispatch"), body)
                    group.update(dispatch_id=result["dispatch_id"], state="admitted")
                    await _call(manager, notes.update_request, session_id, request_id, groups=groups)
            state = await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="admitted", error=None,
                                timings_ms={"pa_admission": round((perf_counter() - started) * 1000, 2)})
        except Exception as exc:
            # Preserve evidence and the same request for safe resume. A provider
            # turn ending never turns this receipt into task completion.
            detail = exc.detail if isinstance(exc, HTTPException) else {"code": type(exc).__name__, "message": "Admission needs recovery; retry this request to look up its durable outcome."}
            state = await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="blocked", error=detail)
        return notes.public(state)


@router.post("/{session_id}/issues/start", status_code=202)
async def start_issues(request: Request, session_id: str, body: StartIssues):
    manager, session, notes = await _context(request, session_id)
    if request.headers.get("idempotency-key") != body.request_id:
        raise HTTPException(409, "Idempotency-Key must match request_id")
    previous = await _call(manager, notes.read, session_id)
    existing = next((r for r in previous["requests"] if r["request_id"] == body.request_id), None)
    context = existing["context"] if existing else await _start_context(request, manager, session)
    await _call(manager, notes.admit, session_id, body, context)
    executor = request.app.state.ctx.services.get("async_runtime")
    if isinstance(executor, AsyncRuntime):
        try:
            return await executor.run_owned(f"conversation-work:{session_id}:{body.request_id}",
                lambda: _execute(request, session_id, body.request_id, manager, notes), wait_timeout=0.25)
        except BlockingOperationTimeout:
            return notes.public(await _call(manager, notes.read, session_id))
    return await _execute(request, session_id, body.request_id, manager, notes)
