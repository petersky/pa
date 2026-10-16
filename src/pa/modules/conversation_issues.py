"""Discussion-only issue lists and explicit, durable work admission."""
from __future__ import annotations

import asyncio
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
    state = await _call(manager, notes.read, session_id)
    for record in state["requests"]:
        if record["state"] in {"accepted", "admitting"}:
            state = await _run_request(request, session_id, record["request_id"], manager, notes)
    result = notes.public(state)
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
    if session.realm_id != settings.primary_realm:
        raise HTTPException(409, "Start work on the conversation's realm authority")
    source = await _call(manager, manager.store.get_card, session.card_id) if session.card_id else None
    if session.card_id and (not source or source.realm_id != session.realm_id):
        raise HTTPException(409, "The conversation's primary card is unavailable in this realm")
    project_id = session.project_id or (source.project_id if source else None)
    project = await _call(manager, manager.store.get_project, project_id) if project_id else None
    if project_id and not project:
        raise HTTPException(409, "The conversation project is unavailable")
    if project and project.memberships and not any(m.principal_id == get_principal_id(request) for m in project.memberships) and getattr(require_user(request), "role", None) != "admin":
        raise HTTPException(403, "Project membership is required to start work")
    return {
        "realm_id": session.realm_id, "project_id": project_id,
        "source_session_id": session.id, "source_card_id": session.card_id,
        "principal_id": get_principal_id(request), "instance_id": settings.instance_id,
        "dispatch": {"provider": session.agent_name, "model_id": session.model_id,
                     "execution_preferences": _preferences(session),
                     "target_instance_id": settings.instance_id},
    }


async def _prepare_card(request, manager, context, group):
    """Settle missing metadata before freezing the first admission payload."""
    from pa.domain.card_enrichment import enrich_card, referenced_projects
    from pa.domain.models import CardUpdate

    ctx = request.app.state.ctx
    card = await _call(manager, manager.store.get_card, group["card_id"], realm_id=context["realm_id"])
    if not card:
        raise HTTPException(409, {"code": "conversation_card_unavailable", "message": "The saved card is unavailable. Restore it before reviewing this start."})
    if not card.project_id:
        matches = await _call(manager, referenced_projects, manager.store, card)
        if len(matches) > 1:
            raise HTTPException(409, {"code": "conversation_project_required", "message": "More than one project matches. Choose the project on the saved card, then review this start."})
        if len(matches) == 1:
            # This is the canonical CAS update, not a planner-only assignment.
            await _check_project(request, manager, matches[0].id, card.realm_id)
            await _call(manager, manager.store.update_card, card.id,
                CardUpdate(project_id=matches[0].id, expected_version=card.updated_at),
                realm_id=card.realm_id, principal_id=context["principal_id"], instance_id=context["instance_id"])
        else:
            # Use the normal conservative enrichment pipeline, awaited. Other
            # selected content/preferences and completion authority stay frozen.
            projects = await _call(manager, manager.store.list_projects, realm_id=card.realm_id)
            eligible = {p.id for p in projects if not p.memberships
                or any(m.principal_id == get_principal_id(request) for m in p.memberships)
                or getattr(require_user(request), "role", None) == "admin"}
            await enrich_card(ctx, card.id, card.realm_id,
                {"title", "body", "kind", "tags", "preferred_capabilities"}, initial_card=card, project_ids=eligible)
        card = await _call(manager, manager.store.get_card, card.id, realm_id=card.realm_id)
    if not card or not card.project_id:
        raise HTTPException(409, {"code": "conversation_project_required", "message": "Project not resolved. Choose a project on the saved card, then review this start."})
    await _check_project(request, manager, card.project_id, card.realm_id)
    return card.project_id


async def _check_project(request, manager, project_id, realm_id):
    project = await _call(manager, manager.store.get_project, project_id, realm_id=realm_id)
    if not project or project.status != "active":
        raise HTTPException(409, {"code": "conversation_project_unavailable", "message": "The selected project is unavailable. Choose an active project on the card."})
    if project.memberships and not any(m.principal_id == get_principal_id(request) for m in project.memberships) and getattr(require_user(request), "role", None) != "admin":
        raise HTTPException(403, {"code": "project_membership_required", "message": "Project membership is required to start this work."})
    return project


def _create_card(store, context, group, key):
    # This is the same canonical event/operation ledger used by POST /cards.
    # Replay recovers a committed event even if the process died before receipt
    # publication. Never retry an ambiguous creation with a different key.
    detail = "\n\n".join(i["title"] + ("\n" + i["detail"] if i["detail"] else "") for i in group["issues"])
    data = CardCreate(
        title=group["title"], body=detail + "\n\nSource conversation: " + context["source_session_id"],
        summary=group["title"], realm_id=context["realm_id"], project_id=context["project_id"],
        # A selected issue is a new deliverable. Parent acceptance is scoped to
        # its subject, repair origin and authority; it is never inheritable.
        parent_id=context["source_card_id"],
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
            if context["realm_id"] != request.app.state.ctx.settings.primary_realm or context["instance_id"] != request.app.state.ctx.settings.instance_id:
                raise HTTPException(409, "Resume the saved request on its realm authority")
            for index, group in enumerate(groups):
                key = f"conversation:{session_id}:{request_id}:{index}"
                if not group.get("card_id"):
                    group["card_id"] = await _call(manager, _create_card, manager.store, context, group, key + ":card")
                    await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="admitting")
                # A secondary association preserves the discussion's execution
                # binding; workers receive separate sessions/worktree leases.
                await _call(manager, manager.store.link_session_card, session_id, group["card_id"], principal_id=context["principal_id"], make_primary=False)
                if not group.get("dispatch_id"):
                    ledger = request.app.state.ctx.services.get("dispatch_store")
                    existing = await _call(manager, ledger.by_authority_idempotency,
                        context["instance_id"], key + ":dispatch") if ledger is not None else None
                    # A frozen payload may have committed even without a local
                    # receipt. Only definitively unadmitted work may be enriched.
                    if "dispatch_inputs" not in group or ledger is not None and existing is None:
                        group["project_id"] = await _prepare_card(request, manager, context, group)
                        await _call(manager, notes.update_request, session_id, request_id, groups=groups)
                    if "dispatch_inputs" not in group:
                        inputs = dict(context["dispatch"])
                        # Older notebooks froze the chat's mode, although the
                        # start contract never accepted a worker-mode selection.
                        # Normalize only before definitive first admission. An
                        # existing/ambiguous dispatch retains its exact payload.
                        ledger = request.app.state.ctx.services.get("dispatch_store")
                        if inputs.get("mode_id") and ledger is not None:
                            existing = await _call(manager, ledger.by_authority_idempotency,
                                context["instance_id"], key + ":dispatch")
                            if existing is None:
                                inputs.pop("mode_id")
                        group["dispatch_inputs"] = inputs
                        await _call(manager, notes.update_request, session_id, request_id, groups=groups)
                    project_id = group.get("project_id", context["project_id"])
                    if project_id:
                        await _check_project(request, manager, project_id, context["realm_id"])
                    body = FleetDispatchBody(**group["dispatch_inputs"], card_id=group["card_id"], project_id=project_id,
                        message="Implement only the selected scope recorded on this card. Preserve its completion requirements and existing authorization. Source conversation: " + session_id,
                        idempotency_key=key + ":dispatch")
                    # Re-enter only the canonical idempotent admission, with the
                    # frozen body and identity. Routine pending/transport states
                    # get a bounded recovery; genuine selection/policy denials
                    # are never retried here.
                    while True:
                        try:
                            result = await dispatch_fleet_work(_child_request(request, key + ":dispatch"), body.model_copy(deep=True))
                            break
                        except Exception as exc:
                            detail = exc.detail if isinstance(exc, HTTPException) else {}
                            transient = isinstance(exc, (OSError, TimeoutError)) or (
                                isinstance(detail, dict) and detail.get("code") == "dispatch_admission_in_progress"
                            )
                            attempts = group.get("automatic_retries", 0)
                            if not transient or attempts >= 3:
                                raise
                            group["automatic_retries"] = attempts + 1
                            await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="admitting", error=None)
                            await asyncio.sleep(0.1 * (attempts + 1))
                    group.update(dispatch_id=result["dispatch_id"], state="admitted")
                    await _call(manager, notes.update_request, session_id, request_id, groups=groups)
            state = await _call(manager, notes.update_request, session_id, request_id, groups=groups, state="admitted", error=None,
                                timings_ms={"pa_admission": round((perf_counter() - started) * 1000, 2)})
        except Exception as exc:
            # Preserve evidence and the same request for safe resume. A provider
            # turn ending never turns this receipt into task completion.
            detail = exc.detail if isinstance(exc, HTTPException) else {"code": type(exc).__name__, "message": "PA could not finish preparing this work. Its saved request and any created card are retained; the outcome is unconfirmed."}
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
    return notes.public(await _run_request(request, session_id, body.request_id, manager, notes))


async def _run_request(request, session_id, request_id, manager, notes):
    executor = request.app.state.ctx.services.get("async_runtime")
    if isinstance(executor, AsyncRuntime):
        try:
            await executor.run_owned(f"conversation-work:{session_id}:{request_id}",
                lambda: _execute(request, session_id, request_id, manager, notes), wait_timeout=0.25)
        except BlockingOperationTimeout:
            pass
    else:
        await _execute(request, session_id, request_id, manager, notes)
    return await _call(manager, notes.read, session_id)
