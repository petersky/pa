"""Authenticated local operational journal routes and passive UI."""
from __future__ import annotations

import html
import hmac
import sqlite3
import httpx
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.routing import APIRoute
from fastapi.exceptions import RequestValidationError

from pa.auth.middleware import get_principal_id, require_user
from pa.core.contracts import Module
from pa.core.operation_dependencies import local_operational
from pa.health_journal.models import Assessment, Observation, Policy, Strict
from pa.health_journal.service import HealthService
from pa.health_journal.store import Journal, JournalError

class JournalRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()
        async def bounded(request):
            try:
                if request.method in {'POST', 'PATCH'}:
                    body = bytearray()
                    async for chunk in request.stream():
                        body.extend(chunk)
                        if len(body) > 32 * 1024:
                            raise JournalError('journal_request_too_large', 413)
                    request._body = bytes(body)
                return await original(request)
            except RequestValidationError as exc:
                return JSONResponse({'detail': [{'loc': list(e['loc']), 'type': e['type'],
                                                'msg': 'Invalid bounded journal field'} for e in exc.errors()[:16]]}, status_code=422)
            except JournalError as exc:
                return JSONResponse({'detail': {'code': exc.code}}, status_code=exc.status)
            except httpx.HTTPError:
                return JSONResponse({"detail": {"code": "health_authority_receipt_unavailable"}}, status_code=503)
            except (sqlite3.Error, TimeoutError):
                # A timed-out worker may have committed. Retain the same key and
                # query/replay; do not falsely assert non-commit.
                return JSONResponse({'detail': {'code': 'journal_outcome_unknown',
                                               'recovery': 'replay_same_idempotency_key'}}, status_code=503)
        return bounded


router = APIRouter(prefix='/health-journal', route_class=JournalRoute)
ui_router = APIRouter()


def service(request):
    value = request.app.state.ctx.services.get('health_journal')
    if value is None:
        raise HTTPException(503, {'code': 'journal_storage_unavailable'})
    return value


def user_scope(request):
    capability = getattr(request.state, 'assigned_session_capability', None)
    if capability:
        from pa.acp.environment import assigned_service_session_capability
        ctx = request.app.state.ctx
        session_id = request.headers.get('X-PA-Assigned-Session-ID', '')
        manager = ctx.services.get('instance_agent')
        runtime = manager.get(session_id) if manager else None
        session = getattr(runtime, 'session', None)
        dispatch_id = str(getattr(session, 'dispatch_id', '') or '')
        if not session or not dispatch_id or request.headers.get('X-PA-Assigned-Dispatch-ID') != dispatch_id:
            raise HTTPException(403, 'Invalid journal session binding')
        expected = assigned_service_session_capability(secret=ctx.settings.session_secret,
            dispatch_id=dispatch_id, session_id=session.id, target_instance_id=ctx.settings.instance_id)
        if not hmac.compare_digest(capability, expected) or getattr(runtime, '_closed', False):
            raise HTTPException(403, 'Invalid journal session capability')
        request.state.health_session = session
        return session.principal_id, [session.realm_id], False
    user = require_user(request)
    # Operational records are shared within authorized realms. Authorship still
    # binds writes/idempotency; it is not a separate read permission boundary.
    principal = get_principal_id(request)
    realms = list(request.app.state.ctx.settings.subscribed_realms)
    membership = request.app.state.ctx.services.get('membership')
    if user.role != 'admin':
        from pa.domain.models import RealmRole
        realms = [realm for realm in realms if membership and membership.has_role(realm, principal, min_role=RealmRole.VIEWER)]
    return principal, realms, user.role == 'admin'


def peer_only(request):
    if not getattr(request.state, 'instance_authenticated', False) or getattr(request.state, 'user_authenticated', False):
        raise HTTPException(403, 'Configured fleet collector authentication required')


def admin_only(request):
    if require_user(request).role != 'admin':
        raise HTTPException(403, 'Administrator access required')


class Gathered(Strict):
    receipt_id: UUID


class Transfer(Strict):
    action: Literal['freeze', 'receive', 'follow']
    target_id: UUID | None = None
    expected_version: int = 0


class Configure(Strict):
    expected_version: int
    policy: Policy


@router.post('/reports', status_code=201)
@local_operational
async def report_problem(request: Request, body: Observation,
                         key: Annotated[str, Header(alias='Idempotency-Key', min_length=1, max_length=160)]):
    principal, realms, _ = user_scope(request)
    assigned = getattr(request.state, 'health_session', None)
    session = assigned
    context = {'session_id': assigned.id, 'dispatch_id': assigned.dispatch_id, 'card_id': assigned.card_id,
               'project_id': assigned.project_id} if assigned else {}
    session_id = request.headers.get('x-pa-health-session-id')
    if assigned and session_id and session_id != assigned.id:
        raise HTTPException(403, 'Journal selector conflicts with signed session binding')
    if session_id and not assigned:
        # The header is only a selector. The live server's owned session supplies
        # principal, dispatch/card/repository context; it never trusts body claims.
        manager = request.app.state.ctx.services.get('instance_agent')
        runtime = manager.get(session_id) if manager else None
        session = getattr(runtime, 'session', None)
        if not session or session.principal_id != principal:
            raise HTTPException(403, 'Session does not belong to authenticated principal')
        context = {'session_id': session.id, 'card_id': session.card_id,
                   'project_id': session.project_id, 'dispatch_id': getattr(session, 'dispatch_id', None),
                   'repository': (session.config_json or {}).get('execution_context', {}).get('repository')}
    realm = body.realm or (session.realm_id if session else request.app.state.ctx.settings.primary_realm)
    if realm not in realms:
        raise HTTPException(403, 'Realm is not available to this journal')
    context['runtime_build'] = service(request).runtime_build
    return await service(request).call(service(request).journal.append, body,
                                      principal=principal, realm=realm, key=key, context=context)


@router.get('/reports')
async def list_problems(request: Request, cursor: str | None = Query(None, max_length=160),
                        limit: int = Query(32, ge=1, le=32)):
    principal, realms, admin = user_scope(request)
    return await service(request).call(service(request).journal.page, principal=None,
                                      realms=realms, cursor=cursor, limit=limit)


@router.get('/reports/{report_id}')
async def get_problem(request: Request, report_id: UUID, before: int | None = Query(None, ge=1)):
    principal, realms, admin = user_scope(request)
    return await service(request).call(service(request).journal.report, str(report_id),
                                      principal=None, realms=realms, before=before)


@router.get('/outbox')
async def outbox(request: Request, cursor: str | None = Query(None, max_length=160),
                 limit: int = Query(8, ge=1, le=8)):
    peer_only(request)
    # Only operational reports, bounded per request; no canonical or transcript reads.
    return await service(request).call(service(request).journal.page,
        realms=request.app.state.ctx.settings.subscribed_realms, cursor=cursor, limit=limit, pending=True)


@router.post('/gathered')
@local_operational
async def gathered(request: Request, body: Gathered):
    peer_only(request)
    return await service(request).verify_gathered(str(body.receipt_id))


@router.get('/receipts/{receipt_id}')
async def receipt(request: Request, receipt_id: UUID):
    peer_only(request)
    return await service(request).call(service(request).journal.receipt, str(receipt_id))


@router.get('/status')
async def status(request: Request):
    user_scope(request)
    current = service(request)
    return {**await current.call(current.journal.status), 'scheduler': dict(current.scheduler_status),
            'scheduler_alive': bool(current.task and not current.task.done())}


@router.get('/groups')
async def groups(request: Request, after: str = Query('', max_length=80)):
    _, realms, admin = user_scope(request)
    if not admin:
        raise HTTPException(403, 'Administrator triage access required')
    return {'items': await service(request).call(service(request).journal.groups, realms, after=after)}


@router.get('/groups/{group_id}')
async def group_history(request: Request, group_id: UUID, after: int = Query(0, ge=0)):
    admin_only(request)
    group = await service(request).call(service(request).journal.group, str(group_id), realms=request.app.state.ctx.settings.subscribed_realms)
    return {**group, 'history': await service(request).call(service(request).journal.group_history, str(group_id), after=after)}


@router.patch('/groups/{group_id}')
@local_operational
async def assess(request: Request, group_id: UUID, body: Assessment):
    principal, realms, admin = user_scope(request)
    if not admin:
        raise HTTPException(403, 'Administrator triage access required')
    accepted = False
    repair = body.disposition in {'reproduced', 'linked', 'in_progress', 'merged'} or bool(body.commit or body.pr_url)
    group = None
    if repair or body.disposition == 'deployed_verified':
        # This mixed endpoint is locally admitted only so reasoned triage stays
        # available. Canonical-dependent work requires the group's trusted realm,
        # never a caller-supplied query/body selector, before proof or acknowledgment.
        group = await service(request).call(service(request).journal.group, str(group_id), realms=realms)
        realm = group.get('realm')
        recovery = request.app.state.ctx.services.get('sync_recovery')
        if not realm or realm not in request.app.state.ctx.settings.subscribed_realms or recovery is None:
            raise JournalError('canonical_admission_unavailable', 503)
        try:
            blocked, _ = recovery.admission_view(realm)
        except Exception:
            raise JournalError('canonical_admission_unavailable', 503) from None
        if blocked is not False:
            raise JournalError('sync_history_recovery', 503)
    if repair and body.disposition != 'deployed_verified':
        await service(request).protect_repair(str(group_id), body, realms=realms)
    if body.disposition == 'deployed_verified':
        from pa.health_journal.acceptance import verify_acceptance
        card_id = body.card_id or group['data'].get('card_id')
        if not card_id:
            raise JournalError('acceptance_card_required')
        card = await service(request).canonical_call(request.app.state.ctx.store.get_card,
                                                     card_id, realm_id=group['realm'])
        if not card:
            raise JournalError('acceptance_card_not_found', 404)
        origins = await service(request).call(service(request).journal.repair_dispatches, str(group_id))
        sessions = []
        if origins:
            ledger = request.app.state.ctx.services.get('dispatch_store')
            if not ledger:
                raise JournalError('acceptance_origin_unknown')
            records = await service(request).canonical_call(lambda: [ledger.get(key) for key in origins])
            if any(record is None for record in records):
                raise JournalError('acceptance_origin_unknown')
            sessions = [record.session_id for record in records if record.session_id]
        accepted = verify_acceptance(card, group, body, repair_dispatches=origins, repair_sessions=sessions)
    return await service(request).call(service(request).journal.assess, str(group_id), body,
                                      actor=principal, realms=realms, accepted=accepted)


@router.patch('/config')
@local_operational
async def configure(request: Request, body: Configure):
    admin_only(request)
    if body.policy.authority_id:
        if body.policy.authority_id != request.app.state.ctx.settings.instance_id:
            service(request).peer_url(body.policy.authority_id)
    # Server binds the normal-dispatch actor; clients cannot expand principal.
    policy = body.policy.model_copy(update={'principal_id': get_principal_id(request)})
    return await service(request).call(service(request).journal.configure, policy,
                                      expected_version=body.expected_version, actor=get_principal_id(request))


@router.post('/run')
@local_operational
async def run(request: Request):
    admin_only(request)
    return await service(request).cycle(manual=True)


@router.post('/transfer')
@local_operational
async def transfer(request: Request, body: Transfer):
    admin_only(request)
    return await service(request).transfer(body.action, target_id=str(body.target_id or ''),
        expected_version=body.expected_version, actor=get_principal_id(request))


@router.get('/handover')
async def handover(request: Request):
    peer_only(request)
    return await service(request).call(service(request).journal.handover)


@router.get('/handover/received')
async def received_handover(request: Request):
    peer_only(request)
    return await service(request).call(service(request).journal.received_handover)


@ui_router.get('/health-journal', response_class=HTMLResponse)
async def journal_page(request: Request, cursor: str | None = Query(None, max_length=160)):
    principal, realms, admin = user_scope(request)
    page = await service(request).call(service(request).journal.page, realms=realms,
                                      principal=None, cursor=cursor)
    status = await service(request).call(service(request).journal.status)
    parts = ['<!doctype html><html><head><title>PA problem journal</title></head><body>',
             '<h1>PA problem journal</h1><p>Gathered means durably collected. A merged PR still requires declared acceptance.</p>',
             '<p><a href="/api/health-journal/status">Authority and collection status</a></p>']
    policy = status['policy']
    esc = html.escape
    parts.append(f'<p>Authority: {esc(policy["authority_id"] or "Not configured")} · Epoch {policy["epoch"]} · {"Paused" if policy["paused"] else "Enabled" if policy["enabled"] else "Disabled"} · Interval {policy["interval_seconds"]} seconds</p>')
    if admin:
        parts.append('<p>Administration: <a href="/api/health-journal/status">current policy and version</a>. Use the bounded configuration API to configure, pause, or transfer the authority.</p>')
    if not page['items']:
        parts.append('<p>No recorded problems on this page.</p>')
    for entry in page['items']:
        payload, custody = entry['payload'], entry['custody'] or {}
        esc = html.escape
        parts.append(f'<article><h2>{esc(payload["observation"]["summary"])}</h2>')
        parts.append(f'<p>{esc(entry["delivery"])} · revision {payload["revision"]} · {esc(custody.get("disposition", "untriaged"))}</p>')
        parts.append(f'<p>Custodian: {esc(custody.get("authority_id", "pending"))}</p>')
        parts.append(f'<a href="/api/health-journal/reports/{payload["report_id"]}">Immutable observation history</a>')
        fix = custody.get('fix') or {}
        if fix.get('card_id'):
            try:
                card_id = str(UUID(fix['card_id']))
                parts.append(f'<p><a href="/cards/{card_id}">Repair card</a></p>')
            except ValueError:
                pass
        import re
        if re.fullmatch(r'https://github.com/[^/]+/[^/]+/pull/[0-9]+', str(fix.get('pr_url', ''))):
            parts.append(f'<p><a rel="noreferrer" href="{esc(fix["pr_url"], quote=True)}">Pull request</a></p>')
        parts.append(f'<p>Disposition: {esc(str(fix.get("reason", "Awaiting triage")))} · Commit: {esc(str(fix.get("commit", "pending")))} · Acceptance: {esc(str(fix.get("acceptance_reference", "not verified")))}</p>')
        parts.append('<pre>' + esc(str(payload['observation']['evidence'])) + '</pre></article>')
    if page['cursor']:
        parts.append(f'<p><a href="/health-journal?cursor={html.escape(page["cursor"], quote=True)}">Next page</a></p>')
    parts.append('</body></html>')
    return ''.join(parts)


class HealthJournalModule(Module):
    @property
    def name(self):
        return 'health_journal'

    async def on_startup(self, app, ctx):
        # Kernel lifespan holds DataDirWriterLock before startup. Registration,
        # CLI boot and stdio MCP never create or mutate this operational store.
        try:
            journal = Journal(ctx.settings.data_dir / 'health_journal.db', ctx.settings.instance_id)
        except (sqlite3.Error, OSError, JournalError):
            ctx.register_service('health_journal_error', 'journal_storage_unavailable')
            return
        ctx.register_service('health_journal', HealthService(ctx, journal))
        current = ctx.require_service('health_journal')
        supervisor = ctx.services.get('pr_supervisor')
        if supervisor is not None and hasattr(supervisor, 'eligibility_journal_hook'):
            supervisor.eligibility_journal_hook = current.eligibility_hook
        await current.start(app)

    async def on_shutdown(self, app, ctx):
        if ctx.services.get('health_journal'):
            await ctx.services['health_journal'].stop()

    def api_routers(self):
        return [('/api', router, ['health-journal'])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx):
        from pa.mcp.tools.health_journal import register_mcp
        register_mcp(mcp, ctx)
