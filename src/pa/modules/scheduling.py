"""Authenticated scheduling API and a small equivalent control surface."""
from __future__ import annotations

import asyncio
import time
from typing import Literal
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.core.contracts import Module
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger, Schedule, Trigger

router = APIRouter(prefix='/schedules')
ui = APIRouter()


class Command(BaseModel):
    model_config = ConfigDict(extra='forbid')
    action: Literal['create','update','pause','resume','delete','cancel','fire','retry','webhook']
    idempotency_key: str = Field(min_length=1, max_length=300)
    schedule_id: str | None = None
    occurrence_id: str | None = None
    version: int | None = None
    schedule: Schedule | None = None


def owner(request):
    require_user(request)
    return get_principal_id(request)


def scheduler(request):
    current = request.app.state.ctx.services.get('scheduling')
    if current is None:
        raise HTTPException(503, 'Scheduling storage unavailable')
    return current


async def call(function, *args, **kwargs):
    try:
        runtime = getattr(getattr(function, '__self__', None), 'runtime', None)
        if runtime:
            return await runtime.run_blocking('scheduling.' + function.__name__, function, *args, wait_for_completion=True, **kwargs)
        return await asyncio.to_thread(function, *args, **kwargs)
    except KeyError:
        raise HTTPException(404, 'Schedule or occurrence not found') from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


@router.get('')
async def list_schedules(request: Request, cursor: str | None = Query(default=None, max_length=36),
                         limit: int = Query(default=50, ge=1, le=200),
                         session_id: str | None = Query(default=None, min_length=1, max_length=200),
                         pending_only: bool = False):
    principal = owner(request)
    current = scheduler(request)
    page = await call(current.ledger.list_page, principal, cursor=cursor, limit=limit, session_id=session_id, pending_only=pending_only)
    health = current.health()
    delivery = await call(current.ledger.delivery_health, principal, time.time())
    health.update(delivery)
    health['overdue'] = health['worker_overdue'] or delivery['overdue_delivery_count'] > 0
    return {**page, 'health':health, 'provider_discovery':{'status':'unsupported', 'reason':'Integrated ACP providers expose no automation discovery/control capability'}}


@router.get('/{schedule_id}')
async def schedule_detail(request: Request, schedule_id: str):
    principal = owner(request)
    ledger = scheduler(request).ledger
    def detail():
        record = ledger.get(principal, schedule_id)
        record.update(ledger.status(principal, schedule_id))
        record['state'] = ledger.lifecycle(record)
        return record
    return await call(detail)


@router.get('/{schedule_id}/history')
async def history(request: Request, schedule_id: str):
    return await call(scheduler(request).ledger.history, owner(request), schedule_id)


@router.post('/preview')
async def preview(request: Request, trigger: Trigger):
    owner(request)
    point = time.time()
    result = []
    for _ in range(10):
        point = trigger.next(point)
        if point is None:
            break
        result.append(point)
    return {'next_times': result, 'timezone':trigger.timezone}


@router.post('')
async def command(request: Request, body: Command):
    principal = owner(request)
    if require_user(request).role not in {'admin','editor'}:
        raise HTTPException(403, 'Editor access required')
    ledger = scheduler(request).ledger
    now = time.time()
    if body.action == 'create':
        if body.schedule is None:
            raise HTTPException(422, 'schedule required')
        return await call(ledger.create, principal, body.idempotency_key, body.schedule, now)
    if not body.schedule_id:
        raise HTTPException(422, 'schedule_id required')
    if body.action in {'fire','webhook'}:
        return await call(ledger.fire, principal, body.schedule_id, body.idempotency_key, now, require_webhook=body.action=='webhook')
    if body.action == 'retry':
        if not body.occurrence_id:
            raise HTTPException(422, 'occurrence_id required')
        return await call(ledger.retry, principal, body.schedule_id, body.occurrence_id, body.idempotency_key, now)
    record = await call(ledger.get, principal, body.schedule_id, include_deleted=True)
    spec = body.schedule or Schedule.model_validate({k:v for k,v in record.items() if k in Schedule.model_fields})
    if body.action in {'pause','resume'}:
        spec = spec.model_copy(update={'enabled':body.action=='resume'})
    if body.version is None:
        raise HTTPException(422, 'version required for update/pause/resume/delete')
    command_args = body.model_dump(mode='json')
    if command_args.get('schedule') and not command_args['schedule'].get('additional_destinations'):
        command_args['schedule'].pop('additional_destinations', None)
    return await call(ledger.update, principal, body.schedule_id, body.idempotency_key, spec,
                      body.version, now, delete=body.action=='delete', cancel=body.action=='cancel', command_args=command_args)


@ui.get('/loops', response_class=HTMLResponse)
@ui.get('/schedules', response_class=HTMLResponse)
async def scheduling_page(request: Request):
    owner(request)
    from pa.modules.ui_shell import render_page
    page = request.app.state.ctx.require_service('pages').get_by_path('/loops')
    return render_page(request, page)


@ui.get('/schedules-controls.js')
async def controls(request: Request):
    owner(request)
    from pathlib import Path
    return Response((Path(__file__).parents[1] / 'server' / 'static' / 'scheduling.js').read_text(), media_type='application/javascript')


class SchedulingModule(Module):
    @property
    def name(self):
        return 'scheduling'

    def on_load(self, ctx):
        from pa.core.ui.pages import PageDefinition
        ctx.require_service('pages').register(PageDefinition(
            id='loops', path='/loops', label='Loops', icon='loops',
            template='pages/loops.html', nav_order=19))

    async def on_startup(self, app, ctx):
        runtime = ctx.require_service('async_runtime')
        ledger = await runtime.run_blocking('scheduling.open', Ledger, ctx.settings.data_dir / 'scheduling.db', wait_for_completion=True)
        ledger.runtime = runtime
        current = Scheduler(ctx, app, ledger)
        ctx.register_service('scheduling', current)
        await current.start()

    async def on_shutdown(self, app, ctx):
        current = ctx.services.get('scheduling')
        if current:
            await current.stop()

    def api_routers(self):
        return [('/api', router, ['scheduling'])]

    def ui_routers(self):
        return [ui]

    def register_mcp(self, mcp, ctx):
        from pa.mcp.tools.scheduling import register_mcp
        register_mcp(mcp, ctx)
