"""Authenticated scheduling API and a small equivalent control surface."""
from __future__ import annotations

import asyncio
import html
import json
import time
from typing import Literal
from fastapi import APIRouter, HTTPException, Request
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
    action: Literal['create','update','pause','resume','delete','fire','retry','webhook']
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
async def list_schedules(request: Request):
    principal = owner(request)
    current = scheduler(request)
    records = await call(current.ledger.list, principal)
    for record in records:
        history = await call(current.ledger.history, principal, record['id'])
        record['last_occurrence'] = history[0] if history else None
        record.update(await call(current.ledger.status, principal, record['id']))
    return {'schedules':records, 'health':current.health()}


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
    return await call(ledger.update, principal, body.schedule_id, body.idempotency_key, spec,
                      body.version, now, delete=body.action=='delete', command_args=body.model_dump(mode='json'))


@ui.get('/schedules', response_class=HTMLResponse)
async def scheduling_page(request: Request):
    owner(request)
    csrf = html.escape(str(getattr(request.state, 'csrf_token', '')), quote=True)
    return f'''<!doctype html><html lang="en"><meta charset="utf-8"><title>Schedules</title>
    <body><main><h1>Schedules</h1><p>PA delivers notifications. Recipients choose actions, reporting and completion.</p>
    <form id="editor"><input id="csrf" type="hidden" value="{csrf}">
    <p><label>Name <input id="name" required maxlength="200"></label></p>
    <p><label>Trigger <select id="kind"><option>interval</option><option>once</option><option>cron</option><option>event</option><option>webhook</option><option>manual</option></select></label>
    <label>Time, seconds, cron expression or event name <input id="trigger-value" value="600"></label>
    <label>Timezone <input id="timezone" value="UTC"></label></p>
    <p><label>Destination <select id="destination"><option value="inbox">PA inbox</option><option value="agent_session">Agent session</option><option value="messaging">Configured messaging conversation</option><option value="webhook">HTTPS webhook</option></select></label>
    <label>Recipient ID or URL <input id="target" required value="{html.escape(owner(request),quote=True)}"></label>
    <label><input id="interrupt" type="checkbox">Interrupt agent</label></p>
    <p><label>Message<br><textarea id="payload" required rows="5" cols="70"></textarea></label></p>
    <p><label>Missed deliveries <select id="missed"><option value="catch_up">One catch-up</option><option value="skip">Skip missed</option><option value="replay">Bounded replay</option></select></label>
    <label>Replay limit <input id="replay" type="number" min="1" max="100" value="10"></label>
    <label><input id="coalesce" type="checkbox">Coalesce undelivered notifications</label></p>
    <p><label>Expiry <input id="expiry" type="datetime-local"></label>
    <label>Maximum occurrences <input id="maximum" type="number" min="1"></label></p>
    <details><summary>Context and event filters</summary>
    <p>Optional key/value objects. Event filter keys: card, session, state, repo.</p>
    <label>Context references <textarea id="refs">{{}}</textarea></label>
    <label>Event filters <textarea id="filters">{{}}</textarea></label></details>
    <p><button type="submit" id="save">Create schedule</button><button type="button" id="new">New schedule</button>
    <button type="button" id="preview">Preview times</button><button type="button" id="refresh">Refresh</button></p></form>
    <p>Delivered means accepted by the destination. An in-flight delivery may finish after pause or deletion.</p>
    <pre id="result" aria-live="polite"></pre><div id="records"></div></main>
    <script src="/schedules-controls.js" defer></script></body></html>'''


@ui.get('/schedules-controls.js')
async def controls(request: Request):
    owner(request)
    from pathlib import Path
    return Response((Path(__file__).parents[1] / 'server' / 'static' / 'scheduling.js').read_text(), media_type='application/javascript')


class SchedulingModule(Module):
    @property
    def name(self):
        return 'scheduling'

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
