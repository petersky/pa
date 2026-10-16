"""Bounded authority pull and one durable normal repair dispatch.

The collector never trusts a caller-supplied source/authority URL. Peer URLs come
from PA's fleet registry. Source acknowledgments obtain receipts from that
configured authority; a fleet bearer alone cannot manufacture custody.
"""
from __future__ import annotations

import asyncio
import functools
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import httpx

from pa.health_journal.models import MAX_BYTES, MAX_PAGE, encode
from pa.health_journal.store import Journal, JournalError


class HealthService:
    def __init__(self, ctx, journal: Journal, *, transport=None):
        self.ctx, self.journal = ctx, journal
        self.transport = transport
        self.owner = str(uuid4())
        from pa import __version__
        import os
        self.runtime_build = {'loaded_module_version': __version__, 'pid': os.getpid(), 'startup_id': self.owner}
        # Reserved capacity independent of canonical repair's shared worker pool.
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='pa-journal')
        self.slots = asyncio.Semaphore(4)
        self.task = None
        self.app = None
        self._cycle = asyncio.Lock()
        self._producer = asyncio.Lock()
        self._repair_transition = asyncio.Lock()
        self.scheduler_status = {'phase': 'not_started', 'last_error': None, 'next_wake_at': None}

    async def call(self, fn, *args, **kwargs):
        try:
            async with asyncio.timeout(2):
                await self.slots.acquire()
        except TimeoutError:
            raise JournalError('journal_busy', 503) from None
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self.pool, functools.partial(fn, *args, **kwargs))
        future.add_done_callback(lambda _: self.slots.release())
        # Shield preserves actual worker occupancy after a caller timeout.
        async with asyncio.timeout(3):
            return await asyncio.shield(future)

    def peer_url(self, instance_id):
        registry = self.ctx.services.get('fleet_registry')
        peer = registry.get_instance(instance_id) if registry else None
        if not peer or not peer.url or getattr(peer, 'lifecycle_state', '') == 'removed':
            raise JournalError('health_peer_unavailable', 503)
        return peer.url.rstrip('/')

    async def peer(self, instance_id, method, path, *, body=None, params=None):
        url = self.peer_url(instance_id)
        token = self.ctx.settings.sync_token
        if not token:
            raise JournalError('health_peer_auth_unconfigured', 503)
        async with httpx.AsyncClient(timeout=httpx.Timeout(5, connect=2), transport=self.transport,
                                     follow_redirects=False, trust_env=False) as client:
            async with client.stream(method, url + '/api/health-journal' + path,
                                     headers={'Authorization': f'Bearer {token}'}, json=body, params=params) as response:
                response.raise_for_status()
                if response.headers.get('X-PA-Instance-ID') != instance_id:
                    raise JournalError('health_peer_identity_mismatch', 403)
                chunks, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_BYTES:
                        raise JournalError('health_peer_response_too_large', 413)
                    chunks.append(chunk)
                return json.loads(b''.join(chunks))

    async def verify_gathered(self, receipt_id):
        status = await self.call(self.journal.status)
        authority = status['policy']['authority_id']
        if not authority:
            raise JournalError('health_authority_unconfigured', 409)
        if authority == self.ctx.settings.instance_id:
            receipt = await self.call(self.journal.receipt, receipt_id)
        else:
            receipt = await self.peer(authority, 'GET', f'/receipts/{receipt_id}')
        return await self.call(self.journal.gathered, receipt)

    async def deliver_custody(self, source_id, receipt):
        latest = await self.call(self.journal.receipt, receipt['receipt_id'])
        if source_id == self.ctx.settings.instance_id:
            await self.verify_gathered(receipt['receipt_id'])
        else:
            await self.peer(source_id, 'POST', '/gathered', body={'receipt_id': receipt['receipt_id']})
        await self.call(self.journal.custody_sent, receipt['receipt_id'], latest['version'])

    async def collect_source(self, source_id, lease):
        settings = self.ctx.settings
        cursor = None
        for receipt in await self.call(self.journal.pending_custody, source_id):
            await self.deliver_custody(source_id, receipt)
        # At most two pages per source/wake; each wake starts with pending rows.
        for _ in range(2):
            if source_id == settings.instance_id:
                page = await self.call(self.journal.page, realms=settings.subscribed_realms,
                                       cursor=cursor, limit=8, pending=True)
            else:
                page = await self.peer(source_id, 'GET', '/outbox', params={'cursor': cursor, 'limit': 8})
            if page.get('source_instance_id') != source_id:
                raise JournalError('source_identity_mismatch', 403)
            entries = page.get('items', [])
            if len(entries) > 8:
                raise JournalError('health_peer_page_too_large', 413)
            for entry in entries:
                receipt = await self.call(self.journal.ingest, entry, source_id=source_id,
                                          realms=settings.subscribed_realms, owner=self.owner, fence=lease['fence'])
                # COMMIT above precedes receipt verification and source acknowledgment.
                await self.deliver_custody(source_id, receipt)
            cursor = page.get('cursor')
            if not cursor:
                break

    async def cycle(self, *, manual=False):
        if self._cycle.locked():
            raise JournalError('health_cycle_owned')
        async with self._cycle:
            lease = await self.call(self.journal.lease, self.owner)
            try:
                status = await self.call(self.journal.status)
                registry = self.ctx.services.get('fleet_registry')
                ids = [self.ctx.settings.instance_id]
                if registry:
                    ids.extend(p.instance_id for p in registry.list_instances() if p.instance_id not in ids)
                deadline = time.monotonic() + 30
                for index in range(min(len(ids), status['policy']['max_sources'])):
                    if time.monotonic() >= deadline:
                        break
                    source = ids[0] if index == 0 else await self.call(self.journal.next_source, ids[1:])
                    source_state = status['sources'].get(source, {})
                    if not manual and source_state.get('next_retry', 0) > time.time():
                        continue
                    lease = await self.call(self.journal.lease, self.owner)
                    try:
                        async with asyncio.timeout(12):
                            await self.collect_source(source, lease)
                        await self.call(self.journal.source_status, source, code=None, phase='collected')
                    except (httpx.HTTPError, JournalError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                        await self.call(self.journal.source_status, source, code=getattr(exc, 'code', type(exc).__name__), phase='unknown')
                await self.dispatch_one(lease)
                return await self.call(self.journal.status)
            finally:
                await self.call(self.journal.release, self.owner, lease['fence'])

    async def local_api(self, action, method, path, *, body=None, key=None):
        # This is normal authenticated API admission, not direct canonical writes.
        from pa.auth.users import UserDirectory
        user_id = action['principal_id'].removeprefix('user:')
        user = UserDirectory(self.ctx.settings.data_dir).get(user_id)
        if not user or not user.cli_token or user.role not in {'admin', 'editor'}:
            raise JournalError('repair_principal_unavailable', 403)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),
                                     base_url='http://pa-owner', timeout=10) as client:
            headers = {'Authorization': f'Bearer {user.cli_token}'}
            if key:
                headers['Idempotency-Key'] = key
            async with asyncio.timeout(10):
                response = await client.request(method, path, json=body, headers=headers)
            response.raise_for_status()
            return response.json()

    async def canonical_call(self, fn, *args, **kwargs):
        runtime = self.ctx.services.get('async_runtime')
        if runtime:
            return await runtime.run_blocking('health.canonical_obligation', fn, *args, timeout=5, **kwargs)
        async with asyncio.timeout(5):
            return await asyncio.to_thread(fn, *args, **kwargs)

    async def repair_obligation(self, action):
        """Reuse the normal session-lifecycle decision; never a second queue."""
        result, payload = action['result'] or {}, action['payload']
        ledger = self.ctx.services.get('dispatch_store')
        lifecycle = self.ctx.services.get('session_lifecycle')
        supervisor = self.ctx.services.get('pr_supervisor_store')
        if not ledger or not lifecycle or not supervisor:
            return 'lifecycle_outcome_unknown'
        record = await self.canonical_call(ledger.get, result['dispatch_id'])
        if not record:
            return 'dispatch_outcome_unknown'
        # Keep the existing lifecycle guards even for a closed legacy session.
        from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
        from pa.execution.reconciliation import RECONCILIATION_TERMINAL_STATES
        if record.state not in TERMINAL_DISPATCH_STATES:
            return 'dispatch_active'
        if (record.state in {'failed', 'cancelled'} and not record.session_id
                and not record.prompt_acknowledged_at and not record.initial_prompt_operation):
            others = await self.canonical_call(ledger.list, card_id=record.card_id,
                realm_id=record.realm_id, exclude_states=TERMINAL_DISPATCH_STATES | {'acknowledged'}, limit=1)
            if others:
                return 'canonical_repair_active'
            watches = await self.canonical_call(supervisor.list_watches_for_cards, {result['card_id']},
                realm_id=payload['realm'], per_card_limit=5)
            if watches:
                return 'actionable_pr_watch'
            # There is no turn/card disposition to reconcile when selection
            # failed before session admission. Keep recovery on this dispatch.
            return 'pre_session_failed'
        if record.state == 'completed' and (not record.acknowledged_at or record.completion_delivery_class != 'acknowledged'):
            return 'completion_delivery_pending'
        if record.reconciliation_state not in RECONCILIATION_TERMINAL_STATES:
            return 'reconciliation_active'
        if record.state in {'failed', 'cancelled'} and record.recoverable:
            return 'dispatch_recoverable'
        watches = await self.canonical_call(supervisor.list_watches_for_cards, {result['card_id']},
            realm_id=payload['realm'], per_card_limit=5)
        if watches:
            return 'actionable_pr_watch'
        if not record.session_id:
            return None  # Definitively terminal before a provider was admitted.
        session = await self.canonical_call(self.ctx.store.get_session, record.session_id)
        if not session:
            return 'session_outcome_unknown'
        manager = self.ctx.services.get('instance_agent')
        if not manager or not getattr(manager, 'workspace_manager', None):
            return 'workspace_obligation_unknown'
        leases = await self.canonical_call(manager.workspace_manager.list, card_id=result['card_id'])
        async with asyncio.timeout(5):
            decision, reason = await lifecycle._decision(session, sessions=[session], dispatches=[record],
                                                        watches=watches, leases=leases, now=datetime.now(UTC))
        return None if decision in {'close', 'retire'} or reason == 'already_closed' else reason

    async def failure_card(self, action, lease, summary, *, lane='waiting'):
        """Reflect a definitive failed start through the ordinary card API/CAS.

        Never overwrite a user's lane or a canonical verification requirement.
        Retain the exact PATCH for replay after an unknown reply.
        """
        from urllib.parse import quote
        result, payload = action['result'], action['payload']
        path = f'/api/cards/{quote(result["card_id"], safe="")}?realm={quote(payload["realm"], safe="")}'
        card = await self.local_api(payload, 'GET', path)
        card = card.get('card') or card
        if card.get('summary') == summary and card.get('lane') == lane:
            return
        prior_summaries = {effect.get('payload', {}).get('body', {}).get('summary')
                          for name, effect in result.get('effects', {}).items()
                          if name.startswith('failed_start_card_')}
        if card.get('lane') != 'active' and not (
                card.get('lane') == 'waiting' and card.get('summary') in prior_summaries):
            return
        if lane == 'active' and card.get('summary') not in prior_summaries:
            return
        from pa.health_journal.models import digest
        effect_name = 'failed_start_card_' + digest({'summary':summary, 'lane':lane})[:16]
        saved = result.get('effects', {}).get(effect_name)
        if saved:
            plan = saved['payload']
        else:
            plan = {'method':'PATCH', 'path':path, 'body':{
                'expected_version':card['updated_at'],
                'field_intent':['lane', 'summary'], 'lane':lane, 'summary':summary}}
            saved = await self.call(self.journal.reserve_effect, action['id'], effect_name, plan,
                                    owner=self.owner, fence=lease['fence'])
        await self.local_api(payload, plan['method'], plan['path'], body=plan['body'], key=saved['id'])

    async def failed_start(self, action, lease, durable):
        """One same-dispatch retry; unknown delivery replays the same key, bounded.

        Normal retry admission rechecks placement and the target revalidates the
        exact selected tuple. This collector never chooses a replacement agent,
        creates a second card, or releases an ambiguous workflow obligation.
        """
        group = await self.call(self.journal.group, action['group_id'], realms=[action['payload']['realm']])
        if group['disposition'] != 'triaged':
            # A human/repair worker has taken ownership since triage. Preserve
            # that assessment and its verification gate; do not start a rival.
            return
        result = action['result']
        retry_key = f'health-start_retry:{action["id"]}'
        admitted = (durable.get('control_operations') or {}).get(retry_key) == 'retry'
        attempted = result.get('start_retry', False)
        retryable = (durable.get('state') == 'failed' and durable.get('recoverable')
                     and durable.get('error_code') == 'no_compatible_execution')
        exhausted = admitted or (attempted and not result.get('retry_pending'))
        transmissions = result.get('retry_transmissions', 0)
        unknown = result.get('retry_pending') and not admitted and transmissions >= 3
        retryable = retryable and not exhausted and not unknown
        reason = ('The automatic investigation could not start. PA will retry the same dispatch once; '
                  'no agent is currently investigating.' if retryable else
                  'The automatic investigation could not start. Review the selected agent connection and settings, '
                  'then retry the existing dispatch. No agent is currently investigating.')
        if unknown:
            reason = ('PA could not confirm the investigation retry after three delivery attempts. '
                      'Check the existing dispatch outcome before retrying; a second investigation was not created.')
        if durable.get('state') == 'cancelled':
            reason = 'The investigation was cancelled before an agent started. The project owner can review the existing dispatch.'
        await self.failure_card(action, lease, reason)
        if not retryable:
            values = {'reason':reason, 'next_actor':'project_owner',
                      'next_action':'inspect_existing_dispatch' if unknown or durable.get('state') == 'cancelled' else 'retry_existing_dispatch',
                      'next_retry_at':None,
                      'dispatch_state':durable['state'], 'retry_outcome':'unknown' if unknown else 'failed'}
            if action['state'] != 'blocked' or any(result.get(k) != v for k, v in values.items()):
                await self.call(self.journal.action_result, action['id'], state='blocked', result=values,
                                owner=self.owner, fence=lease['fence'])
            return
        if not attempted:
            await self.call(self.journal.action_result, action['id'], state='retry_wait', result={
                'start_retry':True, 'retry_pending':True, 'retry_transmissions':0,
                'next_retry_at':time.time() + 30, 'reason':reason,
                'next_actor':'pa', 'next_action':'retry_existing_dispatch'},
                owner=self.owner, fence=lease['fence'])
            return
        if result.get('next_retry_at', 0) > time.time():
            return
        path = f'/api/fleet/dispatch-jobs/{result["dispatch_id"]}/retry'
        plan = {'method':'POST', 'path':path, 'body':{}}
        saved = await self.call(self.journal.reserve_effect, action['id'], 'start_retry', plan,
                                owner=self.owner, fence=lease['fence'])
        await self.call(self.journal.action_result, action['id'], state='retry_pending', result={
            'retry_transmissions':transmissions + 1, 'next_retry_at':time.time() + 30},
            owner=self.owner, fence=lease['fence'])
        try:
            response = await self.local_api(action['payload'], 'POST', path, body={}, key=saved['id'])
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                await self.call(self.journal.action_result, action['id'], state='blocked', result={
                    'retry_pending':False, 'retry_outcome':'rejected', 'next_actor':'project_owner',
                    'reason':'The investigation retry was rejected. Review the existing dispatch and selected agent settings.',
                    'next_action':'inspect_existing_dispatch'}, owner=self.owner, fence=lease['fence'])
            raise
        current = response.get('dispatch') or response
        if current.get('dispatch_id') != result['dispatch_id']:
            raise JournalError('repair_retry_identity_mismatch')
        await self.call(self.journal.action_result, action['id'], state='dispatched', result={
            'retry_pending':False, 'next_retry_at':None, 'retry_outcome':'admitted',
            'reason':'The same investigation dispatch was queued for its one automatic retry.',
            'next_actor':'pa', 'next_action':'start_investigation'},
            owner=self.owner, fence=lease['fence'])

    async def protect_repair(self, group_id, assessment, *, realms):
        """Establish canonical acceptance before acknowledging a repair transition.

        This is the existing workflow's prerequisite, not an OS execution boundary.
        The exact normal PATCH/CAS request is retained with the existing action so
        an unknown reply cannot be replaced with a new declaration on retry.
        """
        from urllib.parse import quote
        async with self._repair_transition:
            action = await self.call(self.journal.repair_action, group_id,
                                     expected_version=assessment.expected_version, realms=realms)
            result = action['result']
            card_id = result.get('card_id')
            if not card_id or (assessment.card_id and assessment.card_id != card_id):
                raise JournalError('repair_card_binding_conflict')
            realm = action['payload']['realm']
            path = f'/api/cards/{quote(card_id, safe="")}?realm={quote(realm, safe="")}'
            async def current_card():
                card = await self.local_api(action['payload'], 'GET', path)
                return card.get('card') or card
            def protected(card):
                req = card.get('completion_requirement') or {}
                return (req.get('mode') == 'explicit_acceptance' and req.get('revision')
                        and 'verified' in req.get('milestones', [])
                        and f'health-group:{group_id}' in req.get('criteria', ''))
            card = await current_card()
            if protected(card):
                return
            saved = result.get('effects', {}).get('requirement')
            if saved:
                plan = saved.get('payload')
                if not plan:
                    raise JournalError('repair_requirement_outcome_unknown')
                await self.local_api(action['payload'], plan['method'], plan['path'],
                                     body=plan['body'], key=saved['id'])
            else:
                from pa.domain.models import CardUpdate
                if 'completion_requirement' not in CardUpdate.model_fields:
                    raise JournalError('repair_completion_contract_unavailable')
                req = dict(card.get('completion_requirement') or {})
                milestones = list(req.get('milestones', []))
                if req.get('mode') == 'integration_only' and 'integrated' not in milestones:
                    milestones.insert(0, 'integrated')
                req.update(schema_version=1, mode='explicit_acceptance',
                    milestones=list(dict.fromkeys([*milestones, 'verified'])),
                    criteria=(req.get('criteria') or '') +
                        f' Verify the declared repair build and scenario for health-group:{group_id}; retain exact canonical acceptance receipt and affected instance references.')
                req.setdefault('acceptance_principals', [])
                plan = {'method':'PATCH', 'path':path, 'body':{
                    'expected_version':card['updated_at'], 'field_intent':['completion_requirement'],
                    'completion_requirement':req}}
                lease = await self.call(self.journal.lease, self.owner)
                saved = await self.call(self.journal.reserve_effect, action['id'], 'requirement', plan,
                                        owner=self.owner, fence=lease['fence'])
                await self.local_api(action['payload'], 'PATCH', path, body=plan['body'], key=saved['id'])
            if not protected(await current_card()):
                raise JournalError('repair_requirement_not_current')

    async def effect(self, action, lease, name, method, path, *, body):
        key = f'health-{name}:{action["id"]}'
        receipt = await self.call(self.journal.reserve_effect, action['id'], name,
            {'method': method, 'path': path, 'body': body, 'key': key}, owner=self.owner, fence=lease['fence'])
        return await self.local_api(action['payload'], method, path, body=body, key=receipt['id'])

    async def dispatch_one(self, lease):
        action = await self.call(self.journal.action, owner=self.owner, fence=lease['fence'])
        if not action:
            return
        payload, result = action['payload'], action['result'] or {}
        try:
            if result.get('dispatch_id'):
                # Existing durable dispatch remains authoritative beyond lease expiry.
                record = await self.local_api(payload, 'GET', f'/api/fleet/dispatch-jobs/{result["dispatch_id"]}')
                durable = record.get('dispatch') or record
                state = durable.get('state')
                if (durable.get('session_id') and state not in {'completed', 'failed', 'cancelled', 'acknowledged'}
                        and result.get('start_retry') and result.get('started_session_id') != durable['session_id']):
                    group = await self.call(self.journal.group, action['group_id'], realms=[payload['realm']])
                    if group['disposition'] == 'triaged':
                        reason = 'The automatic investigation is running. Its diagnosis and any repair acceptance are still pending.'
                        await self.failure_card(action, lease, reason, lane='active')
                        await self.call(self.journal.action_result, action['id'], state='dispatched', result={
                            'started_session_id':durable['session_id'], 'reason':reason,
                            'next_actor':'agent', 'next_action':'finish_investigation'},
                            owner=self.owner, fence=lease['fence'])
                if (result.get('retry_pending') and state not in {'failed', 'cancelled'}
                        and (durable.get('control_operations') or {}).get(f'health-start_retry:{action["id"]}') == 'retry'):
                    group = await self.call(self.journal.group, action['group_id'], realms=[payload['realm']])
                    if group['disposition'] == 'triaged':
                        await self.call(self.journal.action_result, action['id'], state='dispatched', result={
                            'retry_pending':False, 'retry_outcome':'admitted', 'next_retry_at':None,
                            'reason':'The existing investigation retry was confirmed by its durable dispatch receipt.',
                            'next_actor':'pa', 'next_action':'finish_investigation'},
                            owner=self.owner, fence=lease['fence'])
                if state in {'completed', 'failed', 'cancelled', 'acknowledged'}:
                    obligation = await self.repair_obligation(action)
                    if obligation == 'pre_session_failed':
                        await self.failed_start(action, lease, durable)
                        return
                    if obligation:
                        await self.call(self.journal.source_status, 'repair', code=obligation, phase='retained_normal_workflow')
                        return
                    await self.call(self.journal.action_result, action['id'], state='terminal',
                                    result={'dispatch_state': state}, owner=self.owner, fence=lease['fence'])
                return
            from pa.prompts import PROMPTS
            prompt = PROMPTS.render("health.triage", {'group_id': action['group_id'],
                                    'evidence': encode(payload['evidence'])}).text
            if not result.get('card_id'):
                card = await self.effect(action, lease, 'card', 'POST', '/api/cards', body={
                        'title': f'Investigate PA problem {action["group_id"][:8]}',
                        'body': prompt, 'project_id': payload['project_id'], 'lane': 'active',
                        'summary':'Investigate a reported PA failure and record a diagnosis. No repair has started yet.',
                        'realm_id': payload['realm'], 'auto_enrich': False})
                result['card_id'] = (card.get('card') or card)['id']
                await self.call(self.journal.action_result, action['id'], state='card_created',
                                result=result, owner=self.owner, fence=lease['fence'])
            # Renew/revalidate immediately before normal admission. This version only
            # dispatches on the designated authority: no unfenced remote health owner.
            lease = await self.call(self.journal.lease, self.owner)
            dispatched = await self.effect(action, lease, 'dispatch', 'POST', '/api/fleet/dispatch', body={
                    'card_id': result['card_id'], 'target_instance_id': self.ctx.settings.instance_id,
                    'authority_instance_id': self.ctx.settings.instance_id,
                    'message': prompt,
                    'execution_contract': {'version': 1, 'profile': 'repository', 'confirmed': True,
                        'requirements': {'repository_required': True, 'expected_deliverables': ['code', 'pull_request']}}})
            result['dispatch_id'] = dispatched['dispatch_id']
            await self.call(self.journal.action_result, action['id'], state='dispatched',
                            result=result, owner=self.owner, fence=lease['fence'])
        except (httpx.HTTPError, JournalError, ValueError, KeyError, TimeoutError) as exc:
            await self.call(self.journal.source_status, 'repair',
                            code=getattr(exc, 'code', type(exc).__name__), phase='repair_admission_blocked')

    async def start(self, app):
        self.app = app
        self.task = asyncio.create_task(self.run(), name='pa-health-journal')

    async def run(self):
        while True:
            interval = 60
            try:
                self.scheduler_status.update(phase='checking', last_error=None)
                status = await self.call(self.journal.status)
                policy = status['policy']
                interval = policy['interval_seconds']
                if policy['authority_id'] == self.ctx.settings.instance_id and policy['enabled'] and not policy['paused']:
                    await self.cycle()
            except Exception as exc:
                # No report recursion; persistence of this diagnostic is unknown.
                self.scheduler_status.update(phase='backoff', last_error=getattr(exc, 'code', type(exc).__name__), diagnostic_persistence='unknown')
            else:
                self.scheduler_status.update(phase='idle', diagnostic_persistence='not_needed')
            self.scheduler_status['next_wake_at'] = time.time() + interval
            await asyncio.sleep(interval)

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.pool.shutdown(wait=False, cancel_futures=True)


    async def transfer(self, action, *, target_id='', expected_version=0, actor):
        status = await self.call(self.journal.status)
        old_id = status['policy']['authority_id']
        if action == 'freeze':
            self.peer_url(target_id)
            if self._cycle.locked():
                raise JournalError('authority_transfer_active_cycle')
            return await self.call(self.journal.freeze_transfer, target_id,
                                   expected_version=expected_version, actor=actor)
        if action == 'receive':
            handover = await self.peer(old_id, 'GET', '/handover')
            return await self.call(self.journal.import_handover, handover, actor=actor)
        if action == 'follow':
            if old_id == self.ctx.settings.instance_id:
                handover = await self.call(self.journal.handover)
            else:
                handover = await self.peer(old_id, 'GET', '/handover')
            received = await self.peer(handover['target_id'], 'GET', '/handover/received')
            if any(received[k] != handover[k] for k in ('hash', 'transfer_id', 'epoch', 'target_id', 'from_id')):
                raise JournalError('authority_handover_receipt_conflict')
            return await self.call(self.journal.follow_handover, handover, actor=actor)
        raise JournalError('invalid_transfer_action', 422)

    async def capture_failure(self, *, principal, subsystem, error_code, summary,
                              correlation_id, realm=None, context=None):
        """Best-effort typed server producer; never recursively reports failures.

        Callers supply their authenticated server binding, not request-body
        identity. An exact correlation retries the same durable append receipt.
        """
        from pa.health_journal.models import Observation
        try:
            observation = Observation(subsystem=subsystem, error_code=error_code,
                summary=summary[:1000], occurrence_key=correlation_id[:160],
                correlation_ids=[correlation_id[:1000]])
            receipt = await self.call(self.journal.append, observation, principal=principal,
                realm=realm or self.ctx.settings.primary_realm,
                key=f'server:{correlation_id}'[:160], context={**(context or {}), 'runtime_build': self.runtime_build})
            return {'accepted': True, 'receipt': receipt}
        except (JournalError, ValueError, TimeoutError, OSError, sqlite3.Error):
            return {'accepted': False, 'code': 'journal_report_failed'}


    async def eligibility_hook(self, event):
        """Shared supervisor producer, with dependency-specific recovery evidence."""
        from pa.health_journal.models import Observation, digest, sanitized
        async with self._producer:
            report = event.get('report') or {}
            if event.get('component') != 'pr_supervisor.eligibility':
                return
            dependency = report.get('dependency')
            if dependency not in {'capability_inventory', 'github_repository_observation'}:
                return
            authority = report.get('authority_instance_id') or 'configured_authority'
            repository = str(event.get('repository') or '')[:300]
            supervisor = self.ctx.services.get('pr_supervisor_store')
            watch = await self.canonical_call(supervisor.get_watch, event['watch_id']) if supervisor else None
            if not watch:
                return
            realm = watch.realm_id
            scope = digest([realm, event['component'], dependency, authority, repository])
            existing = await self.call(self.journal.producer_issues, scope)
            issues = list(event.get('issues') or [])[:8]
            failures = {(i.get('instance_id'), i.get('reason_code')) for i in issues}
            changes = [(digest([realm, i['issue_key']]), i['issue_key'], i['instance_id'], i['reason_code'], True) for i in issues]
            if report.get('evaluation_state') == 'complete' and not report.get('reason_code'):
                for old in existing:
                    if not old['active'] or (old['instance_id'], old['reason']) in failures:
                        continue
                    candidate_ok = any(c.get('instance_id') == old['instance_id']
                        and c.get('freshness') == 'fresh' and c.get('authenticated') is True
                        and not c.get('reason_code') and c.get('instance_id') in report.get('eligible', [])
                        for c in report.get('candidates', [])[:200])
                    inventory_ok = dependency == 'capability_inventory' and old['instance_id'] == authority and old['reason'] in {'authority_unreachable', 'authority_response_invalid', 'no_candidates'} and bool(report.get('candidates'))
                    if candidate_ok or inventory_ok:
                        changes.append((old['issue_key'], old['correlation_key'], old['instance_id'], old['reason'], False))
            for issue_key, correlation_key, instance_id, reason, active in changes[:16]:
                semantic = sanitized({'realm':realm, 'dependency':dependency, 'authority':authority, 'affected_instance':instance_id,
                                      'repository':repository, 'reason':reason, 'active':active})
                semantic_hash = digest(semantic)
                old = await self.call(self.journal.producer_issue, issue_key)
                if old and old['semantic_hash'] == semantic_hash:
                    continue
                generation = old['generation'] + 1 if old else 1
                body = Observation(subsystem='pr_supervisor.eligibility', error_code=reason,
                    summary=f'{dependency}: {reason}' if active else f'{dependency}: confirmed recovery of {reason}',
                    occurrence_key=issue_key, evidence=[encode(semantic), f'watch:{event["watch_id"]}'],
                    correlation_ids=[correlation_key], report_id=old['report_id'] if old else None,
                    expected='The same dependency and affected instance succeeds within the existing authorized scope.',
                    actual='failure observed' if active else 'same dependency and affected instance confirmed successful')
                receipt = await self.call(self.journal.append, body, principal='system:pr_supervisor', realm=realm,
                    key=f'eligibility:{issue_key}:{generation}', context={'session_id':watch.originating_session_id,
                        'card_id':watch.card_id, 'runtime_build':self.runtime_build})
                await self.call(self.journal.save_producer_issue, {'issue_key':issue_key, 'correlation_key':correlation_key, 'scope':scope,
                    'instance_id':instance_id, 'reason':reason, 'generation':generation, 'active':int(active),
                    'report_id':receipt['report_id'], 'semantic_hash':semantic_hash})
