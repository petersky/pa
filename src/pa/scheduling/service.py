"""A bounded server task. No model process and no task-completion logic."""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import time
from contextlib import suppress

from fastapi import HTTPException
from starlette.requests import Request

from pa.auth.users import UserDirectory
from pa.scheduling.store import Destination, Ledger
from pa.scheduling.errors import DeliveryError

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, ctx, app, ledger: Ledger, deliver=None):
        self.ctx, self.app, self.ledger = ctx, app, ledger
        self.deliver = deliver or self.deliver_to_destination
        self.task = None
        self.last_tick = None
        self.error = None
        self.event_error = None

    async def offload(self, function, *args, **kwargs):
        runtime = self.ctx.services.get('async_runtime') if self.ctx else None
        if runtime:
            return await runtime.run_blocking('scheduling.' + function.__name__, function,
                                              *args, wait_for_completion=True, **kwargs)
        return await asyncio.to_thread(function, *args, **kwargs)

    async def cycle(self, now=None):
        real_clock = now is None
        now = time.time() if real_clock else now
        # Nonblocking process fence, held across acceptance and receipt persistence.
        with self.ledger.path.with_suffix('.runner.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            await self.offload(self.ledger.recover)
            if self.ctx is not None:
                from pa.scheduling.events import poll_events
                try:
                    await self.offload(poll_events, self.ctx, self.ledger)
                    self.event_error = None
                except Exception as exc:
                    self.event_error = type(exc).__name__
            await self.offload(self.ledger.tick, now)
            pending = await self.offload(self.ledger.pending, now, limit=4)
            # One unavailable channel must not hold unrelated destinations behind
            # a serial batch of network timeouts. Reuse bounded server workers.
            results = await asyncio.gather(*(self.deliver_item(item, now, real_clock) for item in pending), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            self.last_tick, self.error = now, None

    async def deliver_item(self, item, now, real_clock):
        claimed = await self.offload(self.ledger.claim, item['id'], now)
        if not claimed:
            return
        try:
            async with asyncio.timeout(30):
                receipt = await self.deliver(claimed)
            if not isinstance(receipt, dict) or receipt.get('accepted') is not True:
                raise DeliveryError('destination_did_not_acknowledge')
        except Exception as exc:
            # Never retain exception text (it may contain tokens or payloads).
            error = str(exc) if isinstance(exc, DeliveryError) else f'destination_failure:{type(exc).__name__}'
            if isinstance(exc, HTTPException):
                error += f':http_{exc.status_code}'
            await self.offload(self.ledger.finish, item['id'], now, error=error)
        else:
            await self.offload(self.ledger.finish, item['id'], time.time() if real_clock else now, accepted=True)

    async def run(self):
        while True:
            try:
                await self.cycle()
            except Exception as exc:
                self.error = type(exc).__name__
                logger.error('Scheduler cycle failed: %s', self.error)
            await asyncio.sleep(1)

    async def start(self):
        self.task = asyncio.create_task(self.run(), name='schedule-delivery')

    async def stop(self):
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task

    def health(self):
        return {'running': bool(self.task and not self.task.done()), 'last_tick':self.last_tick,
                'event_error':self.event_error, 'overdue': self.last_tick is None or time.time() - self.last_tick > 60, 'error':self.error}

    async def deliver_to_destination(self, item):
        destination = Destination.model_validate_json(item['destination'])
        envelope = json.loads(item['envelope'])
        owner = item['owner']
        # Re-read the owner's current authorization; no cached administrator token.
        users = await self.offload(UserDirectory, self.ctx.settings.data_dir)
        user = users.get(owner.removeprefix('user:'))
        if not user:
            raise DeliveryError('schedule_owner_unavailable')
        if destination.kind == 'inbox':
            if destination.target != owner:
                raise DeliveryError('inbox_target_must_be_owner')
            from pa.domain.notifications import NotificationCreate
            service = self.ctx.require_service('notifications')
            notice = NotificationCreate(id=item['id'], visibility='principal', principal_id=owner,
                title='Scheduled notification', body=json.dumps(envelope), deduplication_key='schedule:' + item['id'])
            await self.offload(service.create, notice, principal_id=owner)
            return {'accepted':True}
        if destination.kind == 'agent_session':
            from pa.modules.agent_chat import PromptBody, session_prompt
            from pa.modules.fleet import DispatchFollowupBody, prompt_dispatch_session
            manager = self.ctx.require_service('instance_agent')
            session = await self.offload(manager.store.get_session, destination.target)
            if not session:
                raise DeliveryError('target_session_missing')
            # Use the same authorization and durable acceptance paths as ordinary input.
            request = Request({'type':'http', 'method':'POST', 'path':'/api/schedules',
                               'headers':[], 'app':self.app, 'query_string':b''})
            request.state.user = user
            request.state.principal_id = owner
            request.state.user_authenticated = True
            request.state.instance_authenticated = False
            action = 'interrupt' if destination.interrupt else 'append'
            message = json.dumps(envelope, sort_keys=True)
            if session.dispatch_id:
                return await prompt_dispatch_session(request, session.dispatch_id,
                    DispatchFollowupBody(message=message, action=action, idempotency_key=item['id']))
            return await session_prompt(request, destination.target,
                PromptBody(message=message, action=action, client_prompt_id=item['id']))
        if destination.kind == 'messaging':
            # Target is an existing intake envelope, which fixes the configured
            # channel and conversation. Never infer an alternative conversation.
            from pa.intake.models import Channel, IntakeDisposition
            service = self.ctx.require_service('intake_service')
            source = await self.offload(service.get, destination.target)
            if source is None:
                raise DeliveryError('messaging_target_missing')
            membership = self.ctx.services.get('membership')
            from pa.domain.models import RealmRole
            if user.role != 'admin' and not (membership and membership.has_role(source.realm_id, owner, min_role=RealmRole.EDITOR)):
                raise DeliveryError('messaging_target_access_denied')
            if source.security.disposition in {IntakeDisposition.REJECTED, IntakeDisposition.REDACTED} or not source.reply_capabilities.can_reply:
                raise DeliveryError('messaging_target_cannot_receive')
            text = json.dumps(envelope, sort_keys=True)
            if source.channel == Channel.TELEGRAM:
                if len(text) > 4096:
                    raise DeliveryError('telegram_envelope_too_large')
                receipt = await self.offload(service.transport.send_telegram,
                    conversation_id=source.thread.conversation_id, thread_id=source.thread.thread_id,
                    reply_to_message_id=None, text=text)
            elif source.channel == Channel.DISCORD:
                if len(text) > 2000:
                    raise DeliveryError('discord_envelope_too_large')
                receipt = await self.offload(service.transport.send_discord,
                    conversation_id=source.thread.conversation_id, reply_to_message_id=None,
                    text=text, nonce=item['id'])
            else:
                raise DeliveryError('unsupported_messaging_channel')
            return {'accepted':bool(receipt.get('provider_message_id'))}
        # Messaging integrations and webhooks register a destination-owned adapter;
        # absence is a visible retained failure, never an alternate recipient.
        if destination.kind == 'webhook':
            from pa.scheduling.webhook import send_webhook
            return await self.offload(send_webhook, destination.target, envelope, item['id'])
        adapter = self.ctx.services.get('schedule_destination_' + destination.kind)
        if adapter is None:
            raise DeliveryError('destination_adapter_unavailable')
        return await adapter(owner=owner, destination=destination, envelope=envelope,
                             idempotency_key=item['id'])
