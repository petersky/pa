"""Durable, revision-scoped independent verification through normal dispatch.

This owns work, never evidence: only the bound acceptance producer can certify
a result. Explicit declarations opt in; system health gates use versioned policy.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import httpx
from contextlib import contextmanager
from pathlib import Path

from pa.domain.completion import CompletionConflict
from pa.domain.models import CardLane
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
from pa.execution.reconciliation import RECONCILIATION_TERMINAL_STATES
from pa.execution.disposition import decide_card_disposition, disposition_for_merged_watch
from pa.health_journal.models import digest, encode


class VerificationStore:
    def __init__(self, path: Path):
        self.path = path
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS intents(id TEXT PRIMARY KEY, card_id TEXT NOT NULL, realm TEXT NOT NULL, packet TEXT NOT NULL, state TEXT NOT NULL, dispatch_id TEXT, diagnostic TEXT)")
            db.execute("CREATE INDEX IF NOT EXISTS verification_card ON intents(card_id,realm)")
            db.execute("CREATE TABLE IF NOT EXISTS migrations(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, plan TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS recoveries(intent TEXT NOT NULL, dependency TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY(intent,dependency))")
        self.path.chmod(0o600)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=1)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA synchronous=FULL")
            with db:
                yield db
        finally:
            db.close()

    def reserve(self, packet):
        key = digest(packet)
        with self.connection() as db:
            db.execute("INSERT OR IGNORE INTO intents VALUES(?,?,?,?,?,NULL,NULL)",
                       (key, packet['card_id'], packet['realm'], encode(packet), 'pending'))
        return self.get(key)

    def get(self, key):
        with self.connection() as db:
            row = db.execute("SELECT * FROM intents WHERE id=?", (key,)).fetchone()
        return {**dict(row), 'packet': json.loads(row['packet'])} if row else None

    def list(self, card_id=None, realm=None):
        with self.connection() as db:
            rows = db.execute("SELECT * FROM intents WHERE (? IS NULL OR card_id=?) AND (? IS NULL OR realm=?) ORDER BY rowid", (card_id, card_id, realm, realm)).fetchall()
        return [{**dict(r), 'packet': json.loads(r['packet'])} for r in rows]

    def update(self, key, state, *, dispatch_id=None, diagnostic=None):
        with self.connection() as db:
            db.execute("UPDATE intents SET state=?,dispatch_id=coalesce(?,dispatch_id),diagnostic=? WHERE id=?", (state, dispatch_id, diagnostic, key))

    def claim_admission(self, key):
        with self.connection() as db:
            return db.execute("UPDATE intents SET state='admission_pending' WHERE id=? AND state IN ('pending','blocked','admission_pending')", (key,)).rowcount == 1

    def migration(self, key, fingerprint, plan=None):
        with self.connection() as db:
            if plan is not None:
                db.execute('INSERT OR IGNORE INTO migrations VALUES(?,?,?)', (key, fingerprint, encode(plan)))
            row = db.execute('SELECT * FROM migrations WHERE id=?', (key,)).fetchone()
        if row and row['fingerprint'] != fingerprint:
            raise CompletionConflict('idempotency_conflict')
        return json.loads(row['plan']) if row else None

    def recovery(self, intent, dependency, state=None):
        with self.connection() as db:
            if state:
                db.execute('INSERT INTO recoveries VALUES(?,?,?) ON CONFLICT(intent,dependency) DO UPDATE SET state=excluded.state', (intent, dependency, state))
            row = db.execute('SELECT state FROM recoveries WHERE intent=? AND dependency=?', (intent, dependency)).fetchone()
        return row[0] if row else None


class VerificationService:
    def __init__(self, ctx, health, store):
        self.ctx, self.health, self.store = ctx, health, store
        self.task = None
        self.lock = asyncio.Lock()
        self.offset = 0
        self.last_error = None

    def packet(self, card):
        """One planner for all agent-verifiable declarations. No policy inference."""
        req = card.completion_requirement
        if not req or req.schema_version != 1 or req.verification_policy != 'independent_agent_v1':
            return None
        ledger = self.ctx.services.get('dispatch_store')
        supervisor = self.ctx.services.get('pr_supervisor_store')
        if not ledger or not supervisor:
            raise CompletionConflict('verification_services_unavailable')
        # Include actual child implementation lineage, including failed starts.
        cards = [card]
        for parent in cards:
            children = self.ctx.store.list_cards(realm_id=card.realm_id, parent_id=parent.id, limit=101)
            known = {c.id for c in cards}
            cards.extend(c for c in children if c.id not in known)
            if len(cards) > 100:
                raise CompletionConflict('verification_origin_bound_exceeded')
        jobs = self.store.list(card.id, card.realm_id)
        verifier_dispatches = {j['dispatch_id'] for j in jobs if j['dispatch_id']}
        # A lost admission reply is still identified by the normal mutation key.
        verifier_keys = {'verification:' + j['id'] for j in jobs}
        sessions = set(req.originating_session_ids) | {req.originating_session_id}
        dispatches = set(req.originating_dispatch_ids) | {req.originating_dispatch_id}
        for child in cards:
            records = ledger.list(card_id=child.id, realm_id=card.realm_id, limit=101)
            if len(records) > 100:
                raise CompletionConflict('verification_origin_bound_exceeded')
            for record in records:
                if record.dispatch_id in verifier_dispatches or record.idempotency_key in verifier_keys:
                    continue
                dispatches.add(record.dispatch_id)
                sessions.add(record.session_id)
        watches = supervisor.list_watches_for_cards({c.id for c in cards}, realm_id=card.realm_id,
                                                   include_retired=True, per_card_limit=20)
        if len(watches) > 100:
            raise CompletionConflict('verification_evidence_bound_exceeded')
        for watch in watches:
            sessions.add(watch.originating_session_id)
            dispatches.add(watch.dispatch_id)
        sessions.discard(None)
        dispatches.discard(None)
        sessions.discard('')
        dispatches.discard('')
        if len(sessions) > 100 or len(dispatches) > 100:
            raise CompletionConflict('verification_origin_bound_exceeded')
        integrated = [e for e in card.completion_evidence if e.outcome == 'integrated']
        subject = integrated[-1].subject_revision if integrated else None
        scope = None
        group_id = None
        if req.provenance == 'system_health':
            if not (req.source_reference or '').startswith('health-group:'):
                raise CompletionConflict('verification_health_binding_required')
            group_id = req.source_reference.removeprefix('health-group:')
            scope = self.health.journal.verification_scope(group_id, realms=[card.realm_id])
            group = self.health.journal.group(group_id, realms=[card.realm_id])
            if group['data'].get('card_id') != card.id:
                raise CompletionConflict('verification_health_card_mismatch')
            if group['disposition'] not in {'merged', 'awaiting_acceptance', 'deployed_verified'}:
                subject = None
            else:
                subject = group['data'].get('commit')
                # A journal assessment cannot mint integration evidence.
                if not any(w.state.get('merge_commit_sha') == subject and w.pr_url == group['data'].get('pr_url')
                           and decide_card_disposition(disposition_for_merged_watch(w), current_lane=CardLane.WAITING,
                                                       watches=[w]).applied_lane == CardLane.DONE for w in watches):
                    subject = None
            scope['scenario'] = group['data'].get('acceptance_scenario') or 'health-repair:' + group_id
        prior_receipts = [e for e in card.completion_evidence
                          if e.requirement_revision != req.revision or e.outcome == 'integrated'][-8:]
        bookkeeping = {name for name in type(card).model_fields if name == 'summary' or name.startswith('summary_')}
        card_scope = digest(card.model_dump(mode='json', exclude=bookkeeping | {
            'completion_evidence', 'updated_at', 'lane', 'lease_holder_instance',
            'lease_holder_principal', 'lease_expires_at'}))
        return {'schema_version': 1, 'policy': 'independent_agent_v1', 'card_id': card.id,
                'card_scope': card_scope,
                'realm': card.realm_id, 'requirement_revision': req.revision,
                'requirement': req.model_dump(mode='json'),
                'principal': req.verification_principal,
                'eligible_principals': sorted(req.acceptance_principals),
                'criteria': req.criteria, 'milestones': req.milestones,
                'subject': subject, 'origin_sessions': sorted(sessions),
                'origin_dispatches': sorted(dispatches), 'health_group': group_id,
                'scope': scope, 'runtime_build': self.health.runtime_build if group_id else None,
                'prior_receipts': [{'revision': e.requirement_revision, 'subject': e.subject_revision,
                    'outcome': e.outcome, 'receipt': e.idempotency_key, 'references': e.references[:8]}
                    for e in prior_receipts],
                'integration': sorted([{'watch_id': w.id, 'pr_url': w.pr_url,
                    'head': w.head_sha, 'merge': w.state.get('merge_commit_sha'),
                    'status': str(w.status)} for w in watches], key=lambda w: w['watch_id'])}

    def validate_acceptance(self, card, record, evidence):
        req = card.completion_requirement
        if not req or req.verification_policy != 'independent_agent_v1':
            return
        packet = self.packet(card)
        key = digest(packet)
        job = self.store.get(key)
        if job and record.idempotency_key == 'verification:' + key and not job['dispatch_id']:
            self.store.update(key, 'dispatched', dispatch_id=record.dispatch_id)
            job = self.store.get(key)
        if job and job['dispatch_id'] == record.dispatch_id and job['state'] == 'failed' and record.state not in TERMINAL_DISPATCH_STATES:
            self.store.update(key, 'dispatched')
            job = self.store.get(key)
        if not job or job['dispatch_id'] != record.dispatch_id or job['state'] != 'dispatched':
            raise CompletionConflict('verification_assignment_not_current')
        if record.session_id in packet['origin_sessions'] or record.dispatch_id in packet['origin_dispatches']:
            raise CompletionConflict('completion_self_acceptance_forbidden')
        if record.principal_id != packet['principal'] or not packet['subject'] or evidence.subject_revision != packet['subject']:
            raise CompletionConflict('verification_subject_or_actor_mismatch')
        refs = {'verification-intent:' + key}
        if packet['health_group']:
            refs |= {'health-group:' + packet['health_group'], 'scenario:' + packet['scope']['scenario']}
            refs |= {'instance:' + i for i in packet['scope']['instances']}
            for instance in packet['scope']['instances']:
                for kind in ('runtime-build', 'runtime-startup'):
                    prefix = kind + ':' + instance + ':'
                    if not any(r.startswith(prefix) and len(r) > len(prefix) for r in evidence.references):
                        raise CompletionConflict('verification_runtime_evidence_required')
                if instance == self.ctx.settings.instance_id:
                    build = packet['runtime_build'].get('source_revision')
                    if not build:
                        raise CompletionConflict('verification_loaded_build_unknown')
                    refs |= {'runtime-build:' + instance + ':' + build,
                             'runtime-startup:' + instance + ':' + packet['runtime_build']['startup_id']}
        if not refs <= set(evidence.references) or not (set(packet['milestones']) - {'integrated'}) <= set(evidence.milestones):
            raise CompletionConflict('verification_scope_incomplete')

    async def call(self, fn, *args, **kwargs):
        return await self.health.canonical_call(fn, *args, **kwargs)

    async def ensure(self, card):
        packet = await self.call(self.packet, card)
        if not packet:
            return None
        current = await self.call(self.store.reserve, packet)
        for old in await self.call(self.store.list, card.id, card.realm_id):
            if old['id'] == current['id'] or old['state'] in {'accepted', 'superseded'}:
                continue
            ledger = self.ctx.services['dispatch_store']
            record = ((await self.call(ledger.get, old['dispatch_id'])) if old['dispatch_id'] else
                      (await self.call(ledger.by_authority_idempotency, self.ctx.settings.instance_id, 'verification:' + old['id'])))
            if record and not old['dispatch_id']:
                await self.call(self.store.update, old['id'], 'dispatched', dispatch_id=record.dispatch_id)
                old = await self.call(self.store.get, old['id'])
            unadmitted = not old['dispatch_id'] and (old['state'] == 'pending' or old['diagnostic'] in {
                'verification_principal_unconfigured', 'waiting_for_integration_or_origin', 'repair_workflow_pending',
                'verification_realm_authority_required'})
            if unadmitted or (record and record.state in TERMINAL_DISPATCH_STATES and (
                    not record.session_id or (record.acknowledged_at and record.completion_delivery_class == 'acknowledged'
                                              and record.reconciliation_state in RECONCILIATION_TERMINAL_STATES))):
                await self.call(self.store.update, old['id'], 'superseded', diagnostic='successor:' + current['id'])
        return current

    def origins_settled(self, packet):
        for key in packet['origin_dispatches']:
            record = self.ctx.services['dispatch_store'].get(key)
            if record is None or record.state not in TERMINAL_DISPATCH_STATES:
                return False
            pre_session = (record.state in {'failed', 'cancelled'} and not record.session_id
                           and not record.prompt_acknowledged_at and not record.initial_prompt_operation)
            if pre_session:
                continue
            if record.state == 'completed' and (not record.acknowledged_at or record.completion_delivery_class != 'acknowledged'):
                return False
            if record.reconciliation_state not in RECONCILIATION_TERMINAL_STATES:
                return False
            if record.state in {'failed', 'cancelled'} and record.recoverable:
                return False
        return True

    async def process(self, card):
        req = card.completion_requirement
        if req and card.lane == CardLane.DONE and card.completion_status['accepted']:
            # Recover a lost final lane-write reply from the immutable bound
            # receipt, including after restart. This finishes that old intent;
            # it makes no claim about a later runtime or observation scope.
            for receipt in card.completion_evidence:
                if receipt.outcome != 'accepted' or receipt.requirement_revision != req.revision:
                    continue
                for ref in receipt.references:
                    if not ref.startswith('verification-intent:'):
                        continue
                    old = await self.call(self.store.get, ref.removeprefix('verification-intent:'))
                    if (old and old['card_id'] == card.id and old['realm'] == card.realm_id
                            and old['packet']['requirement_revision'] == req.revision
                            and old['packet']['subject'] == receipt.subject_revision
                            and old['dispatch_id'] == receipt.actor_dispatch_id and receipt.actor_kind == 'bound_session'):
                        await self.call(self.store.update, old['id'], 'accepted')
        if req and (req.provenance == 'system_health' or (not req.acceptance_principals and 'health-group:' in req.criteria)):
            policy = (await self.call(self.health.journal.status))['policy']
            if policy['authority_id'] != self.ctx.settings.instance_id or not policy['enabled'] or policy['paused']:
                return
        if card.lane == CardLane.DONE and card.completion_status['accepted']:
            req = card.completion_requirement
            if not req or req.provenance != 'system_health':
                return
            group_id = (req.source_reference or '').removeprefix('health-group:')
            group = await self.call(self.health.journal.group, group_id, realms=[card.realm_id])
            if group['disposition'] == 'deployed_verified':
                return
        req = card.completion_requirement
        if req and req.verification_policy == 'declared' and not req.acceptance_principals and 'health-group:' in req.criteria and not card.completion_status['accepted']:
            onboarding = 'onboarding:' + card.id + ':' + card.realm_id
            if await self.call(self.store.recovery, onboarding, req.revision):
                return
            try:
                await self.migrate(card.id, card.realm_id, 'system_health', req.revision, card.updated_at.isoformat(),
                    key='verification-onboard:' + card.id + ':' + req.revision, apply=True)
            except CompletionConflict as exc:
                if exc.code in {'explicit_requirement_preserved', 'system_requirement_provenance_unconfirmed',
                                'completion_migration_not_eligible', 'verification_health_card_mismatch'}:
                    await self.call(self.store.recovery, onboarding, req.revision, exc.code)
                return  # Unproven/explicit declarations retain their authority.
            else:
                card = await self.call(self.ctx.store.get_card, card.id, realm_id=card.realm_id)
        job = await self.ensure(card)
        if not job:
            return
        packet, key = job['packet'], job['id']
        if not job['dispatch_id']:
            records = await self.call(self.ctx.services['dispatch_store'].list, card_id=card.id, realm_id=card.realm_id, limit=101)
            recovered = next((r for r in records if r.idempotency_key == 'verification:' + key), None)
            if recovered:
                await self.call(self.store.update, key, 'dispatched', dispatch_id=recovered.dispatch_id)
                job = await self.call(self.store.get, key)
        receipts = [e for e in card.completion_evidence if e.requirement_revision == packet['requirement_revision']
                    and e.outcome == 'accepted' and e.subject_revision == packet['subject']
                    and 'verification-intent:' + key in e.references]
        if receipts and card.completion_status['accepted']:
            # Re-read canonical evidence; successful dispatch transport is not acceptance.
            await self.finish(card, job, receipts[-1])
            return
        if job['state'] == 'accepted':
            return  # Unchanged evidence never starts another verifier.
        if not packet['principal'] or packet['principal'] not in packet['eligible_principals']:
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_principal_unconfigured')
            return
        if not packet['subject'] or not (packet['origin_sessions'] or packet['origin_dispatches']):
            await self.call(self.store.update, key, 'blocked', diagnostic='waiting_for_integration_or_origin')
            return
        if not await self.call(self.origins_settled, packet):
            await self.call(self.store.update, key, 'blocked', diagnostic='repair_workflow_pending')
            return
        if job['dispatch_id']:
            record = await self.call(self.ctx.services['dispatch_store'].get, job['dispatch_id'])
            if not record:
                return  # Unknown remains owned; never a replacement dispatch.
            if record.state in TERMINAL_DISPATCH_STATES:
                await self.call(self.store.update, key, 'failed', diagnostic='verification_ended_without_acceptance')
                await self.retry_after_provider_recovery(job, record)
            elif job['state'] == 'failed':
                await self.call(self.store.update, key, 'dispatched')
            return
        # A revision/scope change cannot start a rival while an old verifier runs.
        for old in await self.call(self.store.list, card.id, card.realm_id):
            if old['id'] == key or old['state'] in {'accepted', 'superseded'}:
                continue
            if not old['dispatch_id']:
                return  # An uncertain earlier admission still owns the work.
            record = await self.call(self.ctx.services['dispatch_store'].get, old['dispatch_id'])
            if record is None or record.state not in TERMINAL_DISPATCH_STATES:
                return
        if card.realm_id != self.ctx.settings.primary_realm:
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_realm_authority_required')
            return
        from pa.prompts import PROMPTS
        prompt = PROMPTS.render("completion.verify", {'intent_id':key, 'packet':encode(packet)}).text
        actor = {'principal_id': packet['principal']}
        # Reservation above precedes the mutation; replay this exact request/key
        # after process death or a lost admission receipt.
        body = {'card_id': card.id, 'target_instance_id': self.ctx.settings.instance_id,
                'authority_instance_id': self.ctx.settings.instance_id, 'message': prompt,
                'execution_contract': {'version': 1, 'profile': 'repository', 'confirmed': True,
                    'requirements': {'repository_required': True, 'expected_deliverables': ['verification']}}}
        dependency, _ = await self.provider_dependency()
        if await self.call(self.store.recovery, key, dependency) is None:
            await self.call(self.store.recovery, key, dependency, 'baseline')
        fresh = await self.call(self.ctx.store.get_card, card.id, realm_id=card.realm_id)
        if fresh is None:
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_card_missing')
            return
        if digest(await self.call(self.packet, fresh)) != key:
            await self.ensure(fresh)
            return
        if not await self.call(self.store.claim_admission, key):
            return
        try:
            result = await self.health.local_api(actor, 'POST', '/api/fleet/dispatch', body=body, key='verification:' + key)
        except Exception as exc:
            await self.call(self.store.update, key, 'blocked', diagnostic=type(exc).__name__)
            return
        await self.call(self.store.update, key, 'dispatched', dispatch_id=result['dispatch_id'])

    async def provider_dependency(self):
        selection = self.ctx.services.get('execution_selection')
        if not selection:
            return 'unavailable', False
        candidates = await selection.local_catalog()
        rows = [c.model_dump(mode='json', exclude={'observed_at', 'catalog_version', 'health_evidence', 'capacity_available'}) for c in candidates]
        return digest(sorted(rows, key=encode)), any(c.readiness == 'ready' and c.freshness == 'fresh' for c in candidates)

    async def retry_after_provider_recovery(self, job, record):
        # Only a definitive pre-session selection failure can recover here.
        # Failed verification scenarios never create another review unchanged.
        if not (record.state == 'failed' and record.recoverable and record.error_code == 'no_compatible_execution'
                and not record.session_id and not record.prompt_acknowledged_at and not record.initial_prompt_operation):
            return
        dependency, ready = await self.provider_dependency()
        state = await self.call(self.store.recovery, job['id'], dependency)
        if not ready or state in {'baseline', 'rejected', 'confirmed'}:
            return
        key = 'verification-retry:' + job['id'] + ':' + dependency
        await self.call(self.store.recovery, job['id'], dependency, 'pending')
        try:
            result = await self.health.local_api({'principal_id': job['packet']['principal']}, 'POST',
                '/api/fleet/dispatch-jobs/' + record.dispatch_id + '/retry', body={}, key=key)
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                await self.call(self.store.recovery, job['id'], dependency, 'rejected')
            return
        except Exception:
            return  # Unknown delivery reuses the same normal retry identity.
        if result.get('dispatch_id') != record.dispatch_id:
            raise CompletionConflict('verification_retry_identity_mismatch')
        await self.call(self.store.recovery, job['id'], dependency, 'confirmed')
        await self.call(self.store.update, job['id'], 'dispatched')

    async def finish(self, card, job, receipt):
        packet = job['packet']
        actor = {'principal_id': packet['principal']}
        watches = await self.call(self.ctx.services['pr_supervisor_store'].list_watches,
                                  card_id=card.id, realm_id=card.realm_id, include_retired=True)
        if watches:
            matching = next((w for w in watches if w.state.get('merge_commit_sha') == packet['subject']), watches[0])
            decision = decide_card_disposition(disposition_for_merged_watch(matching), current_lane=card.lane,
                watches=watches, completion_requirement=card.completion_requirement,
                completion_evidence=card.completion_evidence)
            if decision.applied_lane != CardLane.DONE:
                await self.call(self.store.update, job['id'], 'blocked', diagnostic='waiting_for_current_integration')
                return
        if packet['health_group']:
            from pa.health_journal.acceptance import verify_acceptance
            from pa.health_journal.models import Assessment
            group = await self.call(self.health.journal.group, packet['health_group'], realms=[card.realm_id])
            assessment = Assessment(expected_version=group['version'], disposition='deployed_verified',
                reason='Independent canonical verification receipt recorded.', card_id=card.id,
                commit=packet['subject'], acceptance_reference=receipt.idempotency_key,
                accepted_subject_revision=packet['subject'], acceptance_scenario=packet['scope']['scenario'],
                accepted_instances=packet['scope']['instances'])
            verify_acceptance(card, group, assessment, repair_sessions=packet['origin_sessions'], repair_dispatches=packet['origin_dispatches'])
            if group['disposition'] != 'deployed_verified':
                await self.health.local_api(actor, 'PATCH', '/api/health-journal/groups/' + group['id'],
                    body=assessment.model_dump(mode='json'), key='verification-mirror:' + job['id'] + ':' + str(group['version']))
        if card.lane != CardLane.DONE:
            await self.health.local_api(actor, 'PATCH', '/api/cards/' + card.id + '?realm=' + card.realm_id,
                body={'expected_version': card.updated_at.isoformat(), 'field_intent': ['lane'], 'lane': 'done'},
                key='verification-done:' + job['id'] + ':' + card.updated_at.isoformat())
        await self.call(self.store.update, job['id'], 'accepted')

    async def migrate(self, card_id, realm, kind, revision, version, *, key, apply=False, principal=None):
        from pa.execution.verification_migration import migration_plan
        identity = {'card_id': card_id, 'realm': realm, 'kind': kind, 'revision': revision, 'version': version, 'principal': principal}
        fingerprint = digest(identity)
        plan = await self.call(self.store.migration, key, fingerprint) if apply else None
        if not plan:
            card = await self.call(self.ctx.store.get_card, card_id, realm_id=realm)
            if not card or not card.completion_requirement or card.completion_requirement.revision != revision or card.updated_at.isoformat() != version:
                raise CompletionConflict('stale_completion_requirement')
            requirement = await self.call(migration_plan, self, card, kind)
            plan = {'principal_id': principal or requirement.verification_principal,
                    'path': '/api/cards/' + card.id + '?realm=' + realm,
                    'body': {'expected_version': version, 'field_intent': ['completion_requirement'],
                             'completion_requirement': requirement.model_dump(mode='json')}}
            if not apply:
                return {'eligible': True, **identity, 'replacement': plan['body']['completion_requirement']}
            plan = await self.call(self.store.migration, key, fingerprint, plan)
        return await self.health.local_api({'principal_id': plan['principal_id']}, 'PATCH', plan['path'],
                                          body=plan['body'], key=key)

    async def cycle(self):
        if self.lock.locked():
            return
        async with self.lock:
            cards = await self.call(self.ctx.store.list_cards, realm_id=self.ctx.settings.primary_realm,
                                    limit=32, offset=self.offset)
            processed, deadline = 0, time.monotonic() + 15
            for card in cards:
                try:
                    await self.process(card)
                except Exception as exc:
                    self.last_error = getattr(exc, 'code', type(exc).__name__)
                processed += 1
                if time.monotonic() >= deadline:
                    break
            self.offset = self.offset + processed if processed < len(cards) or len(cards) == 32 else 0

    async def run(self):
        while True:
            try:
                await self.cycle()
            except Exception as exc:
                self.last_error = getattr(exc, 'code', type(exc).__name__)
            await asyncio.sleep(30)

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
