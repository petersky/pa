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
from inspect import getattr_static
from contextlib import contextmanager
from pathlib import Path
from datetime import UTC, datetime
from urllib.parse import quote, urlencode

from pa.domain.completion import CompletionConflict
from pa.domain.models import CardLane
from pa.execution.dispatch import DispatchStore, TERMINAL_DISPATCH_STATES
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
            db.execute("CREATE TABLE IF NOT EXISTS authority_fences(card_id TEXT NOT NULL, realm TEXT NOT NULL, revision TEXT NOT NULL, operation_key TEXT NOT NULL, PRIMARY KEY(card_id,realm,revision))")
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
            return db.execute("""UPDATE intents SET state='admission_pending' WHERE id=?
                AND state IN ('pending','blocked','admission_pending') AND NOT EXISTS (
                    SELECT 1 FROM authority_fences f WHERE f.card_id=intents.card_id AND f.realm=intents.realm
                    AND f.revision=json_extract(intents.packet,'$.requirement_revision'))""", (key,)).rowcount == 1

    def authority_fenced(self, card_id, realm, revision):
        return self.authority_fence(card_id, realm, revision) is not None

    def authority_fence(self, card_id, realm, revision):
        with self.connection() as db:
            row = db.execute('SELECT * FROM authority_fences WHERE card_id=? AND realm=? AND revision=?', (card_id, realm, revision)).fetchone()
        return dict(row) if row else None

    def claim_retry(self, key):
        with self.connection() as db:
            return db.execute("""UPDATE intents SET state='admission_pending' WHERE id=?
                AND state IN ('failed','dispatched','admission_pending') AND NOT EXISTS (
                    SELECT 1 FROM authority_fences f WHERE f.card_id=intents.card_id AND f.realm=intents.realm
                    AND f.revision=json_extract(intents.packet,'$.requirement_revision'))""", (key,)).rowcount == 1

    def pending_recoveries(self, key):
        with self.connection() as db:
            return [r[0] for r in db.execute("SELECT dependency FROM recoveries WHERE intent=? AND state='pending'", (key,))]

    def fence_authority(self, card_id, realm, revision, key, *, require_idle, settled):
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if require_idle:
                rows = db.execute('SELECT * FROM intents WHERE card_id=? AND realm=?', (card_id, realm)).fetchall()
                for row in rows:
                    if row['state'] == 'admission_pending' or db.execute("SELECT 1 FROM recoveries WHERE intent=? AND state='pending' LIMIT 1", (row['id'],)).fetchone():
                        raise CompletionConflict('verification_authority_has_pending_work')
                    unadmitted = not row['dispatch_id'] and (row['state'] in {'pending','superseded'} or row['diagnostic'] in {
                        'verification_principal_unconfigured','waiting_for_integration_or_origin','repair_workflow_pending',
                        'verification_realm_authority_required'})
                    if row['id'] not in settled and not unadmitted:
                        raise CompletionConflict('verification_authority_has_pending_work')
            db.execute('INSERT OR IGNORE INTO authority_fences VALUES(?,?,?,?)', (card_id, realm, revision, key))

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
        self.lock = asyncio.Lock()
        self.offset = 0
        self.last_error = None

    def authority_id(self, requirement):
        if requirement.provenance == 'system_health':
            return self.health.journal.status()['policy']['authority_id'] or None
        return requirement.verification_authority_instance_id

    def owns_authority(self, card):
        req = card.completion_requirement
        if not req or req.verification_paused:
            return False
        if req.provenance == 'system_health':
            policy = self.health.journal.status()['policy']
            owned = policy['authority_id'] == self.ctx.settings.instance_id and policy['enabled'] and not policy['paused']
        else:
            owned = req.verification_authority_instance_id == self.ctx.settings.instance_id
        return owned and not self.store.authority_fenced(card.id, card.realm_id, req.revision)

    def fence_requirement_edit(self, card, data, key):
        """Owner-only handoff; a durable revocation precedes canonical mutation.

        Admission claims and this fence serialize in the same local transaction.
        A failed/unknown canonical edit leaves the old revision safely fenced;
        the normal operation receipt and exact request own its recovery.
        """
        req, replacement = card.completion_requirement, data.completion_requirement
        if not req or req.verification_policy != 'independent_agent_v1' or not replacement or req == replacement:
            return
        authority = self.authority_id(req) or card.created_by_instance
        if authority != self.ctx.settings.instance_id:
            raise CompletionConflict('verification_authority_remote')
        if data.expected_version != card.updated_at or 'completion_requirement' not in (data.field_intent or []):
            raise CompletionConflict('stale_completion_requirement')
        replacement_authority = self.authority_id(replacement) or authority
        settled = set()
        ledger = self.ctx.services['dispatch_store']
        for job in self.store.list(card.id, card.realm_id):
            record = ledger.get(job['dispatch_id']) if job['dispatch_id'] else ledger.by_authority_idempotency(
                self.ctx.settings.instance_id, 'verification:' + job['id'])
            if record and record.state in TERMINAL_DISPATCH_STATES:
                from pa.execution.followup import dispatch_followup_pending
                if dispatch_followup_pending(record):
                    continue
                for dependency in self.store.pending_recoveries(job['id']):
                    if record.control_operations.get('verification-retry:' + job['id'] + ':' + dependency) == 'retry':
                        self.store.recovery(job['id'], dependency, 'confirmed')
                pre_session = not record.session_id and not record.initial_prompt_operation and not record.prompt_acknowledged_at
                manager = self.ctx.services.get('instance_agent')
                session = self.ctx.store.get_session(record.session_id) if record.session_id else None
                runtime = manager.get(record.session_id) if manager and record.session_id else None
                closed = bool(session and session.status == 'closed' and (runtime is None or getattr(runtime,'_closed',False)))
                if (pre_session or closed and record.acknowledged_at and record.completion_delivery_class == 'acknowledged'
                        and record.reconciliation_state in RECONCILIATION_TERMINAL_STATES
                        and (record.state == 'completed' or not record.recoverable)):
                    settled.add(job['id'])
        self.store.fence_authority(card.id, card.realm_id, req.revision, key,
            require_idle=replacement_authority != authority, settled=settled)

    def packet(self, card):
        """One planner for all agent-verifiable declarations. No policy inference."""
        req = card.completion_requirement
        if not req or req.schema_version != 1 or req.verification_policy != 'independent_agent_v1':
            return None
        ledger = self.ctx.services.get('dispatch_store')
        supervisor = self.ctx.services.get('pr_supervisor_store')
        if not ledger or not supervisor:
            raise CompletionConflict('verification_services_unavailable')
        origin_get = getattr(ledger, 'verification_origin', None)
        if not callable(origin_get):
            origin_get = ledger.get
        origin_list = getattr(ledger, 'verification_card_origins', None)
        jobs = self.store.list(card.id, card.realm_id)
        verifier_dispatches = {j['dispatch_id'] for j in jobs if j['dispatch_id']}
        # A lost admission reply is still identified by the normal mutation key.
        verifier_keys = {'verification:' + j['id'] for j in jobs}
        sessions = set(req.originating_session_ids) | {req.originating_session_id}
        dispatches = set(req.originating_dispatch_ids) | {req.originating_dispatch_id}
        # Parentage also represents independent conversation issues. Only the
        # exact card, declared origins and explicit repair/PR provenance count.
        records = (origin_list(card_id=card.id, realm_id=card.realm_id, limit=101)
                   if callable(origin_list) else
                   ledger.list(card_id=card.id, realm_id=card.realm_id, limit=101))
        if len(records) > 100:
            raise CompletionConflict('verification_origin_bound_exceeded')
        for record in records:
            if record.dispatch_id in verifier_dispatches or record.idempotency_key in verifier_keys:
                continue
            dispatches.add(record.dispatch_id)
            sessions.add(record.session_id)
        watches = supervisor.list_watches_for_cards({card.id}, realm_id=card.realm_id,
                                                   include_retired=True, per_card_limit=20)
        integrated = [e for e in card.completion_evidence if e.outcome == 'integrated']
        subject = integrated[-1].subject_revision if integrated else None
        scope = None
        group_id = None
        pending_controls = []
        if req.provenance == 'system_health':
            if not (req.source_reference or '').startswith('health-group:'):
                raise CompletionConflict('verification_health_binding_required')
            group_id = req.source_reference.removeprefix('health-group:')
            scope = self.health.journal.verification_scope(group_id, realms=[card.realm_id])
            group = self.health.journal.group(group_id, realms=[card.realm_id])
            if group['data'].get('card_id') != card.id:
                raise CompletionConflict('verification_health_card_mismatch')
            dispatches.update(self.health.journal.repair_dispatches(group_id))
            pr_url = group['data'].get('pr_url')
            if pr_url:
                from urllib.parse import urlsplit
                parts = urlsplit(pr_url).path.strip('/').split('/')
                if len(parts) != 4 or parts[2] != 'pull':
                    raise CompletionConflict('verification_repair_pr_invalid')
                linked = supervisor.find_watches('/'.join(parts[:2]), int(parts[3]))
                for watch in linked:
                    if watch.realm_id == card.realm_id and watch.pr_url == pr_url and watch.id not in {w.id for w in watches}:
                        watches.append(watch)
            # Retain previously proved actual writers when the repair advances
            # to another source PR. Generic descendant snapshots are not proof.
            for old in jobs:
                prior = old['packet']
                if prior.get('origin_provenance') == 'explicit-repair-v1' and prior.get('health_group') == group_id:
                    sessions.update(prior['origin_sessions'])
                    dispatches.update(prior['origin_dispatches'])
            for action in self.health.journal.repair_actions(group_id, expected_version=group['version'], realms=[card.realm_id]):
                result = action['result']
                if result.get('retry_pending'):
                    record = origin_get(result.get('dispatch_id'))
                    if record is None or record.control_operations.get('health-start_retry:' + action['id']) != 'retry':
                        pending_controls.append(action['id'])
            if group['disposition'] not in {'merged', 'awaiting_acceptance', 'deployed_verified'}:
                subject = None
            else:
                subject = group['data'].get('commit')
                # A journal assessment cannot mint integration evidence.
                if not any(w.provenance_version >= 1 and w.originating_session_id and w.dispatch_id
                           and w.state.get('merge_commit_sha') == subject and w.pr_url == group['data'].get('pr_url')
                           and decide_card_disposition(disposition_for_merged_watch(w), current_lane=CardLane.WAITING,
                                                       watches=[w]).applied_lane == CardLane.DONE for w in watches):
                    subject = None
            scope['scenario'] = group['data'].get('acceptance_scenario') or 'health-repair:' + group_id
        if len(watches) > 100:
            raise CompletionConflict('verification_evidence_bound_exceeded')
        for watch in watches:
            sessions.add(watch.originating_session_id)
            dispatches.add(watch.dispatch_id)
            record = origin_get(watch.dispatch_id) if watch.dispatch_id else (
                ledger.by_session(watch.originating_session_id) if watch.originating_session_id else None)
            if record:
                if ((watch.card_id and record.card_id != watch.card_id)
                        or (watch.originating_session_id and record.session_id and record.session_id != watch.originating_session_id)):
                    raise CompletionConflict('verification_watch_origin_mismatch')
                dispatches.add(record.dispatch_id)
        for key in dispatches - {None, ''}:
            record = origin_get(key)
            if record:
                if record.realm_id != card.realm_id:
                    raise CompletionConflict('verification_origin_realm_mismatch')
                sessions.add(record.session_id)
        sessions.difference_update({None, ''})
        dispatches.difference_update({None, ''})
        if len(sessions) > 100 or len(dispatches) > 100:
            raise CompletionConflict('verification_origin_bound_exceeded')
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
                'origin_provenance': 'explicit-repair-v1',
                'origin_dispatches': sorted(dispatches), 'health_group': group_id,
                'pending_repair_controls': sorted(pending_controls),
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
        if not self.owns_authority(card) or record.authority_instance_id != self.ctx.settings.instance_id:
            raise CompletionConflict('verification_authority_not_current')
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
                    build = packet['runtime_build'].get('source_revision') or packet['runtime_build'].get('package_identity')
                    if not build:
                        raise CompletionConflict('verification_loaded_build_unknown')
                    refs |= {'runtime-build:' + instance + ':' + build,
                             'runtime-startup:' + instance + ':' + packet['runtime_build']['startup_id']}
        if not refs <= set(evidence.references) or not (set(packet['milestones']) - {'integrated'}) <= set(evidence.milestones):
            raise CompletionConflict('verification_scope_incomplete')

    async def call(self, fn, *args, **kwargs):
        return await self.health.canonical_call(fn, *args, **kwargs)

    def job_controls_settled(self, job, record):
        for dependency in self.store.pending_recoveries(job['id']):
            if record.control_operations.get('verification-retry:' + job['id'] + ':' + dependency) != 'retry':
                return False
            self.store.recovery(job['id'], dependency, 'confirmed')
        return True

    async def absent_admission_head(self, job):
        """Consume supported receipt recovery; never infer absence or resend."""
        key = 'verification:' + job['id']
        query = urlencode({'realm': job['realm'], 'owner': 'dispatch', 'operation': 'dispatch.create'})
        try:
            async with asyncio.timeout(2):
                receipt = await self.health.local_api({'principal_id': job['packet']['principal']},
                    'GET', '/api/operations/' + quote(key, safe='') + '?' + query)
        except Exception:
            return None  # Unavailable/pending evidence retains the old owner.
        if (isinstance(receipt, dict) and receipt.get('idempotency_key') == key and receipt.get('owner') == 'dispatch'
                and receipt.get('realm_id') == job['realm'] and receipt.get('status') == 'not_found'
                and all(receipt.get(field) is False for field in ('durable', 'accepted', 'committed'))
                and receipt.get('effect') == 'none' and receipt.get('recovery_state') == 'verified_absent_at_head'
                and isinstance(receipt.get('lookup_head'), str) and receipt['lookup_head']):
            return receipt['lookup_head']
        return None

    async def ensure(self, card):
        packet = await self.call(self.packet, card, diagnostic_phase='verification_packet')
        if not packet:
            return None
        current = await self.call(self.store.reserve, packet, diagnostic_phase='verification_intent_write')
        checked_absence = False
        for old in await self.call(self.store.list, card.id, card.realm_id, diagnostic_phase='verification_intent_read'):
            if old['id'] == current['id'] or old['state'] in {'accepted', 'superseded'}:
                continue
            ledger = self.ctx.services['dispatch_store']
            record = ((await self.call(ledger.get, old['dispatch_id'], diagnostic_phase='verification_dispatch_read')) if old['dispatch_id'] else
                      (await self.call(ledger.by_authority_idempotency, self.ctx.settings.instance_id, 'verification:' + old['id'], diagnostic_phase='verification_dispatch_read')))
            if record and not old['dispatch_id']:
                await self.call(self.store.update, old['id'], 'dispatched', dispatch_id=record.dispatch_id, diagnostic_phase='verification_intent_write')
                old = await self.call(self.store.get, old['id'], diagnostic_phase='verification_intent_read')
            unadmitted = not old['dispatch_id'] and (old['state'] == 'pending' or old['diagnostic'] in {
                'verification_principal_unconfigured', 'waiting_for_integration_or_origin', 'repair_workflow_pending',
                'verification_realm_authority_required'})
            if (not record and not old['dispatch_id'] and old['state'] == 'blocked'
                    and old['diagnostic'] == 'HTTPStatusError' and not checked_absence):
                # A superseded scope may have failed admission before a dispatch
                # existed. Missing local rows alone cannot release that owner.
                # Bound receipt reads to one per cycle and require reconciled
                # non-commit proof at the current head from the public API.
                checked_absence = True
                head = await self.absent_admission_head(old)
                if head:
                    record = await self.call(ledger.by_authority_idempotency,
                        self.ctx.settings.instance_id, 'verification:' + old['id'], diagnostic_phase='verification_dispatch_read')
                    if record:
                        await self.call(self.store.update, old['id'], 'dispatched', dispatch_id=record.dispatch_id, diagnostic_phase='verification_intent_write')
                    else:
                        await self.call(self.store.recovery, old['id'], 'admission-absent:' + head, 'confirmed', diagnostic_phase='verification_recovery_write')
                        unadmitted = True
            settled = record and record.state in TERMINAL_DISPATCH_STATES and (
                    not record.session_id or (record.acknowledged_at and record.completion_delivery_class == 'acknowledged'
                                              and record.reconciliation_state in RECONCILIATION_TERMINAL_STATES))
            if settled:
                settled = await self.call(self.job_controls_settled, old, record, diagnostic_phase='verification_controls') and (await self.origin_readiness(record))['ready']
            if unadmitted or settled:
                await self.call(self.store.update, old['id'], 'superseded', diagnostic='successor:' + current['id'], diagnostic_phase='verification_intent_write')
        return current

    def origins_settled(self, packet):
        if packet['pending_repair_controls']:
            return False
        ledger = self.ctx.services['dispatch_store']
        read_settlement = getattr(ledger, 'verification_settlement', None)
        if isinstance(ledger, DispatchStore) and (
                getattr_static(ledger, 'get') is not DispatchStore.get
                or getattr_static(ledger, '_snapshot') is not getattr_static(DispatchStore, '_snapshot')):
            # Custom getters/converters keep their original full-read contract.
            read_settlement = None
        projected = callable(read_settlement)
        for key in packet['origin_dispatches']:
            record = read_settlement(key) if projected else ledger.get(key)
            if record is None or record.state not in TERMINAL_DISPATCH_STATES:
                return False
            from pa.execution.followup import dispatch_followup_pending
            if (record.followup_pending if projected else dispatch_followup_pending(record)):
                return False
            pre_session = (record.state in {'failed', 'cancelled'} and not record.session_id
                           and not (record.prompt_acknowledged if projected else record.prompt_acknowledged_at)
                           and not (record.initial_prompt_present if projected else record.initial_prompt_operation))
            if pre_session:
                continue
            if record.state == 'completed' and (not (record.completion_acknowledged if projected else record.acknowledged_at)
                                                or record.completion_delivery_class != 'acknowledged'):
                return False
            if record.reconciliation_state not in RECONCILIATION_TERMINAL_STATES:
                return False
            if record.state in {'failed', 'cancelled'} and record.recoverable:
                return False
        return True

    async def origin_readiness(self, record):
        """Read the target's existing lifecycle policy without closing or prompting."""
        identity = {'dispatch_id':record.dispatch_id, 'session_id':record.session_id,
                    'card_id':record.card_id, 'realm':record.realm_id, 'instance_id':self.ctx.settings.instance_id}
        if record.target_instance_id != self.ctx.settings.instance_id:
            return {**identity, 'ready':False, 'reason':'origin_owner_mismatch'}
        probe = {'origin_dispatches':[record.dispatch_id], 'pending_repair_controls':[]}
        if not await self.call(self.origins_settled, probe, diagnostic_phase='verification_origins'):
            return {**identity, 'ready':False, 'reason':'origin_dispatch_pending'}
        if not record.session_id:
            return {**identity, 'ready':True, 'reason':'pre_session_terminal'}
        manager = self.ctx.services.get('instance_agent')
        lifecycle = self.ctx.services.get('session_lifecycle')
        session = await self.call(self.ctx.store.get_session, record.session_id, diagnostic_phase='verification_session_read')
        if not manager or not lifecycle or not session or session.realm_id != record.realm_id:
            return {**identity, 'ready':False, 'reason':'origin_lifecycle_unknown'}
        if record.session_id in getattr(manager, '_admitting_sessions', set()):
            return {**identity, 'ready':False, 'reason':'origin_session_admitting'}
        runtime = manager.get(record.session_id)
        if session.status == 'closed' and runtime and not getattr(runtime, '_closed', False):
            return {**identity, 'ready':False, 'reason':'origin_runtime_still_owned'}
        watches = await self.call(self.ctx.services['pr_supervisor_store'].lifecycle_watch_links,
                                  diagnostic_phase='watch_list')
        leases = await self.call(manager.workspace_manager.list, card_id=record.card_id, diagnostic_phase='verification_workspace_read')
        latest = await self.call(self.ctx.services['dispatch_store'].by_session, record.session_id, diagnostic_phase='verification_dispatch_read')
        records = [record] + ([latest] if latest and latest.dispatch_id != record.dispatch_id else [])
        async with asyncio.timeout(5):
            decision, reason = await lifecycle._decision(session, sessions=[session], dispatches=records,
                watches=watches, leases=leases, now=datetime.now(UTC))
        return {**identity, 'ready':decision in {'close','retire'} or reason == 'already_closed', 'reason':reason}

    async def origin_sessions_settled(self, packet):
        observed_sessions = set()
        for key in packet['origin_dispatches']:
            record = await self.call(self.ctx.services['dispatch_store'].get, key, diagnostic_phase='verification_dispatch_read')
            if not record:
                return False
            if record.session_id:
                observed_sessions.add(record.session_id)
            if record.target_instance_id == self.ctx.settings.instance_id:
                status = await self.origin_readiness(record)
            else:
                try:
                    status = await self.health.peer(record.target_instance_id, 'GET', '/verification-readiness/' + record.dispatch_id)
                except Exception:
                    return False
            if (status.get('ready') is not True or status.get('dispatch_id') != record.dispatch_id
                    or status.get('session_id') != record.session_id or status.get('realm') != packet['realm']
                    or status.get('card_id') != record.card_id or status.get('instance_id') != record.target_instance_id):
                return False
        return set(packet['origin_sessions']) <= observed_sessions

    async def process(self, card):
        req = card.completion_requirement
        if req and req.verification_policy == 'independent_agent_v1' and not await self.call(self.owns_authority, card, diagnostic_phase='verification_authority_read'):
            return
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
                    old = await self.call(self.store.get, ref.removeprefix('verification-intent:'), diagnostic_phase='verification_intent_read')
                    if (old and old['card_id'] == card.id and old['realm'] == card.realm_id
                            and old['packet']['requirement_revision'] == req.revision
                            and old['packet']['subject'] == receipt.subject_revision
                            and old['dispatch_id'] == receipt.actor_dispatch_id and receipt.actor_kind == 'bound_session'):
                        await self.call(self.store.update, old['id'], 'accepted', diagnostic_phase='verification_intent_write')
        if req and (req.provenance == 'system_health' or (not req.acceptance_principals and 'health-group:' in req.criteria)):
            policy = (await self.call(self.health.journal.status, diagnostic_phase='verification_health_read'))['policy']
            if policy['authority_id'] != self.ctx.settings.instance_id or not policy['enabled'] or policy['paused']:
                return
        if card.lane == CardLane.DONE and card.completion_status['accepted']:
            req = card.completion_requirement
            if not req or req.provenance != 'system_health':
                return
            group_id = (req.source_reference or '').removeprefix('health-group:')
            group = await self.call(self.health.journal.group, group_id, realms=[card.realm_id], diagnostic_phase='verification_health_read')
            if group['disposition'] == 'deployed_verified':
                return
        req = card.completion_requirement
        if req and req.verification_policy == 'declared' and not req.acceptance_principals and 'health-group:' in req.criteria and not card.completion_status['accepted']:
            onboarding = 'onboarding:' + card.id + ':' + card.realm_id
            if await self.call(self.store.recovery, onboarding, req.revision, diagnostic_phase='verification_recovery_read'):
                return
            try:
                await self.migrate(card.id, card.realm_id, 'system_health', req.revision, card.updated_at.isoformat(),
                    key='verification-onboard:' + card.id + ':' + req.revision, apply=True)
            except CompletionConflict as exc:
                if exc.code in {'explicit_requirement_preserved', 'system_requirement_provenance_unconfirmed',
                                'completion_migration_not_eligible', 'verification_health_card_mismatch'}:
                    await self.call(self.store.recovery, onboarding, req.revision, exc.code, diagnostic_phase='verification_recovery_write')
                return  # Unproven/explicit declarations retain their authority.
            else:
                card = await self.call(self.ctx.store.get_card, card.id, realm_id=card.realm_id, diagnostic_phase='verification_card_read')
        job = await self.ensure(card)
        if not job:
            return
        packet, key = job['packet'], job['id']
        if not job['dispatch_id']:
            records = await self.call(self.ctx.services['dispatch_store'].list, card_id=card.id, realm_id=card.realm_id, limit=101, diagnostic_phase='verification_dispatch_read')
            recovered = next((r for r in records if r.idempotency_key == 'verification:' + key), None)
            if recovered:
                await self.call(self.store.update, key, 'dispatched', dispatch_id=recovered.dispatch_id, diagnostic_phase='verification_intent_write')
                job = await self.call(self.store.get, key, diagnostic_phase='verification_intent_read')
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
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_principal_unconfigured', diagnostic_phase='verification_intent_write')
            return
        if not packet['subject'] or not (packet['origin_sessions'] or packet['origin_dispatches']):
            await self.call(self.store.update, key, 'blocked', diagnostic='waiting_for_integration_or_origin', diagnostic_phase='verification_intent_write')
            return
        if not await self.call(self.origins_settled, packet, diagnostic_phase='verification_origins') or not await self.origin_sessions_settled(packet):
            await self.call(self.store.update, key, 'blocked', diagnostic='repair_workflow_pending', diagnostic_phase='verification_intent_write')
            return
        if job['dispatch_id']:
            record = await self.call(self.ctx.services['dispatch_store'].get, job['dispatch_id'], diagnostic_phase='verification_dispatch_read')
            if not record:
                return  # Unknown remains owned; never a replacement dispatch.
            if record.state in TERMINAL_DISPATCH_STATES:
                if record.state == 'completed' and not (await self.origin_readiness(record))['ready']:
                    return
                await self.call(self.store.update, key, 'failed', diagnostic='verification_ended_without_acceptance', diagnostic_phase='verification_intent_write')
                await self.retry_after_provider_recovery(job, record)
            elif job['state'] == 'failed':
                await self.call(self.store.update, key, 'dispatched', diagnostic_phase='verification_intent_write')
            return
        # A revision/scope change cannot start a rival while an old verifier runs.
        for old in await self.call(self.store.list, card.id, card.realm_id, diagnostic_phase='verification_intent_read'):
            if old['id'] == key:
                continue
            if not old['dispatch_id']:
                if old['state'] in {'accepted','superseded'}:
                    continue
                return  # An uncertain earlier admission still owns the work.
            record = await self.call(self.ctx.services['dispatch_store'].get, old['dispatch_id'], diagnostic_phase='verification_dispatch_read')
            if record is None or record.state not in TERMINAL_DISPATCH_STATES:
                return
            if not await self.call(self.job_controls_settled, old, record, diagnostic_phase='verification_controls'):
                return
            if not (await self.origin_readiness(record))['ready']:
                return
        if card.realm_id != self.ctx.settings.primary_realm:
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_realm_authority_required', diagnostic_phase='verification_intent_write')
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
        if await self.call(self.store.recovery, key, dependency, diagnostic_phase='verification_recovery_read') is None:
            await self.call(self.store.recovery, key, dependency, 'baseline', diagnostic_phase='verification_recovery_write')
        fresh = await self.call(self.ctx.store.get_card, card.id, realm_id=card.realm_id, diagnostic_phase='verification_card_read')
        if fresh is None:
            await self.call(self.store.update, key, 'blocked', diagnostic='verification_card_missing', diagnostic_phase='verification_intent_write')
            return
        if digest(await self.call(self.packet, fresh, diagnostic_phase='verification_packet')) != key:
            await self.ensure(fresh)
            return
        if not await self.call(self.owns_authority, fresh, diagnostic_phase='verification_authority_read'):
            return
        if not await self.call(self.store.claim_admission, key, diagnostic_phase='verification_intent_write'):
            return
        try:
            result = await self.health.local_api(actor, 'POST', '/api/fleet/dispatch', body=body, key='verification:' + key)
        except Exception as exc:
            await self.call(self.store.update, key, 'blocked', diagnostic=type(exc).__name__, diagnostic_phase='verification_intent_write')
            return
        await self.call(self.store.update, key, 'dispatched', dispatch_id=result['dispatch_id'], diagnostic_phase='verification_intent_write')

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
        current = await self.call(self.ctx.store.get_card, job['card_id'], realm_id=job['realm'], diagnostic_phase='verification_card_read')
        if not current or not await self.call(self.owns_authority, current, diagnostic_phase='verification_authority_read') or current.completion_requirement.revision != job['packet']['requirement_revision']:
            return
        dependency, ready = await self.provider_dependency()
        state = await self.call(self.store.recovery, job['id'], dependency, diagnostic_phase='verification_recovery_read')
        if not ready or state in {'baseline', 'rejected', 'confirmed'}:
            return
        key = 'verification-retry:' + job['id'] + ':' + dependency
        if not await self.call(self.store.claim_retry, job['id'], diagnostic_phase='verification_intent_write'):
            return
        await self.call(self.store.recovery, job['id'], dependency, 'pending', diagnostic_phase='verification_recovery_write')
        try:
            result = await self.health.local_api({'principal_id': job['packet']['principal']}, 'POST',
                '/api/fleet/dispatch-jobs/' + record.dispatch_id + '/retry', body={}, key=key)
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                await self.call(self.store.recovery, job['id'], dependency, 'rejected', diagnostic_phase='verification_recovery_write')
                await self.call(self.store.update, job['id'], 'failed', diagnostic_phase='verification_intent_write')
            return
        except Exception:
            return  # Unknown delivery reuses the same normal retry identity.
        if result.get('dispatch_id') != record.dispatch_id:
            raise CompletionConflict('verification_retry_identity_mismatch')
        await self.call(self.store.recovery, job['id'], dependency, 'confirmed', diagnostic_phase='verification_recovery_write')
        await self.call(self.store.update, job['id'], 'dispatched', diagnostic_phase='verification_intent_write')

    async def finish(self, card, job, receipt):
        packet = job['packet']
        actor = {'principal_id': packet['principal']}
        watches = await self.call(self.ctx.services['pr_supervisor_store'].list_watches,
                                  card_id=card.id, realm_id=card.realm_id, include_retired=True, diagnostic_phase='verification_watch_read')
        if watches:
            matching = next((w for w in watches if w.state.get('merge_commit_sha') == packet['subject']), watches[0])
            decision = decide_card_disposition(disposition_for_merged_watch(matching), current_lane=card.lane,
                watches=watches, completion_requirement=card.completion_requirement,
                completion_evidence=card.completion_evidence)
            if decision.applied_lane != CardLane.DONE:
                await self.call(self.store.update, job['id'], 'blocked', diagnostic='waiting_for_current_integration', diagnostic_phase='verification_intent_write')
                return
        if packet['health_group']:
            from pa.health_journal.acceptance import verify_acceptance
            from pa.health_journal.models import Assessment
            group = await self.call(self.health.journal.group, packet['health_group'], realms=[card.realm_id], diagnostic_phase='verification_health_read')
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
        await self.call(self.store.update, job['id'], 'accepted', diagnostic_phase='verification_intent_write')

    async def migrate(self, card_id, realm, kind, revision, version, *, key, apply=False, principal=None):
        from pa.execution.verification_migration import migration_plan
        identity = {'card_id': card_id, 'realm': realm, 'kind': kind, 'revision': revision, 'version': version, 'principal': principal}
        fingerprint = digest(identity)
        plan = await self.call(self.store.migration, key, fingerprint, diagnostic_phase='verification_migration_read') if apply else None
        if not plan:
            card = await self.call(self.ctx.store.get_card, card_id, realm_id=realm, diagnostic_phase='verification_card_read')
            if not card or not card.completion_requirement or card.completion_requirement.revision != revision or card.updated_at.isoformat() != version:
                raise CompletionConflict('stale_completion_requirement')
            requirement = await self.call(migration_plan, self, card, kind, diagnostic_phase='verification_migration_plan')
            plan = {'principal_id': principal or requirement.verification_principal,
                    'path': '/api/cards/' + card.id + '?realm=' + realm,
                    'body': {'expected_version': version, 'field_intent': ['completion_requirement'],
                             'completion_requirement': requirement.model_dump(mode='json')}}
            if not apply:
                return {'eligible': True, **identity, 'replacement': plan['body']['completion_requirement']}
            plan = await self.call(self.store.migration, key, fingerprint, plan, diagnostic_phase='verification_migration_write')
        return await self.health.local_api({'principal_id': plan['principal_id']}, 'PATCH', plan['path'],
                                          body=plan['body'], key=key)

    async def cycle(self):
        if self.lock.locked():
            return
        async with self.lock:
            cards = await self.call(self.ctx.store.list_cards, realm_id=self.ctx.settings.primary_realm,
                                    limit=32, offset=self.offset, diagnostic_phase='verification_card_read')
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
