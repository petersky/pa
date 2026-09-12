"""Narrow outbox/inbox with immutable revisions and FULL WAL receipts.

No dependency on Store, CardProjection, telemetry retention or canonical locks.
Only the service opens this database for writing. All public inputs are bounded
before a transaction; a failed commit never returns an acceptance receipt.
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pa.health_journal.models import Assessment, MAX_PAGE, Observation, Policy, Revision, digest, encode, sanitized


def now():
    return datetime.now(UTC).isoformat()


class JournalError(ValueError):
    def __init__(self, code, status=409):
        self.code, self.status = code, status
        super().__init__(code)


class Journal:
    def __init__(self, path: Path, instance_id: str, *, max_bytes=128 * 1024 * 1024):
        self.path, self.instance_id, self.max_bytes = path, instance_id, max_bytes
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS reports(
              id TEXT PRIMARY KEY, principal TEXT NOT NULL, realm TEXT NOT NULL,
              occurrence TEXT NOT NULL, revision INTEGER NOT NULL, hash TEXT NOT NULL,
              created TEXT NOT NULL, updated TEXT NOT NULL,
              UNIQUE(principal,realm,occurrence));
            CREATE TABLE IF NOT EXISTS revisions(
              seq INTEGER PRIMARY KEY AUTOINCREMENT, report TEXT NOT NULL,
              revision INTEGER NOT NULL, hash TEXT NOT NULL, payload TEXT NOT NULL,
              gathered TEXT, UNIQUE(report,revision));
            CREATE INDEX IF NOT EXISTS pending ON revisions(gathered,seq);
            CREATE TABLE IF NOT EXISTS replays(
              principal TEXT NOT NULL, key TEXT NOT NULL, hash TEXT NOT NULL,
              receipt TEXT NOT NULL, PRIMARY KEY(principal,key));
            CREATE TABLE IF NOT EXISTS inbox(
              seq INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT UNIQUE NOT NULL,
              hash TEXT NOT NULL, payload TEXT NOT NULL, receipt TEXT NOT NULL,
              group_id TEXT NOT NULL, ack_dirty INTEGER NOT NULL DEFAULT 1, triaged INTEGER NOT NULL DEFAULT 0);
            CREATE UNIQUE INDEX IF NOT EXISTS receipt_ids ON inbox(json_extract(receipt,'$.receipt_id'));
            CREATE TABLE IF NOT EXISTS groups(
              id TEXT PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL, realm TEXT NOT NULL,
              version INTEGER NOT NULL, disposition TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS history(
              seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL,
              payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS custody_history(
              seq INTEGER PRIMARY KEY AUTOINCREMENT, report TEXT NOT NULL, revision INTEGER NOT NULL,
              receipt_id TEXT NOT NULL, epoch INTEGER NOT NULL, version INTEGER NOT NULL, payload TEXT NOT NULL,
              UNIQUE(report,revision,receipt_id,epoch,version));
            CREATE TABLE IF NOT EXISTS producer_issues(
              issue_key TEXT PRIMARY KEY, scope TEXT NOT NULL, instance_id TEXT NOT NULL,
              reason TEXT NOT NULL, generation INTEGER NOT NULL, active INTEGER NOT NULL,
              report_id TEXT NOT NULL, semantic_hash TEXT NOT NULL, correlation_key TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_status(
              id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS actions(
              id TEXT PRIMARY KEY, group_id TEXT NOT NULL,
              payload TEXT NOT NULL, state TEXT NOT NULL, result TEXT);
            CREATE INDEX IF NOT EXISTS actions_group ON actions(group_id);
            ''')
            for key, value in [('incarnation', str(uuid4())), ('instance_id', instance_id),
                               ('policy', encode(Policy().model_dump())),
                               ('lease', encode({'fence': 0, 'owner': '', 'expires': 0}))]:
                db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', (key, value))
            if self._meta(db, 'instance_id') != instance_id:
                raise JournalError('journal_instance_mismatch')
            self.incarnation = self._meta(db, 'incarnation')
        os.chmod(path, 0o600)

    @contextmanager
    def connection(self, *, write=False):
        db = sqlite3.connect(self.path, timeout=0.25, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA busy_timeout=250')
        try:
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.execute('COMMIT')
        except BaseException:
            if db.in_transaction:
                db.execute('ROLLBACK')
            raise
        finally:
            db.close()

    def _capacity(self):
        size = sum(p.stat().st_size for p in (self.path, Path(str(self.path) + '-wal')) if p.exists())
        if size >= self.max_bytes:
            raise JournalError('journal_capacity_exhausted', 507)

    @staticmethod
    def _meta(db, key):
        return db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()[0]

    @staticmethod
    def _set(db, key, value):
        db.execute('UPDATE meta SET value=? WHERE key=?', (encode(value), key))

    def append(self, observation: Observation, *, principal: str, realm: str, key: str, context=None):
        if not principal or not 1 <= len(key) <= 160:
            raise JournalError('invalid_report_binding', 422)
        # Neither raw secrets nor hashes of raw secrets enter receipt/dedupe keys.
        body = sanitized(observation.model_dump(exclude={'realm', 'report_id'}))
        context = sanitized(context or {})
        request_hash = digest({'observation': body, 'realm': realm, 'context': {k: v for k, v in context.items() if k != 'runtime_build'},
                               'report_id': observation.report_id})
        key = sanitized(key)
        occurrence = body['occurrence_key']
        with self.connection(write=True) as db:
            replay = db.execute('SELECT * FROM replays WHERE principal=? AND key=?', (principal, key)).fetchone()
            if replay:
                if replay['hash'] != request_hash:
                    raise JournalError('idempotency_conflict')
                return json.loads(replay['receipt'])
            self._capacity()
            old = db.execute('SELECT * FROM reports WHERE principal=? AND realm=? AND occurrence=?',
                             (principal, realm, occurrence)).fetchone()
            if observation.report_id and (not old or old['id'] != observation.report_id):
                raise JournalError('report_occurrence_mismatch')
            report_id = old['id'] if old else str(uuid4())
            revision = old['revision'] + 1 if old else 1
            stamp = now()
            payload = {'source_instance_id': self.instance_id, 'incarnation': self.incarnation,
                       'report_id': report_id, 'revision': revision,
                       'previous_hash': old['hash'] if old else None,
                       'realm': realm, 'principal': principal, 'context': context,
                       'created_at': old['created'] if old else stamp, 'updated_at': stamp,
                       'recurrence_count': revision, 'observation': body}
            Revision.model_validate(payload)
            if len(encode(payload)) > 16000:
                raise JournalError('journal_revision_too_large', 413)
            hashed = digest(payload)
            db.execute('INSERT INTO revisions(report,revision,hash,payload) VALUES(?,?,?,?)',
                       (report_id, revision, hashed, encode(payload)))
            db.execute('INSERT INTO reports VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,hash=excluded.hash,updated=excluded.updated',
                       (report_id, principal, realm, occurrence, revision, hashed,
                        payload['created_at'], stamp))
            receipt = {'report_id': report_id, 'revision': revision, 'hash': hashed,
                       'source_instance_id': self.instance_id, 'incarnation': self.incarnation,
                       'delivery': 'pending', 'created_at': stamp}
            db.execute('INSERT INTO replays VALUES(?,?,?,?)', (principal, key, request_hash, encode(receipt)))
            return receipt

    def page(self, *, principal=None, realms: list[str], cursor=None, limit=MAX_PAGE, pending=False):
        if not 1 <= limit <= MAX_PAGE:
            raise JournalError('invalid_page_size', 422)
        with self.connection() as db:
            watermark = db.execute('SELECT coalesce(max(seq),0) FROM revisions').fetchone()[0]
            after = 0
            if cursor:
                try:
                    incarnation, last, high = cursor.split(':')
                    after, watermark = int(last), int(high)
                    assert incarnation == self.incarnation and 0 <= after <= watermark
                except (ValueError, AssertionError):
                    raise JournalError('invalid_journal_cursor', 422) from None
            sql = 'SELECT v.* FROM revisions v JOIN reports r ON r.id=v.report WHERE seq>? AND seq<=?'
            args = [after, watermark]
            if principal:
                sql += ' AND r.principal=?'
                args.append(principal)
            sql += ' AND r.realm IN (' + ','.join('?' for _ in realms) + ')'
            args.extend(realms)
            if pending:
                sql += ' AND gathered IS NULL'
            rows = db.execute(sql + ' ORDER BY seq LIMIT ?', (*args, limit + 1)).fetchall()
            more = len(rows) > limit
            rows = rows[:limit]
            return {'source_instance_id': self.instance_id, 'incarnation': self.incarnation,
                    'items': [self._revision(row) for row in rows],
                    'cursor': f'{self.incarnation}:{rows[-1]["seq"]}:{watermark}' if more else None,
                    'watermark': watermark}

    @staticmethod
    def _revision(row):
        return {'payload': json.loads(row['payload']), 'hash': row['hash'],
                'delivery': 'gathered' if row['gathered'] else 'pending',
                'custody': json.loads(row['gathered']) if row['gathered'] else None}

    def report(self, report_id, *, principal=None, realms: list[str], before=None):
        with self.connection() as db:
            row = db.execute('SELECT * FROM reports WHERE id=?', (report_id,)).fetchone()
            if not row or row['realm'] not in realms or (principal and row['principal'] != principal):
                raise JournalError('report_not_found', 404)
            rows = db.execute('SELECT * FROM revisions WHERE report=? AND revision<? ORDER BY revision DESC LIMIT ?',
                              (report_id, before or row['revision'] + 1, MAX_PAGE)).fetchall()
            return {'report_id': report_id, 'revision': row['revision'],
                    'history': [self._revision(r) for r in rows],
                    'before': rows[-1]['revision'] if len(rows) == MAX_PAGE else None,
                    'custody_history': [json.loads(r[0]) for r in db.execute('SELECT payload FROM custody_history WHERE report=? ORDER BY seq DESC LIMIT 32', (report_id,))]}

    @staticmethod
    def identity(payload):
        return ':'.join(str(payload[k]) for k in ('source_instance_id', 'incarnation', 'report_id', 'revision'))

    def ingest(self, entry, *, source_id, realms, fence, owner):
        payload = entry['payload']
        Revision.model_validate(payload)
        if payload.get('source_instance_id') != source_id or payload.get('realm') not in realms:
            raise JournalError('source_or_realm_mismatch', 403)
        if len(encode(payload)) > 16000 or payload != sanitized(payload) or digest(payload) != entry['hash']:
            raise JournalError('invalid_sanitized_revision', 422)
        Observation.model_validate(payload['observation'])
        identity = self.identity(payload)
        with self.connection(write=True) as db:
            policy = self._fenced(db, owner, fence)
            old = db.execute('SELECT * FROM inbox WHERE identity=?', (identity,)).fetchone()
            if old:
                if old['hash'] != entry['hash']:
                    raise JournalError('revision_hash_conflict')
                return json.loads(old['receipt'])
            self._capacity()
            fingerprint = digest({'realm': payload['realm'], 'subsystem': payload['observation']['subsystem'],
                                  'error_code': payload['observation']['error_code']})
            group = db.execute('SELECT * FROM groups WHERE fingerprint=?', (fingerprint,)).fetchone()
            group_id = group['id'] if group else str(uuid4())
            if not group:
                db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)',
                           (group_id, fingerprint, payload['realm'], 1, 'new', encode({'reason': 'Awaiting bounded evidence triage'})))
                self._history(db, group_id, {'disposition': 'new', 'at': now(), 'actor': 'system:collector'})
            if group and group['disposition'] == 'deployed_verified':
                accepted = json.loads(group['data']).get('acceptance_reference')
                if accepted and payload['observation'].get('recurrence_after_acceptance') == accepted:
                    db.execute("UPDATE groups SET disposition='reopened',version=version+1 WHERE id=?", (group_id,))
                    self._history(db, group_id, {'disposition': 'reopened', 'reason': 'Explicit confirmed recurrence after accepted boundary', 'identity': identity, 'at': now()})
                else:
                    db.execute("UPDATE groups SET disposition='awaiting_acceptance',version=version+1 WHERE id=?", (group_id,))
                    self._history(db, group_id, {'disposition':'awaiting_acceptance', 'reason':'New observation lies outside the previous acceptance snapshot.', 'identity':identity, 'at':now()})
                db.execute('UPDATE inbox SET ack_dirty=1 WHERE group_id=?', (group_id,))
            receipt = {'receipt_id': str(uuid4()), 'identity': identity, 'hash': entry['hash'],
                       'authority_id': self.instance_id, 'epoch': policy['epoch'],
                       'group_id': group_id, 'gathered_at': now()}
            db.execute('INSERT INTO inbox(identity,hash,payload,receipt,group_id) VALUES(?,?,?,?,?)',
                       (identity, entry['hash'], encode(payload), encode(receipt), group_id))
            return receipt

    def receipt(self, receipt_id):
        # Indexed identity lookup is separate from canonical generic receipts.
        with self.connection() as db:
            row = db.execute("SELECT * FROM inbox WHERE json_extract(receipt,'$.receipt_id')=?", (receipt_id,)).fetchone()
            if not row:
                raise JournalError('receipt_not_found', 404)
            receipt = json.loads(row['receipt'])
            group = db.execute('SELECT * FROM groups WHERE id=?', (row['group_id'],)).fetchone()
            policy = json.loads(self._meta(db, 'policy'))
            fix = json.loads(group['data'])
            disposition = group['disposition']
            if fix.get('acceptance_reference'):
                source_id = json.loads(row['payload'])['source_instance_id']
                covered = (source_id in fix.get('accepted_instances', [])
                    and row['seq'] <= fix.get('acceptance_watermark', 0)
                    and fix.get('accepted_subject_revision') == fix.get('commit'))
                if disposition in {'deployed_verified', 'awaiting_acceptance'}:
                    disposition = 'deployed_verified' if covered else 'awaiting_acceptance'
                if not covered:
                    fix = {**fix, 'last_group_acceptance_reference':fix['acceptance_reference'],
                           'acceptance_reference':None, 'acceptance_scope':'uncovered',
                           'reason':'This observation is outside the declared canonical acceptance scope.'}
            fix['disposition'] = disposition
            return {**receipt, 'ingested_authority_id': receipt['authority_id'], 'ingested_epoch': receipt['epoch'],
                    'authority_id': policy['authority_id'], 'epoch': policy['epoch'],
                    'disposition': disposition, 'version': group['version'],
                    'fix': fix}

    def gathered(self, verified_receipt):
        """Caller must obtain this receipt from the configured authority channel."""
        with self.connection(write=True) as db:
            policy = json.loads(self._meta(db, 'policy'))
            if verified_receipt['authority_id'] != policy['authority_id'] or verified_receipt['epoch'] != policy['epoch']:
                raise JournalError('stale_health_authority', 403)
            identity = verified_receipt['identity'].split(':')
            if len(identity) != 4 or identity[:2] != [self.instance_id, self.incarnation]:
                raise JournalError('source_identity_mismatch', 403)
            row = db.execute('SELECT * FROM revisions WHERE report=? AND revision=?', (identity[2], identity[3])).fetchone()
            if not row or row['hash'] != verified_receipt['hash']:
                raise JournalError('revision_hash_conflict')
            outcome = {'gathered': True, 'report_id': identity[2], 'revision': int(identity[3])}
            previous = json.loads(row['gathered']) if row['gathered'] else None
            if previous:
                if previous['receipt_id'] != verified_receipt['receipt_id'] or previous['group_id'] != verified_receipt['group_id']:
                    raise JournalError('custody_identity_conflict')
                current_order = (previous['epoch'], previous['version'])
                incoming_order = (verified_receipt['epoch'], verified_receipt['version'])
                if incoming_order < current_order:
                    return {**outcome, 'custody': 'stale_ignored', 'version': previous['version']}
                if incoming_order == current_order:
                    if encode(previous) != encode(verified_receipt):
                        raise JournalError('custody_version_conflict')
                    return {**outcome, 'custody': 'replayed', 'version': previous['version']}
            db.execute('INSERT INTO custody_history(report,revision,receipt_id,epoch,version,payload) VALUES(?,?,?,?,?,?)', (identity[2], identity[3], verified_receipt['receipt_id'], verified_receipt['epoch'], verified_receipt['version'], encode(verified_receipt)))
            db.execute('UPDATE revisions SET gathered=? WHERE seq=?', (encode(verified_receipt), row['seq']))
            return {**outcome, 'custody': 'advanced', 'version': verified_receipt['version']}

    @staticmethod
    def _history(db, group, payload):
        db.execute('INSERT INTO history(group_id,payload) VALUES(?,?)', (group, encode(sanitized(payload))))

    def groups(self, realms, *, after='', limit=MAX_PAGE):
        with self.connection() as db:
            rows = db.execute('SELECT * FROM groups WHERE id>? AND realm IN (' + ','.join('?' for _ in realms) + ') ORDER BY id LIMIT ?',
                              (after, *realms, min(limit, MAX_PAGE))).fetchall()
            return [{**dict(r), 'data': json.loads(r['data'])} for r in rows]

    def assess(self, group_id, assessment: Assessment, *, actor, realms, accepted=False):
        if assessment.disposition == 'deployed_verified' and not accepted:
            raise JournalError('authoritative_acceptance_required')
        with self.connection(write=True) as db:
            row = db.execute('SELECT * FROM groups WHERE id=?', (group_id,)).fetchone()
            if not row or row['realm'] not in realms:
                raise JournalError('group_not_found', 404)
            if row['version'] != assessment.expected_version:
                raise JournalError('assessment_version_conflict')
            data = {**json.loads(row['data']), **sanitized(assessment.model_dump(exclude_none=True))}
            if assessment.disposition == 'deployed_verified':
                affected = db.execute("SELECT DISTINCT json_extract(payload,'$.source_instance_id') FROM inbox WHERE group_id=? LIMIT 33", (group_id,)).fetchall()
                if len(affected) > 32 or not {r[0] for r in affected} <= set(assessment.accepted_instances):
                    raise JournalError('acceptance_scope_incomplete')
                data['acceptance_watermark'] = db.execute('SELECT coalesce(max(seq),0) FROM inbox WHERE group_id=?', (group_id,)).fetchone()[0]
            db.execute('UPDATE groups SET version=version+1,disposition=?,data=? WHERE id=?',
                       (assessment.disposition, encode(data), group_id))
            db.execute('UPDATE inbox SET ack_dirty=1 WHERE group_id=?', (group_id,))
            self._history(db, group_id, {**data, 'actor': actor, 'at': now()})
            return {'group_id': group_id, 'version': row['version'] + 1, 'disposition': assessment.disposition, 'data': data}

    def group_history(self, group_id, *, after=0):
        with self.connection() as db:
            return [{'seq': r['seq'], **json.loads(r['payload'])} for r in db.execute(
                'SELECT * FROM history WHERE group_id=? AND seq>? ORDER BY seq LIMIT ?', (group_id, after, MAX_PAGE))]

    def configure(self, policy: Policy, *, expected_version, actor):
        with self.connection(write=True) as db:
            old = json.loads(self._meta(db, 'policy'))
            if db.execute("SELECT 1 FROM meta WHERE key='handover'").fetchone():
                raise JournalError('authority_permanently_fenced_after_handover')
            if old['version'] != expected_version:
                raise JournalError('policy_version_conflict')
            if old['authority_id'] and (old['authority_id'] != policy.authority_id or old['epoch'] != policy.epoch):
                # No unsafe clock-based failover. Transfer requires a durable handover,
                # including all action receipts; unsupported transitions stay blocked.
                raise JournalError('authority_transfer_requires_fenced_handover')
            data = policy.model_dump() | {'version': expected_version + 1}
            self._set(db, 'policy', data)
            self._history(db, 'policy', {'actor': actor, 'at': now(), 'policy': data})
            return data

    def lease(self, owner, *, seconds=90):
        with self.connection(write=True) as db:
            policy = json.loads(self._meta(db, 'policy'))
            if policy['authority_id'] != self.instance_id or not policy['enabled'] or policy['paused']:
                raise JournalError('health_authority_inactive')
            lease = json.loads(self._meta(db, 'lease'))
            if lease['owner'] != owner and lease['expires'] > time.time():
                raise JournalError('health_cycle_owned')
            if lease['owner'] != owner or lease['expires'] <= time.time():
                lease['fence'] += 1
            lease.update(owner=owner, expires=time.time() + seconds, epoch=policy['epoch'])
            self._set(db, 'lease', lease)
            return lease

    def _fenced(self, db, owner, fence):
        policy = json.loads(self._meta(db, 'policy'))
        lease = json.loads(self._meta(db, 'lease'))
        if (policy['authority_id'] != self.instance_id or not policy['enabled'] or policy['paused']
                or lease['owner'] != owner or lease['fence'] != fence or lease['expires'] <= time.time()
                or lease.get('epoch') != policy['epoch']):
            raise JournalError('stale_health_fence')
        return policy

    def release(self, owner, fence):
        with self.connection(write=True) as db:
            lease = json.loads(self._meta(db, 'lease'))
            if lease['owner'] == owner and lease['fence'] == fence:
                lease['expires'] = 0
                self._set(db, 'lease', lease)

    def source_status(self, source_id, *, code, phase):
        with self.connection(write=True) as db:
            old = db.execute('SELECT payload FROM source_status WHERE id=?', (source_id,)).fetchone()
            previous = json.loads(old[0]) if old else {}
            # A polling failure is status, not a fresh report every wake.
            data = {'code': code, 'phase': phase, 'observed_at': now(),
                    'consecutive_failures': previous.get('consecutive_failures', 0) + 1 if code else 0}
            data['next_retry'] = time.time() + min(1800, 30 * 2 ** min(data['consecutive_failures'], 6)) * random.uniform(0.8, 1.2) if code else 0
            db.execute('INSERT INTO source_status VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload', (source_id, encode(data)))

    def status(self):
        with self.connection() as db:
            return {'instance_id': self.instance_id, 'incarnation': self.incarnation,
                    'policy': json.loads(self._meta(db, 'policy')), 'lease': json.loads(self._meta(db, 'lease')),
                    'pending_revisions': db.execute('SELECT count(*) FROM revisions WHERE gathered IS NULL').fetchone()[0],
                    'inbox_revisions': db.execute('SELECT count(*) FROM inbox').fetchone()[0],
                    'sources': {r['id']: json.loads(r['payload']) for r in db.execute('SELECT * FROM source_status LIMIT 64')},
                    'active_actions': [dict(r) for r in db.execute("SELECT id,group_id,state,result FROM actions WHERE state!='terminal' LIMIT 1")]}

    def action(self, *, owner, fence):
        """Reserve one stable normal-dispatch action before any external mutation."""
        with self.connection(write=True) as db:
            policy = self._fenced(db, owner, fence)
            active = db.execute("SELECT * FROM actions WHERE state!='terminal' LIMIT 1").fetchone()
            if active:
                return {**dict(active), 'payload': json.loads(active['payload']),
                        'result': json.loads(active['result']) if active['result'] else None}
            if not policy['project_id'] or not policy['principal_id']:
                return None
            group = db.execute("SELECT g.* FROM groups g JOIN inbox i ON i.group_id=g.id WHERE i.triaged=0 ORDER BY i.seq LIMIT 1").fetchone()
            if not group:
                return None
            pending = db.execute('SELECT seq,payload FROM inbox WHERE group_id=? AND triaged=0 ORDER BY seq LIMIT 3', (group['id'],)).fetchall()
            entries = [json.loads(r['payload']) for r in pending]
            for row in pending:
                db.execute('UPDATE inbox SET triaged=1 WHERE seq=?', (row['seq'],))
            action_id = str(uuid4())
            db.execute("UPDATE groups SET disposition=CASE WHEN disposition IN ('new','reopened') THEN 'triaged' ELSE disposition END,version=version+1 WHERE id=?", (group['id'],))
            self._history(db, group['id'], {'disposition': 'triaged', 'action_id': action_id, 'at': now()})
            payload = {'id': action_id, 'group_id': group['id'], 'realm': group['realm'],
                       'epoch': policy['epoch'], 'project_id': policy['project_id'],
                       'principal_id': policy['principal_id'], 'evidence': entries,
                       'created_at': now()}
            # A fresh evidence generation keeps the canonical repair-card owner.
            # Normal dispatch admission validates that existing card and its scope.
            card_id = json.loads(group['data']).get('card_id')
            result = {'card_id': card_id} if card_id else None
            db.execute('INSERT INTO actions VALUES(?,?,?,?,?)',
                       (action_id, group['id'], encode(payload), 'reserved', encode(result) if result else None))
            return {'id': action_id, 'group_id': group['id'], 'payload': payload, 'state': 'reserved', 'result': result}

    def action_result(self, action_id, *, state, result, owner, fence):
        with self.connection(write=True) as db:
            self._fenced(db, owner, fence)
            old = db.execute('SELECT * FROM actions WHERE id=?', (action_id,)).fetchone()
            previous = json.loads(old['result']) if old['result'] else {}
            merged = {**previous, **sanitized(result)}
            if previous.get('effects'):
                merged['effects'] = previous['effects']
            db.execute('UPDATE actions SET state=?,result=? WHERE id=?', (state, encode(merged), action_id))
            group = db.execute('SELECT * FROM groups WHERE id=?', (old['group_id'],)).fetchone()
            data = {**json.loads(group['data']), **merged}
            disposition = 'in_progress' if state == 'dispatched' and group['disposition'] in {'new', 'triaged', 'reopened'} else group['disposition']
            data['phase'] = 'triaging' if state == 'dispatched' else state
            if state == 'terminal' and disposition == 'in_progress':
                disposition = 'needs_input'
                data['reason'] = 'Repair dispatch ended; declared acceptance or a reasoned disposition is still required.'
            db.execute('UPDATE groups SET version=version+1,data=?,disposition=? WHERE id=?', (encode(data), disposition, old['group_id']))
            db.execute('UPDATE inbox SET ack_dirty=1 WHERE group_id=?', (old['group_id'],))
            self._history(db, old['group_id'], {'action_id': action_id, 'state': state, **merged, 'at': now()})


    def pending_custody(self, source_id):
        with self.connection() as db:
            return [json.loads(r['receipt']) for r in db.execute(
                "SELECT receipt FROM inbox WHERE ack_dirty=1 AND json_extract(payload,'$.source_instance_id')=? ORDER BY seq LIMIT 16", (source_id,))]

    def custody_sent(self, receipt_id, version):
        with self.connection(write=True) as db:
            # An assessment racing the network acknowledgment must remain dirty.
            db.execute("UPDATE inbox SET ack_dirty=0 WHERE json_extract(receipt,'$.receipt_id')=? AND group_id IN (SELECT id FROM groups WHERE version=?)", (receipt_id, version))

    def freeze_transfer(self, target_id, *, expected_version, actor):
        """Positive local fencing, not partition election. No unresolved actions."""
        with self.connection(write=True) as db:
            policy = json.loads(self._meta(db, 'policy'))
            existing = db.execute("SELECT value FROM meta WHERE key='handover'").fetchone()
            if existing:
                handover = json.loads(existing[0])
                if handover['target_id'] != target_id:
                    raise JournalError('authority_transfer_target_conflict')
                return handover
            lease = json.loads(self._meta(db, 'lease'))
            if (policy['authority_id'] != self.instance_id or policy['version'] != expected_version
                    or target_id == self.instance_id or not target_id):
                raise JournalError('authority_transfer_policy_conflict')
            if lease['expires'] > time.time() or db.execute("SELECT 1 FROM actions WHERE state!='terminal' LIMIT 1").fetchone():
                raise JournalError('authority_transfer_active_or_unknown_action')
            tables = {}
            # The first version deliberately refuses an oversized transfer instead
            # of partially activating. Ordinary collection remains paged.
            for table in ('inbox', 'groups', 'history', 'actions'):
                rows = db.execute(f'SELECT * FROM {table} LIMIT 1001').fetchall()
                if len(rows) > 1000:
                    raise JournalError('authority_transfer_snapshot_too_large', 413)
                tables[table] = [dict(r) for r in rows]
            snapshot = {'tables': tables, 'from_id': self.instance_id, 'target_id': target_id,
                        'epoch': policy['epoch'] + 1, 'policy': dict(policy), 'transfer_id': str(uuid4())}
            if len(encode(snapshot)) > 192 * 1024:
                raise JournalError('authority_transfer_snapshot_too_large', 413)
            handover = {**snapshot, 'hash': digest(snapshot), 'fenced_at': now(), 'actor': actor}
            policy.update(paused=True, version=policy['version'] + 1)
            self._set(db, 'policy', policy)
            self._set(db, 'lease', {'owner': '', 'fence': lease['fence'] + 1, 'expires': 0})
            db.execute("INSERT INTO meta VALUES('handover',?)", (encode(handover),))
            self._history(db, 'policy', {'at': now(), 'action': 'authority_fenced_for_transfer',
                                         'target_id': target_id, 'transfer_id': handover['transfer_id'], 'actor': actor})
            return handover

    def handover(self):
        with self.connection() as db:
            row = db.execute("SELECT value FROM meta WHERE key='handover'").fetchone()
            if not row:
                raise JournalError('authority_handover_not_ready', 409)
            return json.loads(row[0])

    def import_handover(self, handover, *, actor):
        expected = {k: v for k, v in handover.items() if k not in {'hash', 'fenced_at', 'actor'}}
        if digest(expected) != handover['hash'] or handover['target_id'] != self.instance_id:
            raise JournalError('authority_handover_integrity_conflict')
        with self.connection(write=True) as db:
            old = json.loads(self._meta(db, 'policy'))
            received = db.execute("SELECT value FROM meta WHERE key='received_handover'").fetchone()
            if received:
                if json.loads(received[0])['hash'] == handover['hash']:
                    return old
                raise JournalError('authority_handover_conflict')
            if old['authority_id'] != handover['from_id'] or handover['epoch'] != old['epoch'] + 1:
                raise JournalError('authority_handover_policy_conflict')
            if db.execute('SELECT 1 FROM inbox LIMIT 1').fetchone() or db.execute('SELECT 1 FROM actions LIMIT 1').fetchone():
                raise JournalError('authority_handover_nonempty_target')
            for table in ('inbox', 'groups', 'history', 'actions'):
                allowed = {r[1] for r in db.execute(f'PRAGMA table_info({table})')}
                for row in handover['tables'][table]:
                    if set(row) != allowed:
                        raise JournalError('authority_handover_schema_conflict')
                    columns = [key for key in row if not (table == 'history' and key == 'seq')]
                    db.execute(f'INSERT INTO {table}(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')', tuple(row[c] for c in columns))
            policy = {**handover['policy'], 'authority_id': self.instance_id,
                      'epoch': handover['epoch'], 'version': old['version'] + 1, 'paused': True}
            self._set(db, 'policy', policy)
            db.execute('UPDATE inbox SET ack_dirty=1')
            db.execute("INSERT INTO meta VALUES('received_handover',?)", (encode(handover),))
            self._history(db, 'policy', {'at': now(), 'action': 'authority_handover_received', 'actor': actor,
                                        'transfer_id': handover['transfer_id'], 'hash': handover['hash']})
            return policy

    def follow_handover(self, handover, *, actor):
        """Source changes custodian only after service verifies old and new stores."""
        with self.connection(write=True) as db:
            policy = json.loads(self._meta(db, 'policy'))
            if policy['authority_id'] == handover['target_id'] and policy['epoch'] == handover['epoch']:
                return policy
            if policy['authority_id'] != handover['from_id'] or policy['epoch'] + 1 != handover['epoch']:
                raise JournalError('authority_handover_policy_conflict')
            policy.update(authority_id=handover['target_id'], epoch=handover['epoch'], version=policy['version'] + 1)
            self._set(db, 'policy', policy)
            self._history(db, 'policy', {'at': now(), 'action': 'source_followed_handover', 'actor': actor, 'hash': handover['hash']})
            return policy

    def received_handover(self):
        with self.connection() as db:
            row = db.execute("SELECT value FROM meta WHERE key='received_handover'").fetchone()
            if not row:
                raise JournalError('authority_handover_not_received', 409)
            handover = json.loads(row[0])
            return {k: handover[k] for k in ('transfer_id', 'hash', 'from_id', 'target_id', 'epoch')}


    def next_source(self, ids):
        with self.connection(write=True) as db:
            row = db.execute("SELECT value FROM meta WHERE key='source_cursor'").fetchone()
            index = int(row[0]) if row else 0
            selected = ids[index % len(ids)]
            db.execute("INSERT INTO meta VALUES('source_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(index + 1),))
            return selected

    def group(self, group_id, *, realms):
        with self.connection() as db:
            row = db.execute('SELECT * FROM groups WHERE id=?', (group_id,)).fetchone()
            if not row or row['realm'] not in realms:
                raise JournalError('group_not_found', 404)
            return {**dict(row), 'data': json.loads(row['data']),
                    'members': [{'identity': r['identity'], 'hash': r['hash'], 'payload': json.loads(r['payload'])}
                                for r in db.execute('SELECT * FROM inbox WHERE group_id=? ORDER BY seq DESC LIMIT 3', (group_id,))]}

    def reserve_effect(self, action_id, effect, payload, *, owner, fence):
        """Durably admit this exact effect under the fence before sending it.

        Admitted effects may complete after pause/lease loss, like ordinary PA
        dispatches. They retain the sole action slot and replay key. A pause
        before this transaction rejects a new effect; it never claims to revoke
        a side effect whose admission receipt is already durable.
        """
        with self.connection(write=True) as db:
            row = db.execute('SELECT * FROM actions WHERE id=?', (action_id,)).fetchone()
            if not row or row['state'] == 'terminal':
                raise JournalError('health_action_not_active')
            result = json.loads(row['result']) if row['result'] else {}
            effects = result.get('effects', {})
            hashed = digest(sanitized(payload))
            if effect in effects:
                if effects[effect]['hash'] != hashed:
                    raise JournalError('health_effect_payload_conflict')
                return effects[effect]
            policy = self._fenced(db, owner, fence)
            receipt = {'id': f'health-{effect}:{action_id}', 'hash': hashed, 'fence': fence,
                       'epoch': policy['epoch'], 'authority_id': self.instance_id,
                       'admitted_at': now(), 'state': 'admitted_outcome_pending'}
            effects[effect] = receipt
            result['effects'] = effects
            db.execute('UPDATE actions SET result=? WHERE id=?', (encode(result), action_id))
            self._history(db, row['group_id'], {'action_id': action_id, 'effect_admitted': receipt, 'at': now()})
            return receipt


    def producer_issues(self, scope):
        with self.connection() as db:
            return [dict(r) for r in db.execute('SELECT * FROM producer_issues WHERE scope=? AND active=1 ORDER BY issue_key LIMIT 64', (scope,))]

    def producer_issue(self, issue_key):
        with self.connection() as db:
            row = db.execute('SELECT * FROM producer_issues WHERE issue_key=?', (issue_key,)).fetchone()
            return dict(row) if row else None

    def save_producer_issue(self, value):
        with self.connection(write=True) as db:
            db.execute('INSERT INTO producer_issues VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(issue_key) DO UPDATE SET generation=excluded.generation,active=excluded.active,report_id=excluded.report_id,semantic_hash=excluded.semantic_hash',
                tuple(value[k] for k in ('issue_key','scope','instance_id','reason','generation','active','report_id','semantic_hash','correlation_key')))

    def repair_dispatches(self, group_id):
        with self.connection() as db:
            rows = db.execute("SELECT json_extract(result,'$.dispatch_id') FROM actions WHERE group_id=? AND json_extract(result,'$.dispatch_id') IS NOT NULL LIMIT 33", (group_id,)).fetchall()
            if len(rows) > 32:
                raise JournalError('acceptance_origin_bound_exceeded')
            return [row[0] for row in rows]
