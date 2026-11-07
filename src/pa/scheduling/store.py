"""Transactional scheduler ledger, independent of cards and agent lifetimes.

Only the owning server opens this ledger. SQLite serializes occurrence creation;
all instances consuming a shared ledger reuse the same occurrence identity.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import re
from pathlib import Path
import sqlite3
from typing import Literal
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


class Trigger(BaseModel):
    model_config = ConfigDict(extra='forbid')
    kind: Literal['once', 'interval', 'cron', 'event', 'webhook', 'manual']
    at: datetime | None = None
    seconds: int | None = Field(default=None, ge=1)
    cron: str | None = None
    timezone: str = 'UTC'
    event_type: str | None = None
    filters: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode='after')
    def validate_trigger(self):
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError('unknown IANA timezone') from None
        if self.kind == 'once' and (self.at is None or self.at.tzinfo is None):
            raise ValueError('once requires an aware date/time')
        if self.kind == 'interval' and not self.seconds:
            raise ValueError('interval requires seconds')
        if self.kind == 'cron' and (not self.cron or len(self.cron.split()) != 5 or not croniter.is_valid(self.cron)):
            raise ValueError('cron requires a standard five-field expression')
        if self.kind == 'cron':
            for index, field in enumerate(self.cron.upper().split()):
                names = ('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split() if index == 3 else
                         'SUN MON TUE WED THU FRI SAT'.split() if index == 4 else [])
                for name in names:
                    field = field.replace(name, '1')
                if not re.fullmatch(r'[0-9*/,\-]+', field):
                    raise ValueError('cron extensions are not supported')
        if self.kind == 'event' and not self.event_type:
            raise ValueError('event requires an event type')
        if set(self.filters) - {'card', 'session', 'state', 'repo'}:
            raise ValueError('unsupported event filter')
        return self

    def next(self, after: float) -> float | None:
        if self.kind == 'once':
            return self.at.timestamp() if self.at.timestamp() > after else None
        if self.kind == 'interval':
            return after + self.seconds
        if self.kind == 'cron':
            return croniter(self.cron, datetime.fromtimestamp(after, ZoneInfo(self.timezone))).get_next(datetime).timestamp()
        return None


class Destination(BaseModel):
    model_config = ConfigDict(extra='forbid')
    kind: Literal['agent_session', 'inbox', 'messaging', 'webhook']
    target: str = Field(min_length=1, max_length=2048)
    interrupt: bool = False


class Schedule(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=200)
    trigger: Trigger
    destination: Destination
    payload: JsonValue
    context_refs: dict[str, str] = Field(default_factory=dict, max_length=30)
    enabled: bool = True
    expires_at: datetime | None = None
    max_occurrences: int | None = Field(default=None, ge=1)
    missed_policy: Literal['catch_up', 'skip', 'replay'] = 'catch_up'
    replay_limit: int = Field(default=10, ge=1, le=100)
    coalesce: bool = False

    @model_validator(mode='after')
    def validate_expiry(self):
        if isinstance(self.payload, str) and not self.payload:
            raise ValueError('text payload cannot be empty')
        if len(json.dumps(self.payload, ensure_ascii=False, allow_nan=False).encode()) > 16000:
            raise ValueError('payload exceeds 16000 JSON bytes')
        if self.expires_at and self.expires_at.tzinfo is None:
            raise ValueError('expiry requires timezone')
        return self


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class Ledger:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS schedules (
              id TEXT PRIMARY KEY, owner TEXT NOT NULL, spec TEXT NOT NULL,
              version INTEGER NOT NULL, next_due REAL, created_at REAL NOT NULL, count INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0, backlog TEXT);
            CREATE TABLE IF NOT EXISTS occurrences (
              id TEXT PRIMARY KEY, schedule_id TEXT NOT NULL, identity TEXT NOT NULL,
              envelope TEXT NOT NULL, destination TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
              next_retry REAL NOT NULL, delivered_at REAL, error TEXT,
              UNIQUE(schedule_id,identity));
            CREATE INDEX IF NOT EXISTS occurrence_due ON occurrences(state,next_retry);
            CREATE TABLE IF NOT EXISTS source_commits (realm TEXT NOT NULL, hash TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(realm,hash));
            CREATE TABLE IF NOT EXISTS source_heads (realm TEXT PRIMARY KEY, hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS commands (
              owner TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
              response TEXT NOT NULL, PRIMARY KEY(owner,key));
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def command(self, owner, key, operation, args, mutate):
        if not key or len(key) > 300:
            raise ValueError('bounded idempotency key required')
        fingerprint = hashlib.sha256(encode([operation, args]).encode()).hexdigest()
        with self.connect() as db:
            previous = db.execute('SELECT * FROM commands WHERE owner=? AND key=?', (owner, key)).fetchone()
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise ValueError('idempotency conflict')
                return json.loads(previous['response'])
            result = mutate(db)
            db.execute('INSERT INTO commands VALUES (?,?,?,?)', (owner, key, fingerprint, encode(result)))
            return result

    def create(self, owner, key, spec: Schedule, now: float):
        data = spec.model_dump(mode='json')
        def mutate(db):
            sid = str(uuid4())
            # A past once is due immediately; intervals anchor to creation.
            due = spec.trigger.at.timestamp() if spec.trigger.kind == 'once' else spec.trigger.next(now)
            db.execute('INSERT INTO schedules(id,owner,spec,version,next_due,created_at) VALUES (?,?,?,1,?,?)',
                       (sid, owner, encode(data), due, now))
            return self._get(db, owner, sid)
        return self.command(owner, key, 'create', data, mutate)

    def _get(self, db, owner, sid, include_deleted=False):
        row = db.execute('SELECT * FROM schedules WHERE id=? AND owner=? AND (deleted=0 OR ?)', (sid, owner, include_deleted)).fetchone()
        if row is None:
            raise KeyError(sid)
        return {**json.loads(row['spec']), 'id': sid, 'owner': owner,
                'version': row['version'], 'next_due': row['next_due'], 'occurrence_count': row['count']}

    def get(self, owner, sid, include_deleted=False):
        with self.connect() as db:
            return self._get(db, owner, sid, include_deleted)

    def list(self, owner):
        return self.list_page(owner)['schedules']

    def list_page(self, owner, cursor=None, limit=50):
        if not 1 <= limit <= 200:
            raise ValueError('page limit must be between 1 and 200')
        with self.connect() as db:
            rows = db.execute("SELECT id FROM schedules WHERE owner=? AND deleted=0 AND id>? ORDER BY id LIMIT ?",
                              (owner, cursor or '', limit + 1)).fetchall()
            records = []
            for row in rows[:limit]:
                record = self._get(db, owner, row['id'])
                record.update(self._summary(db, row['id']))
                records.append(record)
            return {'schedules':records, 'next_cursor':rows[limit-1]['id'] if len(rows)>limit else None}

    def update(self, owner, sid, key, spec: Schedule, version: int, now: float, delete=False, command_args=None):
        data = spec.model_dump(mode='json')
        def mutate(db):
            old = self._get(db, owner, sid)
            if old['version'] != version:
                raise ValueError('schedule version conflict')
            # Preserve cadence for pause/resume; changing trigger anchors anew.
            due = old['next_due'] if old['trigger'] == data['trigger'] else (
                spec.trigger.at.timestamp() if spec.trigger.kind == 'once' else spec.trigger.next(now))
            db.execute('UPDATE schedules SET spec=?,version=version+1,next_due=?,deleted=? WHERE id=?',
                       (encode(data), due, int(delete), sid))
            if old['trigger'] != data['trigger']:
                db.execute('UPDATE schedules SET backlog=NULL WHERE id=?', (sid,))
            if old['destination'] != data['destination']:
                db.execute("UPDATE occurrences SET destination=? WHERE schedule_id=? AND state IN ('pending','failed')",
                           (spec.destination.model_dump_json(), sid))
            if delete:
                db.execute("UPDATE occurrences SET state='cancelled' WHERE schedule_id=? AND state IN ('pending','failed')", (sid,))
                return {'id': sid, 'deleted': True}
            return self._get(db, owner, sid)
        return self.command(owner, key, 'update', command_args if command_args is not None else [sid, version, data, delete], mutate)

    def _occurrence(self, db, row, identity, due, now, missed=0, event_time=None):
        spec = Schedule.model_validate_json(row['spec'])
        if not spec.enabled or row['deleted'] or (spec.expires_at and now >= spec.expires_at.timestamp()):
            return None
        if spec.max_occurrences and row['count'] >= spec.max_occurrences:
            return None
        prior = db.execute('SELECT id FROM occurrences WHERE schedule_id=? AND identity=?', (row['id'], identity)).fetchone()
        if prior:
            return prior[0]
        oid = str(uuid4())
        envelope = {'schedule_id': row['id'], 'occurrence_id': oid, 'due_at': due,
                    'event_at': event_time, 'created_at': now, 'payload': spec.payload,
                    'context_refs': spec.context_refs, 'missed_count': missed, 'coalesced_count': 0}
        if spec.coalesce:
            pending = db.execute("SELECT * FROM occurrences WHERE schedule_id=? AND state='pending' AND attempts=0 AND json_extract(envelope,'$.delivery_started_at') IS NULL", (row['id'],)).fetchall()
            for item in pending:
                previous = json.loads(item['envelope'])
                envelope['coalesced_count'] += 1 + previous['coalesced_count']
                envelope['missed_count'] += previous['missed_count']
                db.execute("UPDATE occurrences SET state='coalesced' WHERE id=?", (item['id'],))
        db.execute('INSERT INTO occurrences(id,schedule_id,identity,envelope,destination,next_retry) VALUES (?,?,?,?,?,?)',
                   (oid, row['id'], identity, encode(envelope), spec.destination.model_dump_json(), now))
        db.execute('UPDATE schedules SET count=count+1 WHERE id=?', (row['id'],))
        return oid

    def fire(self, owner, sid, key, now, require_webhook=False):
        def mutate(db):
            current = self._get(db, owner, sid)
            if require_webhook and current['trigger']['kind'] != 'webhook':
                raise ValueError('not a webhook schedule')
            row = db.execute('SELECT * FROM schedules WHERE id=?', (sid,)).fetchone()
            oid = self._occurrence(db, row, 'manual:' + key, now, now)
            if oid is None:
                raise ValueError('schedule paused, expired or occurrence limit reached')
            return {'occurrence_id': oid}
        return self.command(owner, key, 'webhook' if require_webhook else 'fire', [sid], mutate)

    def event(self, event_id, event_type, fields, now, *, owner=None, event_time=None):
        # Event IDs are supplied by the durable event producer, never synthesized on replay.
        if not event_id:
            raise ValueError('stable event identity required')
        with self.connect() as db:
            result = []
            for row in db.execute('SELECT * FROM schedules WHERE deleted=0').fetchall():
                if owner is not None and row['owner'] != owner:
                    continue
                if event_time is not None and event_time < row['created_at']:
                    continue
                spec = Schedule.model_validate_json(row['spec'])
                t = spec.trigger
                if t.kind == 'event' and t.event_type == event_type and all(fields.get(k) == v for k, v in t.filters.items()):
                    result.append(self._occurrence(db, row, 'event:' + event_id, now, now, event_time=event_time if event_time is not None else now))
            return [oid for oid in result if oid]

    def tick(self, now):
        with self.connect() as db:
            for row in db.execute('SELECT * FROM schedules WHERE deleted=0 AND next_due<=?', (now,)).fetchall():
                spec = Schedule.model_validate_json(row['spec'])
                if not spec.enabled:
                    continue
                if (spec.expires_at and now >= spec.expires_at.timestamp()) or (spec.max_occurrences and row['count'] >= spec.max_occurrences):
                    db.execute('UPDATE schedules SET next_due=NULL,backlog=NULL WHERE id=?', (row['id'],))
                    continue
                due = row['next_due']
                backlog = json.loads(row['backlog']) if row['backlog'] else {'due':due, 'total':0, 'times':[]}
                if spec.trigger.kind == 'interval':
                    total = int((now - due) // spec.trigger.seconds) + 1
                    following = due + total * spec.trigger.seconds
                    times = [due + i * spec.trigger.seconds for i in range(min(total, spec.replay_limit))]
                    backlog = {'due':due, 'total':total, 'times':times}
                else:
                    following = due
                    for _ in range(1000):
                        if following is None or following > now:
                            break
                        backlog['total'] += 1
                        if len(backlog['times']) < spec.replay_limit:
                            backlog['times'].append(following)
                        following = spec.trigger.next(following)
                    if following is not None and following <= now:
                        db.execute('UPDATE schedules SET next_due=?,backlog=? WHERE id=?',
                                   (following, encode(backlog), row['id']))
                        continue
                total, times = backlog['total'], backlog['times']
                if spec.missed_policy == 'replay':
                    selected = [(t, total - len(times) if i == len(times)-1 else 0) for i, t in enumerate(times)]
                elif spec.missed_policy == 'skip' and (total > 1 or (spec.trigger.kind == 'once' and due < now - 1)):
                    selected = []
                else:
                    selected = [(backlog['due'], total - 1)]
                for t, missed in selected:
                    fresh = db.execute('SELECT * FROM schedules WHERE id=?', (row['id'],)).fetchone()
                    self._occurrence(db, fresh, 'time:' + str(t), t, now, missed)
                db.execute('UPDATE schedules SET next_due=?,backlog=NULL WHERE id=?', (following, row['id']))

    def pending(self, now, limit=100):
        with self.connect() as db:
            return [dict(row) for row in db.execute('''SELECT o.* FROM occurrences o JOIN schedules s ON s.id=o.schedule_id
              WHERE o.state='pending' AND o.next_retry<=? AND s.deleted=0
              AND json_extract(s.spec,'$.enabled')=1 ORDER BY next_retry LIMIT ?''', (now, limit))]

    def finish(self, oid, now, *, accepted=False, error=None):
        with self.connect() as db:
            row = db.execute('SELECT * FROM occurrences WHERE id=?', (oid,)).fetchone()
            if row['state'] not in {'pending', 'delivering'}:
                return
            attempts = row['attempts'] + 1
            state = 'delivered' if accepted else ('failed' if attempts >= 8 else 'pending')
            db.execute('UPDATE occurrences SET state=?,attempts=?,next_retry=?,delivered_at=?,error=? WHERE id=?',
                       (state, attempts, now + min(3600, 2 ** attempts), now if accepted else None, error, oid))
            db.execute("UPDATE occurrences SET envelope='{}' WHERE schedule_id=? AND state IN ('delivered','cancelled','coalesced') AND id NOT IN (SELECT id FROM occurrences WHERE schedule_id=? ORDER BY rowid DESC LIMIT 1000)",
                       (row['schedule_id'], row['schedule_id']))

    def history(self, owner, sid):
        with self.connect() as db:
            self._get(db, owner, sid)
            return [dict(r) for r in db.execute('SELECT * FROM occurrences WHERE schedule_id=? ORDER BY rowid DESC LIMIT 100', (sid,))]

    def retry(self, owner, sid, oid, key, now):
        def mutate(db):
            self._get(db, owner, sid)
            row = db.execute('SELECT state FROM occurrences WHERE id=? AND schedule_id=?', (oid, sid)).fetchone()
            if row is None:
                raise KeyError(oid)
            if row['state'] != 'failed':
                raise ValueError('only exhausted failures can be manually retried')
            db.execute("UPDATE occurrences SET state='pending',attempts=0,next_retry=? WHERE id=?", (now, oid))
            return {'occurrence_id': oid, 'state': 'pending'}
        return self.command(owner, key, 'retry', [sid, oid], mutate)

    def recover(self):
        # Caller holds the exclusive runner lock; no live delivery can be reset.
        with self.connect() as db:
            db.execute("UPDATE occurrences SET state='pending' WHERE state='delivering'")

    def claim(self, oid, now):
        with self.connect() as db:
            row = db.execute('SELECT * FROM occurrences WHERE id=?', (oid,)).fetchone()
            schedule = db.execute('SELECT * FROM schedules WHERE id=?', (row['schedule_id'],)).fetchone()
            spec = Schedule.model_validate_json(schedule['spec'])
            if row['state'] != 'pending' or schedule['deleted'] or not spec.enabled:
                return None
            envelope = json.loads(row['envelope'])
            # Freeze bytes before first send so a lost reply can be retried through
            # destinations which compare idempotent input fingerprints.
            if 'delivery_started_at' not in envelope:
                envelope.update(delivery_started_at=now, retry_metadata={'max_attempts':8, 'deduplication_key':oid})
            db.execute("UPDATE occurrences SET state='delivering',envelope=? WHERE id=?", (encode(envelope), oid))
            return {**dict(row), 'envelope':encode(envelope), 'owner':schedule['owner']}

    def _summary(self, db, sid):
        row = db.execute("""SELECT
          SUM(CASE WHEN state IN ('pending','delivering') THEN 1 ELSE 0 END) AS pending,
          MAX(delivered_at) AS last_successful_delivery,
          MIN(CASE WHEN state='pending' THEN next_retry END) AS next_retry
          FROM occurrences WHERE schedule_id=?""", (sid,)).fetchone()
        latest = db.execute("""SELECT id,state,attempts,delivered_at,error,next_retry,
          json_extract(envelope,'$.due_at') AS due_at,
          json_extract(envelope,'$.missed_count') AS missed_count,
          json_extract(envelope,'$.coalesced_count') AS coalesced_count
          FROM occurrences WHERE schedule_id=? ORDER BY rowid DESC LIMIT 1""", (sid,)).fetchone()
        return {**dict(row), 'pending':row['pending'] or 0,
                'last_occurrence':dict(latest) if latest else None}

    def status(self, owner, sid):
        with self.connect() as db:
            self._get(db, owner, sid)
            return self._summary(db, sid)
