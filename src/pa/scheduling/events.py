"""Bounded, restartable reader of PA's durable event DAG (never a writer)."""
import time


def poll_events(ctx, ledger):
    log = ctx.services.get('event_log')
    if log is None:
        return
    for realm in ctx.settings.subscribed_realms:
        head = log.get_head(realm)
        if not head:
            continue
        with ledger.connect() as db:
            previous = db.execute('SELECT hash FROM source_heads WHERE realm=?',(realm,)).fetchone()
            if previous is None:
                # Start at installation; historical events are not new triggers.
                db.execute('INSERT INTO source_commits VALUES (?,?,1)',(realm,head))
            elif previous[0] != head:
                db.execute('INSERT OR IGNORE INTO source_commits VALUES (?,?,0)',(realm,head))
            if previous is None or previous[0] != head:
                db.execute('INSERT OR REPLACE INTO source_heads VALUES (?,?)',(realm,head))
        for _ in range(100):
            with ledger.connect() as db:
                row=db.execute('SELECT hash FROM source_commits WHERE realm=? AND processed=0 LIMIT 1',(realm,)).fetchone()
            if row is None:
                break
            commit=log.get_commit(row[0])
            if commit is None:
                raise ValueError('scheduler_event_commit_unavailable')
            for event_hash in commit.event_hashes:
                event=log.get_event(event_hash)
                if event is None:
                    raise ValueError('scheduler_event_unavailable')
                fields = event_fields(event)
                # Match only schedules whose owners can currently read the realm.
                owners={record['owner'] for record in _records(ledger)}
                from pa.domain.models import RealmRole
                membership=ctx.services.get('membership')
                from pa.auth.users import UserDirectory
                users=UserDirectory(ctx.settings.data_dir)
                for owner in owners:
                    user=users.get(owner.removeprefix('user:'))
                    allowed=user and (user.role=='admin' or (membership and membership.has_role(realm,owner,min_role=RealmRole.VIEWER)))
                    if allowed:
                        ledger.event(event.id,event.type.value,fields,time.time(),owner=owner,event_time=event.timestamp.timestamp())
            # If the process stops before this transaction, stable event IDs dedupe replay.
            with ledger.connect() as db:
                db.execute('UPDATE source_commits SET processed=1 WHERE realm=? AND hash=?',(realm,row[0]))
                for parent in commit.parent_hashes:
                    db.execute('INSERT OR IGNORE INTO source_commits VALUES (?,?,0)',(realm,parent))


def _records(ledger):
    with ledger.connect() as db:
        return [dict(row) for row in db.execute('SELECT owner FROM schedules WHERE deleted=0')]


def event_fields(event):
    payload = event.payload
    repository_event = event.type.value.startswith('repository_')
    return {'card':event.card_id, 'session':payload.get('session_id'),
            'state':payload.get('state', payload.get('status', payload.get('lane'))),
            'repo':payload.get('repository_id', payload.get('id') if repository_event else None)}
