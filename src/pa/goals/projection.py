from __future__ import annotations

import json


def init_goal_schema(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS durable_goals (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, project_id TEXT,
            state TEXT NOT NULL, owner_principal TEXT NOT NULL,
            revision INTEGER NOT NULL, version INTEGER NOT NULL,
            policy_revision INTEGER NOT NULL, next_wake_at TEXT,
            updated_at TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_durable_goals_realm_state
            ON durable_goals(realm_id, state, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_durable_goals_wakeup
            ON durable_goals(next_wake_at) WHERE next_wake_at IS NOT NULL;
        CREATE TABLE IF NOT EXISTS durable_goal_events (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL,
            goal_id TEXT NOT NULL, event_type TEXT NOT NULL,
            actor_principal TEXT NOT NULL, authority_instance_id TEXT NOT NULL,
            policy_revision INTEGER NOT NULL, idempotency_key TEXT NOT NULL,
            version INTEGER NOT NULL, payload TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL, UNIQUE(realm_id, idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS idx_durable_goal_events_goal
            ON durable_goal_events(goal_id, version);
        """
    )


def apply_goal_event(projection, event) -> None:
    goal = event.payload.get("goal") or {}
    record = event.payload.get("goal_event") or {}
    goal_id = str(goal.get("id") or record.get("goal_id") or "")
    if not goal_id:
        return
    wakeup = goal.get("wakeup") or {}
    with projection._conn() as conn:
        conn.execute(
            """
            INSERT INTO durable_goals
                (id, realm_id, project_id, state, owner_principal, revision,
                 version, policy_revision, next_wake_at, updated_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                realm_id=excluded.realm_id, project_id=excluded.project_id,
                state=excluded.state, owner_principal=excluded.owner_principal,
                revision=excluded.revision, version=excluded.version,
                policy_revision=excluded.policy_revision,
                next_wake_at=excluded.next_wake_at,
                updated_at=excluded.updated_at, payload=excluded.payload
            WHERE excluded.version >= durable_goals.version
            """,
            (
                goal_id,
                event.realm_id,
                goal.get("project_id"),
                goal.get("state", "draft"),
                goal.get("owner_principal", ""),
                int(goal.get("revision", 1)),
                int(goal.get("version", 1)),
                int((goal.get("policy") or {}).get("revision", 1)),
                wakeup.get("wake_at"),
                goal.get("updated_at") or event.timestamp.isoformat(),
                json.dumps(goal),
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO durable_goal_events
                (id, realm_id, goal_id, event_type, actor_principal,
                 authority_instance_id, policy_revision, idempotency_key,
                 version, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.realm_id,
                goal_id,
                record.get("event_type", "goal.updated"),
                record.get("actor_principal", event.author_principal),
                record.get("authority_instance_id", event.author_instance),
                int(record.get("policy_revision", 1)),
                record.get("idempotency_key", event.id),
                int(record.get("version", goal.get("version", 1))),
                json.dumps(record.get("payload") or {}),
                event.timestamp.isoformat(),
            ),
        )


def get_goal_payload(
    projection, goal_id: str, realm_id: str | None = None
) -> dict | None:
    query = "SELECT payload FROM durable_goals WHERE id=?"
    params: list[object] = [goal_id]
    if realm_id:
        query += " AND realm_id=?"
        params.append(realm_id)
    with projection._conn() as conn:
        row = conn.execute(query, params).fetchone()
    return json.loads(row["payload"]) if row else None


def list_goal_payloads(
    projection, realm_id: str | None = None, state: str | None = None
) -> list[dict]:
    query = "SELECT payload FROM durable_goals WHERE 1=1"
    params: list[object] = []
    if realm_id:
        query += " AND realm_id=?"
        params.append(realm_id)
    if state:
        query += " AND state=?"
        params.append(state)
    query += " ORDER BY updated_at DESC"
    with projection._conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [json.loads(row["payload"]) for row in rows]


def list_goal_events(projection, goal_id: str) -> list[dict]:
    with projection._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM durable_goal_events WHERE goal_id=? ORDER BY version, created_at",
            (goal_id,),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["payload"] = json.loads(item["payload"] or "{}")
        result.append(item)
    return result


def find_goal_event_by_idempotency(projection, realm_id: str, key: str) -> dict | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT * FROM durable_goal_events WHERE realm_id=? AND idempotency_key=?",
            (realm_id, key),
        ).fetchone()
    return dict(row) if row else None
