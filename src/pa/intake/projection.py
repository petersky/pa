from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pa.domain.models import EventType


def init_intake_schema(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS intake_envelopes (
            id TEXT PRIMARY KEY,
            realm_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            direction TEXT NOT NULL,
            kind TEXT NOT NULL,
            correlation_id TEXT NOT NULL,
            channel_message_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            sender_channel_user_id TEXT NOT NULL,
            principal_id TEXT,
            disposition TEXT NOT NULL,
            version INTEGER NOT NULL,
            occurred_at TEXT NOT NULL,
            received_at TEXT NOT NULL,
            canonical_expires_at TEXT,
            updated_at TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_intake_realm_received
            ON intake_envelopes(realm_id, received_at DESC);
        CREATE INDEX IF NOT EXISTS idx_intake_channel_message
            ON intake_envelopes(channel, conversation_id, channel_message_id);
        CREATE INDEX IF NOT EXISTS idx_intake_correlation
            ON intake_envelopes(correlation_id, received_at);
        CREATE INDEX IF NOT EXISTS idx_intake_expiry
            ON intake_envelopes(canonical_expires_at)
            WHERE canonical_expires_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS channel_identities (
            id TEXT PRIMARY KEY,
            realm_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            channel_user_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            verified_at TEXT NOT NULL,
            revoked_at TEXT,
            payload TEXT NOT NULL,
            UNIQUE(realm_id, channel, channel_user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_channel_identities_principal
            ON channel_identities(realm_id, principal_id, channel);

        CREATE TABLE IF NOT EXISTS intake_events (
            id TEXT PRIMARY KEY,
            realm_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            action TEXT NOT NULL,
            actor_principal TEXT NOT NULL,
            authority_instance_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            version INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(realm_id, idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS idx_intake_events_entity
            ON intake_events(entity_type, entity_id, version);

        CREATE TABLE IF NOT EXISTS intake_link_challenges (
            code_hash TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            realm_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_intake_link_expiry
            ON intake_link_challenges(expires_at);
        """
    )


def _clean_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in payload.items()
        if not str(key).startswith("_")
    }


def _existing_payload(conn, table: str, entity_id: str) -> dict[str, Any]:
    row = conn.execute(
        f"SELECT payload FROM {table} WHERE id=?", (entity_id,)
    ).fetchone()
    return json.loads(row["payload"]) if row else {}


def apply_intake_event(projection, event) -> None:
    payload = _clean_payload(dict(event.payload))
    entity_id = str(payload.get("id") or "")
    if not entity_id:
        return
    if event.type == EventType.INTAKE_ENVELOPE_UPSERTED:
        _apply_envelope(projection, event, payload)
        entity_type = "intake"
    elif event.type == EventType.CHANNEL_IDENTITY_UPSERTED:
        _apply_identity(projection, event, payload)
        entity_type = "channel_identity"
    else:
        return
    with projection._conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO intake_events
                (id, realm_id, entity_type, entity_id, action, actor_principal,
                 authority_instance_id, idempotency_key, version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.realm_id,
                entity_type,
                entity_id,
                str(event.payload.get("_event_action") or f"{entity_type}.updated"),
                event.author_principal,
                event.author_instance,
                str(event.payload.get("_idempotency_key") or event.id),
                int(payload.get("version") or 1),
                event.timestamp.isoformat(),
            ),
        )


def _apply_envelope(projection, event, payload: dict[str, Any]) -> None:
    with projection._conn() as conn:
        current = _existing_payload(conn, "intake_envelopes", str(payload["id"]))
        current.update(payload)
        sender = current.get("sender") or {}
        thread = current.get("thread") or {}
        security = current.get("security") or {}
        retention = current.get("retention") or {}
        conn.execute(
            """
            INSERT INTO intake_envelopes
                (id, realm_id, channel, direction, kind, correlation_id,
                 channel_message_id, conversation_id, sender_channel_user_id,
                 principal_id, disposition, version, occurred_at, received_at,
                 canonical_expires_at, updated_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                realm_id=excluded.realm_id, channel=excluded.channel,
                direction=excluded.direction, kind=excluded.kind,
                correlation_id=excluded.correlation_id,
                channel_message_id=excluded.channel_message_id,
                conversation_id=excluded.conversation_id,
                sender_channel_user_id=excluded.sender_channel_user_id,
                principal_id=excluded.principal_id,
                disposition=excluded.disposition, version=excluded.version,
                occurred_at=excluded.occurred_at, received_at=excluded.received_at,
                canonical_expires_at=excluded.canonical_expires_at,
                updated_at=excluded.updated_at, payload=excluded.payload
            WHERE excluded.version >= intake_envelopes.version
            """,
            (
                current["id"],
                current.get("realm_id") or event.realm_id,
                current.get("channel", "web"),
                current.get("direction", "inbound"),
                current.get("kind", "message"),
                current.get("correlation_id", current["id"]),
                current.get("channel_message_id", current["id"]),
                thread.get("conversation_id", "unknown"),
                sender.get("channel_user_id", "unknown"),
                sender.get("principal_id"),
                security.get("disposition", "accepted"),
                int(current.get("version") or 1),
                current.get("occurred_at") or event.timestamp.isoformat(),
                current.get("received_at") or event.timestamp.isoformat(),
                retention.get("canonical_expires_at"),
                current.get("updated_at") or event.timestamp.isoformat(),
                json.dumps(current, sort_keys=True, separators=(",", ":")),
            ),
        )


def _apply_identity(projection, event, payload: dict[str, Any]) -> None:
    with projection._conn() as conn:
        current = _existing_payload(conn, "channel_identities", str(payload["id"]))
        current.update(payload)
        conn.execute(
            """
            INSERT INTO channel_identities
                (id, realm_id, channel, channel_user_id, principal_id, version,
                 verified_at, revoked_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                realm_id=excluded.realm_id, channel=excluded.channel,
                channel_user_id=excluded.channel_user_id,
                principal_id=excluded.principal_id, version=excluded.version,
                verified_at=excluded.verified_at, revoked_at=excluded.revoked_at,
                payload=excluded.payload
            WHERE excluded.version >= channel_identities.version
            """,
            (
                current["id"],
                current.get("realm_id") or event.realm_id,
                current["channel"],
                current["channel_user_id"],
                current["principal_id"],
                int(current.get("version") or 1),
                current.get("verified_at") or event.timestamp.isoformat(),
                current.get("revoked_at"),
                json.dumps(current, sort_keys=True, separators=(",", ":")),
            ),
        )


def get_envelope_payload(projection, envelope_id: str) -> dict[str, Any] | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT payload FROM intake_envelopes WHERE id=?", (envelope_id,)
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def list_envelope_payloads(
    projection,
    *,
    realm_id: str | None = None,
    channel: str | None = None,
    correlation_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = "SELECT payload FROM intake_envelopes WHERE 1=1"
    params: list[Any] = []
    if realm_id:
        query += " AND realm_id=?"
        params.append(realm_id)
    if channel:
        query += " AND channel=?"
        params.append(channel)
    if correlation_id:
        query += " AND correlation_id=?"
        params.append(correlation_id)
    query += " ORDER BY received_at DESC LIMIT ?"
    params.append(max(1, min(limit, 1000)))
    with projection._conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [json.loads(row["payload"]) for row in rows]


def find_intake_event_by_idempotency(
    projection, realm_id: str, key: str
) -> dict[str, Any] | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT * FROM intake_events WHERE realm_id=? AND idempotency_key=?",
            (realm_id, key),
        ).fetchone()
    return dict(row) if row else None


def get_identity_payload(
    projection, realm_id: str, channel: str, channel_user_id: str
) -> dict[str, Any] | None:
    with projection._conn() as conn:
        row = conn.execute(
            """
            SELECT payload FROM channel_identities
            WHERE realm_id=? AND channel=? AND channel_user_id=?
            """,
            (realm_id, channel, channel_user_id),
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def get_identity_payload_by_id(projection, binding_id: str) -> dict[str, Any] | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT payload FROM channel_identities WHERE id=?", (binding_id,)
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def list_identity_payloads(
    projection, realm_id: str, principal_id: str
) -> list[dict[str, Any]]:
    with projection._conn() as conn:
        rows = conn.execute(
            """
            SELECT payload FROM channel_identities
            WHERE realm_id=? AND principal_id=? AND revoked_at IS NULL
            """,
            (realm_id, principal_id),
        ).fetchall()
    return [json.loads(row["payload"]) for row in rows]


def put_link_challenge(
    projection,
    *,
    code_hash: str,
    channel: str,
    realm_id: str,
    principal_id: str,
    expires_at: datetime,
) -> None:
    with projection._conn() as conn:
        conn.execute(
            "DELETE FROM intake_link_challenges WHERE expires_at <= ?",
            (datetime.now(UTC).isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO intake_link_challenges
                (code_hash, channel, realm_id, principal_id, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                code_hash,
                channel,
                realm_id,
                principal_id,
                expires_at.isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )


def consume_link_challenge(
    projection, *, code_hash: str, channel: str, now: datetime
) -> dict[str, Any] | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT * FROM intake_link_challenges WHERE code_hash=? AND channel=?",
            (code_hash, channel),
        ).fetchone()
        if not row:
            return None
        record = dict(row)
        if datetime.fromisoformat(record["expires_at"]) <= now:
            conn.execute(
                "DELETE FROM intake_link_challenges WHERE code_hash=?", (code_hash,)
            )
            return None
        if int(record["attempts"]) >= 5:
            conn.execute(
                "DELETE FROM intake_link_challenges WHERE code_hash=?", (code_hash,)
            )
            return None
        conn.execute(
            "DELETE FROM intake_link_challenges WHERE code_hash=?", (code_hash,)
        )
        return record


def expired_envelope_ids(projection, now: datetime, *, limit: int = 500) -> list[str]:
    with projection._conn() as conn:
        rows = conn.execute(
            """
            SELECT id FROM intake_envelopes
            WHERE canonical_expires_at IS NOT NULL AND canonical_expires_at <= ?
              AND disposition != 'redacted'
            ORDER BY canonical_expires_at LIMIT ?
            """,
            (now.isoformat(), max(1, min(limit, 5000))),
        ).fetchall()
    return [str(row["id"]) for row in rows]


def referenced_blob_digests(projection) -> set[str]:
    digests: set[str] = set()
    with projection._conn() as conn:
        intake_rows = conn.execute("SELECT payload FROM intake_envelopes").fetchall()
        card_rows = conn.execute("SELECT attachments FROM cards").fetchall()
    for row in intake_rows:
        payload = json.loads(row["payload"])
        if raw := payload.get("raw_payload_sha256"):
            digests.add(str(raw))
        for artifact in payload.get("artifacts") or []:
            if digest := artifact.get("sha256"):
                digests.add(str(digest))
    for row in card_rows:
        for artifact in json.loads(row["attachments"] or "[]"):
            if digest := artifact.get("sha256"):
                digests.add(str(digest))
    return digests
