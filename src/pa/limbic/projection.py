from __future__ import annotations

import json


def init_limbic_schema(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS limbic_signals (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, event_class TEXT NOT NULL,
            subject_type TEXT NOT NULL, subject_id TEXT NOT NULL,
            dedupe_key TEXT NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL,
            UNIQUE(realm_id, dedupe_key)
        );
        CREATE INDEX IF NOT EXISTS idx_limbic_signals_realm_time
            ON limbic_signals(realm_id, received_at DESC);
        CREATE TABLE IF NOT EXISTS limbic_appraisals (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, signal_id TEXT NOT NULL,
            path TEXT NOT NULL, evaluator_version TEXT NOT NULL,
            created_at TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_limbic_appraisals_signal
            ON limbic_appraisals(signal_id, created_at DESC);
        CREATE TABLE IF NOT EXISTS memory_records (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, tier TEXT NOT NULL,
            goal_id TEXT, subject TEXT NOT NULL, predicate TEXT NOT NULL,
            owner_principal TEXT NOT NULL, sensitivity TEXT NOT NULL,
            contradiction INTEGER NOT NULL, superseded_by TEXT, expires_at TEXT,
            version INTEGER NOT NULL, updated_at TEXT NOT NULL, payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_memory_scope
            ON memory_records(realm_id, goal_id, tier, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_fact
            ON memory_records(realm_id, subject, predicate);
        CREATE TABLE IF NOT EXISTS memory_events (
            id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, record_id TEXT NOT NULL,
            event_type TEXT NOT NULL, actor_principal TEXT NOT NULL,
            authority_instance_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
            created_at TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '{}',
            UNIQUE(realm_id, idempotency_key)
        );
        """
    )


def apply_limbic_event(projection, event) -> None:
    signal = event.payload.get("signal") or {}
    appraisal = event.payload.get("appraisal") or {}
    route = event.payload.get("route") or {}
    if not signal.get("id") or not appraisal.get("id"):
        return
    combined = {"appraisal": appraisal, "route": route}
    with projection._conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO limbic_signals
                (id, realm_id, event_class, subject_type, subject_id, dedupe_key,
                 received_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal["id"], event.realm_id, signal["event_class"],
                signal["subject_type"], signal["subject_id"], signal["dedupe_key"],
                signal["received_at"], json.dumps(signal),
            ),
        )
        canonical = conn.execute(
            "SELECT id FROM limbic_signals WHERE realm_id=? AND dedupe_key=?",
            (event.realm_id, signal["dedupe_key"]),
        ).fetchone()
        if not canonical or canonical["id"] != signal["id"]:
            return
        conn.execute(
            """
            INSERT OR IGNORE INTO limbic_appraisals
                (id, realm_id, signal_id, path, evaluator_version, created_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                appraisal["id"], event.realm_id, signal["id"], route.get("path", ""),
                appraisal.get("evaluator_version", "unknown"),
                appraisal.get("created_at", event.timestamp.isoformat()),
                json.dumps(combined),
            ),
        )


def apply_memory_event(projection, event) -> None:
    records = event.payload.get("records") or []
    record_event = event.payload.get("memory_event") or {}
    if not records:
        return
    with projection._conn() as conn:
        key = record_event.get("idempotency_key", event.id)
        if conn.execute(
            "SELECT 1 FROM memory_events WHERE realm_id=? AND idempotency_key=?",
            (event.realm_id, key),
        ).fetchone():
            return
        for record in records:
            conn.execute(
                """
                INSERT INTO memory_records
                    (id, realm_id, tier, goal_id, subject, predicate, owner_principal,
                     sensitivity, contradiction, superseded_by, expires_at, version,
                     updated_at, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    tier=excluded.tier, goal_id=excluded.goal_id,
                    subject=excluded.subject, predicate=excluded.predicate,
                    owner_principal=excluded.owner_principal,
                    sensitivity=excluded.sensitivity,
                    contradiction=excluded.contradiction,
                    superseded_by=excluded.superseded_by,
                    expires_at=excluded.expires_at, version=excluded.version,
                    updated_at=excluded.updated_at, payload=excluded.payload
                WHERE excluded.version >= memory_records.version
                """,
                (
                    record["id"], event.realm_id, record["tier"], record.get("goal_id"),
                    record["subject"], record["predicate"], record["owner_principal"],
                    record["sensitivity"], int(record.get("contradiction", False)),
                    record.get("superseded_by"), record.get("expires_at"),
                    int(record.get("version", 1)), record["updated_at"],
                    json.dumps(record),
                ),
            )
        primary = records[-1]
        conn.execute(
            """
            INSERT OR IGNORE INTO memory_events
                (id, realm_id, record_id, event_type, actor_principal,
                 authority_instance_id, idempotency_key, created_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id, event.realm_id, primary["id"],
                record_event.get("event_type", "memory.recorded"),
                record_event.get("actor_principal", event.author_principal),
                record_event.get("authority_instance_id", event.author_instance),
                key,
                event.timestamp.isoformat(), json.dumps(record_event.get("payload") or {}),
            ),
        )


def find_signal_by_dedupe(projection, realm_id: str, dedupe_key: str) -> dict | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT id, payload FROM limbic_signals WHERE realm_id=? AND dedupe_key=?",
            (realm_id, dedupe_key),
        ).fetchone()
        if not row:
            return None
        appraisal = conn.execute(
            "SELECT payload FROM limbic_appraisals WHERE signal_id=? ORDER BY created_at DESC LIMIT 1",
            (row["id"],),
        ).fetchone()
    return {
        "signal": json.loads(row["payload"]),
        **(json.loads(appraisal["payload"]) if appraisal else {}),
    }


def limbic_operations(projection, realm_id: str, limit: int = 500) -> dict:
    """Return bounded, content-free rollout metrics for operator inspection."""

    with projection._conn() as conn:
        rows = conn.execute(
            "SELECT payload FROM limbic_appraisals WHERE realm_id=? "
            "ORDER BY created_at DESC LIMIT ?",
            (realm_id, max(1, min(limit, 5_000))),
        ).fetchall()
    samples = [json.loads(row["payload"]) for row in rows]
    durations = sorted(float(item.get("duration_ms") or 0) for item in samples)
    diagnostics = [
        str(diag.get("code"))
        for item in samples
        for diag in (item.get("appraisal") or {}).get("diagnostics", [])
    ]
    hits = [int(item.get("retrieval_hits") or 0) for item in samples]
    usefulness = [
        float(item["usefulness_score"])
        for item in samples
        if item.get("usefulness_score") is not None
    ]
    bypasses = [
        (item.get("appraisal") or {}).get("deterministic_bypass") for item in samples
    ]
    return {
        "realm_id": realm_id,
        "sample_count": len(samples),
        "shadow_count": sum(bool(item.get("shadow_mode")) for item in samples),
        "latency_ms": {
            "average": sum(durations) / len(durations) if durations else None,
            "p95": durations[min(len(durations) - 1, int(len(durations) * 0.95))]
            if durations else None,
        },
        "timeouts": diagnostics.count("provider_timeout"),
        "fallbacks": sum(code.startswith("provider_") for code in diagnostics),
        "spoof_attempts": diagnostics.count("control_provenance_spoof"),
        "privileged_bypasses": sum(bool(value) for value in bypasses),
        "retrieval": {
            "hit_count": sum(value > 0 for value in hits),
            "records": sum(hits),
        },
        "usefulness": {
            "sample_count": len(usefulness),
            "average": sum(usefulness) / len(usefulness) if usefulness else None,
        },
        "promotion_candidates": sum(
            bool((item.get("signal") or {}).get("metadata", {}).get("promotion_candidate"))
            for item in samples
        ),
        "evaluator_versions": sorted(
            {
                str((item.get("appraisal") or {}).get("evaluator_version"))
                for item in samples
                if (item.get("appraisal") or {}).get("evaluator_version")
            }
        ),
    }


def get_memory_payload(projection, record_id: str) -> dict | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT payload FROM memory_records WHERE id=?", (record_id,)
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def list_memory_payloads(projection, realm_id: str) -> list[dict]:
    with projection._conn() as conn:
        rows = conn.execute(
            "SELECT payload FROM memory_records WHERE realm_id=? ORDER BY updated_at DESC",
            (realm_id,),
        ).fetchall()
    return [json.loads(row["payload"]) for row in rows]


def find_memory_event(projection, realm_id: str, key: str) -> dict | None:
    with projection._conn() as conn:
        row = conn.execute(
            "SELECT record_id FROM memory_events WHERE realm_id=? AND idempotency_key=?",
            (realm_id, key),
        ).fetchone()
    return dict(row) if row else None
