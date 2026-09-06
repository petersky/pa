"""Instance-local policy, catalog and auditable selection evidence storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from pa.execution.selection import OutcomeEvidence, SelectionError, SelectionPolicy


class SelectionStore:
    def __init__(self, data_dir: Path):
        self.path = data_dir / "execution_selection.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS policy (scope TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS decisions (id TEXT PRIMARY KEY, realm TEXT NOT NULL, principal TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS evidence (id TEXT PRIMARY KEY, decision_id TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS catalog (scope TEXT PRIMARY KEY, payload TEXT NOT NULL, observed_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS bindings (scope TEXT PRIMARY KEY, decision_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, decision_id TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS policy_history (scope TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(scope, revision));
                CREATE TABLE IF NOT EXISTS connections (id TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fallback_attempts (id TEXT PRIMARY KEY, decision_id TEXT NOT NULL, prompt_digest TEXT NOT NULL, ordinal INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS prompt_bindings (id TEXT PRIMARY KEY, prompt_digest TEXT NOT NULL, attempts INTEGER NOT NULL);
            """)

    def connect(self):
        return sqlite3.connect(self.path, timeout=20)

    def policy(self, realm: str) -> SelectionPolicy:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload FROM policy WHERE scope=?", (realm,)
            ).fetchone()
        return SelectionPolicy.model_validate_json(row[0]) if row else SelectionPolicy()

    def connections(self):
        from pa.execution.selection_connections import ConnectionProfile

        with self.connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM connections ORDER BY id LIMIT 20"
            ).fetchall()
        return [ConnectionProfile.model_validate_json(r[0]) for r in rows]

    def save_connection(self, profile, expected_revision: int):
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision FROM connections WHERE id=?", (profile.id,)
            ).fetchone()
            revision = row[0] if row else 0
            count = conn.execute("SELECT count(*) FROM connections").fetchone()[0]
            if revision != expected_revision:
                raise SelectionError(
                    "stale_connection",
                    "Connection changed; refresh its revision before saving",
                )
            if not row and count >= 20:
                raise SelectionError(
                    "connection_limit",
                    "At most 20 named connection profiles are supported per instance",
                )
            profile = profile.model_copy(update={"revision": revision + 1})
            conn.execute(
                "INSERT OR REPLACE INTO connections VALUES (?, ?, ?)",
                (profile.id, profile.revision, profile.model_dump_json()),
            )
            conn.execute(
                "DELETE FROM catalog WHERE scope=?", ("connection:" + profile.id,)
            )
            # Composite instance caches must not keep an old account/endpoint
            # usable after its profile changes or is disabled.
            for scope, payload in conn.execute(
                "SELECT scope, payload FROM catalog"
            ).fetchall():
                rows = json.loads(payload)
                filtered = [r for r in rows if r.get("connection") != profile.id]
                if len(rows) != len(filtered):
                    conn.execute(
                        "UPDATE catalog SET payload=? WHERE scope=?",
                        (json.dumps(filtered), scope),
                    )
        return profile

    def save_policy(
        self, realm: str, policy: SelectionPolicy, expected_revision: int
    ) -> SelectionPolicy:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision FROM policy WHERE scope=?", (realm,)
            ).fetchone()
            current = row[0] if row else 1
            if current != expected_revision:
                raise SelectionError(
                    "stale_policy",
                    "Policy changed; refresh its revision before saving.",
                )
            policy = policy.model_copy(update={"revision": current + 1})
            conn.execute(
                "INSERT OR REPLACE INTO policy VALUES (?, ?, ?)",
                (realm, policy.revision, policy.model_dump_json()),
            )
            conn.execute(
                "INSERT INTO policy_history VALUES (?, ?, ?)",
                (realm, policy.revision, policy.model_dump_json()),
            )
        return policy

    def binding(self, scope: str, realm: str, principal: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT decision_id FROM bindings WHERE scope=?", (scope,)
            ).fetchone()
        return self.decision(row[0], realm, principal) if row else None

    def bind(self, scope: str, decision_id: str, *, replace: bool = False) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bindings VALUES (?, ?)"
                if replace
                else "INSERT OR IGNORE INTO bindings VALUES (?, ?)",
                (scope, decision_id),
            )

    def record_attempt(self, attempt_id: str, decision_id: str, payload: dict) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT decision_id FROM attempts WHERE id=?", (attempt_id,)
            ).fetchone()
            if prior and prior[0] != decision_id:
                raise SelectionError(
                    "attempt_identity_conflict",
                    "Attempt ID already belongs to another selection",
                )
            conn.execute(
                "INSERT OR REPLACE INTO attempts VALUES (?, ?, ?)",
                (attempt_id, decision_id, json.dumps(payload)),
            )

    def attempt(self, attempt_id: str, decision_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload FROM attempts WHERE id=? AND decision_id=?",
                (attempt_id, decision_id),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def reserve_fallback(
        self, receipt, alternative, *, key: str, prompt_digest: str, limit: int
    ):
        from pa.execution.selection import authorize_fallback, digest

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT decision_id, prompt_digest, payload FROM fallback_attempts WHERE id=?",
                (key,),
            ).fetchone()
            if prior:
                if prior[:2] != (receipt["decision_id"], prompt_digest):
                    raise SelectionError(
                        "fallback_identity_conflict",
                        "The fallback request belongs to a different prompt or selection",
                    )
                return json.loads(prior[2])
            count = (
                conn.execute(
                    "SELECT count(*) FROM fallback_attempts WHERE decision_id=?",
                    (receipt["decision_id"],),
                ).fetchone()[0]
                + 1
            )
            if count > limit:
                raise SelectionError(
                    "fallback_budget_exhausted",
                    "The preauthorized fallback attempt budget is exhausted; operator action is required",
                )
            authorize_fallback(
                receipt,
                alternative,
                attempt=count,
                prompt_digest=prompt_digest,
                original_prompt_digest=prompt_digest,
            )
            result = {
                **receipt,
                "selected": alternative["selected"],
                "candidate_key": alternative["candidate_key"],
                "recovery": {
                    "original_decision_id": receipt["decision_id"],
                    "attempt": count,
                    "idempotency_key": key,
                    "prompt_digest": prompt_digest,
                    "context_boundary": True,
                },
                "explanation": receipt["explanation"]
                + " A bounded preauthorized linked recovery attempt was reserved; native context continuity is not assumed.",
            }
            result["decision_id"] = digest(
                {k: v for k, v in result.items() if k != "decision_id"}
            )
            conn.execute(
                "INSERT INTO fallback_attempts VALUES (?, ?, ?, ?, ?)",
                (key, receipt["decision_id"], prompt_digest, count, json.dumps(result)),
            )
            return result

    def attempts(
        self, decision_id: str, *, offset: int = 0, limit: int = 100
    ) -> list[dict]:
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("Attempt pages require offset >= 0 and limit 1..100")
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM attempts WHERE decision_id=? ORDER BY rowid LIMIT ? OFFSET ?",
                (decision_id, limit, offset),
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def attempt_count(self, decision_id: str) -> int:
        with self.connect() as conn:
            return conn.execute(
                "SELECT count(*) FROM attempts WHERE decision_id=?", (decision_id,)
            ).fetchone()[0]

    def begin_prompt(
        self, *, session_id: str, prompt_id: str, prompt_digest: str, receipt: dict
    ):
        key = f"session:{session_id}:prompt:{prompt_id}"
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT prompt_digest, attempts FROM prompt_bindings WHERE id=?", (key,)
            ).fetchone()
            if prior and prior[0] != prompt_digest:
                raise SelectionError(
                    "prompt_identity_changed",
                    "Retry content differs from the original prompt identity; create a distinct prompt instead",
                )
            attempt = prior[1] + 1 if prior else 1
            conn.execute(
                "INSERT OR REPLACE INTO prompt_bindings VALUES (?, ?, ?)",
                (key, prompt_digest, attempt),
            )
            attempt_id = f"{key}:attempt:{attempt}"
            payload = {
                "id": attempt_id,
                "session_id": session_id,
                "prompt_id": prompt_id,
                "prompt_digest": prompt_digest,
                "selected": receipt["selected"],
                "state": "started",
                "at": datetime.now(UTC).isoformat(),
            }
            conn.execute(
                "INSERT INTO attempts VALUES (?, ?, ?)",
                (attempt_id, receipt["decision_id"], json.dumps(payload)),
            )
            return attempt_id

    def save_decision(self, receipt: dict, realm: str, principal: str) -> dict:
        payload = json.dumps(receipt, sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO decisions VALUES (?, ?, ?, ?, ?)",
                (
                    receipt["decision_id"],
                    realm,
                    principal,
                    payload,
                    datetime.now(UTC).isoformat(),
                ),
            )
            row = conn.execute(
                "SELECT payload, realm, principal FROM decisions WHERE id=?",
                (receipt["decision_id"],),
            ).fetchone()
            if row != (payload, realm, principal):
                raise SelectionError(
                    "decision_identity_conflict",
                    "Decision ID already belongs to another input or principal.",
                )
        return receipt

    def decision(self, decision_id: str, realm: str, principal: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload FROM decisions WHERE id=? AND realm=? AND principal=?",
                (decision_id, realm, principal),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def record_evidence(
        self, evidence: OutcomeEvidence, realm: str, principal: str
    ) -> OutcomeEvidence:
        decision = self.decision(evidence.decision_id, realm, principal)
        if not decision or decision["candidate_key"] != evidence.candidate_key:
            raise SelectionError(
                "evidence_provenance_mismatch",
                "Evidence must refer to an owned decision and its selected tuple.",
            )
        with self.connect() as conn:
            payload = evidence.model_dump_json()
            existing = conn.execute(
                "SELECT payload FROM evidence WHERE id=?", (evidence.id,)
            ).fetchone()
            if existing and existing[0] != payload:
                raise SelectionError(
                    "evidence_conflict",
                    "Evidence IDs are immutable; append a distinct observation.",
                )
            conn.execute(
                "INSERT OR IGNORE INTO evidence VALUES (?, ?, ?)",
                (evidence.id, evidence.decision_id, payload),
            )
        return evidence

    def evidence(self, realm: str, principal: str) -> list[OutcomeEvidence]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT e.payload FROM evidence e JOIN decisions d ON d.id=e.decision_id WHERE d.realm=? AND d.principal=? ORDER BY d.created_at DESC LIMIT 1000",
                (realm, principal),
            ).fetchall()
        return [OutcomeEvidence.model_validate_json(row[0]) for row in rows]

    def save_catalog(self, scope: str, payload: list[dict]) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO catalog VALUES (?, ?, ?)",
                (scope, json.dumps(payload), datetime.now(UTC).isoformat()),
            )

    def catalog(self, scope: str) -> tuple[list[dict], datetime | None]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload, observed_at FROM catalog WHERE scope=?", (scope,)
            ).fetchone()
        return (
            (json.loads(row[0]), datetime.fromisoformat(row[1])) if row else ([], None)
        )
