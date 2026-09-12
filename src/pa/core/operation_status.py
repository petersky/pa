"""Passive operation reads and separately owned, bounded reconciliation.

The journal owns only reconciliation requests, never an operation's outcome.
Canonical history and dispatch/restart receipts retain their existing owners.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout, BlockingQueueFull


class OperationStatusService:
    def __init__(self, data_dir: Path, *, capacity: int = 32):
        self.path = data_dir / "operation_reconciliation.db"
        self.capacity = capacity
        self.reads = AsyncRuntime(max_workers=2, max_queue=8, default_timeout=0.5)
        self.repairs = AsyncRuntime(max_workers=1, max_queue=capacity, default_timeout=120)
        self.tasks: dict[str, asyncio.Task] = {}
        self.closing = False
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""CREATE TABLE IF NOT EXISTS reconciliation (
                id TEXT PRIMARY KEY, realm TEXT NOT NULL, owner TEXT NOT NULL,
                operation_key TEXT NOT NULL, state TEXT NOT NULL,
                error TEXT, result TEXT, retry_at REAL NOT NULL DEFAULT 0, revision TEXT)""")
            conn.execute("CREATE INDEX IF NOT EXISTS reconciliation_state ON reconciliation(state)")
            conn.execute("UPDATE reconciliation SET state='interrupted' WHERE state IN ('queued','running')")

    @contextmanager
    def _connect(self, *, readonly=False):
        # Optional observations must not spend a known receipt's response budget
        # waiting for this auxiliary writer. Explicit admission retains its wait.
        conn = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0) if readonly else sqlite3.connect(self.path, timeout=0.05)
        try:
            yield conn
            if not readonly:
                conn.commit()
        except sqlite3.OperationalError as exc:
            raise BlockingOperationTimeout("operation reconciliation storage is unavailable") from exc
        finally:
            conn.close()

    @staticmethod
    def job_id(owner: str, realm: str, key: str) -> str:
        identity = json.dumps([owner, realm, key], separators=(",", ":"))
        return hashlib.sha256(identity.encode()).hexdigest()

    def read_job(self, owner: str, realm: str, key: str) -> dict | None:
        with self._connect(readonly=True) as conn:
            row = conn.execute("SELECT state,result,error,retry_at FROM reconciliation WHERE id=?",
                               (self.job_id(owner, realm, key),)).fetchone()
        return {"id": self.job_id(owner, realm, key), "accepted": True, "state": row[0], "result": json.loads(row[1]) if row[1] else None, "error": row[2], "retry_at": row[3]} if row else None

    def admission(self, owner: str, realm: str, key: str, revision: str | None = None) -> dict:
        job_id = self.job_id(owner, realm, key)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state,error,result,retry_at,revision FROM reconciliation WHERE id=?", (job_id,)).fetchone()
            if row is None:
                count = conn.execute("SELECT count(*) FROM reconciliation WHERE state IN ('queued','running','interrupted','waiting')").fetchone()[0]
                if count >= self.capacity or self.closing:
                    raise BlockingQueueFull("operation reconciliation capacity unavailable")
                conn.execute("INSERT INTO reconciliation VALUES (?,?,?,?, 'queued',NULL,NULL,0,?)", (job_id, realm, owner, key, revision))
                row = ('queued', None, None, 0, revision)
            elif row[0] in {"completed", "failed", "waiting"} and row[4] != revision:
                if row[0] != "waiting":
                    count = conn.execute("SELECT count(*) FROM reconciliation WHERE state IN ('queued','running','interrupted','waiting')").fetchone()[0]
                    if count >= self.capacity or self.closing:
                        raise BlockingQueueFull("operation reconciliation capacity unavailable")
                conn.execute("UPDATE reconciliation SET state='queued',error=NULL,result=NULL,retry_at=0,revision=? WHERE id=?", (revision, job_id))
                row = ('queued', None, None, 0, revision)
        return {"id": job_id, "accepted": True, "state": row[0], "error": row[1],
                "result": json.loads(row[2]) if row[2] else None, "retry_at": row[3]}

    def _record(self, job_id, state, *, result=None, error=None):
        if isinstance(result, dict):
            result = {key: result[key] for key in ("status", "durable", "recovery_state", "lookup_head") if key in result}
        with self._connect() as conn:
            conn.execute("UPDATE reconciliation SET state=?,result=?,error=?,retry_at=? WHERE id=?",
                         (state, json.dumps(result) if result is not None else None, error,
                          time.time() + 5 if state in {"waiting", "failed"} else 0, job_id))

    def schedule(self, job: dict, factory) -> None:
        """Coalesce before worker admission. Only explicit recovery admission starts this task."""
        job_id = job["id"]
        if (self.closing or job_id in self.tasks or job["state"] == "completed"
                or job.get("retry_at", 0) > time.time()):
            return
        if len(self.tasks) >= self.capacity:
            return  # Durable queued receipt remains truthful and can be resumed.

        async def owned():
            try:
                await self.reads.run_blocking("operation.repair_start", self._record, job_id, "running", wait_for_completion=True)
                result = await factory(self.repairs)
                state = "waiting" if result is None or (isinstance(result, dict) and result.get("status") in {"pending", "retryable", "resumable"}) else "completed"
                await self.reads.run_blocking("operation.repair_finish", self._record, job_id, state, result=result, wait_for_completion=True)
            except Exception as exc:
                await self.reads.run_blocking("operation.repair_failure", self._record, job_id, "failed", error=type(exc).__name__, wait_for_completion=True)

        task = asyncio.create_task(owned(), name=f"pa-operation-repair:{job_id}")
        self.tasks[job_id] = task

        def finished(done):
            self.tasks.pop(job_id, None)
            if not done.cancelled():
                done.exception()
        task.add_done_callback(finished)

    async def close(self):
        self.closing = True
        if self.tasks:
            await asyncio.wait(set(self.tasks.values()), timeout=0.1)
        await self.repairs.close(drain_timeout=0.1)
        await self.reads.close(drain_timeout=0.1)
