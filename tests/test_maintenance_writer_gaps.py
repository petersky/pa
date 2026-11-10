"""Explicit uncovered writers and initially busy rewrites must retain ownership."""
import sqlite3
from contextlib import closing

import pytest

from pa.config import Settings
from pa.domain.models import KnowledgeEntry, KnowledgeUpdate, KnowledgeAuditEvent
from pa.domain.projection import CardProjection
from pa.instance.maintenance import InstanceMaintenanceService


class Handle:
    def interrupt(self):
        pass


@pytest.mark.parametrize("operation", ["add", "update", "audit"])
def test_knowledge_mutations_announce_to_owner_and_preserve_values(tmp_path, operation):
    store = CardProjection(tmp_path / "fixture.db")
    original = store.add_knowledge(KnowledgeEntry(id="kept", summary="original"))
    try:
        with store.maintenance.maintenance(Handle()) as ticket:
            assert store.get_knowledge("kept").summary == "original"
            assert ticket.reason(None) is None
            if operation == "add":
                created = store.add_knowledge(KnowledgeEntry(id="new", summary="new value"))
                assert store.get_knowledge(created.id).summary == "new value"
            elif operation == "update":
                updated = store.update_knowledge(original.id, KnowledgeUpdate(summary="changed"))
                assert store.get_knowledge(updated.id).summary == "changed"
            else:
                audit = KnowledgeAuditEvent(id="audit", knowledge_id="kept", action="read", actor="fixture", payload={"kept": True})
                store.add_knowledge_audit(audit)
                store.add_knowledge_audit(audit)
                values = store.list_knowledge_audit("kept")
                assert len(values) == 1 and values[0] == audit
            assert ticket.reason(None) == "foreground_write"
    finally:
        store.close_thread_connection()


@pytest.mark.asyncio
async def test_initial_busy_reader_keeps_reclamation_pending_until_quiet_retry(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = CardProjection(settings.db_path)
    with closing(sqlite3.connect(store.db_path)) as seed, seed:
        seed.execute("CREATE TABLE retained_fixture(id INTEGER PRIMARY KEY, body BLOB)")
        seed.executemany("INSERT INTO retained_fixture VALUES (?,zeroblob(10000))", ((i,) for i in range(100)))
        seed.execute("DELETE FROM retained_fixture WHERE id!=1")
    service = InstanceMaintenanceService(settings, store, {})
    try:
        with closing(sqlite3.connect(store.db_path)) as reader:
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM retained_fixture").fetchall()
            with closing(sqlite3.connect(store.db_path)) as writer, writer:
                writer.execute("INSERT INTO retained_fixture VALUES (2,zeroblob(10000))")
            initial = await service.run_once()
            assert initial["sqlite"]["status"] == "deferred"
            assert initial["sqlite"]["reason"] == "busy"
            assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
            reader.rollback()
        retried = await service._run_once(database_only=True)
        assert retried["sqlite"]["status"] == "ok" and retried["sqlite"]["compacted"]
        assert retried["sqlite"]["after"]["free_bytes"] == 0
        assert retried["sqlite"]["after"]["bytes"] < retried["sqlite"]["before"]["bytes"]
        assert service.snapshot()["pending_database_compactions"] == []
        with closing(sqlite3.connect(store.db_path)) as verify:
            assert verify.execute("SELECT id,length(body) FROM retained_fixture ORDER BY id").fetchall() == [(1,10000),(2,10000)]
    finally:
        await service.close()
        store.close_thread_connection()
