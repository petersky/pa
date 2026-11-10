"""Cancellation belongs to one owner and ends before its connection closes."""
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from unittest.mock import patch

import pytest

from pa.core.sqlite_maintenance import DatabaseMaintenanceGate
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.maintenance import _maintain_database


class Handle:
    def __init__(self, *, fail=False):
        self.called = threading.Event()
        self.calls = 0
        self.closed = False
        self.fail = fail

    def interrupt(self):
        assert not self.closed
        self.calls += 1
        self.called.set()
        if self.fail:
            raise RuntimeError("fixture observer failure")


def test_writer_before_registration_and_nested_intent_stays_latched():
    gate, handle = DatabaseMaintenanceGate(), Handle()
    with gate.writer():
        with gate.maintenance(handle) as ticket:
            assert ticket.reason(None) == "foreground_write"
    with gate.maintenance(handle) as ticket:
        assert ticket.reason(None) is None
        with gate.writer(), gate.writer():
            assert ticket.reason(None) == "foreground_write"
        assert ticket.reason(None) == "foreground_write"
    assert gate._writers == 0 and not gate._tickets


@pytest.mark.parametrize("shutdown", [False, True])
def test_request_after_registration_interrupts_only_live_owned_handle(shutdown):
    gate, handle, stop = DatabaseMaintenanceGate(), Handle(), threading.Event()
    with gate.maintenance(handle, stop=stop) as ticket:
        if shutdown:
            stop.set()
            assert handle.called.wait(2)
            assert ticket.reason(stop) == "stopping"
        else:
            with gate.writer():
                assert handle.called.wait(2)
            assert ticket.reason(stop) == "foreground_write"
    # Joining is the oracle: future writers cannot reach this closed handle.
    handle.closed = True
    count = handle.calls
    with gate.writer():
        pass
    assert handle.calls == count and not gate._tickets
    assert not any(t.name == "pa-maintenance-interrupt" for t in threading.enumerate())


def test_other_owner_and_read_only_calls_do_not_request_interruption():
    first, second, handle = DatabaseMaintenanceGate(), DatabaseMaintenanceGate(), Handle()
    with first.maintenance(handle) as ticket:
        with second.writer():
            assert ticket.reason(None) is None
        assert handle.calls == 0


@pytest.mark.parametrize("construction", [False, True])
def test_observer_creation_or_start_failure_does_not_publish_a_stale_ticket(construction):
    gate = DatabaseMaintenanceGate()
    target = "pa.core.sqlite_maintenance.threading.Thread" + ("" if construction else ".start")
    with patch(target, side_effect=RuntimeError("fixture thread failure")):
        with pytest.raises(RuntimeError, match="fixture thread failure"):
            with gate.maintenance(Handle()):
                pytest.fail("failed observer cannot own maintenance")
    assert not gate._tickets and gate._writers == 0
    with gate.writer():
        pass


@pytest.mark.parametrize("failed_body", [False, True])
def test_observer_failure_is_not_silent_and_does_not_mask_primary_error(failed_body):
    gate, handle = DatabaseMaintenanceGate(), Handle(fail=True)
    with pytest.raises(ValueError if failed_body else RuntimeError, match="primary|observer failed"):
        with gate.maintenance(handle):
            with gate.writer():
                assert handle.called.wait(2)
            if failed_body:
                raise ValueError("primary failure")
    assert gate._writers == 0 and not gate._tickets


def test_concurrent_waiters_and_failed_writer_release_intent():
    gate, entered, release = DatabaseMaintenanceGate(), threading.Barrier(5), threading.Event()
    def writer():
        with gate.writer():
            entered.wait(timeout=2)
            assert release.wait(2)
            raise ValueError("fixture write failure")
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(writer) for _ in range(4)]
        entered.wait(timeout=2)
        with gate.maintenance(Handle()) as ticket:
            assert ticket.reason(None) == "foreground_write"
        release.set()
        for future in futures:
            with pytest.raises(ValueError):
                future.result(timeout=2)
    with gate.maintenance(Handle()) as ticket:
        assert ticket.reason(None) is None


@pytest.mark.parametrize("transcript", [False, True])
def test_real_owner_mutation_announces_before_existing_lock_and_read_stays_passive(tmp_path, transcript):
    store = CardProjection(tmp_path / "fixture.db")
    owner = store.transcripts if transcript else store
    handle = Handle()
    try:
        with owner.maintenance.maintenance(handle) as ticket:
            if transcript:
                owner.list("session")
            else:
                store.get_session("session")
            assert ticket.reason(None) is None
            # The real mutation boundary still owns its original lock. Observe
            # intent on acquisition, without replacing that lock or SQL.
            from pa.core.operation_budget import measured_lock
            def lock_after_intent(*args, **kwargs):
                assert ticket.reason(None) == "foreground_write"
                return measured_lock(*args, **kwargs)
            module = "pa.domain.transcript_storage" if transcript else "pa.domain.projection"
            with patch(module + ".measured_lock", side_effect=lock_after_intent):
                if transcript:
                    owner.append([TranscriptEvent(session_id="session", seq=1, event_type="output", payload={"text": "kept"})])
                else:
                    store.save_session(AgentSession(id="session", agent_name="codex"))
    finally:
        store.close_thread_connection()


def test_pending_writer_defers_then_quiet_retry_preserves_rows_and_reclaims(tmp_path):
    path, gate = tmp_path / "fixture.db", DatabaseMaintenanceGate()
    with closing(sqlite3.connect(path)) as seed, seed:
        seed.execute("PRAGMA journal_mode=WAL")
        seed.execute("CREATE TABLE retained(id INTEGER PRIMARY KEY, body BLOB)")
        seed.executemany("INSERT INTO retained VALUES (?,zeroblob(10000))", ((i,) for i in range(100)))
        seed.execute("DELETE FROM retained WHERE id!=1")
    inode = path.stat().st_ino
    with gate.writer():
        result = _maintain_database(path, writers=gate)
    assert result["status"] == "deferred" and result["reason"] == "foreground_write"
    assert not result["compacted"]
    result = _maintain_database(path, writers=gate)
    assert result["status"] == "ok" and result["compacted"] and result["quick_check"] == "ok"
    assert result["after"]["bytes"] < result["before"]["bytes"]
    assert result["after"]["free_bytes"] == 0 and path.stat().st_ino == inode
    with closing(sqlite3.connect(path)) as verify:
        assert verify.execute("SELECT id,length(body) FROM retained").fetchall() == [(1, 10000)]
