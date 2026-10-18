"""Maintenance observations preserve exact SQL, evidence, pins and failures."""
import asyncio
import json
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import (
    RETENTION_PHASES, capture_retention_phases, retention_phase,
)
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.maintenance import run_maintenance


def seed(root):
    root.mkdir(parents=True, exist_ok=True)
    store = CardProjection(root / "pa.db")
    now = datetime(2026, 10, 1, tzinfo=UTC)
    store.save_session(AgentSession(id="expired-private-id", agent_name="codex", status="closed"))
    store.save_session(AgentSession(id="live", agent_name="codex", status="idle"))
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET updated_at=? WHERE id=?",
                     ((now - timedelta(days=30)).isoformat(), "expired-private-id"))
    store.append_transcript_events([
        TranscriptEvent(session_id="expired-private-id", seq=1, event_type="chunk",
                        created_at=now, payload={"text": "private-fixture-body" * 500}),
        TranscriptEvent(session_id="expired-private-id", seq=2, event_type="turn_completed",
                        created_at=now, payload={"result": "audit"}),
        TranscriptEvent(session_id="live", seq=1, event_type="chunk", created_at=now,
                        payload={"text": "live-fixture-body"}),
    ])
    return store, now


def test_active_capture_adds_no_sql_and_keeps_same_rows_and_audit(tmp_path):
    outcomes=[]; traces=[]
    for enabled in (False, True):
        store, cutoff = seed(tmp_path / str(enabled))
        phases={}; sql=[]
        def observe(original):
            @contextmanager
            def connection(*args, **kwargs):
                with original(*args, **kwargs) as conn:
                    conn.set_trace_callback(lambda statement: sql.append(" ".join(statement.split())))
                    yield conn  # Retain context-manager commit/close SQL too.
            return connection
        with patch.object(store, "_conn", observe(store._conn)), patch.object(
                store.transcripts, "_conn", observe(store.transcripts._conn)):
            with capture_retention_phases(phases) if enabled else nullcontext():
                deleted=store.prune_closed_session_transcripts(before=cutoff)
        assert deleted==2
        assert store.list_transcript_events("expired-private-id")==[]
        assert [e.seq for e in store.list_transcript_events("live")]==[1]
        with store._conn() as conn:
            mirrors=[tuple(r) for r in conn.execute(
                "SELECT session_id,seq,payload FROM agent_transcript_events ORDER BY session_id,seq")]
        with store.transcripts._conn() as conn:
            audit=[tuple(r) for r in conn.execute(
                "SELECT session_id,seq,event_type,payload_hash FROM transcript_evidence ORDER BY seq")]
        outcomes.append((mirrors,audit));traces.append(sql)
        if enabled:
            assert set(phases)==RETENTION_PHASES
            assert all(p["calls"]==1 and p["errors"]==0 for p in phases.values())
            assert all(p["wall_ms"]>=0 and p["thread_cpu_ms"]>=0 for p in phases.values())
            assert not any(value in json.dumps(phases) for value in ("private-id","fixture-body","SELECT","DELETE"))
        else:
            assert phases=={}
    assert traces[0]==traces[1]
    assert outcomes[0]==outcomes[1]


def test_error_is_recorded_without_swallowing_or_leaking_capture(tmp_path):
    store,cutoff=seed(tmp_path)
    phases={}
    interruption=RuntimeError("fixture collection interrupted")
    with capture_retention_phases(phases), patch.object(
            store.transcripts,"collect_unreferenced_objects",side_effect=interruption):
        with pytest.raises(RuntimeError) as caught:
            store.prune_closed_session_transcripts(before=cutoff)
    assert caught.value is interruption
    assert phases["retention_object_collection"]["errors"]==1
    assert phases["retention_canonical"]["errors"]==1
    assert "retention_mirror" not in phases
    previous=json.dumps(phases,sort_keys=True)
    store.prune_closed_session_transcripts(before=cutoff)
    assert json.dumps(phases,sort_keys=True)==previous


def test_pinned_object_bytes_survive_measured_retention(tmp_path):
    store,cutoff=seed(tmp_path)
    digest=next(iter(store.transcripts.referenced_hashes()))
    path=store.transcripts._object_path(digest)
    original=path.read_bytes()
    phases={}
    with store.transcripts.retain_objects(), capture_retention_phases(phases):
        store.prune_closed_session_transcripts(before=cutoff)
        assert path.read_bytes()==original
    assert phases["retention_object_collection"]["calls"]==1
    store.transcripts.collect_unreferenced_objects()
    assert not path.exists()


@pytest.mark.asyncio
async def test_worker_maintenance_collects_sql_deltas_and_resets_next_operation(tmp_path):
    store,now=seed(tmp_path / "data")
    from pa.config import Settings
    settings=Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                      transcript_retention_days=14)
    runtime=AsyncRuntime(max_workers=1)
    try:
        result=await runtime.run_blocking("fixture.maintenance",run_maintenance,settings,store,
                                          now=now,wait_for_completion=True)
        phases=result["phases"]
        assert RETENTION_PHASES<=set(phases)
        assert phases["retention_transaction"]["sqlite_calls"]>0
        assert phases["retention_selection"]["sqlite_rows"]==1
        assert phases["retention_canonical"]["wall_ms"]>=phases["retention_transaction"]["wall_ms"]
        before=json.dumps(phases,sort_keys=True)
        await runtime.run_blocking("fixture.later",store.prune_closed_session_transcripts,
                                   before=now,wait_for_completion=True)
        assert json.dumps(phases,sort_keys=True)==before
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_capture_isolated_between_tasks_and_nested_scopes():
    async def task():
        outer={};inner={}
        with capture_retention_phases(outer):
            await asyncio.sleep(0)
            with retention_phase("retention_selection"):
                pass
            with capture_retention_phases(inner), retention_phase("retention_mirror"):
                await asyncio.sleep(0)
            with retention_phase("retention_selection"):
                pass
        return outer,inner
    first,second=await asyncio.gather(task(),task())
    for outer,inner in (first,second):
        assert outer["retention_selection"]["calls"]==2
        assert set(outer)=={"retention_selection"} and set(inner)=={"retention_mirror"}
    assert first[0] is not second[0]


def test_unknown_phase_cannot_publish_an_unbounded_label():
    phases={}
    with capture_retention_phases(phases), pytest.raises(ValueError,match="Unknown internal"):
        with retention_phase("fixture-private-label"):
            pytest.fail("Unknown label entered")
    assert phases=={}
