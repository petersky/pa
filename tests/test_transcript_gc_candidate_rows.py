"""Speculative GC avoids hydrating retained rows and preserves path semantics."""
import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import capture_retention_phases, retention_phase
from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


@pytest.mark.asyncio
@pytest.mark.parametrize("orphans", [0, 5])
async def test_only_unreferenced_candidates_cross_the_sqlite_row_boundary(tmp_path, orphans):
    storage = TranscriptStorage(tmp_path / "fixture.db")
    storage.append([
        TranscriptEvent(session_id="retained", seq=i + 1, event_type="chunk",
                        payload={"text": f"{i}:" + "live fixture " * 300})
        for i in range(385)
    ])
    for i in range(orphans):
        path = storage._object_path(f"ff{i:062x}")
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"unreferenced fixture")

    def collect():
        phases = {}
        with capture_retention_phases(phases), retention_phase("retention_object_collection"):
            removed = storage.collect_unreferenced_objects()
        return removed, phases["retention_object_collection"]

    runtime = AsyncRuntime(max_workers=1)
    try:
        removed, phase = await runtime.run_blocking("fixture.gc_candidates", collect, wait_for_completion=True)
    finally:
        await runtime.close()
    assert removed == orphans
    assert phase["sqlite_rows"] == orphans
    assert len(list(storage.objects_dir.glob("*/*.zlib"))) == 385
    assert [event.seq for event in storage.list_range("retained")] == list(range(1, 386))
    assert storage.metrics()["missing_objects"] == 0


def test_candidate_json_keeps_literal_unicode_quotes_and_positive_reference_semantics(tmp_path):
    storage = TranscriptStorage(tmp_path / "fixture.db")
    directory = storage.objects_dir / 'q"'
    directory.mkdir()
    retained = directory / "雪.zlib"
    zero = directory / "zero.zlib"
    absent = directory / "absent.zlib"
    for path in (retained, zero, absent):
        path.write_bytes(b"literal fixture")
    with storage._conn() as conn:
        for path, references in ((retained, 1), (zero, 0)):
            conn.execute(
                """INSERT INTO transcript_objects
                   (hash,codec,uncompressed_bytes,compressed_bytes,ref_count,created_at)
                   VALUES(?, 'zlib', 15, 15, ?, '2026-10-25T00:00:00Z')""",
                (path.parent.name + path.stem, references),
            )
    assert storage.collect_unreferenced_objects() == 2
    assert retained.read_bytes() == b"literal fixture"
    assert not zero.exists() and not absent.exists()
