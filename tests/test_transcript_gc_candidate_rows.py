"""Speculative GC avoids hydrating retained rows and preserves path semantics."""
import pytest

import pa.domain.transcript_storage as module
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


def test_canonical_candidates_avoid_path_allocation_and_keep_noncanonical_oracle(tmp_path, monkeypatch):
    storage = TranscriptStorage(tmp_path / "fixture.db")
    storage.append([
        TranscriptEvent(session_id="retained", seq=i+1, event_type="chunk",
                        payload={"text": f"{i}:" + "retained fixture " * 300})
        for i in range(2)
    ])
    before = {path:path.read_bytes() for path in storage.objects_dir.glob("*/*.zlib")}
    for i in range(3):
        path = storage._object_path(f"fe{i:062x}")
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"unreferenced canonical fixture")
    odd_names = (".zlib", "...zlib", "雪.zlib")
    directory = storage.objects_dir / ".hidden"
    directory.mkdir()
    for name in odd_names:
        (directory / name).write_bytes(b"unreferenced noncanonical fixture")
    original = module.Path
    constructed = []

    def observed(name):
        constructed.append(name)
        return original(name)

    monkeypatch.setattr(module, "Path", observed)
    assert storage.collect_unreferenced_objects() == 6
    assert sorted(constructed) == sorted(odd_names)
    assert all(path.read_bytes() == raw for path,raw in before.items())
    assert set(storage.objects_dir.glob("*/*.zlib")) == set(before)
    assert [row.seq for row in storage.list_range("retained")] == [1,2]
