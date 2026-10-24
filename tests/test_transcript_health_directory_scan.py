"""Fresh existence diagnostics retain missing/corrupt reference semantics."""
import os
from unittest.mock import patch

import pytest

from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


def fixture(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([TranscriptEvent(session_id="cold", seq=i + 1, event_type="output",
                                  payload={"text": f"{i}:" + "x" * 3000})
                  for i in range(385)])
    return store


def legacy_missing(store):
    return sum(not store._object_path(d).exists() for d in store.referenced_hashes())


def test_fresh_scan_matches_missing_files_directories_and_symlinks(tmp_path):
    store = fixture(tmp_path)
    paths = [store._object_path(d) for d in sorted(store.referenced_hashes())[:5]]
    contents = [p.read_bytes() for p in paths]
    for p in paths:
        p.unlink()
    paths[1].mkdir()  # Existing Path.exists semantics, not content validation.
    paths[2].symlink_to(paths[1])
    paths[3].symlink_to(tmp_path / "absent-target")
    paths[4].symlink_to(paths[3])  # A dangling chain remains missing.
    assert store.metrics()["missing_objects"] == legacy_missing(store) == 3
    paths[0].write_bytes(contents[0])
    (tmp_path / "absent-target").write_bytes(b"now present")
    assert store.metrics()["missing_objects"] == legacy_missing(store) == 0
    (tmp_path / "absent-target").unlink()
    assert store.metrics()["missing_objects"] == legacy_missing(store) == 2


def test_incomplete_or_denied_directory_read_falls_back_without_partial_result(tmp_path):
    store = fixture(tmp_path)
    # Dense directory deterministically exercises the scan and fallback paths.
    hashes = {f"aa{i:062x}" for i in range(16)}
    for digest in hashes:
        path = store._object_path(digest)
        path.parent.mkdir(exist_ok=True)
        path.touch()
    digest = sorted(hashes)[0]
    store._object_path(digest).unlink()
    real = os.scandir

    class Interrupted:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            with real(store._object_path(digest).parent) as entries:
                yield next(entries)
            raise PermissionError("disposable directory read interrupted")

    with patch.object(store, "referenced_hashes", return_value=hashes):
        with patch("pa.domain.transcript_storage.os.scandir", return_value=Interrupted()) as scan:
            assert store.metrics()["missing_objects"] == legacy_missing(store) == 1
            assert scan.call_count == 1
        with patch("pa.domain.transcript_storage.os.scandir", side_effect=PermissionError) as scan:
            assert store.metrics()["missing_objects"] == legacy_missing(store) == 1
            assert scan.call_count == 1


def test_health_uses_event_references_even_without_object_metadata(tmp_path):
    store = fixture(tmp_path)
    hashes = store.referenced_hashes()
    digest = sorted(hashes)[-1]
    store._object_path(digest).unlink()
    with store._conn() as conn:
        conn.execute("DELETE FROM transcript_objects")
    assert store.metrics()["missing_objects"] == legacy_missing(store) == 1
    assert store.referenced_hashes() == hashes


def test_malformed_reference_uses_existing_path_resolution(tmp_path):
    store = fixture(tmp_path)
    bad = "legacy-reference"
    path = store._object_path(bad)
    path.parent.mkdir(exist_ok=True)
    with patch.object(store, "referenced_hashes", return_value={bad}):
        assert store._missing_referenced_objects() == legacy_missing(store) == 1
        path.touch()
        assert store._missing_referenced_objects() == legacy_missing(store) == 0


def test_dense_scan_checks_dangling_symlinks_and_ignores_unreferenced_entries(tmp_path):
    store = fixture(tmp_path)
    hashes = {f"ab{i:062x}" for i in range(16)}
    for digest in hashes:
        path = store._object_path(digest)
        path.parent.mkdir(exist_ok=True)
        path.touch()
    target = tmp_path / "target"
    path = store._object_path(sorted(hashes)[0])
    path.unlink()
    path.symlink_to(target)
    with patch.object(store, "referenced_hashes", return_value=hashes):
        assert store.metrics()["missing_objects"] == legacy_missing(store) == 1
        target.touch()
        assert store.metrics()["missing_objects"] == legacy_missing(store) == 0


def test_sparse_health_does_not_enumerate_unreferenced_directory(tmp_path):
    store = fixture(tmp_path)
    digest = next(iter(store.referenced_hashes()))
    with patch.object(store, "referenced_hashes", return_value={digest}), patch(
        "pa.domain.transcript_storage.os.scandir", side_effect=AssertionError("sparse scan")
    ):
        assert store.metrics()["missing_objects"] == 0


def test_dense_case_aliases_match_actual_filesystem_resolution(tmp_path):
    store = fixture(tmp_path)
    hashes = {f"ac{i:062x}" for i in range(16)}
    for digest in hashes:
        path = store._object_path(digest)
        path.parent.mkdir(exist_ok=True)
        path.touch()
    path = store._object_path(sorted(hashes)[-1])
    upper = path.with_name(path.name.upper())
    path.rename(upper)
    with patch.object(store, "referenced_hashes", return_value=hashes):
        assert store.metrics()["missing_objects"] == legacy_missing(store)


def test_readable_directory_without_search_permission_is_not_false_healthy(tmp_path):
    store = fixture(tmp_path)
    hashes = {f"ad{i:062x}" for i in range(16)}
    for digest in hashes:
        path = store._object_path(digest)
        path.parent.mkdir(exist_ok=True)
        path.touch()
    directory = path.parent
    mode = directory.stat().st_mode
    try:
        directory.chmod(0o400)
        if store._object_path(next(iter(hashes))).exists():
            pytest.skip("principal can traverse mode400 fixture")
        with patch.object(store, "referenced_hashes", return_value=hashes):
            assert store.metrics()["missing_objects"] == legacy_missing(store) == 16
    finally:
        directory.chmod(mode)
