"""Cold-object traversal preserves pins, late publication and path matching."""
import threading
from pathlib import Path

import pytest

import pa.domain.transcript_storage as module
from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


def storage_at(root):
    root.mkdir(parents=True)
    return TranscriptStorage(root / "metadata.db")


def event(session, seq, text):
    return TranscriptEvent(session_id=session, seq=seq, event_type="chunk",
                           payload={"text": text})


def orphaned(storage, count=257):
    texts = [f"{i}:" + "cold object fixture " * 300 for i in range(count)]
    storage.append([event("expired", i + 1, text) for i, text in enumerate(texts)])
    with storage.retain_objects():
        assert storage.prune(["expired"], keep_audit=False) == count
    return texts


@pytest.mark.parametrize("linked", [False, True])
def test_collection_keeps_original_hidden_suffix_and_literal_root_matching(tmp_path, linked):
    storage = storage_at(tmp_path / "literal [root] ?")
    live_text = "retained cold fixture " * 300
    storage.append([event("live", 1, live_text)])
    orphaned(storage, 1)
    for relative in (".hidden/.zlib", ".hidden/extra.zlib", "ab/.secret.zlib",
                     "ab/UPPER.ZLIB", "ab/ignored.tmp"):
        path = storage.objects_dir / relative
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"unreferenced fixture bytes")
    if linked:
        target = tmp_path / "linked directory"
        target.mkdir()
        (target / "orphan.zlib").write_bytes(b"unreferenced fixture bytes")
        try:
            (storage.objects_dir / "link").symlink_to(target, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("directory symlinks unavailable")
    # Path.glob is the compatibility oracle, including host case rules and
    # hidden names. No assumption that glob metacharacters in the root match.
    before = {path: path.read_bytes() for path in storage.objects_dir.glob("*/*")
              if path.is_file()}
    with storage._conn() as conn:
        live = {row[0] for row in conn.execute(
            "SELECT hash FROM transcript_objects WHERE ref_count>0")}
    selected = set(storage.objects_dir.glob("*/*.zlib"))
    removed = {path for path in selected if path.parent.name + path.stem not in live}
    assert storage.collect_unreferenced_objects() == len(removed)
    assert all(not path.exists() for path in removed)
    assert all(path.read_bytes() == raw for path, raw in before.items() if path not in removed)
    assert storage.list("live")[0].payload == {"text": live_text}
    with storage._conn() as conn:
        assert conn.execute("SELECT SUM(ref_count) FROM transcript_objects").fetchone()[0] == 1


def test_collection_propagates_unlink_failure_without_changing_retained_rows(tmp_path):
    storage = storage_at(tmp_path / "failure")
    text = "retained cold fixture " * 300
    storage.append([event("live", 1, text)])
    directory = storage.objects_dir / "odd" / "directory.zlib"
    directory.mkdir(parents=True)
    with pytest.raises(OSError) as native:
        directory.unlink()
    with pytest.raises(type(native.value)) as collected:
        storage.collect_unreferenced_objects()
    assert collected.value.errno == native.value.errno
    assert directory.is_dir()
    assert storage.list("live")[0].payload == {"text": text}


def test_reader_pin_acquired_between_batches_preserves_remaining_bytes(tmp_path, monkeypatch):
    storage = storage_at(tmp_path / "pins")
    orphaned(storage)
    start, ready, release = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def reader():
        try:
            assert start.wait(5)
            with storage.retain_objects():
                ready.set()
                assert release.wait(5)
        except BaseException as exc:
            errors.append(exc)

    original = module.iglob

    def enumerated(*args, **kwargs):
        for index, path in enumerate(original(*args, **kwargs)):
            if index == 128:
                start.set()
                assert ready.wait(5)
            yield path

    monkeypatch.setattr(module, "iglob", enumerated)
    thread = threading.Thread(target=reader)
    thread.start()
    try:
        assert storage.collect_unreferenced_objects() == 128
        assert len(list(storage.objects_dir.glob("*/*.zlib"))) == 129
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and not errors
    monkeypatch.setattr(module, "iglob", original)
    assert storage.collect_unreferenced_objects() == 129


def test_new_reference_published_during_enumeration_survives_batch_recheck(tmp_path, monkeypatch):
    storage = storage_at(tmp_path / "publication")
    texts = orphaned(storage)
    start, published = threading.Event(), threading.Event()
    errors = []

    def writer():
        try:
            assert start.wait(5)
            storage.append([event("live", 1, texts[0])])
        except BaseException as exc:
            errors.append(exc)
        finally:
            published.set()

    original = module.iglob

    def enumerated(*args, **kwargs):
        for index, path in enumerate(original(*args, **kwargs)):
            if index == 0:
                start.set()
                assert published.wait(5)
            yield path

    monkeypatch.setattr(module, "iglob", enumerated)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        assert storage.collect_unreferenced_objects() == 256
    finally:
        thread.join(5)
    assert not thread.is_alive() and not errors
    assert storage.list("live")[0].payload == {"text": texts[0]}
    assert len(list(storage.objects_dir.glob("*/*.zlib"))) == 1
