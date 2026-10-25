"""Cold-object traversal preserves pins, late publication and path matching."""
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
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


def after_first_live_query(storage, monkeypatch, action):
    original = storage._conn
    pending = [True]

    @contextmanager
    def observed():
        selected = []
        with original() as conn:
            conn.set_trace_callback(lambda sql: selected.append(True) if (
                sql.lstrip().startswith("SELECT") and "transcript_objects" in sql
            ) else None)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)
        if selected and pending[0]:
            pending[0] = False
            action()

    monkeypatch.setattr(storage, "_conn", observed)


@pytest.mark.parametrize("pin", [False, True])
def test_speculative_absence_cannot_authorize_unlink(tmp_path, monkeypatch, pin):
    storage = storage_at(tmp_path / "late-protection")
    text = orphaned(storage, 1)[0]
    reader = storage.retain_objects()
    action = reader.__enter__ if pin else lambda: storage.append([event("rescued", 1, text)])
    after_first_live_query(storage, monkeypatch, action)
    try:
        assert storage.collect_unreferenced_objects() == 0
        assert len(list(storage.objects_dir.glob("*/*.zlib"))) == 1
        if not pin:
            assert storage.list("rescued")[0].payload == {"text": text}
    finally:
        if pin:
            reader.__exit__(None, None, None)
    if pin:
        assert storage.collect_unreferenced_objects() == 1


def test_stale_positive_only_defers_collection_until_next_pass(tmp_path, monkeypatch):
    storage = storage_at(tmp_path / "stale-positive")
    storage.append([event("expired", 1, "cold fixture " * 300)])

    def release():
        with storage.retain_objects():
            assert storage.prune(["expired"], keep_audit=False) == 1

    after_first_live_query(storage, monkeypatch, release)
    assert storage.collect_unreferenced_objects() == 0
    assert len(list(storage.objects_dir.glob("*/*.zlib"))) == 1
    assert storage.collect_unreferenced_objects() == 1


@pytest.mark.parametrize("rollback", [False, True])
def test_speculation_during_uncommitted_publication_rechecks_after_lock(tmp_path, monkeypatch, rollback):
    storage = storage_at(tmp_path / "uncommitted")
    enumerating, published, release, speculated = (threading.Event() for _ in range(4))
    original_glob, original_prepare = module.iglob, storage._prepare_object

    def enumerated(*args, **kwargs):
        # Let GC pass its initial reader-pin check before starting the writer.
        enumerating.set()
        assert published.wait(5)
        yield from original_glob(*args, **kwargs)

    def prepare(*args):
        result = original_prepare(*args)
        published.set()
        assert release.wait(5)
        if rollback:
            raise ValueError("disposable publication failure")
        return result

    monkeypatch.setattr(module, "iglob", enumerated)
    monkeypatch.setattr(storage, "_prepare_object", prepare)
    after_first_live_query(storage, monkeypatch, speculated.set)
    with ThreadPoolExecutor(2) as pool:
        collector = pool.submit(storage.collect_unreferenced_objects)
        try:
            assert enumerating.wait(5)
            writer = pool.submit(storage.append, [event("new", 1, "cold fixture " * 300)])
            assert published.wait(5) and speculated.wait(5)
            assert not collector.done()
        finally:
            release.set()
        if rollback:
            with pytest.raises(ValueError, match="disposable publication failure"):
                writer.result(5)
        else:
            writer.result(5)
        assert collector.result(5) == int(rollback)
    assert len(storage.list("new")) == int(not rollback)


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
