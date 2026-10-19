"""Complete ordered catalog reads with finite fetches and fresh snapshots."""
import hashlib

import pytest

from pa.core.sqlite_metrics import ObservedCursor
from pa.sync.object_catalog import ObjectCatalog


@pytest.fixture
def catalog(tmp_path):
    catalog = ObjectCatalog(tmp_path / "derived-catalog.db")
    hashes = [hashlib.sha256(f"fixture-{i}".encode()).hexdigest() for i in range(257)]
    with catalog._db() as conn:
        conn.executemany(
            "INSERT INTO objects(object_hash,size_bytes,mtime_ns,object_class,realm_id,recorded_at) VALUES(?,?,?,?,?,?)",
            [(h, 1, 1, "unknown", "fixture", "fixture") for h in hashes],
        )
    return catalog, sorted(hashes)


@pytest.mark.parametrize("limit", [None, 0, 1, 127, 128, 129, 257, 258, -1])
def test_order_and_existing_limit_semantics(catalog, limit):
    catalog, hashes = catalog
    expected = hashes if limit is None or limit < 0 else hashes[:limit]
    assert catalog.iter_hashes(limit=limit) == expected


def test_empty_catalog(tmp_path):
    assert ObjectCatalog(tmp_path / "empty.db").iter_hashes() == []


def test_large_read_never_falls_back_to_per_row_cursor_steps(catalog, monkeypatch):
    catalog, hashes = catalog
    original = ObservedCursor.fetchmany
    sizes = []

    def fetch(cursor, size=None):
        assert size is not None and 0 < size <= 128
        sizes.append(size)
        return original(cursor, size)

    def deny_step(cursor):
        raise AssertionError("Catalog hash inventory must not observe every row separately")

    monkeypatch.setattr(ObservedCursor, "fetchmany", fetch)
    monkeypatch.setattr(ObservedCursor, "__next__", deny_step)
    assert catalog.iter_hashes() == hashes
    assert sizes and len(sizes) < len(hashes)


def test_one_read_keeps_its_sql_snapshot_and_the_next_read_is_fresh(catalog, monkeypatch):
    catalog, hashes = catalog
    original = ObservedCursor.fetchmany
    late = hashlib.sha256(b"late-catalog-object").hexdigest()
    inserted = False

    def fetch(cursor, size=None):
        nonlocal inserted
        rows = original(cursor, size)
        if not inserted:
            inserted = True
            catalog.record(late, b"late-catalog-object")
        return rows

    monkeypatch.setattr(ObservedCursor, "fetchmany", fetch)
    assert catalog.iter_hashes() == hashes
    assert catalog.iter_hashes() == sorted([*hashes, late])


def test_read_failure_returns_no_partial_inventory_and_releases_connection(catalog, monkeypatch):
    catalog, hashes = catalog
    original = ObservedCursor.fetchmany
    calls = 0

    def fail(cursor, size=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("fixture fetch failed")
        return original(cursor, size)

    with monkeypatch.context() as patch:
        patch.setattr(ObservedCursor, "fetchmany", fail)
        with pytest.raises(RuntimeError, match="fixture fetch failed"):
            catalog.iter_hashes()
    assert catalog.iter_hashes() == hashes
