from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from tests.conftest import pytest_collection_modifyitems


def _items(count: int) -> list[SimpleNamespace]:
    return [SimpleNamespace(nodeid=f"t{i}") for i in range(count)]


def test_shard_filter_applies_on_xdist_workers(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv("PYTEST_SHARD", "0/2")
    items = _items(6)
    config = MagicMock()

    pytest_collection_modifyitems(config, items)

    assert [item.nodeid for item in items] == ["t0", "t2", "t4"]
    deselected = config.hook.pytest_deselected.call_args.kwargs["items"]
    assert [item.nodeid for item in deselected] == ["t1", "t3", "t5"]


def test_odd_shard_keeps_the_complement(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw1")
    monkeypatch.setenv("PYTEST_SHARD", "1/2")
    items = _items(6)
    config = MagicMock()

    pytest_collection_modifyitems(config, items)

    assert [item.nodeid for item in items] == ["t1", "t3", "t5"]


def test_four_way_shard_keeps_every_fourth_item(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv("PYTEST_SHARD", "2/4")
    items = _items(8)
    config = MagicMock()

    pytest_collection_modifyitems(config, items)

    assert [item.nodeid for item in items] == ["t2", "t6"]


def test_no_shard_env_leaves_collection_intact(monkeypatch) -> None:
    monkeypatch.delenv("PYTEST_SHARD", raising=False)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    items = _items(4)
    config = MagicMock()

    pytest_collection_modifyitems(config, items)

    assert [item.nodeid for item in items] == ["t0", "t1", "t2", "t3"]
    config.hook.pytest_deselected.assert_not_called()
