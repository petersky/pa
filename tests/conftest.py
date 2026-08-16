"""Shared pytest hooks for CI sharding and worker isolation."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def pytest_configure(config) -> None:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker and "PA_DATA_DIR" not in os.environ:
        root = Path(tempfile.mkdtemp(prefix=f"pa-pytest-{worker}-"))
        os.environ["PA_DATA_DIR"] = str(root)


def pytest_collection_modifyitems(config, items) -> None:
    # xdist does not collect on the controller. Each worker collects and the
    # scheduler requires identical nodeid lists, so the shard filter must run
    # on workers (and on a non-xdist controller when PYTEST_SHARD is set).
    raw = os.environ.get("PYTEST_SHARD")
    if not raw:
        return
    index_text, total_text = raw.split("/", 1)
    index = int(index_text)
    total = int(total_text)
    if total <= 1:
        return
    selected = [item for i, item in enumerate(items) if i % total == index]
    deselected = [item for i, item in enumerate(items) if i % total != index]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = selected
