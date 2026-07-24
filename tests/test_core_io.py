import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

from pa.core.io import atomic_write_text
from pa.modules.sync import _apply_sync_push_local
from pa.sync.compaction import SyncMetrics


def _temporary_files(directory: Path, destination: str) -> list[Path]:
    return list(directory.glob(f".{destination}.*.tmp"))


def test_atomic_write_text_uses_distinct_temporary_files(tmp_path: Path) -> None:
    destination = tmp_path / "value.json"
    replace_barrier = threading.Barrier(2)
    real_replace = os.replace

    def synchronized_replace(source: Path, target: Path) -> None:
        replace_barrier.wait(timeout=5)
        real_replace(source, target)

    with (
        patch("pa.core.io.os.replace", side_effect=synchronized_replace),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        futures = [
            executor.submit(atomic_write_text, destination, json.dumps({"writer": writer}))
            for writer in range(2)
        ]
        for future in futures:
            future.result(timeout=5)

    assert json.loads(destination.read_text())["writer"] in {0, 1}
    assert _temporary_files(tmp_path, destination.name) == []


def test_concurrent_sync_metrics_writes_are_valid_and_do_not_leak(
    tmp_path: Path,
) -> None:
    metrics = [SyncMetrics(tmp_path), SyncMetrics(tmp_path)]
    replace_barrier = threading.Barrier(2)
    real_replace = os.replace

    def record_pull(instance: SyncMetrics, count: int) -> None:
        instance.record_pull(count)

    def synchronized_replace(source: Path, target: Path) -> None:
        replace_barrier.wait(timeout=5)
        real_replace(source, target)

    with (
        patch("pa.core.io.os.replace", side_effect=synchronized_replace),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        futures = [
            executor.submit(record_pull, instance, count)
            for instance, count in zip(metrics, (3, 7), strict=True)
        ]
        for future in futures:
            future.result(timeout=5)

    stored = json.loads((tmp_path / "sync_metrics.json").read_text())
    assert stored["pulls"] == 1
    assert stored["objects_imported"] in {3, 7}
    assert _temporary_files(tmp_path, "sync_metrics.json") == []


def test_concurrent_sync_push_transactions_remain_successful(tmp_path: Path) -> None:
    metrics = SyncMetrics(tmp_path)
    engine = MagicMock()
    engine.ingest_objects.return_value = []
    context = MagicMock()
    context.require_service.side_effect = {
        "sync_engine": engine,
        "event_log": MagicMock(),
        "sync_metrics": metrics,
    }.__getitem__
    start_barrier = threading.Barrier(2)

    def push() -> tuple[int, str, bool]:
        start_barrier.wait(timeout=5)
        return _apply_sync_push_local(context, "default", "", {})

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(push) for _ in range(2)]
        results = [future.result(timeout=5) for future in futures]

    assert results == [(0, "", False), (0, "", False)]
    assert metrics.snapshot()["pulls"] == 2
    assert json.loads((tmp_path / "sync_metrics.json").read_text())["pulls"] == 2
    assert _temporary_files(tmp_path, "sync_metrics.json") == []


def test_sync_metrics_serializes_updates_within_server_process(
    tmp_path: Path,
) -> None:
    metrics = SyncMetrics(tmp_path)
    start_barrier = threading.Barrier(8)

    def record_pull() -> None:
        start_barrier.wait(timeout=5)
        metrics.record_pull(2)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(record_pull) for _ in range(8)]
        for future in futures:
            future.result(timeout=5)

    assert metrics.snapshot()["pulls"] == 8
    stored = json.loads((tmp_path / "sync_metrics.json").read_text())
    assert stored["pulls"] == 8
    assert stored["objects_imported"] == 16
    assert _temporary_files(tmp_path, "sync_metrics.json") == []


def test_atomic_write_text_applies_mode_before_replace(tmp_path: Path) -> None:
    destination = tmp_path / "secret"

    atomic_write_text(destination, "value", mode=0o600)

    assert destination.stat().st_mode & 0o777 == 0o600
