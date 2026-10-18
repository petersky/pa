"""Reject fixture root escapes before WorkspaceManager can touch a path."""
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("escaped", ["data", "workspace"])
def test_benchmark_rejects_constructor_root_escape(tmp_path, monkeypatch, escaped):
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_isolation", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py"
    )
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    fixture = tmp_path / "fixture"
    touched = []

    def native_constructor(*args):
        touched.append(True)
        raise AssertionError("unguarded constructor")

    def boot(*, settings):
        invalid = SimpleNamespace(data_dir=fixture / "data", workspace_root=fixture / "workspaces")
        setattr(invalid, "data_dir" if escaped == "data" else "workspace_root", tmp_path / "outside")
        manager = object.__new__(benchmark.WorkspaceManager)
        benchmark.WorkspaceManager.__init__(manager, invalid, None)

    monkeypatch.setattr(benchmark.WorkspaceManager, "__init__", native_constructor)
    monkeypatch.setattr(benchmark.Kernel, "boot", boot)
    with pytest.raises(AssertionError, match=f"benchmark {escaped} root escaped fixture"):
        asyncio.run(benchmark.run(fixture))
    assert not touched
