"""Reject fixture root escapes before WorkspaceManager can touch a path."""
import asyncio
import importlib.util
import gc
import logging
import time
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


def test_validation_does_not_extend_workload_window_or_counters(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_boundary", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py"
    )
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    original = benchmark.run_app
    services = {}
    validation = []
    counters = []
    original_counts = benchmark.Counts

    def counts():
        value = original_counts()
        counters.append(value)
        return value

    async def run_app(app, *args, **kwargs):
        services.update(app.state.ctx.services)
        return await original(app, *args, **kwargs)

    async def observe(value):
        if value["kind"] == "retained_cold_verified":
            validation.append(time.perf_counter())
            # Observable postmeasurement activity must not change the frozen
            # workload warning/GC/diagnostic snapshots or its end timestamp.
            logging.warning("benchmark-validation-sentinel")
            gc.collect()
            diagnostic = services["runtime_diagnostics"]
            await diagnostic.run_blocking("validation.sentinel", lambda: None)
            assert "validation.sentinel" in diagnostic.snapshot()["operations"]
            with counters[0].connection(":memory:") as connection:
                connection.execute("VALUES (1)").fetchall()
            assert counters[0].statements["VALUES"] == 1

    monkeypatch.setattr(benchmark, "run_app", run_app)
    monkeypatch.setattr(benchmark, "Counts", counts)
    result = asyncio.run(benchmark.run(tmp_path / "fixture", history=2, iterations=1,
        retention_events=128, retained_cold_objects=3, sample_observer=observe))
    start, finish = result["measurement_start_monotonic"], result["measurement_finish_monotonic"]
    assert result["fixture"]["duration_seconds"] == round(finish - start, 3)
    capture = result["measurement_capture"]
    assert finish <= capture["finished_monotonic"] == result["validation"]["started_monotonic"]
    assert result["validation"]["started_monotonic"] <= validation[0] <= result["validation"]["finished_monotonic"]
    assert "validation.sentinel" not in result["diagnostics_runtime"]["operations"]
    assert "validation.sentinel" not in result["diagnostics_counter_deltas"]
    assert "benchmark-validation-sentinel" not in result["warnings"]
    assert "VALUES" not in result["sql_statements"]
    assert all(row["finished_at_monotonic"] <= capture["finished_monotonic"]
               for row in result["gc_pauses"]["observations"])
    assert result["transcript_input_rows"] == result["transcript_retained_rows"] == 96
    assert result["fixture"]["retained_cold_verification"]["events"] == 3
    assert result["fixture"]["retained_cold_verification"]["all_compressed_content_hashes_verified"]
