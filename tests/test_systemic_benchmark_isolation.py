"""Reject fixture root escapes before WorkspaceManager can touch a path."""
import asyncio
import importlib.util
import gc
import logging
import json
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
            logging.getLogger("benchmark.validation").warning("benchmark-validation-sentinel")
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
    assert result["warnings"].get("benchmark.validation:WARNING", 0) == 0
    assert result["all_fixture_warning_counts"]["benchmark.validation:WARNING"] == 1
    assert "VALUES" not in result["sql_statements"]
    latest_gc = result["gc_pauses"]["latest_finished_monotonic"]
    assert latest_gc is None or latest_gc <= capture["finished_monotonic"]
    assert result["transcript_input_rows"] == result["transcript_retained_rows"] == 96
    assert result["fixture"]["retained_cold_verification"]["events"] == 3
    assert result["fixture"]["retained_cold_verification"]["all_compressed_content_hashes_verified"]


@pytest.mark.parametrize("fail", [False, True])
def test_bounded_runner_removes_generated_state_and_never_exports_failure_payload(tmp_path, monkeypatch, fail):
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_cleanup", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py")
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    temporary = benchmark.tempfile.TemporaryDirectory
    created = []
    def owned(*args, **kwargs):
        value = temporary(*args, **kwargs, dir=tmp_path)
        created.append(Path(value.name))
        return value
    async def run(root, **kwargs):
        (root / "fixture").write_bytes(b"generated")
        if fail:
            raise RuntimeError("synthetic body must not be exported")
        return {"latency": {"fixed.operation": {"samples": 1, "max_ms": 1}}}
    monkeypatch.setattr(benchmark.tempfile, "TemporaryDirectory", owned)
    monkeypatch.setattr(benchmark, "run", run)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: SimpleNamespace(free=4*1024**3))
    sentinel = tmp_path / "preexisting"
    sentinel.write_bytes(b"preserve")
    result = benchmark.benchmark(history=1, iterations=1)
    assert result["temporary_fixture_removed"] and not created[0].exists()
    assert result.get("failed", False) == fail
    assert "synthetic body" not in json.dumps(result)
    assert sentinel.read_bytes() == b"preserve"


def test_aggregate_projection_keeps_all_names_counts_and_quantiles_without_raw_snapshots():
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_projection", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py")
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    result = benchmark.compact_result({
        "raw_latency_samples_ms": {"first": [1,2]}, "raw_decoded_response_bytes": {"first": [3]},
        "runtime": {"operations": {"first": {"submitted": 2, "runtime_ms": {"samples": 2,"p50": 1,"p95": 2,"p99": 2,"max": 2}},
                                    "second": {"failed": 1}},
                    "requests": {"routes": {"/fixed": {"count": 2,"response_bytes": 4}}},
                    "slow_operations": {"observations": [{"operation":"first","elapsed_ms":501,"payload":"omit"}]}},
        "gc_pauses": {"counts_by_generation": {0: 2}, "observations": [{"finished_at_monotonic":3}], "long_observations": [{"payload":"omit"}]}})
    assert "raw_latency_samples_ms" not in result and "raw_decoded_response_bytes" not in result
    runtime = result["runtime"]
    assert set(runtime["operations"]) == {"first", "second"}
    column = runtime["operation_columns"].index("runtime_ms")
    assert runtime["operations"]["first"][column] == [2,1,2,2,2]
    assert runtime["routes"]["/fixed"] == {"count":2,"response_bytes":4}
    assert runtime["slow_worst_by_operation_in_last64"]["first"] == {"elapsed_ms":501}
    assert result["gc_pauses"]["counts_by_generation"] == {0:2}
    assert result["gc_pauses"]["latest_finished_monotonic"] == 3
    assert "observations" not in result["gc_pauses"] and "omit" not in json.dumps(result)


def test_unbounded_inputs_and_low_capacity_rejected_before_workload(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_bounds", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py")
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    with pytest.raises(ValueError):
        benchmark.benchmark(history=1001)
    async def reject(*args, **kwargs):
        pytest.fail("low capacity must not begin workload")
    monkeypatch.setattr(benchmark, "run", reject)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: SimpleNamespace(free=0))
    with pytest.raises(RuntimeError, match="At least4GiB"):
        benchmark.benchmark(history=1, iterations=1)


def test_real_reclamation_and_retained_data_finish_before_fixture_cleanup(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "systemic_benchmark_reclamation", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py")
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    import pa.instance.agent_session as agent_module
    previous_manager = object()
    monkeypatch.setattr(agent_module, "_instance_agent", previous_manager)
    result = benchmark.benchmark(history=2, iterations=1, retention_events=128,
                                 retained_cold_objects=3, maintenance_rows=16384)
    assert not result.get("failed"), {key: result.get(key) for key in ("location", "http_status", "route")}
    assert agent_module._instance_agent is previous_manager
    assert result["temporary_fixture_removed"]
    assert result["transcript_retained_rows"] == 96
    assert result["fixture"]["knowledge_audit_replays"] == 1
    assert result["fixture"]["reclamation_retained_rows"] == 8192
    completion = result["quiet_completion"]
    assert completion["rewrite_observed"] and completion["pending"] == []
    assert all(report["status"] == "ok" for report in completion["database_reports"].values())
    projection = completion["database_reports"]["sqlite"]
    assert projection["after"]["free_bytes"] == 0
    assert projection["after"]["bytes"] < result["fixture"]["reclamation_before"]["bytes"]
