"""The private route scenario keeps real routes, expected denials and safe failure aggregates."""
from collections import Counter
import importlib.util
from pathlib import Path

import pytest


def module():
    spec=importlib.util.spec_from_file_location("file_route_inventory",Path(__file__).parents[1]/"scripts/benchmark_file_routes.py")
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


@pytest.mark.parametrize("options",[{"sessions":2},{"iterations":21}])
def test_input_bounds_precede_fixture_creation(options):
    with pytest.raises(ValueError):
        module().benchmark(**options)


def test_failure_keeps_aggregate_progress_and_removes_fixture_without_exception_body(monkeypatch):
    value=module()
    async def fail(root,**kwargs):
        kwargs["progress"].update(statuses=Counter({"before:raw:200":1}),samples={"before:raw":[3.0]})
        raise RuntimeError("private-response-body-must-not-escape")
    monkeypatch.setattr(value,"run",fail)
    result=value.benchmark(sessions=3,iterations=1)
    assert result["failed"] and result["temporary_fixture_removed"]
    assert result["completed_status_counts"] == {"before:raw:200":1}
    assert result["partial_request_ms"]["before:raw"]["max_ms"] == 3
    assert "private-response-body" not in str(result)


def test_small_real_kernel_scenario_counts_guards_and_git_processes():
    result=module().benchmark(sessions=3,iterations=1)
    assert not result.get("failed") and result["temporary_fixture_removed"]
    assert result["unexpected_failed_operation_deltas"] == {}
    assert result["operation_counter_deltas"]["files.browse_render"]["submitted"] == 10
    assert result["operation_counter_deltas"]["files.raw_resolve"]["submitted"] == 6
    assert result["background_read_ms"]["samples"] == 16
    assert sum(n for key,n in result["status_counts"].items() if key.endswith(":403")) == 6
    assert result["measured_git_process_calls"] == {
        "before:rev-parse":9,"before:diff":3,"after:rev-parse":7,"after:diff":3}
    assert set(result["runtime"]["routes"]) == {"/browse","/api/files/raw"}
