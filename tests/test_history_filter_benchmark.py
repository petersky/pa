"""The bounded private scenario preserves real routes and complete response parity."""
import importlib.util
from pathlib import Path

import pytest


def module():
    spec=importlib.util.spec_from_file_location("history_filter_inventory",Path(__file__).parents[1]/"scripts/benchmark_history_card_filter.py")
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


def test_bounds_precede_fixture_creation():
    with pytest.raises(ValueError):
        module().benchmark(entries=20001)


def test_failure_retains_aggregates_without_private_exception_body(monkeypatch):
    value=module()
    async def fail(root,**kwargs):
        kwargs["progress"].update(statuses={"before:200":1},timings={"before":[1.2]})
        raise RuntimeError("private response must not escape")
    monkeypatch.setattr(value,"run",fail)
    result=value.benchmark(sessions=3,iterations=1,entries=1000)
    assert result["failed"] and result["temporary_fixture_removed"]
    assert result["completed_status_counts"]=={"before:200":1}
    assert result["partial_request_ms"]["before"]["max_ms"]==1.2
    assert "private response" not in str(result)


def test_small_full_kernel_routes_and_query_oracle():
    result=module().benchmark(sessions=3,iterations=1,entries=1000)
    assert not result.get("failed") and result["temporary_fixture_removed"]
    assert result["query_counts"]=={"before:per_session":4,"after:batch":1,"after:per_session":2}
    assert result["decoded_sessions"]=={"before":3,"after":3}
    assert result["status_counts"]["raw_range:206"]==3
    assert result["status_counts"]["raw_too_large:413"]==3
    assert result["operation_deltas"]["files.raw_resolve"]["submitted"]==18
    assert set(result["runtime"]["routes"]) >= {"/browse","/api/files/raw","/api/agent/history"}
