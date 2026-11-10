"""Resource bounds and a client oracle independent of list-query implementation."""
import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def benchmark():
    path = Path(__file__).parents[1] / "scripts" / "benchmark_scheduling_reads.py"
    spec = importlib.util.spec_from_file_location("read_api_inventory_contract", path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("params", [
    {"population": 23}, {"population": 481}, {"iterations": 0}, {"iterations": 7},
    {"payloads": ()}, {"payloads": (0,)}, {"payloads": (12289,)}, {"payloads": (1, 2, 3)},
])
def test_invalid_inventory_bounds_fail_before_fixture_creation(benchmark, params):
    with patch.object(benchmark.tempfile, "TemporaryDirectory", side_effect=AssertionError("fixture started")):
        with pytest.raises(ValueError): benchmark.benchmark(**params)


def test_oracle_filters_before_page_and_counts_health_outside_page(benchmark):
    def record(owner, state, **extra):
        return {"owner": owner, "state": state, "context_refs": {},
            "destination": {"kind": "inbox", "target": owner}, **extra}
    records = {
        "a": record("owner", "idle"),
        "b": record("owner", "pending", additional_destinations=[{"kind": "agent_session", "target": "session"}]),
        "c": record("foreign", "pending", context_refs={"session": "session"}),
        "d": record("owner", "pending", context_refs={"session_id": "session"}),
    }
    for sid, value in records.items(): value["id"] = sid
    histories = {"a": [], "b": [{"next_retry": 10}], "c": [{"next_retry": 1}], "d": [{"next_retry": 20}]}
    page = benchmark.expected_page(records, histories, "owner",
        {"limit": 1, "session_id": "session", "pending_only": "true"})
    assert [row["id"] for row in page["schedules"]] == ["b"] and page["next_cursor"] == "b"
    assert page["health"]["overdue_delivery_count"] == 2 and page["health"]["oldest_overdue_due_at"] == 10
    page = benchmark.expected_page(records, histories, "owner",
        {"cursor": "b", "session_id": "session", "pending_only": "true"})
    assert [row["id"] for row in page["schedules"]] == ["d"] and page["next_cursor"] is None
