"""The bounded report must retain partial grids and every positive operation."""
import copy
import importlib.util
from pathlib import Path


def test_scheduler_capture_round_trip_keeps_sparse_samples_and_runtime_windows():
    path = Path(__file__).parents[1] / "scripts" / "benchmark_kernel_scheduler_phases.py"
    spec = importlib.util.spec_from_file_location("scheduler_capture_contract", path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    source = {"runs": [
        {"phase_ms": {"list": {"samples": 4, "max_ms": 12.5}}, "select_counts": {"page": 4},
         "response_byte_totals": {"page": 4096}, "runtime": {"operations": {"poll": [3, [2, 1, 8, 8, 10]]},
             "routes": {"/api/notifications": {"count": 4, "max_ms": 19}}, "window_scope": "bounded"}},
        {"phase_ms": {"present": {"samples": 1, "max_ms": 0.2}}, "select_counts": {},
         "response_byte_totals": {"page": 0}, "runtime": {"operations": {"poll": [4, [3, 1, 6, 7, 11]],
             "recover": [1, [1, 2, 2, 2, 2]]}, "routes": {}, "window_scope": "bounded"}},
    ]}
    original = copy.deepcopy(source)
    packed = module.compact_runs(source)
    assert source == original
    for expected, actual in zip(source["runs"], packed["runs"]):
        for field in ("phase_ms", "select_counts", "response_byte_totals"):
            header, cells = packed["numeric_grid_headers"][field], actual[field]
            rebuilt = {header["names"][i]: dict(zip(header["columns"], value)) if header["columns"] else value
                for i, value in zip(cells["present"], cells["values"])}
            assert rebuilt == expected[field]
        cells = actual["runtime"]["operations"]
        assert {packed["runtime_operation_names"][i]: value for i, value in
            zip(cells["present"], cells["values"])} == expected["runtime"]["operations"]
        assert actual["runtime"]["routes"] == expected["runtime"]["routes"]
        assert actual["runtime"]["window_scope"] == "bounded"
