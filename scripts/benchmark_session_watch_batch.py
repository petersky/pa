"""Compare independent full-watch reads with response-local native batches."""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    baseline = "d0027d0e587c048209d8e079389152205d2e6939"
    with tempfile.TemporaryDirectory(prefix="pa-watch-batch-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"), PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.core.performance import distribution
        from pa.pr_supervisor import store as module
        from pa.pr_supervisor.models import PRWatch
        cls = module.PRSupervisorStore
        original = subprocess.check_output(["git", "show", baseline + ":src/pa/pr_supervisor/store.py"], text=True)
        def reader(source):
            tree = ast.parse(source)
            return ast.dump(next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "list_session_watches"), include_attributes=False)
        assert reader(original) == reader(Path(module.__file__).read_text())
        populations = {}
        for shape in ("shared_ascii", "shared_unicode", "independent", "empty"):
            store = cls(root / (shape + ".db"))
            sessions = [SimpleNamespace(id=f"session-{i}", card_id="shared" if shape.startswith("shared") else None) for i in range(10)]
            for i in range(0 if shape == "empty" else 100):
                store.upsert_watch(PRWatch(
                    id=f"fixture-{i}", repository="fixture/repo", pr_number=i + 1,
                    pr_url=f"https://example.invalid/{i}", originating_instance_id="fixture",
                    originating_session_id=f"session-{i % 10}",
                    card_id="shared" if shape.startswith("shared") and i < 90 else None,
                    state={"proof": ("界" if shape == "shared_unicode" else "x") * 65536},
                ), preserve_lease=False)
            variants = {
                "baseline": lambda: {s.id: [w.model_dump(mode="json") for w in store.list_session_watches(s)] for s in sessions},
                "candidate": lambda: store.session_watch_payloads_for_sessions(sessions),
            }
            expected = variants["baseline"]()
            assert variants["candidate"]() == expected
            body = json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode()
            rows = []
            for pair in range(30):
                for name in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                    start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                    result = variants[name]()
                    rows.append(dict(pair=pair, variant=name, wall_ms=(time.perf_counter_ns() - start) / 1e6,
                                     thread_cpu_ms=(time.thread_time_ns() - cpu) / 1e6))
                    assert result == expected
            counts = {}
            code = cls._row_to_watch.__code__
            for name, call in variants.items():
                count = dict(models=0, logical_state_utf8_bytes=0)
                def profile(frame, event, arg):
                    if event == "call" and frame.f_code is code:
                        count["models"] += 1
                        count["logical_state_utf8_bytes"] += len(frame.f_locals["row"]["state_json"].encode())
                previous = sys.getprofile()
                sys.setprofile(profile)
                try:
                    assert call() == expected
                finally:
                    sys.setprofile(previous)
                counts[name] = count
            populations[shape] = dict(rows=rows, latency_ms={n: distribution([r["wall_ms"] for r in rows if r["variant"] == n]) for n in variants},
                                      untimed_amplification=counts, response_utf8_bytes=len(body), response_sha256=hashlib.sha256(body).hexdigest(), complete_values_equal=True)
        value = dict(contract="pa.full-watch-batch-comparison/v1", baseline=baseline,
                     source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(), populations=populations,
                     scope="120 alternating private pairs, four separate fixture shapes. Current source method versus unchanged exact baseline single-session reader. Complete watch values/order and independent dumps retained; no response serialization/profile/verification inside timers. Logical UTF8 counts are not physical IO. Watch payloads only, not entire HTTP session response. Normal GC unchanged; shared host, no idle-host/native-cause or 2MiB acceptance claim. All adverse samples retained.")
        with args.output.open("x") as output:
            json.dump(value, output, indent=2)
            output.write("\n")
        print(json.dumps({n: {k: p[k] for k in ("latency_ms", "untimed_amplification", "response_utf8_bytes")} for n, p in populations.items()}))


if __name__ == "__main__":
    main()
