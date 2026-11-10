"""Fixed private root-inventory comparison, aggregate stdout and automatic cleanup.

Includes real full session decoding and path resolution; excludes route rendering,
file reads, Git/diff and network. Does not change authorization or validate deployment.
"""
import hashlib
import json
import math
from pathlib import Path
import shutil
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from starlette.applications import Starlette
from starlette.requests import Request

from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules import files


def original_membership(candidates):
    roots = []
    for candidate in candidates:
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        if resolved not in roots:
            roots.append(resolved)
    return roots


def quantiles(samples):
    ordered = sorted(samples)
    return {"samples":len(samples), **{name:round(ordered[math.ceil(len(ordered)*fraction)-1],3)
            for name,fraction in (("p50_ms",.5),("p95_ms",.95),("p99_ms",.99),("max_ms",1))}}


def benchmark():
    report = {"contract":"pa.file-root-inventory-benchmark/v1", "populations":{}}
    with tempfile.TemporaryDirectory(prefix="pa-file-root-inventory-") as temporary:
        root = Path(temporary)
        if shutil.disk_usage(root).free < 4*1024**3:
            raise RuntimeError("At least4GiB free required before seeding")
        store = CardProjection(root/"pa.db")
        try:
            for index in range(1000):
                store.save_session(AgentSession(id=f"s-{index}",agent_name="codex",
                    cwd=str(root/f"session-{index}"),principal_id="user:alice" if index%2 else "user:bob",
                    metrics_json={"retained":"x"*8192}))
            app = Starlette()
            app.state.ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=root,auth_required=False),store=store)
            request = Request({"type":"http","method":"GET","path":"/browse","headers":[],"app":app})
            request.state.principal_id = "user:alice"
            request.state.user_authenticated = True
            optimized = files._resolved_trusted_roots
            modes = {"before":original_membership,"after":optimized}
            for population in ("unique_1000","duplicate_20"):
                if population == "duplicate_20":
                    with store._conn() as conn:
                        conn.executemany("UPDATE agent_sessions SET cwd=? WHERE id=?",
                                         ((str(root/f"shared-{i%20}"),f"s-{i}") for i in range(1000)))
                        conn.commit()
                for auth in (False,True):
                    app.state.ctx.settings.auth_required = auth
                    values = {key:[] for key in modes}
                    expected = None
                    for mode, resolver in modes.items():
                        with patch.object(files,"_resolved_trusted_roots",resolver):
                            result = files._trusted_roots(request)
                        if expected is None:
                            expected = result
                        assert result == expected
                    for pair in range(6):
                        for mode in (("before","after") if pair%2==0 else ("after","before")):
                            with patch.object(files,"_resolved_trusted_roots",modes[mode]):
                                started = time.perf_counter()
                                result = files._trusted_roots(request)
                                elapsed = (time.perf_counter()-started)*1000
                            assert result == expected
                            values[mode].append(elapsed)
                    # Counters are separate from timed calls. Full decoder is
                    # intentionally retained, including malformed-field errors.
                    with patch.object(store,"_row_to_session",wraps=store._row_to_session) as decoder:
                        assert files._trusted_roots(request) == expected
                    report["populations"][f"{population}:auth={auth}"] = {
                        "latency":{key:quantiles(samples) for key,samples in values.items()},
                        "ordered_roots":len(expected), "full_session_decodes_per_call":decoder.call_count,
                        "exact_ordered_root_parity":True}
            size = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
            assert size <= 128*1024**2
            class CountedPath(type(Path())):
                comparisons = 0
                def __eq__(self, other):
                    type(self).comparisons += 1
                    return super().__eq__(other)
                __hash__ = type(Path()).__hash__
                def expanduser(self):
                    return self
                def resolve(self):
                    return self
            counts = {}
            candidates = [CountedPath(root/f"count-{i}") for i in range(1001)]
            for mode, resolver in modes.items():
                CountedPath.comparisons = 0
                assert len(resolver(candidates)) == 1001
                counts[mode] = CountedPath.comparisons
            report.update({"sessions":1000,"metrics_bytes_per_session":8192,"alternating_pairs":6,
                "untimed_membership_equality_calls_for_1001_distinct_roots":counts,
                "postmeasurement_logical_bytes":size,"fixture_budget_bytes":128*1024**2,
                "scope":"Private full root inventory; no route/file/Git/network, deployed, contention, physical-IO or whole-PA acceptance. Decoder and every path resolution retained; p95/p99 equal max at six samples.",
                "source_sha256":hashlib.sha256(Path(files.__file__).read_bytes()).hexdigest()})
        finally:
            store.close_thread_connection()
    report["fixture_removed"] = not root.exists()
    return report


if __name__ == "__main__":
    print(json.dumps(benchmark(),sort_keys=True))
