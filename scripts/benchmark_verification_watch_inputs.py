"""Compare complete origin-readiness reads with identical native private stores.

The archived method uses full retained PR models. The candidate uses the existing
lifecycle links. No verifier is admitted and no acceptance, lifecycle, deadline,
GC or configuration policy is changed. Logical JSON bytes hydrated are not disk
I/O or write amplification. Historical health-call attribution remains unknown.
"""
import argparse
import ast
import asyncio
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

BASELINE = "6818042b3c132356a4b3d7dcbd179a705e686583"


def distribution(values):
    ordered = sorted(values)
    return {"samples": len(values), "p50_ms": ordered[math.ceil(len(values)*.50)-1],
            "p95_ms": ordered[math.ceil(len(values)*.95)-1],
            "p99_ms": ordered[math.ceil(len(values)*.99)-1], "max_ms": max(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-readiness-watch-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from pa.core.async_runtime import AsyncRuntime
        from pa.execution import verification
        from pa.pr_supervisor.models import PRWatch, PRWatchStatus
        from tests.test_autonomous_verification import ordinary, system

        source = subprocess.check_output(["git", "show", BASELINE+":src/pa/execution/verification.py"], text=True)
        cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == "VerificationService")
        node = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "origin_readiness")
        namespace = dict(vars(verification))
        exec(compile(textwrap.dedent(ast.get_source_segment(source, node)), "<exact-readiness-baseline>", "exec"), namespace)
        value = dict(baseline=BASELINE, baseline_source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                     source_sha256=hashlib.sha256(Path(verification.__file__).read_bytes()).hexdigest(),
                     started_at=datetime.now(UTC).isoformat(), watches=100, proof_characters_per_watch=65536,
                     calls_per_variant=30, private_roots_before_imports=True, rows=[], scope=__doc__)
        args.output.write_text(json.dumps(value, indent=2)+"\n")

        async def run():
            fixture = system.__wrapped__(Path(tmp))
            target = next(fixture)
            runtime = AsyncRuntime(max_workers=4)
            target.ctx.services["async_runtime"] = runtime
            store = target.ctx.services["pr_supervisor_store"]
            card, record = ordinary(target)
            for index in range(100):
                store.upsert_watch(PRWatch(repository="fixture/repo", pr_number=index+1,
                    pr_url="https://example.invalid/"+str(index),
                    originating_session_id="other-session", card_id=card.id if index == 0 else "other-card",
                    status=PRWatchStatus.MERGED, state={"proof": "x"*65536}))
            expected = None
            original = store._row_to_watch
            try:
                for pair in range(30):
                    for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                        hydration = {"models": 0, "state_policy_json_bytes": 0}
                        def counted(row):
                            hydration["models"] += 1
                            hydration["state_policy_json_bytes"] += sum(len((row[key] or "").encode()) for key in ["state_json", "policy_json"])
                            return original(row)
                        started = time.perf_counter()
                        with patch.object(store, "_row_to_watch", side_effect=counted):
                            call = namespace["origin_readiness"] if variant == "baseline" else verification.VerificationService.origin_readiness
                            result = await call(target.service, record)
                        elapsed = (time.perf_counter()-started)*1000
                        expected = expected or result
                        assert result == expected and result["ready"] and not target.calls
                        assert hydration["models"] == (100 if variant == "baseline" else 0)
                        value["rows"].append(dict(pair=pair, variant=variant, elapsed_ms=elapsed,
                            result=result, **hydration))
                        args.output.write_text(json.dumps(value, indent=2)+"\n")
                value["runtime"] = runtime.snapshot()
            finally:
                await runtime.close()
                try:
                    next(fixture)
                except StopIteration:
                    pass

        asyncio.run(run())
        value.update(finished_at=datetime.now(UTC).isoformat(), identical_readiness=True,
            variants={variant: dict(latency=distribution([r["elapsed_ms"] for r in value["rows"] if r["variant"] == variant]),
                hydrated_models=sum(r["models"] for r in value["rows"] if r["variant"] == variant),
                hydrated_state_policy_json_bytes=sum(r["state_policy_json_bytes"] for r in value["rows"] if r["variant"] == variant))
                for variant in ["baseline", "candidate"]})
        args.output.write_text(json.dumps(value, indent=2)+"\n")
        print(json.dumps(dict(variants=value["variants"], identical_readiness=True)))


if __name__ == "__main__":
    main()
