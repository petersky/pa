"""Private loop comparison with identical delayed native project/policy reads.

Candidate probes, joint selection and the retry effect boundary are controlled.
This is not real admission, provider/network or physical I/O acceptance.
"""
import argparse
import ast
import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

BASELINE = "abe51586f2b1220e981f94b765e005d1d1de7763"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-placement-read-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp) / "data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp) / "workspaces"), PA_AGENT_ENABLED="false")
        from fastapi import HTTPException
        from pa.config import Settings
        from pa.core.async_runtime import AsyncRuntime
        from pa.domain.models import Project
        from pa.domain.projection import CardProjection
        from pa.execution.dispatch import DispatchRecord
        from pa.fleet.placement import PlacementCandidate, PlacementError
        from pa.fleet.policy import FleetPolicyService
        from pa.modules import fleet

        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/modules/fleet.py"], text=True)
        namespace = dict(vars(fleet))
        tree = ast.parse(source)
        for name in ("_resolve_policy_placement", "_retry_dispatch_api"):
            node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            exec(compile(ast.get_source_segment(source, node), "<archived-placement-baseline>", "exec"), namespace)
        result = dict(baseline=BASELINE, source_sha256=hashlib.sha256(Path(fleet.__file__).read_bytes()).hexdigest(),
                      started_at=datetime.now(UTC).isoformat(), rows=[],
                      scope=__doc__, private_roots_before_imports=True, delay_ms=200,
                      complete_response_bytes_equal=False)
        args.output.write_text(json.dumps(result, indent=2) + "\n")

        async def run():
            runtime = AsyncRuntime(max_workers=1)
            store = CardProjection(Path(tmp) / "fixture.db")
            candidates = [PlacementCandidate(instance_id="local", name="Local")]
            selection = SimpleNamespace(joint=lambda *a, **k: SimpleNamespace(
                execution_selection={"selected": {"harness": "codex", "model": "fixture-balanced", "options": {}}},
                eligible_candidates=[{"instance_id": "local", "provider_id": "codex"}],
                rejected_candidates=[], chosen_instance_id="local"))
            placement = SimpleNamespace(resolve=Mock(side_effect=PlacementError("fixture_stop", "Read-only retry fixture")))
            services = {"async_runtime": runtime, "fleet_policy": FleetPolicyService(store),
                        "fleet_registry": SimpleNamespace(list_instances=lambda: []), "placement_service": placement}
            ctx = SimpleNamespace(store=store, services=services, require_service=services.__getitem__,
                                  settings=Settings(data_dir=Path(tmp)/"data", workspace_root=Path(tmp)/"workspaces",
                                                    instance_id="local", agent_enabled=False, auth_required=False))
            request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
                                      headers={"idempotency-key": "fixture-retry"})
            record = DispatchRecord(dispatch_id="retry", mutation_id="retry-mutation", realm_id="default",
                                    authority_instance_id="local", authority_url="http://fixture.invalid",
                                    state="failed", recoverable=True, target_instance_id="local", principal_id="user:fixture")
            ledger = SimpleNamespace(get=lambda _: record, retry_with_capacity=Mock())
            controls = dict(_placement_candidates=AsyncMock(return_value=candidates),
                            selection_service_for=lambda _: selection, get_principal_id=lambda _: "user:fixture",
                            require_user=lambda _: SimpleNamespace(role="user"),
                            _require_dispatch_access=lambda *a: None, _dispatch_store=lambda _: ledger)
            namespace.update(controls)
            expected = {}
            try:
                with patch.multiple(fleet, **controls):
                    for pair in range(3):
                        for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                            for family, method in [("planning", "list_project_repositories"),
                                                   ("policy", "get_instance_participation_policy"),
                                                   ("retry", "get_instance_participation_policy")]:
                                original = getattr(store, method)
                                threads, ticks = [], []
                                owner = threading.get_ident()
                                def delayed(*a, **k):
                                    threads.append(threading.get_ident())
                                    time.sleep(.2)
                                    return original(*a, **k)
                                done = False
                                async def heartbeat():
                                    previous = time.perf_counter()
                                    while not done:
                                        await asyncio.sleep(.005)
                                        now = time.perf_counter()
                                        ticks.append((now - previous) * 1000)
                                        previous = now
                                pulse = asyncio.create_task(heartbeat())
                                await asyncio.sleep(.01)
                                started = time.perf_counter()
                                try:
                                    with patch.object(store, method, side_effect=delayed):
                                        function_name = "_retry_dispatch_api" if family == "retry" else "_resolve_policy_placement"
                                        call = namespace[function_name] if variant == "baseline" else getattr(fleet, function_name)
                                        if family == "retry":
                                            try:
                                                await call(request, record.dispatch_id)
                                                raise AssertionError("Controlled retry must stop before effect")
                                            except HTTPException as exc:
                                                payload = {"status": exc.status_code, "detail": exc.detail}
                                        else:
                                            body = fleet.FleetDispatchBody(execution_contract={
                                                "version": 1, "profile": "research", "confirmed": True, "requirements": {}})
                                            decision, plan = await call(request, body, card=None,
                                                project=Project(id="p", title="Fixture"), project_id="p")
                                            payload = dict(decision=vars(decision), plan=plan.model_dump(mode="json"),
                                                           body=body.model_dump(mode="json"))
                                    request_ms = (time.perf_counter() - started) * 1000
                                finally:
                                    done = True
                                    await pulse
                                encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                                expected.setdefault(family, encoded)
                                assert encoded == expected[family], "Complete controlled response changed"
                                assert threads and all((t == owner) == (variant == "baseline") for t in threads)
                                result["rows"].append(dict(pair=pair, variant=variant, family=family,
                                    request_ms=request_ms, max_heartbeat_ms=max(ticks), read_calls=len(threads),
                                    response_bytes=len(encoded), response_sha256=hashlib.sha256(encoded).hexdigest()))
                                args.output.write_text(json.dumps(result, indent=2) + "\n")
                ledger.retry_with_capacity.assert_not_called()
                result["runtime"] = runtime.snapshot()
            finally:
                await runtime.close()
        asyncio.run(run())
        result.update(finished_at=datetime.now(UTC).isoformat(), complete_response_bytes_equal=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"rows": len(result["rows"]), "response_bytes_equal": True,
                          "max_heartbeat_ms": {v: max(r["max_heartbeat_ms"] for r in result["rows"] if r["variant"] == v)
                                               for v in ("baseline", "candidate")}}))


if __name__ == "__main__":
    main()
