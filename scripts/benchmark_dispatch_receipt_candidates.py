"""Compare receipt-expiry discovery against the exact pre-change compactor.

All state is disposable. Receipt reseeding, SQL tracing and byte checks run
outside the timed calls. Each receipt population is reported separately.
"""
import argparse
import ast
import hashlib
import json
import os
import statistics
import subprocess
import tempfile
import textwrap
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path


BASELINE = "695b46e7211de17961b93e2ad86b26991fd1927f"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--records", type=int, default=1000)
    args = parser.parse_args()
    assert args.records > 0 and not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-receipt-comparison-") as tmp:
        root = Path(tmp)
        os.environ.update(
            PA_DATA_DIR=str(root / "data"),
            PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false",
        )
        from pa.execution import dispatch as module
        from pa.execution.progress import CompletionReportV1
        from pa.execution.post_turn import (
            FollowupActionName, FollowupActionV1, PostTurnDecision,
            PostTurnEvaluationV1, SafetyClassification,
        )
        source = subprocess.check_output(
            ["git", "show", BASELINE + ":src/pa/execution/dispatch.py"], text=True,
        )
        cls = next(n for n in ast.parse(source).body
                   if isinstance(n, ast.ClassDef) and n.name == "DispatchStore")
        method = next(n for n in cls.body
                      if isinstance(n, ast.FunctionDef) and n.name == "compact")
        namespace = dict(vars(module))
        exec(textwrap.dedent(ast.get_source_segment(source, method)), namespace)
        now = datetime.now(UTC)
        evaluation = PostTurnEvaluationV1(
            snapshot_id="fixture", context_digest="a" * 64,
            decision=PostTurnDecision.OUTCOME_ACHIEVED,
            rationale="Private completed fixture", confidence=1.0,
            operator_status_text="Completed",
            recommended_actions=[FollowupActionV1(
                name=FollowupActionName.NO_ACTION,
                parameters={"reason": "Completed"},
                idempotency_key_inputs=["fixture"],
                safety=SafetyClassification.RECORD_ONLY, human_approval_required=False,
            )],
        )
        populations = {}
        for population, age in [("none", None), ("recent", 0), ("expired", 60)]:
            store = module.DispatchStore(root / population)
            for i in range(args.records):
                store.put(module.DispatchRecord(
                    dispatch_id=f"d-{i}", mutation_id=f"m-{i}",
                    idempotency_key=f"k-{i}", card_id=f"c-{i}",
                    authority_instance_id="fixture-authority",
                    authority_url="https://example.invalid",
                    target_instance_id="fixture-target", state="completed",
                    acknowledged_at=now, final_report=CompletionReportV1(outcome="Completed"),
                    post_turn_evaluations=[evaluation],
                ))
            original = [tuple(r) for r in store._conn.execute(
                "SELECT dispatch_id,payload_json FROM dispatches ORDER BY dispatch_id"
            )]

            def seed():
                if age is not None:
                    with store._transaction() as conn:
                        # Recent receipts survive the preceding call. Restore
                        # the population outside the timer for every variant.
                        conn.execute("DELETE FROM dispatch_receipts")
                        for record in store._records.values():
                            store._persist_control_receipt_conn(
                                conn, record, idempotency_key="control",
                                operation="fixture", payload={},
                            )
                        conn.execute("UPDATE dispatch_receipts SET created_at=?", (
                            (now - timedelta(days=age)).isoformat(),
                        ))

            expected = {"events": 0, "receipts": args.records if age == 60 else 0}
            calls = {
                "baseline": lambda: namespace["compact"](store, now=now),
                "candidate": lambda: store.compact(now=now),
            }

            def verify(result):
                assert result == expected
                assert [tuple(r) for r in store._conn.execute(
                    "SELECT dispatch_id,payload_json FROM dispatches ORDER BY dispatch_id"
                )] == original
                assert store._conn.execute("SELECT COUNT(*) FROM dispatch_receipts").fetchone()[0] == (
                    args.records if age == 0 else 0
                )

            samples = []
            for pair in range(5):
                for variant in (["baseline", "candidate"] if pair % 2 == 0
                                else ["candidate", "baseline"]):
                    seed()
                    start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                    result = calls[variant]()
                    samples.append(dict(
                        pair=pair, variant=variant,
                        wall_ms=(time.perf_counter_ns() - start) / 1e6,
                        thread_cpu_ms=(time.thread_time_ns() - cpu) / 1e6,
                    ))
                    verify(result)
            counts = {}
            for variant, run in calls.items():
                seed()
                statements = []
                store._conn.set_trace_callback(statements.append)
                try: result = run()
                finally: store._conn.set_trace_callback(None)
                verify(result)
                counts[variant] = dict(
                    sqlite_statements=len(statements),
                    receipt_deletes=sum(s.startswith("DELETE FROM dispatch_receipts") for s in statements),
                )
            latency = {}
            for variant in calls:
                values = sorted(s["wall_ms"] for s in samples if s["variant"] == variant)
                latency[variant] = dict(samples=len(values), p50=statistics.median(values),
                                        p95=values[-1], p99=values[-1], max=values[-1])
            populations[population] = dict(
                receipt_age_days=age, samples=samples, latency_ms=latency,
                untimed_sql_counts=counts, expected_removed=expected,
                dispatch_payload_bytes_unchanged=True, receipt_results_verified=True,
            )
            store.close()
        out = dict(
            contract="pa.dispatch-receipt-candidates/v1", baseline_source=BASELINE,
            source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
            records_per_population=args.records, populations=populations,
            scope="Five alternating pairs per independent receipt population. Normal GC and uncontrolled host load. Receipt reseeding outside timers prevents consuming expiry inputs across variants. No progress events or production state. Small-sample percentiles do not establish whole-maintenance, physical disk-write or historical-cause acceptance.",
        )
        args.output.write_text(json.dumps(out, indent=2) + "\n")
        print(json.dumps(out))


if __name__ == "__main__":
    main()
