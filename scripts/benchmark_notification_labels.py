"""Compare full notice presentation with the pre-repair implementation privately."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import tempfile
import time
import types
from pathlib import Path
from types import SimpleNamespace

from starlette.requests import Request

from pa.domain.models import AgentSession, CardCreate, ProjectCreate
from pa.domain.notifications import Notification
from pa.domain.projection import CardProjection
from pa.modules.notifications import _present_page


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), "max_ms": max(values), **{
        f"p{p}_ms": values[math.ceil(len(values) * p / 100) - 1]
        for p in (50, 95, 99)
    }}


def run(base):
    old = types.ModuleType("notification_baseline")
    source = subprocess.check_output(["git", "show", f"{base}:src/pa/modules/notifications.py"])
    exec(compile(source, "notification_baseline", "exec"), old.__dict__)
    with tempfile.TemporaryDirectory(prefix="pa-notice-labels-") as directory:
        store = CardProjection(Path(directory) / "private.db")
        project = store.create_project(ProjectCreate(title="Project", description="x" * 65536), via_log=False)
        cards = [store.create_card(CardCreate(title=f"Card {i}", body="x" * 65536), via_log=False)
                 for i in range(20)]
        for i in range(20):
            store.save_session(AgentSession(id=f"session-{i}", agent_name="codex", title=f"Session {i}",
                                            metrics={"retained": "x" * 65536}))
        notices = [Notification(title="Notice", card_id=cards[i % 20].id,
                                project_id=project.id, session_id=f"session-{i % 20}")
                   for i in range(200)]
        ctx = SimpleNamespace(store=store, settings=SimpleNamespace(instance_id="local"), services={})
        request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace(ctx=ctx))})
        samples = {"before": [], "after": []}
        queries = {"before": [], "after": []}
        expected = [old._public_notice(request, item) for item in notices]
        with store._conn() as conn:
            for iteration in range(30):
                for name in (["before", "after"] if iteration % 2 == 0 else ["after", "before"]):
                    statements = []
                    conn.set_trace_callback(statements.append)
                    started = time.perf_counter_ns()
                    try:
                        actual = ([old._public_notice(request, item) for item in notices]
                                  if name == "before" else _present_page(request, notices))
                    finally:
                        conn.set_trace_callback(None)
                    samples[name].append((time.perf_counter_ns() - started) / 1e6)
                    queries[name].append(len([s for s in statements if s.startswith("SELECT")]))
                    assert actual == expected
        return {"contract": "pa.notification-labels-benchmark/v1", "baseline": base,
                "baseline_module_sha256": hashlib.sha256(source).hexdigest(),
                "notices": 200, "distinct_cards_sessions": 20, "retained_field_bytes": 65536,
                "iterations": 30, "alternating_order": True, "exact_response_parity": True,
                "latency": {k: distribution(v) for k, v in samples.items()},
                "raw_samples_ms": samples, "select_counts": queries,
                "scope": "Private SQLite and in-process presentation; no peer/provider/network or deployed acceptance."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.base)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["latency"]))
