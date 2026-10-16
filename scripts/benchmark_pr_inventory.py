"""Disposable retained-watch metrics benchmark; never opens installed PA state."""
import argparse
import hashlib
import json
import math
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from pa.pr_supervisor import store as store_module
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore


def run(root):
    store = PRSupervisorStore(root / "supervisor.db")
    for index in range(100):
        store.upsert_watch(PRWatch(
            repository="example/repo", pr_number=index + 1,
            pr_url=f"https://github.com/example/repo/pull/{index + 1}",
            status=("active", "blocked", "merged", "closed", "retired")[index % 5],
            owner_instance_id="local" if index % 2 else "peer", fence_token=index,
            retired_at=datetime.now(UTC) if index % 7 == 0 else None,
            state={"proof": ["fixture " * 1000 for _ in range(8)]},
        ))
    counts = {"hydrated_rows": 0, "state_policy_bytes": 0}
    original = store._row_to_watch

    def observed(row):
        counts["hydrated_rows"] += 1
        counts["state_policy_bytes"] += len(row["state_json"] or "") + len(row["policy_json"] or "")
        return original(row)

    store._row_to_watch = observed
    samples = []
    for _ in range(30):
        started = time.perf_counter()
        result = store.metrics()
        samples.append((time.perf_counter() - started) * 1000)
    samples.sort()
    return {
        "scope": "100 retained watches, 30 metrics calls, 64 KB proof per watch; disposable, no provider/peer work",
        "source_store_sha256": hashlib.sha256(Path(store_module.__file__).read_bytes()).hexdigest(),
        "latency_ms": {"max": samples[-1], **{
            f"p{percentile}": samples[math.ceil(len(samples) * percentile / 100) - 1]
            for percentile in (50, 95, 99)}},
        "hydration": counts, "result": result,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-pr-inventory-") as tmp:
        result = run(Path(tmp))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
