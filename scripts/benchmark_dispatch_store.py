"""Reproducible isolated benchmark for the incremental dispatch WAL store."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import (
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    ProgressPhase,
)


def record(index: int) -> DispatchRecord:
    return DispatchRecord(
        dispatch_id=f"benchmark-dispatch-{index}",
        mutation_id=f"benchmark-mutation-{index}",
        idempotency_key=f"benchmark-admission-{index}",
        card_id=f"benchmark-card-{index}",
        project_id=f"benchmark-project-{index % 10}",
        session_id=f"benchmark-session-{index}",
        authority_instance_id="benchmark-authority",
        authority_url="https://benchmark.invalid",
        target_instance_id="benchmark-target",
        state="running",
        progress_protocol_version=1,
    )


def checkpoint(
    item: DispatchRecord, sequence: int, index: int
) -> DispatchProgressEventV1:
    return DispatchProgressEventV1(
        card_id=item.card_id,
        dispatch_id=item.dispatch_id,
        acp_session_id=item.session_id or "",
        originating_instance_id=item.target_instance_id,
        authority_instance_id=item.authority_instance_id,
        sequence=sequence,
        idempotency_key=f"benchmark-checkpoint-{index}",
        phase=ProgressPhase.IMPLEMENTING,
        summary=f"bounded benchmark checkpoint {index}",
    )


def heartbeat(
    item: DispatchRecord, sequence: int, index: int
) -> DispatchProgressHeartbeatV1:
    return DispatchProgressHeartbeatV1(
        card_id=item.card_id,
        dispatch_id=item.dispatch_id,
        acp_session_id=item.session_id or "",
        originating_instance_id=item.target_instance_id,
        authority_instance_id=item.authority_instance_id,
        sequence=sequence,
        idempotency_key=f"benchmark-heartbeat-{index}",
        phase=ProgressPhase.IMPLEMENTING,
        summary="bounded benchmark heartbeat",
    )


def run(dispatches: int, prior_receipts: int, writes: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="pa-dispatch-benchmark-") as tmp:
        root = Path(tmp)
        records = [record(index) for index in range(dispatches)]
        for index in range(prior_receipts):
            records[index % dispatches].progress_seen_keys.append(
                f"benchmark-prior-receipt-{index}"
            )
        (root / "dispatch_mutations.json").write_text(
            json.dumps(
                {item.dispatch_id: item.model_dump(mode="json") for item in records},
                separators=(",", ":"),
            )
        )
        store = DispatchStore(root)
        started = time.perf_counter()
        for index in range(writes):
            item = records[index % dispatches]
            sequence = index + 1
            if index % 2:
                store.ingest_progress(checkpoint(item, sequence, index))
            else:
                store.ingest_heartbeat(heartbeat(item, sequence, index))
        elapsed = time.perf_counter() - started
        metrics = store.storage_metrics()
        return {
            "fixture": {
                "dispatches": dispatches,
                "prior_receipts": prior_receipts,
                "mixed_writes": writes,
            },
            "elapsed_seconds": round(elapsed, 3),
            "writes_per_second": round(writes / elapsed, 1),
            "commit_latency_ms": metrics["writes"]["latency_ms"],
            "row_counts": metrics["rows"],
            "store_bytes": metrics["store_bytes"],
            "legacy_bytes_unchanged": metrics["bytes"]["legacy_source"],
            "wal_checkpoint": store.checkpoint(),
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dispatches", type=int, default=200)
    parser.add_argument("--prior-receipts", type=int, default=25_000)
    parser.add_argument("--writes", type=int, default=500)
    args = parser.parse_args()
    print(
        json.dumps(
            run(args.dispatches, args.prior_receipts, args.writes),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
