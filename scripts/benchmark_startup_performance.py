"""Synthetic read-amplification benchmark; never opens the running PA data dir."""
from __future__ import annotations

import json
import statistics
import tempfile
import time
from pathlib import Path

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.pr_supervisor.models import PRWatch, PRWatchStatus
from pa.pr_supervisor.store import PRSupervisorStore


def measure(call, repeats=7):
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        call()
        samples.append((time.perf_counter() - start) * 1000)
    return {"median_ms": round(statistics.median(samples), 3), "max_ms": round(max(samples), 3)}


def main():
    with tempfile.TemporaryDirectory(prefix="pa-perf-fixture-") as temp:
        root = Path(temp)
        store = CardProjection(root / "fixture.db")
        for index in range(300):
            store.save_session(AgentSession(
                id=f"session-{index}", agent_name="codex",
                status="closed" if index else "idle",
                config_json={"fixture": "x" * 16000},
            ))
        store.append_transcript_events([
            TranscriptEvent(session_id="session-0", seq=index + 1,
                            event_type="agent_message_chunk", payload={"text": "x" * 1000})
            for index in range(5000)
        ])
        watches = PRSupervisorStore(root / "watches.db")
        for index in range(250):
            watches.upsert_watch(PRWatch(
                id=f"watch-{index}", realm_id="default", repository="fixture/repo",
                pr_number=index+1, pr_url=f"https://github.com/fixture/repo/pull/{index+1}",
                originating_instance_id="fixture", card_id=f"card-{index}",
                state={"fixture": "x" * 16000, "card_lane": "done"},
                status=PRWatchStatus.MERGED,
            ))
        result = {
            "fixture": {"sessions": 300, "closed_sessions": 299, "transcript_events": 5000, "merged_done_watches": 250},
            "recovery_all_sessions": measure(store.list_sessions),
            "recovery_filtered": measure(lambda: store.list_sessions(exclude_statuses=("closed",), include_archived=False)),
            "historical_watches": measure(lambda: watches.list_watches(include_retired=True)),
            "completion_due": measure(watches.list_card_completion_due),
            "pending_prompt_history": measure(lambda: store.list_transcript_events_before("session-0", limit=5000)),
            "pending_prompt_completion_evidence": measure(lambda: store.find_prompt_completion("session-0", "pending")),
            "deep_storage_diagnostics": measure(store.transcript_storage_metrics),
            "storage_status_snapshot": measure(store.transcript_storage_status),
        }
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
