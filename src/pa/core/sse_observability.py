"""Process-local observability for bounded server-sent event transports."""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4


@dataclass(frozen=True)
class SSEConnection:
    id: str
    endpoint: str
    direction: str
    client_id: str | None
    peer_id: str | None
    session_scope: str
    opened_at: datetime
    opened_monotonic: float
    paired_id: str | None


class SSEConnectionRegistry:
    """Track active transports and lifecycle counters without retaining secrets."""

    def __init__(self, *, over_age_seconds: float = 300.0) -> None:
        self.over_age_seconds = over_age_seconds
        self._lock = threading.Lock()
        self._active: dict[str, SSEConnection] = {}
        self._counts: Counter[str] = Counter()

    def open(
        self,
        *,
        endpoint: str,
        direction: str,
        client_id: str | None = None,
        peer_id: str | None = None,
        session_scope: str = "none",
        paired_id: str | None = None,
    ) -> str:
        connection_id = str(uuid4())
        record = SSEConnection(
            id=connection_id,
            endpoint=endpoint,
            direction=direction,
            client_id=str(client_id or "")[:80] or None,
            peer_id=str(peer_id or "")[:80] or None,
            session_scope=session_scope[:80],
            opened_at=datetime.now(UTC),
            opened_monotonic=time.monotonic(),
            paired_id=paired_id,
        )
        with self._lock:
            self._active[connection_id] = record
            self._counts["opened"] += 1
            if paired_id:
                self._counts["paired_opened"] += 1
        return connection_id

    def close(self, connection_id: str, outcome: str = "closed") -> None:
        normalized = (
            outcome
            if outcome in {"closed", "cancelled", "errored", "reconnecting"}
            else "closed"
        )
        with self._lock:
            record = self._active.pop(connection_id, None)
            if not record:
                return
            self._counts[normalized] += 1
            if record.paired_id:
                self._counts["paired_closed"] += 1

    def increment(self, event: str) -> None:
        if event != "reconnecting":
            return
        with self._lock:
            self._counts[event] += 1

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            active = list(self._active.values())
            counts = dict(self._counts)
        by_endpoint = Counter(item.endpoint for item in active)
        by_direction = Counter(item.direction for item in active)
        by_peer = Counter(item.peer_id or "local" for item in active)
        by_client = Counter(item.client_id or "unknown" for item in active)
        over_age = [
            item
            for item in active
            if now - item.opened_monotonic > self.over_age_seconds
        ]
        paired_downstream = sum(
            1 for item in active if item.direction == "downstream" and item.paired_id
        )
        paired_upstream = sum(
            1 for item in active if item.direction == "upstream" and item.paired_id
        )
        return {
            "active": len(active),
            "opened": counts.get("opened", 0),
            "closed": counts.get("closed", 0),
            "cancelled": counts.get("cancelled", 0),
            "errored": counts.get("errored", 0),
            "reconnecting": counts.get("reconnecting", 0),
            "leaked": len(over_age),
            "over_age": len(over_age),
            "over_age_seconds": self.over_age_seconds,
            "paired": {
                "downstream": paired_downstream,
                "upstream": paired_upstream,
                "balanced": paired_downstream == paired_upstream,
                "opened": counts.get("paired_opened", 0),
                "closed": counts.get("paired_closed", 0),
            },
            "by_endpoint": dict(sorted(by_endpoint.items())),
            "by_direction": dict(sorted(by_direction.items())),
            "by_peer": dict(sorted(by_peer.items())),
            "by_client": dict(sorted(by_client.items())),
            "connections": [
                {
                    "id": item.id,
                    "endpoint": item.endpoint,
                    "direction": item.direction,
                    "client_id": item.client_id,
                    "peer_id": item.peer_id,
                    "session_scope": item.session_scope,
                    "paired_id": item.paired_id,
                    "opened_at": item.opened_at.isoformat(),
                    "age_seconds": round(now - item.opened_monotonic, 3),
                    "over_age": item in over_age,
                }
                for item in active
            ],
        }

    def reset_for_tests(self) -> None:
        with self._lock:
            self._active.clear()
            self._counts.clear()


sse_connections = SSEConnectionRegistry()
