"""Resilient client for PA's provider-neutral cloud coordination protocol."""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import httpx

from pa.config import Settings

logger = logging.getLogger(__name__)


class CloudLeaseResult(StrEnum):
    ACQUIRED = "acquired"
    DENIED = "denied"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class CloudPublication:
    kind: str
    payload: dict[str, Any]


class CloudCoordinator:
    """HTTPS adapter with bounded, best-effort publication and lease fencing.

    Cloud implementations only need four endpoints under ``/v1``: lease
    acquire/release and event/dispatch ingestion. 409/423 fence a competing
    lease; transport and 5xx failures are classified as unavailable so policy
    can choose safe fail-closed or local-first fail-open operation.
    """

    def __init__(self, settings: Settings, *, client: httpx.Client | None = None) -> None:
        self.endpoint = settings.cloud_endpoint
        self.instance_id = settings.instance_id
        self.fleet_id = settings.fleet_id
        self.fail_open = settings.cloud_lease_fail_open
        self._client = client or httpx.Client(
            base_url=self.endpoint,
            headers={
                "Authorization": f"Bearer {settings.cloud_token}",
                "User-Agent": "pa-cloud/1",
            },
            timeout=settings.cloud_timeout_seconds,
        )
        self._owns_client = client is None
        self._queue: queue.Queue[CloudPublication | None] = queue.Queue(
            maxsize=settings.cloud_publish_queue_capacity
        )
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None
        self._dropped = 0

    @property
    def configured(self) -> bool:
        return bool(self.endpoint)

    def start(self) -> None:
        if not self.configured or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._publish_loop, name="pa-cloud-publisher", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        if self._thread is not None:
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
            self._thread.join(timeout=5)
            self._thread = None
        if self._owns_client:
            self._client.close()

    def acquire_lease(self, payload: dict[str, Any]) -> CloudLeaseResult:
        return self._lease_request("/v1/leases/acquire", payload)

    def release_lease(self, payload: dict[str, Any]) -> CloudLeaseResult:
        return self._lease_request("/v1/leases/release", payload)

    def _lease_request(self, path: str, payload: dict[str, Any]) -> CloudLeaseResult:
        if not self.configured:
            return CloudLeaseResult.UNAVAILABLE
        try:
            response = self._client.post(path, json=self._envelope(payload))
            if response.status_code in {409, 423}:
                self._record_success()
                return CloudLeaseResult.DENIED
            response.raise_for_status()
            data = response.json()
            self._record_success()
            return (
                CloudLeaseResult.ACQUIRED
                if data.get("acquired", data.get("released", True)) is True
                else CloudLeaseResult.DENIED
            )
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            self._record_error(exc)
            return CloudLeaseResult.UNAVAILABLE

    def publish_event(self, payload: dict[str, Any]) -> None:
        self._enqueue("events", payload)

    def publish_dispatch(self, payload: dict[str, Any]) -> None:
        self._enqueue("dispatches", payload)

    def _enqueue(self, kind: str, payload: dict[str, Any]) -> None:
        if not self.configured:
            return
        try:
            self._queue.put_nowait(CloudPublication(kind, self._envelope(payload)))
        except queue.Full:
            with self._lock:
                self._dropped += 1
            logger.warning("Cloud publication queue full; dropped %s update", kind)

    def _publish_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            try:
                response = self._client.post(f"/v1/{item.kind}", json=item.payload)
                response.raise_for_status()
                self._record_success()
            except httpx.HTTPError as exc:
                self._record_error(exc)
                logger.warning("Cloud %s publication failed: %s", item.kind, exc)

    def _envelope(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "protocol_version": 1,
            "fleet_id": self.fleet_id,
            "instance_id": self.instance_id,
            "sent_at": datetime.now(UTC).isoformat(),
            "payload": payload,
        }

    def _record_success(self) -> None:
        with self._lock:
            self._last_success_at = datetime.now(UTC)
            self._last_error = None

    def _record_error(self, exc: Exception) -> None:
        with self._lock:
            self._last_error = type(exc).__name__

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "configured": self.configured,
                "endpoint": self.endpoint,
                "lease_fail_open": self.fail_open,
                "queued_publications": self._queue.qsize(),
                "dropped_publications": self._dropped,
                "last_success_at": self._last_success_at.isoformat()
                if self._last_success_at
                else None,
                "last_error": self._last_error,
            }
