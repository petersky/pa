from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import Request

from pa.config import Settings
from pa.domain.models import CardEvent, EventType, PeerRoute
from pa.fleet.membership import MembershipStore
from pa.modules.instance import health
from pa.modules.sync import sync_recovery as retry_sync_recovery
from pa.network.peer_table import PeerTable
from pa.server.readiness import (
    REQUIRED_READY_PATHS,
    REQUIRED_READY_SERVICES,
    evaluate_ready,
)
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventHistoryObjectError, EventLog
from pa.sync.object_store import ObjectStore, object_hash
from pa.sync.recovery import SyncRecovery


def _event() -> CardEvent:
    return CardEvent(
        type=EventType.CARD_CREATED,
        realm_id="default",
        card_id="card-1",
        author_principal="user:local",
        author_instance="local",
        payload={"id": "card-1", "realm_id": "default", "title": "Recovered"},
    )


def _fixture(tmp_path: Path):
    settings = Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False)
    objects = ObjectStore(settings.objects_dir)
    log = EventLog(objects, tmp_path, "local")
    _, commit = log.append_event(_event())
    event_hash = commit.event_hashes[0]
    event_bytes = objects.get(event_hash)
    assert event_bytes is not None
    objects._path_for(event_hash).unlink()
    log.index.reset_realm("default")
    peers = PeerTable(tmp_path)
    peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy"))
    engine = SyncEngine(settings, objects, log, peers, MembershipStore(tmp_path))
    return settings, objects, log, engine, event_hash, event_bytes


class _SyntheticDeepStore:
    """Generate a deterministic content-addressed chain without disk/CPU churn."""

    event_hash = "event-00000"

    def __init__(self, commit_count: int) -> None:
        self.commit_count = commit_count
        event = _event().model_dump(mode="json")
        event["test_hash"] = self.event_hash
        self.event_bytes = json.dumps(event).encode()
        self.event_repaired = False

    @staticmethod
    def hash(raw: bytes) -> str:
        return str(json.loads(raw)["test_hash"])

    @property
    def head(self) -> str:
        return f"commit-{self.commit_count - 1:05d}"

    def get(self, expected: str) -> bytes | None:
        if expected == self.event_hash:
            return self.event_bytes if self.event_repaired else None
        if not expected.startswith("commit-"):
            return None
        index = int(expected.removeprefix("commit-"))
        if not 0 <= index < self.commit_count:
            return None
        return json.dumps(
            {
                "schema_version": 1,
                "hash": "",
                "realm_id": "default",
                "instance_id": "local",
                "parent_hashes": [f"commit-{index - 1:05d}"] if index else [],
                "event_hashes": [self.event_hash],
                "author_principal": "user:local",
                "timestamp": "2026-08-23T00:00:00Z",
                "signature": None,
                "test_hash": expected,
            }
        ).encode()

    def has(self, expected: str) -> bool:
        return self.get(expected) is not None

    def repair(self, expected: str, data: bytes) -> str:
        assert self.hash(data) == expected == self.event_hash
        self.event_repaired = True
        return expected


class _DeepIndexedLog:
    """Minimal ref/index harness that locally validates every reachable object."""

    def __init__(
        self,
        store: _SyntheticDeepStore,
        root: Path,
        head: str,
        expected_commits: int,
    ) -> None:
        self.store = store
        self.head = head
        self.expected_commits = expected_commits
        self.refs_path = root / "sync_refs.json"
        self.refs_path.write_text(json.dumps({"default/local": head}))
        self._status = {"state": "stale", "ready": False, "commit_count": 0}

    def get_head(self, realm_id: str) -> str | None:
        return self.head if realm_id == "default" else None

    def verify_index(self, realm_id: str, head: str) -> None:
        assert realm_id == "default" and head == self.head
        event_raw = self.store.get(self.store.event_hash)
        assert event_raw is not None
        assert self.store.hash(event_raw) == self.store.event_hash
        self._status = {
            "state": "ready",
            "ready": True,
            "commit_count": self.expected_commits,
        }

    def index_status(self, realm_id: str) -> dict:
        assert realm_id == "default"
        return self._status


def _deep_fixture(tmp_path: Path, commit_count: int = 20_005):
    """Build and locally validate a real >20k content-addressed commit DAG."""
    settings = Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False)
    objects = _SyntheticDeepStore(commit_count)
    log = _DeepIndexedLog(objects, tmp_path, objects.head, commit_count)
    peers = PeerTable(tmp_path)
    peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy"))
    engine = SyncEngine(settings, objects, log, peers, MembershipStore(tmp_path))
    return (
        settings,
        objects,
        log,
        engine,
        objects.event_hash,
        objects.event_bytes,
        commit_count,
    )


@pytest.mark.asyncio
async def test_missing_event_recovers_without_moving_ref(tmp_path: Path) -> None:
    settings, objects, log, engine, event_hash, event_bytes = _fixture(tmp_path)
    original_head = log.get_head("default")

    async def request(method, url, *, payload=None, **_kwargs):
        assert payload == {"hashes": [event_hash]}
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {event_hash: base64.b64encode(event_bytes).decode()}},
        )

    engine._request = request
    rebuilt: list[str] = []
    recovery = SyncRecovery(settings, engine, rebuilt.append)
    failure = EventHistoryObjectError("missing_event", event_hash, "event")
    assert await recovery.recover([("default", failure)]) is True
    assert objects.get(event_hash) == event_bytes
    assert log.get_head("default") == original_head
    assert rebuilt == ["default"]
    assert recovery.public()["state"] == "healthy"
    assert recovery.public()["work"]["peer_requests"] == 1
    assert recovery.public()["work"]["fetched_objects"] == 1


@pytest.mark.asyncio
async def test_mismatched_peer_object_is_rejected_without_partial_projection(tmp_path: Path) -> None:
    settings, objects, log, engine, event_hash, _event_bytes = _fixture(tmp_path)

    async def request(method, url, *, payload=None, **_kwargs):
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {event_hash: base64.b64encode(b"wrong").decode()}},
        )

    engine._request = request
    rebuilt: list[str] = []
    recovery = SyncRecovery(settings, engine, rebuilt.append)
    failure = EventHistoryObjectError("missing_event", event_hash, "event")
    assert await recovery.recover([("default", failure)]) is False
    assert objects.get(event_hash) is None
    assert rebuilt == []
    assert recovery.public()["state"] == "unrecoverable"
    assert recovery.public()["attempts"] == [
        {"peer": "configured_peer", "result": "corrupt_object"}
    ]

    adopted = SyncRecovery(settings, engine, rebuilt.append)
    assert adopted.public()["state"] == "unrecoverable"
    assert adopted.public()["object_hash"] == event_hash


@pytest.mark.asyncio
async def test_missing_parent_commit_is_recovered_with_original_topology(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False)
    objects = ObjectStore(settings.objects_dir)
    log = EventLog(objects, tmp_path, "local")
    _, parent = log.append_event(_event())
    second = _event().model_copy(update={"type": EventType.CARD_UPDATED, "payload": {"title": "new"}})
    _, head = log.append_event(second)
    parent_bytes = objects.get(parent.hash)
    assert parent_bytes is not None
    objects._path_for(parent.hash).unlink()
    log.index.reset_realm("default")
    peers = PeerTable(tmp_path)
    peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy"))
    engine = SyncEngine(settings, objects, log, peers, MembershipStore(tmp_path))

    async def request(method, url, *, payload=None, **_kwargs):
        requested = payload["hashes"][0]
        supplied = parent_bytes if requested == parent.hash else objects.get(requested)
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {requested: base64.b64encode(supplied).decode()} if supplied else {}},
        )

    engine._request = request
    rebuilt: list[str] = []
    recovery = SyncRecovery(settings, engine, rebuilt.append)
    failure = EventHistoryObjectError("missing_parent", parent.hash, "commit")
    assert await recovery.recover([("default", failure)]) is True
    assert log.get_head("default") == head.hash
    assert log.get_commit(head.hash).parent_hashes == [parent.hash]
    assert rebuilt == ["default"]


@pytest.mark.asyncio
async def test_one_missing_event_in_over_20k_history_uses_one_peer_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("pa.sync.recovery.object_hash", _SyntheticDeepStore.hash)
    (
        settings,
        objects,
        log,
        engine,
        event_hash,
        event_bytes,
        commit_count,
    ) = _deep_fixture(tmp_path)
    original_ref = log.refs_path.read_bytes()
    requests: list[list[str]] = []

    async def request(method, url, *, payload=None, **_kwargs):
        requests.append(payload["hashes"])
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {event_hash: base64.b64encode(event_bytes).decode()}},
        )

    engine._request = request
    rebuilt: list[str] = []
    recovery = SyncRecovery(settings, engine, rebuilt.append)
    failure = EventHistoryObjectError("missing_event", event_hash, "event")

    assert await recovery.recover([("default", failure)]) is True
    assert requests == [[event_hash]]
    assert recovery.public()["work"] == {
        "peer_requests": 1,
        "fetched_objects": 1,
        "validation_passes": 1,
        "head_changes": 0,
        "max_peer_requests": 64,
        "max_fetched_objects": 32,
        "max_head_changes": 8,
        "limit_hit": None,
    }
    assert log.index_status("default")["commit_count"] == commit_count
    assert log.index_status("default")["ready"] is True
    assert rebuilt == ["default"]
    assert log.refs_path.read_bytes() == original_ref


@pytest.mark.asyncio
async def test_retry_after_peer_availability_transitions_health_and_readiness(
    tmp_path: Path,
) -> None:
    settings, objects, log, engine, event_hash, event_bytes = _fixture(tmp_path)
    original_ref = log.refs_path.read_bytes()

    async def unavailable(method, url, *, payload=None, **_kwargs):
        raise httpx.ConnectError("peer unavailable", request=httpx.Request(method, url))

    engine._request = unavailable
    rebuilt: list[str] = []
    recovery = SyncRecovery(settings, engine, rebuilt.append)
    failure = EventHistoryObjectError("missing_event", event_hash, "event")
    assert await recovery.recover([("default", failure)]) is False
    assert recovery.public()["attempts"][0]["result"] == "peer_unavailable"

    membership = engine.membership
    membership.ensure_owner_membership("default", "local")
    services = {name: object() for name in REQUIRED_READY_SERVICES}
    services.update(
        {
            "agent_lifecycle": {"phase": "ready"},
            "membership": membership,
            "sync_recovery": recovery,
            "sync_startup_repaired": False,
        }
    )
    ctx = SimpleNamespace(
        settings=settings,
        services=services,
        require_service=lambda name: services[name],
    )
    app = FastAPI()
    app.state.ctx = ctx
    app.state.ready_openapi_warmed = True
    app.state.ready_paths = REQUIRED_READY_PATHS
    app.state.required_ready_paths = REQUIRED_READY_PATHS
    request = Request({"type": "http", "method": "POST", "path": "/", "app": app})
    request.state.principal_id = "user:local"

    assert (await health(request))["status"] == "degraded"
    assert evaluate_ready(app, ctx, settings)["status"] == "degraded"

    async def available(method, url, *, payload=None, **_kwargs):
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {event_hash: base64.b64encode(event_bytes).decode()}},
        )

    engine._request = available
    result = await retry_sync_recovery(request, {"realm_id": "default"})
    assert result["recovered"] is True
    assert ctx.services["sync_startup_repaired"] is True
    assert (await health(request))["status"] == "ok"
    assert evaluate_ready(app, ctx, settings) is None
    assert objects.get(event_hash) == event_bytes
    assert rebuilt == ["default"]
    assert log.refs_path.read_bytes() == original_ref


@pytest.mark.asyncio
async def test_fetched_object_limit_is_precise_and_preserves_ref(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False)
    objects = ObjectStore(settings.objects_dir)
    log = EventLog(objects, tmp_path, "local")
    first_event, _ = log.append_event(_event())
    second_event = _event().model_copy(
        update={"type": EventType.CARD_UPDATED, "payload": {"title": "second"}}
    )
    second_event, _ = log.append_event(second_event)
    first_hash = objects.put_json(first_event.model_dump(mode="json"))
    second_hash = objects.put_json(second_event.model_dump(mode="json"))
    event_bytes = {
        first_hash: objects.get(first_hash),
        second_hash: objects.get(second_hash),
    }
    assert all(event_bytes.values())
    objects._path_for(first_hash).unlink()
    objects._path_for(second_hash).unlink()
    log.index.reset_realm("default")
    original_ref = log.refs_path.read_bytes()
    peers = PeerTable(tmp_path)
    peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy"))
    engine = SyncEngine(settings, objects, log, peers, MembershipStore(tmp_path))

    async def request(method, url, *, payload=None, **_kwargs):
        requested = payload["hashes"][0]
        raw = event_bytes[requested]
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            json={"objects": {requested: base64.b64encode(raw).decode()}},
        )

    engine._request = request
    monkeypatch.setattr("pa.sync.recovery.MAX_RECOVERY_FETCHED_OBJECTS", 1)
    recovery = SyncRecovery(settings, engine, lambda _realm: None)
    failure = EventHistoryObjectError("missing_event", first_hash, "event")

    assert await recovery.recover([("default", failure)]) is False
    public = recovery.public()
    assert public["attempts"][-1]["result"] == "fetched_object_limit_exceeded"
    assert public["work"]["peer_requests"] == 2
    assert public["work"]["fetched_objects"] == 1
    assert public["work"]["max_fetched_objects"] == 1
    assert public["work"]["limit_hit"] == "fetched_object_limit"
    assert log.refs_path.read_bytes() == original_ref
