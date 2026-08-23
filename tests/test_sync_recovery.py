from __future__ import annotations

import base64
from pathlib import Path

import httpx
import pytest

from pa.config import Settings
from pa.domain.models import CardEvent, EventType, PeerRoute
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventHistoryObjectError, EventLog
from pa.sync.object_store import ObjectStore
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
