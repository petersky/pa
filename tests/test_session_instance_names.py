"""Response-local native origin labels retain fallback and custom readers."""
from unittest.mock import patch

import pytest

from pa.core.ui import instance_identity
from pa.domain.models import FleetInstance
from pa.fleet.registry import FleetRegistry
from pa.modules import agent_chat
from tests.test_session_list_snapshot import listing


@pytest.fixture
def names(listing, tmp_path):
    store, _, request = listing
    fleet = FleetRegistry(tmp_path, "fixture")
    fleet._instances["origin"] = FleetInstance(instance_id="origin", name="Before", url="")
    fleet._save_instances()
    request.app.state.ctx.services["fleet_registry"] = fleet
    for session in store.list_sessions():
        session.origin_instance_id = "origin"
        session.origin_instance_name = "Snapshot"
        store.save_session(session)
    with patch.object(agent_chat, "current_instance_name", instance_identity.current_instance_name):
        yield store, request, fleet


@pytest.mark.parametrize("detail", ["summary", "full"])
def test_complete_response_matches_independent_reads(names, detail):
    _, request, fleet = names
    with patch.object(fleet, "list_instances", wraps=fleet.list_instances) as reader:
        expected = agent_chat.list_agent_sessions(request, view="all", detail=detail)
    assert reader.call_count == 12
    with patch.object(instance_identity, "resolve_instance_identity", wraps=instance_identity.resolve_instance_identity) as reader:
        actual = agent_chat.list_agent_sessions(request, view="all", detail=detail)
    assert actual == expected
    assert reader.call_count == 1


def test_first_read_is_response_local_next_request_sees_rename(names):
    _, request, fleet = names
    original = agent_chat._session_list_item
    rows = []

    def row(*args, **kwargs):
        value = original(*args, **kwargs)
        rows.append(value)
        if len(rows) == 1:
            fleet._instances["origin"].name = "After"
            fleet._save_instances()
        return value

    with patch.object(agent_chat, "_session_list_item", side_effect=row):
        result = agent_chat.list_agent_sessions(request, view="all")
    assert [r["origin_instance_name"] for r in result] == ["Before"] * 12
    assert [r["origin_instance_name"] for r in agent_chat.list_agent_sessions(request, view="all")] == ["After"] * 12


def test_unknown_removed_empty_and_snapshot_fallbacks_stay_distinct(names):
    store, request, fleet = names
    fleet._instances["origin"].lifecycle_state = "removed"
    fleet._save_instances()
    for key, identity, snapshot in [("s-00", "origin", "First"), ("s-01", "origin", "Second"),
                                    ("s-02", "missing", None), ("s-03", None, "Empty")]:
        session = store.get_session(key)
        session.origin_instance_id, session.origin_instance_name = identity, snapshot
        store.save_session(session)
    actual = agent_chat.list_agent_sessions(request, view="all")
    with patch.object(fleet, "list_instances", wraps=fleet.list_instances):
        expected = agent_chat.list_agent_sessions(request, view="all")
    assert actual == expected
    assert [r["origin_instance_name"] for r in actual[:4]] == ["First", "Second", "Unknown instance · missing", "Empty"]


@pytest.mark.parametrize("extension", ["instance", "subclass"])
@pytest.mark.parametrize("method", ["list_instances", "_reload_instances", "_instances_disk_key"])
def test_custom_registry_readers_remain_per_row(names, extension, method):
    _, request, fleet = names
    native = getattr(FleetRegistry, method)
    calls = []

    def custom(self, *args, **kwargs):
        calls.append(True)
        return native(self, *args, **kwargs)

    if extension == "instance":
        setattr(fleet, method, custom.__get__(fleet, type(fleet)))
    else:
        class CustomRegistry(FleetRegistry):
            pass
        setattr(CustomRegistry, method, custom)
        fleet.__class__ = CustomRegistry
    try:
        result = agent_chat.list_agent_sessions(request, view="all")
        assert len(result) == len(calls) == 12
    finally:
        if extension == "instance":
            delattr(fleet, method)


def test_custom_display_resolver_remains_per_row(names):
    _, request, _ = names
    with patch.object(agent_chat, "current_instance_name", side_effect=[f"Custom {i}" for i in range(12)]) as read:
        result = agent_chat.list_agent_sessions(request, view="all")
    assert [r["origin_instance_name"] for r in result] == [f"Custom {i}" for i in range(12)]
    assert read.call_count == 12
