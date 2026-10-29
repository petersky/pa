"""One-invocation native display labels preserve validation and custom readers."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.domain.models import Project
from pa.domain.projection import CardProjection
from pa.modules import agent_chat
from tests.test_session_list_snapshot import listing


def attach_projects(store):
    store._upsert_project(Project(id="project", title="Before", description="x" * 65536))
    for session in store.list_sessions():
        session.project_id = "project"
        store.save_session(session)


@pytest.mark.parametrize("detail", ["summary", "full"])
def test_complete_response_matches_independent_project_reads(listing, detail):
    store, _, request = listing
    attach_projects(store)
    with patch.object(store, "get_project", wraps=store.get_project) as getter:
        expected = agent_chat.list_agent_sessions(request, view="all", detail=detail)
    assert getter.call_count == 12
    with patch.object(agent_chat, "_session_project_payload", wraps=agent_chat._session_project_payload) as labels:
        actual = agent_chat.list_agent_sessions(request, view="all", detail=detail)
    assert actual == expected
    assert labels.call_count == 1
    assert actual[0]["project"] is not actual[1]["project"]
    actual[0]["project"]["title"] = "Caller edit"
    assert actual[1]["project"]["title"] == "Before"


@pytest.mark.parametrize("initially_missing", [False, True])
def test_first_read_label_is_consistent_and_next_invocation_reads_again(listing, initially_missing):
    store, _, request = listing
    attach_projects(store)
    if initially_missing:
        with store._conn() as conn:
            conn.execute("DELETE FROM projects WHERE id='project'")
    original = agent_chat._session_list_item
    calls = []

    def row(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(result)
        if len(calls) == 1:
            store._upsert_project(Project(id="project", title="After"))
        return result

    with patch.object(agent_chat, "_session_list_item", side_effect=row):
        result = agent_chat.list_agent_sessions(request, view="all")
    first = None if initially_missing else {"id": "project", "title": "Before"}
    assert [r["project"] for r in result] == [first] * 12
    refreshed = agent_chat.list_agent_sessions(request, view="all")
    assert [r["project"] for r in refreshed] == [{"id": "project", "title": "After"}] * 12


def test_realm_missing_and_null_project_are_distinct(listing):
    store, _, request = listing
    attach_projects(store)
    for key, realm, project in [("s-00", "other", "project"), ("s-01", "default", "missing"),
                                ("s-02", "default", None)]:
        session = store.get_session(key)
        session.realm_id, session.project_id = realm, project
        store.save_session(session)
    result = agent_chat.list_agent_sessions(request, view="all")
    assert [r["project"] for r in result[:3]] == [None, None, None]
    assert result[3]["project"] == {"id": "project", "title": "Before"}


@pytest.mark.parametrize("extension", ["instance", "subclass"])
@pytest.mark.parametrize("reader", ["getter", "converter"])
def test_custom_project_reader_preserves_per_row_behavior(listing, extension, reader):
    store, manager, request = listing
    attach_projects(store)
    calls = []
    native = CardProjection.get_project if reader == "getter" else CardProjection._row_to_project

    def custom(self, *args, **kwargs):
        project = native(self, *args, **kwargs)
        calls.append(project.id)
        project.title = f"Custom {len(calls)}"
        return project

    name = "get_project" if reader == "getter" else "_row_to_project"
    if extension == "subclass":
        class CustomProjection(CardProjection):
            pass
        setattr(CustomProjection, name, custom)
        target = CustomProjection(store.db_path)
    else:
        target = store
        setattr(target, name, custom.__get__(target, type(target)))
    manager.store = request.app.state.ctx.store = target
    try:
        result = agent_chat.list_agent_sessions(request, view="all")
        assert [r["project"]["title"] for r in result] == [f"Custom {i}" for i in range(1, 13)]
        assert len(calls) == 12
    finally:
        if extension == "instance":
            # Do not leave a bound method/self cycle in the fixture heap.
            delattr(target, name)
        else:
            target.close_thread_connection()


def test_native_read_keeps_full_project_validation_and_selected_bounds(listing):
    store, _, request = listing
    attach_projects(store)
    store._upsert_project(Project(id="invalid", title="Invalid"))
    last = store.get_session("s-11")
    last.project_id = "invalid"
    store.save_session(last)
    with store._conn() as conn:
        conn.execute("UPDATE projects SET tool_config='{malformed' WHERE id='invalid'")
    assert len(agent_chat.list_agent_sessions(request, view="all", limit=2)) == 2
    errors = []
    for native in [False, True]:
        with pytest.raises(ValueError) as caught:
            if native:
                agent_chat.list_agent_sessions(request, view="all", limit=2, selected_session_id="s-11")
            else:
                with patch.object(store, "get_project", wraps=store.get_project):
                    agent_chat.list_agent_sessions(request, view="all", limit=2, selected_session_id="s-11")
        errors.append((type(caught.value), str(caught.value)))
    assert errors[0] == errors[1]
