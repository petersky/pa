"""Root inventory keeps authorization and fresh resolution while bounding comparisons."""
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.requests import Request

from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules.files import _trusted_roots


def request(root, sessions=(), projects=(), *, auth=False, store=None):
    app = Starlette()
    app.state.ctx = SimpleNamespace(
        settings=SimpleNamespace(data_dir=root, auth_required=auth),
        store=store or SimpleNamespace(list_sessions=lambda: sessions, list_projects=lambda: projects),
    )
    value = Request({"type":"http", "method":"GET", "path":"/browse", "headers":[], "app":app})
    value.state.principal_id = "user:alice"
    value.state.user_authenticated = True
    return value


def test_distinct_root_membership_does_not_compare_every_prior_path(tmp_path, monkeypatch):
    class CountedPath(type(Path())):
        comparisons = 0

        def __eq__(self, other):
            type(self).comparisons += 1
            return super().__eq__(other)

        __hash__ = type(Path()).__hash__

    calls = []
    def resolve(path, *args, **kwargs):
        calls.append(str(path))
        return CountedPath(path)
    monkeypatch.setattr(Path, "resolve", resolve)
    sessions = [SimpleNamespace(cwd=str(tmp_path/f"root-{index}")) for index in range(1000)]
    roots = _trusted_roots(request(tmp_path, sessions))
    comparisons = CountedPath.comparisons
    assert len(roots) == len(calls) == 1001
    assert [str(path) for path in roots] == calls
    assert comparisons <= 2*len(roots)


def test_duplicate_candidates_are_each_resolved_and_can_observe_changed_symlink(tmp_path, monkeypatch):
    shared = tmp_path/"link"
    calls = []
    def resolve(path, *args, **kwargs):
        calls.append(path)
        if path == shared:
            return tmp_path/("first" if calls.count(shared) == 1 else "second")
        if path == tmp_path/"bad":
            raise OSError("unavailable")
        return path
    monkeypatch.setattr(Path, "resolve", resolve)
    sessions = [SimpleNamespace(cwd=str(path)) for path in
                (shared, tmp_path/"bad", shared, tmp_path/"bad", tmp_path/"first")]
    assert _trusted_roots(request(tmp_path,sessions)) == [tmp_path,tmp_path/"first",tmp_path/"second"]
    assert calls == [tmp_path,shared,tmp_path/"bad",shared,tmp_path/"bad",tmp_path/"first"]


def test_roots_remain_fresh_per_call_and_preserve_first_resolved_order(tmp_path):
    first, second = tmp_path/"first", tmp_path/"second"
    first.mkdir(); second.mkdir()
    alias = tmp_path/"alias"; alias.symlink_to(first, target_is_directory=True)
    sessions = [SimpleNamespace(cwd=str(alias)),SimpleNamespace(cwd=str(first))]
    value = request(tmp_path,sessions)
    assert _trusted_roots(value) == [tmp_path.resolve(),first.resolve()]
    alias.unlink();alias.symlink_to(second,target_is_directory=True)
    assert _trusted_roots(value) == [tmp_path.resolve(),second.resolve(),first.resolve()]


def test_authority_filters_and_project_membership_preserve_order(tmp_path):
    sessions = [SimpleNamespace(cwd=str(tmp_path/name),principal_id=principal) for name,principal in
                (("bob","user:bob"),("legacy",None),("alice","user:alice"))]
    projects = [SimpleNamespace(created_by_principal="user:bob",
                 memberships=[SimpleNamespace(principal_id="user:alice")],
                 repos=[SimpleNamespace(path=str(tmp_path/"project")),SimpleNamespace(path=str(tmp_path/"alice"))])]
    assert _trusted_roots(request(tmp_path,sessions,projects,auth=True)) == [
        tmp_path/"users/alice",tmp_path/"alice",tmp_path/"project"]


def test_full_session_decoder_still_rejects_invalid_non_auth_fields(tmp_path):
    store = CardProjection(tmp_path/"pa.db")
    try:
        store.save_session(AgentSession(id="s",agent_name="codex",cwd=str(tmp_path/"session")))
        value = request(tmp_path,store=store)
        assert tmp_path/"session" in _trusted_roots(value)
        with store._conn() as conn:
            conn.execute("UPDATE agent_sessions SET metrics_json='[]' WHERE id='s'")
            conn.commit()
        with pytest.raises(ValidationError):
            _trusted_roots(value)
    finally:
        store.close_thread_connection()


def test_archived_and_legacy_sessions_keep_existing_root_eligibility(tmp_path):
    store = CardProjection(tmp_path/"pa.db")
    try:
        store.save_session(AgentSession(id="archived",agent_name="codex",cwd=str(tmp_path/"archived"),
                            archived_at=datetime.now(UTC),principal_id="user:alice"))
        store.save_session(AgentSession(id="legacy",agent_name="codex",cwd=str(tmp_path/"legacy"),principal_id=None))
        assert set(_trusted_roots(request(tmp_path,store=store))) == {tmp_path,tmp_path/"archived",tmp_path/"legacy"}
        assert _trusted_roots(request(tmp_path,auth=True,store=store)) == [tmp_path/"users/alice",tmp_path/"archived"]
    finally:
        store.close_thread_connection()
