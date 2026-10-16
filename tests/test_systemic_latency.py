"""Shared latency regressions using only disposable stores and runtimes."""
import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import AgentSession, CardCreate, CardEvent, EventType, SyncCommit, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.session_presentation import build_session_presentation
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import list_agent_sessions
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


def event(title):
    return CardEvent(type=EventType.CARD_CREATED, realm_id="default", card_id=title,
                     author_principal="user:test", author_instance="local",
                     payload={"id": title, "title": title})


def imported(log, title, parents):
    value = event(title)
    digest = log.store.put_json(value.model_dump(mode="json"))
    commit = SyncCommit(hash="", realm_id="default", instance_id="remote",
                        author_principal="user:test", parent_hashes=parents, event_hashes=[digest])
    commit.hash = log.store.put_json(commit.model_dump(mode="json"))
    return commit


def test_remote_suffix_never_decodes_projected_history_and_merge_keeps_both_parents(tmp_path):
    log = EventLog(ObjectStore(tmp_path / "objects"), tmp_path, "local")
    projection = CardProjection(tmp_path / "pa.db", log)
    old_events = set()
    heads = []
    for i in range(60):
        commit = projection.commit_event(event(f"old-{i}"))
        old_events.update(commit.event_hashes)
        heads.append(commit.hash)
    side = imported(log, "new-side", [heads[10]])
    merge = imported(log, "new-merge", [heads[-1], side.hash])
    original = log.get_event
    def only_new(digest):
        assert digest not in old_events, "redecoded a previously indexed event"
        return original(digest)
    with patch.object(log, "get_event", side_effect=only_new), patch.object(
        log, "get_commit", wraps=log.get_commit
    ) as read:
        assert log.is_ancestor(heads[-1], merge.hash)
        assert log.ensure_indexed("default", merge.hash)
        result = projection.catch_up_projection("default", merge.hash)
        assert result["commits_applied"] == 2
        assert read.call_count < 20
    assert projection.get_card("new-side")
    assert projection.get_card("new-merge")
    assert projection.get_projection_head("default") == merge.hash
    assert log._ancestors(merge.hash) == {h for h, _ in log._iter_commits_parent_first(merge.hash)}
    assert not log.is_ancestor(heads[-1], side.hash)
    assert projection.catch_up_projection("default", merge.hash)["commits_applied"] == 0


def test_incomplete_index_is_not_used_as_ancestry_evidence(tmp_path):
    log = EventLog(ObjectStore(tmp_path / "objects"), tmp_path, "local")
    _, first = log.append_event(event("first"))
    _, second = log.append_event(event("second"))
    log.index.mark_failed("default", "disposable test rebuild pending")
    assert log.index.ancestors(second.hash) is None
    with patch.object(log, "get_commit", wraps=log.get_commit) as read:
        assert log._ancestors(second.hash) == {first.hash, second.hash}
        assert read.call_count == 2


def test_sync_derived_index_io_does_not_hold_session_provenance_lock(tmp_path):
    from pa.modules.sync import _apply_sync_push_local
    log = EventLog(ObjectStore(tmp_path / "objects"), tmp_path, "local")
    store = CardProjection(tmp_path / "pa.db", log)
    base = store.commit_event(event("base"))
    target = imported(log, "next", [base.hash])
    store.save_session(AgentSession(id="new-session", agent_name="codex"))
    services = {"event_log": log, "sync_engine": SimpleNamespace(ingest_objects=lambda _: []),
                "sync_metrics": SimpleNamespace(record_pull=lambda _: None)}
    ctx = SimpleNamespace(store=store, require_service=services.__getitem__)
    entered, release = threading.Event(), threading.Event()
    original = log.ensure_indexed
    def index(realm, head):
        entered.set()
        assert release.wait(5)
        return original(realm, head)
    with ThreadPoolExecutor(max_workers=2) as pool, patch.object(log, "ensure_indexed", side_effect=index):
        push = pool.submit(_apply_sync_push_local, ctx, "default", target.hash, {})
        assert entered.wait(2)
        try:
            assert store.get_projection_head("default") == log.get_head("default") == target.hash
            binding = pool.submit(store.set_session_execution_binding, "new-session", {"version": 1},
                                  reason="workspace_binding_initialized", expected_binding={})
            assert binding.result(timeout=1).execution_binding == {"version": 1}
        finally:
            release.set()
        assert push.result()["commits_applied"] == 1
    assert log.index.indexed_head("default") == target.hash
    assert store.get_card("base") and store.get_card("next")


@pytest.mark.asyncio
async def test_worker_completion_delay_is_separate_from_service_and_cpu():
    runtime = AsyncRuntime()
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    def work():
        started.set()
        assert release.wait(2)
        finished.set()
    try:
        task = asyncio.create_task(runtime.run_blocking("fixture.work", work))
        await asyncio.to_thread(started.wait, 2)
        release.set()
        assert finished.wait(2)
        # Only this test's loop is stalled; the production server is untouched.
        time.sleep(0.05)
        await task
        metrics = runtime.snapshot()["operations"]["fixture.work"]
        assert metrics["completion_delivery_ms"]["p50"] >= 40
        assert metrics["runtime_ms"]["p50"] > metrics["worker_service_ms"]["p50"]
        assert metrics["thread_cpu_ms"]["p50"] < metrics["worker_service_ms"]["p50"]
        assert metrics["slow_calls"] == 0
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_required_harness_discovery_does_not_wait_for_optional_providers_or_publish_partial_full_catalog(tmp_path):
    from pa.execution.selection_service import SelectionService
    from tests.selection_browser_app import FixtureProvider

    service = SelectionService(Settings(data_dir=tmp_path, instance_id="local"), None)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def probe(_directory, *, manager=None, provider_ids=None):
        calls.append(provider_ids)
        if provider_ids is None:
            entered.set()
            await release.wait()
        return [FixtureProvider(harness).status(tmp_path).model_dump(mode="json")
                for harness in (sorted(provider_ids) if provider_ids else ("codex", "cursor"))]
    with patch("pa.acp.providers.resolve.list_provider_summaries_bounded", side_effect=probe):
        full = asyncio.create_task(service.local_catalog(refresh=True, force=True))
        await entered.wait()
        try:
            targeted = await asyncio.wait_for(
                service.local_catalog(refresh=True, force=True, required_harness="codex"), 1
            )
            assert targeted and {candidate.harness for candidate in targeted} == {"codex"}
            assert service.store.catalog("local")[0] == []
            # Forcing one harness must not throttle a different scope's first refresh.
            cursor = await service.local_catalog(refresh=True, force=True, required_harness="cursor")
            assert {candidate.harness for candidate in cursor} == {"cursor"}
            assert calls == [None, {"codex"}, {"cursor"}]
        finally:
            release.set()
            combined = await full
        assert {candidate.harness for candidate in combined} == {"codex", "cursor"}
    await service.close()


def test_catalog_scope_obeys_automatic_preferred_and_required_precedence(tmp_path):
    from pa.execution.selection import ExecutionPreferences, SelectionPolicy
    from pa.execution.selection_service import SelectionService
    service = SelectionService(Settings(data_dir=tmp_path, agent_provider="codex"), None)
    policy = SelectionPolicy(defaults=ExecutionPreferences(harness={"intent": "required", "value": "codex"}))
    with patch.object(service.store, "policy", return_value=policy):
        for intent, value, expected in (("automatic", None, None), ("preferred", "cursor", None),
                                        ("required", "cursor", "cursor")):
            assert service.required_catalog_harness(principal="user:test", realm="default", surface="chat.default",
                overrides=ExecutionPreferences(harness={"intent": intent, "value": value})) == expected


def test_sidebar_preserves_settings_status_and_links_without_full_proofs(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    dispatches = DispatchStore(tmp_path)
    watches = PRSupervisorStore(tmp_path / "watches.db")
    settings = Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False)
    manager = AgentSessionManager(settings, store)
    session = AgentSession(id="session", agent_name="codex", purpose="chat", config_json={
        "execution_selection": {"proof": "large proof " * 50000},
        "execution_context": {"repositories": [{"repository_url": "https://github.com/example/repo",
                                                 "branch": "branch", "proof": "large " * 10000}]},
        "values": {"model": "model", "reasoningEffort": "high"},
        "durable_runtime": {"queued_prompts": [{"id": "queued", "text": "private large " * 10000}],
                            "last_event_cursor": 12},
    })
    store.save_session(session)
    record = dispatches.put(DispatchRecord(mutation_id="m", session_id=session.id,
        authority_instance_id="local", target_instance_id="local", authority_url="http://fixture",
        placement_decision={"proof": "large " * 100000}))
    watches.upsert_watch(PRWatch(repository="example/repo", pr_number=1, pr_url="https://github.com/example/repo/pull/1",
        realm_id="default", originating_session_id=session.id, state={"proof": "large " * 10000}))
    ctx = SimpleNamespace(store=store, settings=settings, services={"dispatch_store": dispatches,
        "pr_supervisor_store": watches}, require_service=lambda name: manager)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
    with patch.object(dispatches, "_snapshot", side_effect=AssertionError("copied dispatch history")), patch.object(
        watches, "list_watches", side_effect=AssertionError("hydrated all watches")
    ), patch("pa.modules.agent_chat.current_instance_name", return_value="Local"):
        result = list_agent_sessions(request, view="chats")
    item = result[0]
    assert len(json.dumps(result)) < 6000
    assert item["queue_length"] == 1 and item["last_seq"] == 12
    assert item["config_json"]["values"]["reasoningEffort"] == "high"
    assert item["config_json"]["execution_context"]["repositories"][0]["branch"] == "branch"
    assert item["pr_watches"][0]["pr_number"] == 1
    expected = build_session_presentation(session, dispatch=record)
    assert {k: v for k, v in item["presentation"].items() if k != "observed_at"} == {
        k: v for k, v in expected.items() if k != "observed_at"
    }
    assert store.get_session(session.id).config_json == session.config_json
    assert dispatches.get(record.dispatch_id).placement_decision == record.placement_decision
    with patch("pa.modules.agent_chat.current_instance_name", return_value="Local"):
        full = list_agent_sessions(request, view="chats", detail="full")
    assert full[0]["config_json"]["execution_selection"] == session.config_json["execution_selection"]
    dispatches.close()
    store.close_thread_connection()


@pytest.mark.asyncio
async def test_burst_transcript_drain_batches_without_losing_order_or_durability(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    executor = AsyncRuntime()
    manager = AgentSessionManager(Settings(data_dir=tmp_path, agent_enabled=False), store)
    manager.async_runtime = executor
    session = AgentSession(id="burst", agent_name="codex")
    store.save_session(session)
    runtime = AgentSessionRuntime(manager, session, initial_transcript_seq=0)
    try:
        with patch.object(store, "append_transcript_events", wraps=store.append_transcript_events) as append:
            expected = [runtime._append_transcript("output", {"text": f"event {i}"}) for i in range(512)]
            await runtime._drain_transcripts(raise_on_timeout=True)
        assert append.call_count == 8
        assert [len(call.args[0]) for call in append.call_args_list] == [64] * 8
        rows = store.list_transcript_events(session.id, limit=1000)
        assert [row.id for row in rows] == [row["id"] for row in expected]
        assert [row.seq for row in rows] == list(range(1, 513))
        assert runtime._transcript_queue._unfinished_tasks == 0
        assert not runtime._transcript_buffer
        assert store.transcript_storage_metrics()["events"] == 512
    finally:
        await executor.close()
        store.close_thread_connection()


def test_batched_sidebar_links_and_lifecycle_keep_exact_membership_and_bounds(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    card = store.create_card(CardCreate(title="Linked", body="large body " * 10000), via_log=False)
    for session_id in ("one", "two"):
        store.save_session(AgentSession(id=session_id, agent_name="codex", card_id=card.id))
        store.append_transcript_events([
            TranscriptEvent(session_id=session_id, seq=1, event_type="session_started"),
            TranscriptEvent(session_id=session_id, seq=2, event_type="session_closed", payload={"reason": "closed"}),
        ])
    with patch.object(store, "_row_to_card", side_effect=AssertionError("hydrated full cards")), patch.object(
        store, "_conn", wraps=store._conn
    ) as connect:
        links = store.list_session_card_summaries(["one", "two", "empty"])
        assert connect.call_count == 1
    assert links["one"] == links["two"] == [{"id": card.id, "title": "Linked", "project_id": None}]
    assert links["empty"] == []
    with patch.object(store.transcripts, "_conn", wraps=store.transcripts._conn) as connect:
        lifecycle = store.transcript_lifecycle_summaries(["one", "two", "empty"])
        assert connect.call_count == 1
    assert lifecycle["one"] == lifecycle["two"] == {"provider_attempts": 1, "closure_reason": "closed"}
    assert lifecycle["empty"] == {"provider_attempts": 0, "closure_reason": None}
    store.close_thread_connection()
