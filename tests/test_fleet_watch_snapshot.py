from types import SimpleNamespace
from unittest.mock import patch

from pa.config import Settings
from pa.fleet import overview
from pa.domain.models import FleetInstance
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore


def fixture(root):
    store = PRSupervisorStore(root / "supervisor.db")
    watch = PRWatch(repository="example/repo", pr_number=1,
                    pr_url="https://github.com/example/repo/pull/1",
                    owner_instance_id="local", originating_instance_id="local",
                    state={"proof": {"checks": ["preserved"]}})
    store.upsert_watch(watch)
    ctx = SimpleNamespace(
        settings=Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                          instance_id="local", agent_enabled=False, peers=[]),
        services={"pr_supervisor_store": store},
        store=SimpleNamespace(list_repositories=lambda _: []),
    )
    return ctx, store


def build(ctx, instances):
    # Isolate supervisor work from unrelated local dimensions, which have their
    # own tests. The actual supervisor function and durable store are exercised.
    local = overview.local_dimension
    with patch.object(overview, "local_dimension", side_effect=lambda c, d:
                      local(c, d) if d == "supervisor" else {}):
        return overview.build_overview(ctx, instances, [])


def test_one_watch_read_gives_both_views_and_detached_public_results(tmp_path):
    ctx, store = fixture(tmp_path)
    with patch.object(store, "list_watches", wraps=store.list_watches) as read:
        result = build(ctx, [FleetInstance(instance_id="local", name="Local", url="http://local")])
    assert read.call_count == 1
    read.assert_called_with(include_retired=False)
    dimension = result["nodes"][0]["dimensions"]["supervisor"]["value"]["watches"][0]
    edge = result["edges"][0]["details"]["items"][0]["details"]
    assert edge == dimension
    edge["state"]["proof"]["checks"].append("caller changed")
    assert dimension["state"]["proof"]["checks"] == ["preserved"]
    assert store.list_watches()[0].state["proof"]["checks"] == ["preserved"]


def test_watch_edges_remain_when_local_instance_is_absent(tmp_path):
    ctx, store = fixture(tmp_path)
    with patch.object(store, "list_watches", wraps=store.list_watches) as read:
        result = build(ctx, [FleetInstance(instance_id="peer", name="Peer", url="http://peer")])
    assert read.call_count == 1
    assert result["edges"][0]["kind"] == "supervisor"
    assert result["edges"][0]["status"] == "unavailable"


def test_standalone_supervisor_dimension_still_reads_current_watches(tmp_path):
    ctx, store = fixture(tmp_path)
    first = overview.local_dimension(ctx, "supervisor")
    watch = store.list_watches()[0]
    watch.state["proof"]["checks"] = ["new"]
    store.upsert_watch(watch)
    second = overview.local_dimension(ctx, "supervisor")
    assert first["watches"][0]["state"]["proof"]["checks"] == ["preserved"]
    assert second["watches"][0]["state"]["proof"]["checks"] == ["new"]
