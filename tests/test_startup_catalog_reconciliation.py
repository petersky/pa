"""Two ordinary live catalogs must admit the original explicit first request."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.execution.selection import ExecutionPreferences, SelectionError
from pa.execution.selection_service import SelectionService, status_candidates
from tests.test_execution_selection import resolve


def advertisement(current, **scope):
    return {
        "source": "live_acp_session", "model_provider": "openai", **scope,
        "config_options": [
            {"id": "model", "currentValue": current,
             "options": [{"value": m} for m in ["gpt-6.1-sol[xhigh]", "gpt-6-astra[xhigh]"]]},
            {"id": "fast-mode", "currentValue": "on", "options": [{"value": "off"}, {"value": "on"}]},
        ],
    }


def rows(catalogs, **kwargs):
    return status_candidates("local", [{"id": "codex", "available": True,
        "auth_state": "authenticated", "execution_catalogs": catalogs}], **kwargs)


def pinned_preferences():
    return ExecutionPreferences.model_validate({
        "harness": {"intent": "required", "value": "codex"},
        "connection": {"intent": "required", "value": "default"},
        "model": {"intent": "required", "value": "gpt-6.1-sol"},
        "reasoning": {"intent": "required", "value": "xhigh"},
        "options": {"fast-mode": {"intent": "required", "value": "on"}},
    })


@pytest.mark.parametrize("reverse", [False, True])
def test_distinct_current_models_preserve_first_request_settings(reverse):
    catalogs = [advertisement("gpt-6.1-sol[xhigh]"), advertisement("gpt-6-astra[xhigh]")]
    if reverse:
        catalogs.reverse()
    candidates = rows(catalogs)
    assert len(candidates) == 2
    assert all(c.options["fast-mode"].values == ["off", "on"] for c in candidates)
    selected = resolve(candidates, prefs=pinned_preferences())["selected"]
    assert (selected["model"], selected["reasoning"], selected["options"]) == (
        "gpt-6.1-sol", "xhigh", {"fast-mode": "on"})


@pytest.mark.parametrize("boundary", ["connection", "connection_revision", "model_provider", "native_model_provider", "account_label", "endpoint_label"])
@pytest.mark.parametrize("reverse", [False, True])
def test_settings_never_cross_scope_boundaries(boundary, reverse):
    catalogs = [advertisement("gpt-6.1-sol[xhigh]", **{boundary: "one"}),
                advertisement("gpt-6-astra[xhigh]", **{boundary: "two"})]
    if reverse:
        catalogs.reverse()
    candidates = [c for c in rows(catalogs) if c.model == "gpt-6.1-sol"]
    assert len(candidates) == 2
    assert "fast-mode" in next(c for c in candidates if getattr(c, boundary) == "one").options
    assert not next(c for c in candidates if getattr(c, boundary) == "two").options


@pytest.mark.parametrize("boundary", ["age", "freshness", "envelope"])
def test_old_or_unknown_support_cannot_be_renewed_by_other_session(boundary):
    now = datetime.now(UTC)
    supported = advertisement("gpt-6.1-sol[xhigh]")
    kwargs = {}
    if boundary == "age":
        supported["observed_at"] = (now - timedelta(minutes=6)).isoformat()
    elif boundary == "freshness":
        supported["freshness"] = "unknown"
    else:
        kwargs["observed_at"] = now - timedelta(minutes=6)
    candidates = rows([supported, advertisement("gpt-6-astra[xhigh]")], **kwargs)
    sol = next(c for c in candidates if c.model == "gpt-6.1-sol")
    assert sol.freshness == "stale" if boundary == "envelope" else not sol.options
    with pytest.raises(SelectionError):
        resolve(candidates, prefs=pinned_preferences())


def test_newer_current_model_removal_is_not_union_with_old_support():
    old = advertisement("gpt-6.1-sol[xhigh]", observed_at=(datetime.now(UTC) - timedelta(seconds=5)).isoformat())
    new = advertisement("gpt-6.1-sol[xhigh]")
    new["config_options"] = new["config_options"][:1]
    for catalogs in ([old, new], [new, old]):
        assert not next(c for c in rows(catalogs) if c.model == "gpt-6.1-sol").options


@pytest.mark.asyncio
async def test_normal_admission_reads_new_live_evidence_while_cache_is_warm(tmp_path):
    service = SelectionService(Settings(data_dir=tmp_path, instance_id="local"), None)
    service.store.save_catalog("local", [c.model_dump(mode="json") for c in rows([advertisement("gpt-6-astra[xhigh]")])])
    live = advertisement("gpt-6.1-sol[xhigh]")
    runtime = SimpleNamespace(session=SimpleNamespace(agent_name="codex", config_json={}),
        connected=True, _closed=False, connection=SimpleNamespace(models=None, config_options=live["config_options"]))
    service.manager = SimpleNamespace(list_runtimes=lambda: [runtime])
    result = resolve(await service.local_catalog(refresh=True), prefs=pinned_preferences())
    assert result["selected"]["options"] == {"fast-mode": "on"}
