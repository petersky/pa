"""History display summaries keep full Card validation and detached fresh views."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.modules import agent_chat


@pytest.fixture
def store(tmp_path):
    return CardProjection(tmp_path/"fixture.db")


def seed(store,count=20):
    cards=[store.create_card(CardCreate(title=f"Card{i}",body="x"*8192)) for i in range(2)]
    for i in range(count):
        store.save_session(AgentSession(id=f"s-{i:04d}",agent_name="fixture",status="closed",card_id=cards[i%2].id))
    return cards


def request_for(store):
    manager=SimpleNamespace(store=store,get=lambda key:None)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(store=store,
        settings=SimpleNamespace(instance_id="fixture",instance_name="Fixture"),services={},
        require_service=lambda name:manager))))


def test_history_decodes_each_distinct_card_once(store):
    seed(store)
    original=CardProjection._row_to_card
    count=0
    def decode(row):
        nonlocal count
        count+=1
        return original(row)
    with (patch.object(CardProjection,"_row_to_card",staticmethod(decode)),
          patch.object(agent_chat,"_presentation",return_value={})):
        rows=agent_chat.list_agent_session_history(request_for(store),limit=20)
    assert len(rows)==20 and count==2
    assert all(row["cards"][0]["primary"] for row in rows)


def test_order_primary_secondary_retired_missing_and_detachment_match_full_oracle(store):
    cards=seed(store,4)
    store.link_session_card("s-0000",cards[1].id,make_primary=False)
    store.unlink_session_card("s-0001",cards[1].id)
    with store._conn() as conn:
        conn.execute("INSERT INTO agent_session_cards(session_id,card_id,realm_id,linked_at) VALUES ('s-0002','missing','default','2020-01-01T00:00:00Z')")
    keys=["s-0000","s-0001","s-0002","absent","s-0000"]
    def values(key):
        return [{"id":c.id,"title":c.title,"project_id":c.project_id,"lane":c.lane.value}
                for c in store.list_cards_for_session(key)]
    actual=store.list_history_card_summaries(keys)
    assert actual=={key:values(key) for key in keys}
    actual["s-0000"][0]["title"]="changed"
    assert actual["s-0002"][0]["title"]!="changed"
    assert store.list_history_card_summaries(["s-0000"])["s-0000"][0]["title"]!="changed"
    assert store.list_history_card_summaries([])=={}


def test_invalid_linked_card_still_uses_full_decoder_and_error_order(store):
    cards=seed(store,2)
    with store._conn() as conn:
        conn.execute("UPDATE cards SET tags='{}' WHERE id=?",(cards[0].id,))
    with pytest.raises(ValidationError) as original:
        store.list_cards_for_session("s-0000")
    with pytest.raises(type(original.value)):
        store.list_history_card_summaries(["s-0000","s-0001"])
    # Unselected invalid cards do not start affecting other history rows.
    assert store.list_history_card_summaries(["s-0001"])["s-0001"][0]["id"]==cards[1].id


def test_subclass_lookup_and_existing_payload_extensions_are_authoritative(store):
    cards=seed(store,2)
    class Extended(CardProjection):
        def list_cards_for_session(self,key):
            return [cards[1]]
    child=Extended(store.db_path)
    assert child.list_history_card_summaries(["s-0000"])["s-0000"][0]["id"]==cards[1].id
    request=request_for(store)
    with patch.object(agent_chat,"_session_cards_payload",return_value=[{"custom":True}]),patch.object(agent_chat,"_presentation",return_value={}):
        rows=agent_chat.list_agent_session_history(request,limit=1)
    assert rows[0]["cards"]==[{"custom":True}]


def test_decoder_errors_follow_requested_session_order(store):
    import json
    cards=seed(store,2)
    with store._conn() as conn:
        conn.execute("UPDATE cards SET tags='{}' WHERE id=?",(cards[0].id,))
        conn.execute("UPDATE cards SET attachments='not json' WHERE id=?",(cards[1].id,))
    for keys,error in ((["s-0000","s-0001"],ValidationError),(["s-0001","s-0000"],json.JSONDecodeError)):
        with pytest.raises(error):store.list_cards_for_session(keys[0])
        with pytest.raises(error):store.list_history_card_summaries(keys)


def test_legacy_store_without_capability_keeps_payload_hook(store):
    seed(store,2)
    class Legacy:
        def __getattr__(self,name):return getattr(store,name)
    request=request_for(Legacy())
    assert agent_chat._history_card_summaries(request,store.list_sessions()) is None


def test_complete_history_with_secondary_primary_flags_matches_original(store):
    cards=seed(store,4)
    store.link_session_card("s-0000",cards[1].id,make_primary=False)
    request=request_for(store)
    with patch.object(agent_chat,"_presentation",return_value={}):
        actual=agent_chat.list_agent_session_history(request,limit=4)
        with patch.object(agent_chat,"_history_card_summaries",lambda *args:None):
            original=agent_chat.list_agent_session_history(request,limit=4)
    assert actual==original
    linked=next(row["cards"] for row in actual if row["id"]=="s-0000")
    assert [row["primary"] for row in linked]==[True,False]


def test_new_request_observes_card_change_and_read_snapshot_allows_wal_writer(store):
    cards=seed(store,4)
    other=CardProjection(store.db_path)
    original=CardProjection._row_to_card
    changed=False
    def decode(row):
        nonlocal changed
        value=original(row)
        if not changed:
            changed=True
            with other._conn() as conn:
                conn.execute("UPDATE cards SET title='new title' WHERE id=?",(cards[0].id,))
        return value
    with patch.object(CardProjection,"_row_to_card",staticmethod(decode)):
        first=store.list_history_card_summaries(["s-0000","s-0002"])
    assert first["s-0000"][0]["title"]==first["s-0002"][0]["title"]=="Card0"
    assert store.list_history_card_summaries(["s-0000"])["s-0000"][0]["title"]=="new title"


def test_read_savepoint_does_not_commit_outer_transaction(store):
    cards=seed(store,2)
    with pytest.raises(RuntimeError):
        with store._conn() as conn:
            conn.execute("UPDATE cards SET title='uncommitted' WHERE id=?",(cards[0].id,))
            assert store.list_history_card_summaries(["s-0000"])["s-0000"][0]["title"]=="uncommitted"
            raise RuntimeError("roll back private transaction")
    assert store.list_cards_for_session("s-0000")[0].title=="Card0"


def benchmark_module():
    import importlib.util
    from pathlib import Path
    spec=importlib.util.spec_from_file_location("display_benchmark",Path(__file__).parents[1]/"scripts/benchmark_history_card_display.py")
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


def test_private_benchmark_bounds_and_failed_partial_aggregates(monkeypatch):
    value=benchmark_module()
    with pytest.raises(ValueError):value.benchmark(sessions=501)
    async def fail(root,**kwargs):
        kwargs["progress"].update(profile="shared_large",samples={"before":[2.0]},statuses={"before:200":1})
        raise RuntimeError("private fixture response must not escape")
    monkeypatch.setattr(value,"profile",fail)
    result=value.benchmark(sessions=3,iterations=1)
    assert result["failed"] and result["temporary_fixture_removed"]
    assert result["partial_status_counts"]=={"before:200":1}
    assert "private fixture response" not in str(result)


def test_small_full_http_benchmark_retains_complete_bytes_and_unique_control():
    result=benchmark_module().benchmark(sessions=3,iterations=1)
    assert not result.get("failed") and result["temporary_fixture_removed"]
    large,small,unique=result["profiles"]
    assert large["request_decoder_counts"]["before:card_models"]==3
    assert large["request_decoder_counts"]["after:card_models"]==2
    assert small["request_decoder_counts"]["after:session_models"]==3
    assert unique["request_decoder_counts"]["before:card_models"]==unique["request_decoder_counts"]["after:card_models"]==3
    assert all(p["status_counts"]=={"before:200":1,"after:200":1} for p in result["profiles"])
