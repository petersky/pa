"""Batch history membership retains complete decoding and legacy read semantics."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules.agent_chat import list_agent_session_history


@pytest.fixture
def store(tmp_path):
    return CardProjection(tmp_path/"fixture.db")


def seed(store,count=1001):
    for index in range(count):
        store.save_session(AgentSession(id=f"s-{index:04d}",agent_name="fixture",status="closed",
            card_id="target" if index%20==0 else "other",project_id="project" if index%2 else None))


def request_for(store):
    manager=SimpleNamespace(store=store,get=lambda key:None)
    request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(
        store=store,settings=SimpleNamespace(instance_id="fixture",instance_name="Fixture"),
        services={},require_service=lambda name:manager))))
    return request


@contextmanager
def lightweight_payload():
    with (patch("pa.modules.agent_chat._session_cards_payload",return_value=[]),
          patch("pa.modules.agent_chat._session_project_payload",return_value=None),
          patch("pa.modules.agent_chat._presentation",return_value={})):
        yield


def test_history_filter_queries_are_bounded_before_payload_hydration(store):
    seed(store)
    statements=[]
    with store._conn() as conn:
        conn.set_trace_callback(lambda sql:statements.append(sql))
    try:
        with lightweight_payload():
            rows=list_agent_session_history(request_for(store),card_id="target",limit=1)
    finally:
        conn.set_trace_callback(None)
    # Two membership batches plus one selected response's existing card-ID read.
    membership=[sql for sql in statements if "FROM agent_session_cards" in sql]
    assert len(membership)==3
    assert len(rows)==1 and rows[0]["card_id"]=="target"


def test_native_membership_matches_individual_reads_with_missing_and_retired_cards(store):
    seed(store,5)
    with store._conn() as conn:
        conn.execute("UPDATE agent_session_cards SET retired_at='2026-01-01T00:00:00Z' WHERE session_id='s-0001'")
    ids=["s-0000","s-0001","s-0002","absent","s-0002"]
    for card in ("target","other","missing"):
        oracle={key for key in ids if card in store.list_card_ids_for_session(key)}
        assert store.session_ids_linked_to_card(card,ids)==oracle
    assert store.session_ids_linked_to_card("other",[])==set()


def test_subclass_and_instance_lookup_extensions_remain_authoritative(store):
    class Extended(CardProjection):
        def list_card_ids_for_session(self,key):
            return ["extension"] if key=="custom" else []
    child=Extended(store.db_path)
    assert child.session_ids_linked_to_card("extension",["custom","other"])=={"custom"}
    with patch.object(store,"list_card_ids_for_session",side_effect=lambda key:["override"]):
        assert store.session_ids_linked_to_card("override",["custom"])=={"custom"}


@pytest.mark.parametrize("limit",[0,1,2,500,900])
def test_history_full_payload_order_project_and_selected_match_legacy(store,limit):
    seed(store,25)
    # Secondary target association without changing the primary legacy session.
    with store._conn() as conn:
        conn.execute("INSERT INTO agent_session_cards(session_id,card_id,realm_id,linked_at) VALUES ('s-0001','target','default','2026-01-01T00:00:00Z')")
    request=request_for(store)
    options=dict(card_id="target",project_id="project",limit=limit,selected_session_id="s-0001")
    with lightweight_payload():
        actual=list_agent_session_history(request,**options)
        with patch.object(CardProjection,"session_ids_linked_to_card",None):
            oracle=list_agent_session_history(request,**options)
    assert actual==oracle and [row["id"] for row in actual]==["s-0001"]


def test_full_invalid_unselected_session_is_still_decoded_before_filter(store):
    seed(store,3)
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET metrics_json='[]' WHERE id='s-0002'")
    with lightweight_payload(),pytest.raises(ValidationError) as after:
        list_agent_session_history(request_for(store),card_id="target")
    with patch.object(CardProjection,"session_ids_linked_to_card",None),lightweight_payload(),pytest.raises(type(after.value)):
        list_agent_session_history(request_for(store),card_id="target")


def test_legacy_store_without_batch_capability_uses_original_lookup(store):
    seed(store,3)
    class Legacy:
        def __getattr__(self,name):
            return getattr(store,name)
    with lightweight_payload():
        rows=list_agent_session_history(request_for(Legacy()),card_id="target")
    assert [row["id"] for row in rows]==["s-0000"]
