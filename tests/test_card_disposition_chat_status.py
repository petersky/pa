from __future__ import annotations

from pathlib import Path

from pa.execution.disposition import claims_card_disposition_contract


def test_explicit_contract_detection_does_not_capture_ordinary_json() -> None:
    assert claims_card_disposition_contract(
        '{"contract":"pa.card-disposition/v1","lane":"active"}'
    )
    assert claims_card_disposition_contract(
        '{"contract":"pa.card-disposition/v2","lane":"done"}'
    )
    assert not claims_card_disposition_contract(
        '{"lane":"active","outcome":"ordinary"}'
    )
    assert not claims_card_disposition_contract("Here is some JSON: {}")


def test_chat_status_supports_ack_states_diagnostics_and_accessibility() -> None:
    root = Path(__file__).parents[1] / "src" / "pa" / "server" / "static"
    script = (root / "js" / "agent-chat.js").read_text()
    style = (root / "style.css").read_text()

    for state in (
        "accepted",
        "invalid",
        "stale-head",
        "incomplete-evidence",
        "persistence-failed",
        "rejected",
    ):
        assert state in script
    assert 'setAttribute("role", "status")' in script
    assert "Diagnostics and raw payload" in script
    assert "Copy raw payload" in script
    assert "Awaiting durable PA authority acknowledgement" in script
    assert "[data-card-disposition-status" in style
