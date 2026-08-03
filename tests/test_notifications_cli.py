from __future__ import annotations

import json
from unittest.mock import patch

from typer.testing import CliRunner

from pa.cli.main import app

NOTICE = {
    "id": "notice-123456789",
    "priority": "high",
    "type": "interaction",
    "source_instance_name": "worker",
    "updated_at": "2026-08-02T12:00:00+00:00",
    "title": "Permission requested",
    "interaction": {
        "state": "outstanding",
        "choices": [{"id": "allow", "label": "Allow once"}],
    },
}


def test_list_filters_json_pagination_and_no_color() -> None:
    payload = {"items": [NOTICE], "next_offset": 25, "outstanding_count": 1}
    with patch("pa.cli.notifications._request", return_value=payload) as request:
        result = CliRunner().invoke(
            app,
            [
                "notifications",
                "list",
                "--priority",
                "high",
                "--outstanding",
                "--limit",
                "25",
                "--no-color",
            ],
        )
    assert result.exit_code == 0, result.output
    assert "Permission" in result.output
    assert "Next page: --offset 25" in result.output
    assert "\x1b[" not in result.output
    assert request.call_args.kwargs["params"]["priority"] == "high"
    assert request.call_args.kwargs["params"]["outstanding"] is True

    with patch("pa.cli.notifications._request", return_value=payload):
        result = CliRunner().invoke(app, ["notifications", "list", "--json"])
    assert result.exit_code == 0
    assert json.loads(result.output) == payload


def test_view_and_all_response_shapes() -> None:
    viewed = {
        **NOTICE,
        "body": "Approve the tool",
        "routing": {
            "response_mode": "remote",
            "destination": "https://worker.example/agent?session=1",
        },
    }
    with patch("pa.cli.notifications._request", return_value=viewed):
        result = CliRunner().invoke(
            app, ["notifications", "view", NOTICE["id"], "--no-color"]
        )
    assert result.exit_code == 0
    assert "Complete on owning instance" in result.output
    assert "allow: Allow once" in result.output

    cases = (
        (["--choice", "allow"], {"choice_id": "allow"}),
        (["--value", "done"], {"value": "done"}),
        (
            ["--fields-json", '{"environment":"staging"}'],
            {"fields": {"environment": "staging"}},
        ),
        (["--cancel"], {"cancel": True}),
    )
    for arguments, expected in cases:
        with patch(
            "pa.cli.notifications._request",
            return_value={"interaction": {"state": "delivered"}},
        ) as request:
            result = CliRunner().invoke(
                app,
                ["notifications", "respond", NOTICE["id"], *arguments],
            )
        assert result.exit_code == 0, result.output
        for key, value in expected.items():
            assert request.call_args.kwargs["body"][key] == value


def test_invalid_response_shape_has_automation_failure_exit() -> None:
    result = CliRunner().invoke(
        app,
        [
            "notifications",
            "respond",
            NOTICE["id"],
            "--choice",
            "allow",
            "--value",
            "also supplied",
        ],
    )
    assert result.exit_code == 2
    assert "exactly one" in result.output
