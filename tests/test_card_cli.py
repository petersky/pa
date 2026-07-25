from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import httpx
from typer.testing import CliRunner

from pa.auth.users import UserDirectory
from pa.config import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        host="127.0.0.1",
        port=8123,
    )


def _response(status: int, payload: object) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "http://127.0.0.1"),
    )


def test_dispatch_resolves_instance_infers_project_and_sends_options(
    tmp_path: Path,
) -> None:
    from pa.cli.main import app

    settings = _settings(tmp_path)
    token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    calls: list[dict] = []

    def request(method: str, url: str, **kwargs) -> httpx.Response:
        calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/api/cards/card-1"):
            return _response(200, {"id": "card-1", "project_id": "project-1"})
        if url.endswith("/api/fleet/instances"):
            return _response(
                200,
                [
                    {
                        "instance_id": "instance-1",
                        "name": "Monica",
                        "url": "https://monica.example",
                    }
                ],
            )
        return _response(
            202,
            {
                "accepted": True,
                "duplicate": False,
                "dispatch": {
                    "dispatch_id": "dispatch-1",
                    "state": "queued",
                    "target_instance_name": "Monica",
                    "events": [{"message": "Dispatch admitted."}],
                },
            },
        )

    with (
        patch("pa.cli.card.get_settings", return_value=settings),
        patch("pa.cli.card.httpx.request", side_effect=request),
    ):
        result = CliRunner().invoke(
            app,
            [
                "card",
                "dispatch",
                "card-1",
                "--instance",
                "monica",
                "--provider",
                "codex",
                "--model",
                "gpt-5",
                "--mode",
                "code",
                "--effort",
                "high",
                "--idempotency-key",
                "attempt-1",
            ],
        )

    assert result.exit_code == 0, result.output
    admission = calls[-1]
    assert admission["url"].endswith("/api/fleet/instances/instance-1/agent/start")
    assert admission["headers"]["Authorization"] == f"Bearer {token}"
    assert admission["headers"]["Idempotency-Key"] == "attempt-1"
    assert admission["json"] == {
        "card_id": "card-1",
        "project_id": "project-1",
        "provider": "codex",
        "model_id": "gpt-5",
        "mode_id": "code",
        "effort": "high",
        "message": "Execute this card completely.",
    }
    assert "Queued durable card dispatch" in result.output
    assert "queued" in result.output


def test_dispatch_duplicate_is_reported_as_recovered(tmp_path: Path) -> None:
    from pa.cli.main import app

    settings = _settings(tmp_path)
    responses = [
        _response(200, {"id": "card-1", "project_id": None}),
        _response(
            200, [{"instance_id": "instance-1", "name": "worker", "url": "https://w"}]
        ),
        _response(
            202,
            {
                "duplicate": True,
                "dispatch": {
                    "dispatch_id": "dispatch-1",
                    "state": "running",
                    "target_instance_id": "instance-1",
                },
            },
        ),
    ]
    with (
        patch("pa.cli.card.get_settings", return_value=settings),
        patch("pa.cli.card.httpx.request", side_effect=responses),
    ):
        result = CliRunner().invoke(
            app,
            [
                "card",
                "dispatch",
                "card-1",
                "--instance",
                "instance-1",
                "--idempotency-key",
                "same-key",
            ],
        )
    assert result.exit_code == 0, result.output
    assert "Recovered durable card dispatch" in result.output
    assert "running" in result.output


def test_dispatch_surfaces_actionable_api_error(tmp_path: Path) -> None:
    from pa.cli.main import app

    settings = _settings(tmp_path)
    with (
        patch("pa.cli.card.get_settings", return_value=settings),
        patch(
            "pa.cli.card.httpx.request",
            return_value=_response(
                404, {"detail": {"code": "not_found", "message": "Card not found"}}
            ),
        ),
    ):
        result = CliRunner().invoke(
            app, ["card", "dispatch", "missing", "--instance", "worker"]
        )
    assert result.exit_code == 1
    assert "PA rejected the request (404): Card not found (not_found)" in result.output


def test_dispatch_list_get_retry_and_cancel(tmp_path: Path) -> None:
    from pa.cli.main import app

    settings = _settings(tmp_path)
    dispatch = {
        "dispatch_id": "dispatch-1",
        "state": "failed",
        "target_instance_name": "worker",
        "events": [{"message": "Peer unavailable."}],
        "last_error": "connection refused",
        "can_retry": True,
        "can_cancel": False,
    }
    commands = (
        ["card", "dispatch-list", "--limit", "5"],
        ["card", "dispatch-get", "dispatch-1"],
        ["card", "dispatch-retry", "dispatch-1"],
        ["card", "dispatch-cancel", "dispatch-1"],
    )
    for args in commands:
        payload: object = [dispatch] if args[1] == "dispatch-list" else dispatch
        with (
            patch("pa.cli.card.get_settings", return_value=settings),
            patch(
                "pa.cli.card.httpx.request", return_value=_response(200, payload)
            ) as request,
        ):
            result = CliRunner().invoke(app, args)
        assert result.exit_code == 0, result.output
        assert "dispatch-1  failed  target=worker" in result.output
        assert request.call_args.kwargs["headers"]["Authorization"].startswith(
            "Bearer "
        )
        if args[1] in {"dispatch-retry", "dispatch-cancel"}:
            assert request.call_args.args[0] == "POST"
