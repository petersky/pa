from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from pa.cli.main import app
from pa.config import Settings
from pa.mcp.local_api import LocalPARequestError, LocalPAServerUnavailable


def test_status_uses_owner_api_snapshot() -> None:
    payload = {
        "available": True,
        "running": True,
        "interval_seconds": 21600,
        "last_error": None,
        "last_finished_at": "2026-08-16T00:00:00+00:00",
        "last_result": {"transcript_events_deleted": 0},
        "last_started_at": "2026-08-16T00:00:00+00:00",
        "mutation_operation_retention_days": 14,
        "transcript_retention_days": 14,
    }
    with patch("pa.cli.maintain.request_local_pa", return_value=payload) as request:
        result = CliRunner().invoke(app, ["maintain", "status"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == payload
    assert request.call_args.args[1:] == ("GET", "/api/instance/maintenance")
    assert request.call_args.kwargs["timeout_seconds"] == 10.0


def test_status_reports_timeout_without_claiming_scheduler_state() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            agent_enabled=False,
            maintenance_interval_seconds=21600,
            transcript_retention_days=14,
            mutation_operation_retention_days=14,
        )
        error = LocalPAServerUnavailable(
            "The PA API request failed (operation=GET "
            "endpoint=/api/instance/maintenance correlation_id=abc "
            "cause=timeout type=ReadTimeout)."
        )
        with (
            patch("pa.cli.maintain.get_settings", return_value=settings),
            patch("pa.cli.maintain.request_local_pa", side_effect=error),
        ):
            result = CliRunner().invoke(app, ["maintain", "status"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["available"] is False
        assert payload["running"] is False
        assert "cause=timeout type=ReadTimeout" in payload["server_error"]
        assert payload["interval_seconds"] == 21600
        assert payload["transcript_retention_days"] == 14
        assert payload["mutation_operation_retention_days"] == 14


def test_status_explains_missing_route_on_older_service() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(data_dir=Path(tmp), agent_enabled=False)
        error = LocalPARequestError(
            "rejected",
            operation="GET",
            endpoint="/api/instance/maintenance",
            status=404,
            correlation_id="abc",
        )
        with (
            patch("pa.cli.maintain.get_settings", return_value=settings),
            patch("pa.cli.maintain.request_local_pa", side_effect=error),
        ):
            result = CliRunner().invoke(app, ["maintain", "status"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["available"] is False
        assert "does not expose /api/instance/maintenance" in payload["server_error"]
