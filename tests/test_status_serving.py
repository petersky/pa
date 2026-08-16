from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx

from pa.config import Settings
from pa.status.serving import (
    classify_bind,
    diagnose_serving,
    format_serving_line,
    probe_health,
    sync_from_context,
)


def test_classify_bind_detects_loopback_specific_and_wildcard() -> None:
    loopback = classify_bind(Settings(host="127.0.0.1", port=8080))
    assert loopback.mode == "loopback"
    assert loopback.binds_loopback is True
    assert loopback.binds_non_loopback is False

    specific = classify_bind(Settings(host="100.78.2.112", port=8080))
    assert specific.mode == "specific"
    assert specific.binds_loopback is False

    wildcard = classify_bind(Settings(host="0.0.0.0", port=8080))
    assert wildcard.mode == "wildcard"
    assert wildcard.binds_loopback is True
    assert wildcard.binds_non_loopback is True


def test_classify_bind_prefers_loaded_service_environment() -> None:
    settings = Settings(host="", port=8080, instance_url="http://100.113.226.91:8080")
    report = classify_bind(settings, {"PA_HOST": "127.0.0.1", "PA_PORT": "8080"})
    assert report.mode == "loopback"
    assert report.listeners == (("127.0.0.1", 8080),)


def test_probe_health_classifies_refused_timeout_and_ok() -> None:
    ok = MagicMock(status_code=200)
    with patch("pa.status.serving.httpx.Client") as client_cls:
        client_cls.return_value.__enter__.return_value.get.return_value = ok
        probe = probe_health("http://127.0.0.1:8080")
    assert probe.ok is True
    assert probe.status_code == 200

    with patch("pa.status.serving.httpx.Client") as client_cls:
        client_cls.return_value.__enter__.return_value.get.side_effect = (
            httpx.ConnectError("Connection refused")
        )
        probe = probe_health("http://127.0.0.1:8080")
    assert probe.ok is False
    assert probe.error == "refused"

    with patch("pa.status.serving.httpx.Client") as client_cls:
        client_cls.return_value.__enter__.return_value.get.side_effect = (
            httpx.ReadTimeout("timed out")
        )
        probe = probe_health("http://100.78.2.112:8080")
    assert probe.ok is False
    assert probe.error == "timeout"


def test_diagnose_serving_classifies_alex_and_macmini_shapes() -> None:
    from pa.status.serving import HealthProbe

    def probes(url: str, **_kwargs):
        if "127.0.0.1" in url:
            return HealthProbe(f"{url}/api/health", True, 2.0, 200, None)
        if "100.113.226.91" in url:
            return HealthProbe(f"{url}/api/health", False, 6.0, None, "refused")
        return HealthProbe(f"{url}/api/health", False, 3000.0, None, "timeout")

    alex = Settings(
        host="127.0.0.1",
        port=8080,
        instance_url="http://100.113.226.91:8080",
    )
    with patch("pa.status.serving.probe_health", side_effect=probes):
        diagnosis = diagnose_serving(alex, service_running=True)
    assert diagnosis.serving == "loopback_only"
    assert "loopback only" in format_serving_line(diagnosis)

    def mini_probes(url: str, **_kwargs):
        if "127.0.0.1" in url:
            return HealthProbe(f"{url}/api/health", False, 3.0, None, "refused")
        return HealthProbe(f"{url}/api/health", False, 3000.0, None, "timeout")

    mini = Settings(
        host="100.78.2.112",
        port=8080,
        instance_url="http://100.78.2.112:8080",
    )
    with patch("pa.status.serving.probe_health", side_effect=mini_probes):
        diagnosis = diagnose_serving(mini, service_running=True)
    assert diagnosis.serving == "timeout"
    assert diagnosis.bind.mode == "specific"
    assert "timeout" in format_serving_line(diagnosis)


def test_diagnose_sync_reads_projection_head_without_opening_store(
    tmp_path,
) -> None:
    import sqlite3

    from pa.status.serving import diagnose_sync

    settings = Settings(data_dir=tmp_path)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.db_path)
    conn.execute(
        "CREATE TABLE sync_projection_heads "
        "(realm_id TEXT PRIMARY KEY, head_hash TEXT, updated_at TEXT)"
    )
    conn.execute(
        "INSERT INTO sync_projection_heads VALUES ('default', 'projection', 'now')"
    )
    conn.commit()
    conn.close()
    log = MagicMock()
    log.get_head.return_value = "durable"
    with patch("pa.sync.infrastructure.get_event_log", return_value=log):
        sync = diagnose_sync(settings)
    assert sync.consistent is False
    assert sync.head == "durable"
    assert sync.projection_head == "projection"
    store = MagicMock()
    store.get_projection_head.return_value = "projection"
    store.event_log.get_head.return_value = "durable"
    ctx = SimpleNamespace(
        settings=SimpleNamespace(primary_realm="default"),
        services={"event_log": store.event_log},
        store=store,
    )
    sync = sync_from_context(ctx)
    assert sync.consistent is False
    assert sync.head == "durable"
    assert sync.projection_head == "projection"
