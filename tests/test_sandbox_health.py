from datetime import UTC, datetime, timedelta

from pa.acp.sandbox_health import (
    SandboxHealthRegistry,
    classify_sandbox_failure,
    sanitize_provider_error,
)


def test_classifies_exact_codex_loopback_failure() -> None:
    assert (
        classify_sandbox_failure(
            "bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted"
        )
        == "sandbox_loopback_init_failed"
    )


def test_sanitizes_paths_and_secrets() -> None:
    sanitized = sanitize_provider_error(
        "token=abc123 bwrap /home/service/.cache/codex/bwrap failed"
    )
    assert "abc123" not in sanitized
    assert "/home/service" not in sanitized
    assert "<path>" in sanitized


def test_failure_opens_bounded_circuit_and_success_closes_it() -> None:
    registry = SandboxHealthRegistry(failure_threshold=2, unhealthy_seconds=60)
    first = registry.failure(
        "codex", "workspace-write", "RTM_NEWADDR Operation not permitted"
    )
    assert (first["state"], first["recovery"]) == ("degraded", "retry_session_once")
    second = registry.failure(
        "codex", "workspace-write", "RTM_NEWADDR Operation not permitted"
    )
    assert (second["state"], second["recovery"]) == (
        "unhealthy",
        "reroute_or_operator_probe",
    )
    health = registry.success("codex", "workspace-write", metadata={"probe": "session"})
    assert health["state"] == "healthy"
    assert health["consecutive_failures"] == 0


def test_expired_circuit_requires_fresh_probe() -> None:
    registry = SandboxHealthRegistry(failure_threshold=1)
    registry.failure("codex", "workspace-write", "RTM_NEWADDR Operation not permitted")
    registry._records[("codex", "workspace-write")].unhealthy_until = datetime.now(
        UTC
    ) - timedelta(seconds=1)
    health = registry.get("codex", "workspace-write")
    assert (health["state"], health["recovery"]) == ("stale", "probe")
