"""Failure classification must not turn deterministic blocks into transient ones."""
import pytest

from pa.acp.errors import classify_acp_failure


@pytest.mark.parametrize(
    "message",
    [
        "workspace authority mismatch",
        "Session authority instance differs from the dispatch authority",
        "author identity is not configured for commits",
        "authoritative origin disagrees with the stored binding",
        "Authority lease holder changed",
    ],
)
def test_authority_and_author_words_are_not_auth_failures(message):
    detail = classify_acp_failure(RuntimeError(message), stage="session_recovery")
    assert detail["code"] != "auth_missing", detail


@pytest.mark.parametrize(
    "message",
    [
        "authentication failed",
        "Authorization header rejected by provider",
        "auth expired, please log in again",
        "auth token expired",
        "Not authorized to use this model",
        "401 Unauthorized",
        "OPENAI_API_KEY is not set",
        "missing credential for provider",
    ],
)
def test_real_credential_failures_still_classify_as_auth(message):
    detail = classify_acp_failure(RuntimeError(message), stage="session_recovery")
    assert detail["code"] == "auth_missing", detail
    assert detail["recoverable"] is True


@pytest.mark.parametrize(
    "message,code",
    [
        (
            "Card already has a live worktree lease; resume its owner or "
            "explicitly allow concurrent dispatch",
            "workspace_lease_conflict",
        ),
        (
            "governed session does not match its durable assigned dispatch binding",
            "dispatch_binding_mismatch",
        ),
        (
            "Empty-index checkout retained; use targeted materialization "
            "preservation/repair before admission",
            "empty_index_checkout",
        ),
        (
            "Interrupted checkout has retained changes; explicit repair is required",
            "interrupted_checkout_retained",
        ),
    ],
)
def test_deterministic_failures_get_precise_non_transient_codes(message, code):
    from pa.acp.errors import deterministic_recovery_block

    assert deterministic_recovery_block(message)["code"] == code
    detail = classify_acp_failure(RuntimeError(message), stage="session_recovery")
    assert detail["code"] == code
    assert detail["deterministic"] is True
    assert detail["recoverable"] is True
    assert detail["action"]


def test_transient_failures_are_not_deterministic():
    detail = classify_acp_failure(ConnectionError("connection closed"))
    assert detail["code"] == "connection_closed"
    assert "deterministic" not in detail
