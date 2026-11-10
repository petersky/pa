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


@pytest.mark.parametrize(
    "message,code",
    [
        ("429 Too Many Requests: usage limit reached", "quota_exhausted"),
        ("You exceeded your current quota, please check your plan and billing details (api key sk-x)", "quota_exhausted"),
        ("rate limit exceeded for model gpt", "rate_limited"),
        ("Error: status 429", "rate_limited"),
        ("Overloaded", "provider_overloaded"),
        ("HTTP 503 Service Unavailable", "provider_overloaded"),
        ("upstream returned 502 Bad Gateway", "provider_overloaded"),
    ],
)
def test_waitable_provider_errors_get_retryable_codes(message, code):
    from pa.acp.errors import RETRYABLE_PROVIDER_CODES
    from acp.exceptions import RequestError

    for exc in (RuntimeError(message), RequestError(-32603, "Internal error", message)):
        detail = classify_acp_failure(exc, stage="prompt")
        assert detail["code"] == code, detail
        assert code in RETRYABLE_PROVIDER_CODES
        assert detail["retryable"] is True
        assert detail["retry_unbounded"] is (code != "provider_overloaded")


@pytest.mark.parametrize(
    "message,seconds",
    [
        ("rate limit exceeded, retry-after: 30", 30.0),
        ("429: Try again in 2 hours 5 minutes", 7500.0),
        ("Too many requests. Please retry in 20s", 20.0),
        ("usage limit reached; resets in 3h", 10800.0),
    ],
)
def test_provider_retry_after_is_parsed(message, seconds):
    detail = classify_acp_failure(RuntimeError(message))
    assert detail["retry_after_seconds"] == seconds


def test_provider_reset_clock_time_is_parsed_as_a_future_wait():
    detail = classify_acp_failure(RuntimeError("You've hit your usage limit. Try again at 3:41 PM"))
    assert detail["code"] == "quota_exhausted"
    assert 0 < detail["retry_after_seconds"] <= 24 * 3600


@pytest.mark.parametrize(
    "message",
    ["error at line 4290", "Internal error", "authority mismatch 429 files", "401 Unauthorized"],
)
def test_ordinary_failures_are_not_mistaken_for_waitable_ones(message):
    detail = classify_acp_failure(RuntimeError(message))
    assert not detail.get("retryable"), detail
