"""Bounded restore-attempt metadata; provider messages and identities stay private."""

from __future__ import annotations

import math
import time
from typing import Any

from acp.exceptions import RequestError

METHODS = frozenset({'session/list', 'session/resume', 'session/load'})
OUTCOMES = frozenset({'ok', 'not_listed', 'failed'})
CATEGORIES = frozenset({'rpc_error', 'timeout', 'transport_error', 'exception'})
RPC_CODES = frozenset({-32700, -32600, -32601, -32602, -32603})
MAX_ATTEMPTS = 3


def safe_restore_attempts(value: Any) -> list[dict[str, Any]]:
    """Revalidate persisted/untrusted fields at the public diagnostic boundary."""
    if not isinstance(value, list):
        return []
    result = []
    for item in value[:MAX_ATTEMPTS]:
        if not isinstance(item, dict):
            continue
        method, outcome, duration = item.get('method'), item.get('outcome'), item.get('duration_ms')
        if (not isinstance(method, str) or method not in METHODS
                or not isinstance(outcome, str) or outcome not in OUTCOMES
                or type(duration) not in (int, float)
                or not 0 <= duration <= 1_000_000_000_000
                or not math.isfinite(duration)):
            continue
        row = {'method': method, 'outcome': outcome, 'duration_ms': duration}
        category = item.get('category')
        if isinstance(category, str) and category in CATEGORIES and outcome == 'failed':
            row['category'] = category
        code = item.get('rpc_code')
        if row.get('category') == 'rpc_error' and type(code) is int and code in RPC_CODES:
            row['rpc_code'] = code
        result.append(row)
    return result


def record_restore_attempt(
    attempts: list[dict[str, Any]], method: str, started: float,
    outcome: str, error: Exception | None = None,
) -> None:
    row: dict[str, Any] = {
        'method': method, 'outcome': outcome,
        'duration_ms': round(max(0.0, time.perf_counter() - started) * 1000, 3),
    }
    if error is not None:
        if isinstance(error, RequestError):
            row.update(category='rpc_error', rpc_code=error.code)
        elif isinstance(error, TimeoutError):
            row['category'] = 'timeout'
        elif isinstance(error, (ConnectionError, BrokenPipeError, EOFError)):
            row['category'] = 'transport_error'
        else:
            row['category'] = 'exception'
    attempts[:] = safe_restore_attempts([*attempts, row])
