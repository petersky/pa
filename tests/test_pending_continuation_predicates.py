"""Pending receipt read semantics, including legacy nullable catalog values."""
import sqlite3

import pytest

from pa.domain.projection import CardProjection


@pytest.mark.parametrize('status', ['pending', 'restarting', 'failed', 'continuation_delivered', 'restart_completed', None])
@pytest.mark.parametrize('delivered', [None, 'delivered'])
@pytest.mark.parametrize('prompt', [None, '', ' ', 'saved', 0, b'saved'])
def test_pending_read_keeps_status_delivery_and_prompt_semantics(status, delivered, prompt):
    # A minimal legacy catalog can contain values excluded by today's schema.
    # Keep SQL's original truth/NULL/type semantics without decoding a prompt.
    with sqlite3.connect(':memory:') as connection:
        connection.execute('CREATE TABLE agent_restart_handoffs(session_id,card_id,continuation_prompt,status,delivered_at)')
        connection.execute('INSERT INTO agent_restart_handoffs VALUES(?,?,?,?,?)', ('session', 'card', prompt, status, delivered))
        expected = connection.execute(
            "SELECT 1 FROM agent_restart_handoffs WHERE session_id=? "
            "AND continuation_prompt != '' AND delivered_at IS NULL "
            "AND status NOT IN ('continuation_delivered', 'restart_completed') LIMIT 1",
            ('session',),
        ).fetchone() is not None
        assert CardProjection._pending_restart_continuation(connection, session_id='session') == expected
        assert CardProjection._pending_restart_continuation(connection, card_id='card') == expected
        assert not CardProjection._pending_restart_continuation(connection, session_id='other')
        assert not CardProjection._pending_restart_continuation(connection, card_id='other')
        # The established exact-session precedence remains when both are given.
        assert CardProjection._pending_restart_continuation(connection, session_id='session', card_id='other') == expected
        assert connection.total_changes == 1
