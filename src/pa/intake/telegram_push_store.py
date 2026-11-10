"""Local bookkeeping for Telegram notification push and replies.

Three small tables in ``pa.db`` (created next to the intake tables, never synced):

* ``telegram_push``: one row per notification that was pushed, with its short reply
  token, how many messages were sent and when, and a send lease that fences
  concurrent senders so one notification is never pushed twice at once.
* ``telegram_push_messages``: every Telegram message id PA sent for a notification,
  so a reply to any of them (first push or a reminder) binds to that notification.
* ``telegram_updates``: Telegram update ids already acted on, which makes a
  redelivered webhook harmless.

The rows hold ids and timestamps only: never message text, tokens or secrets.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS telegram_push (
    notification_id TEXT PRIMARY KEY,
    realm_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    push_count INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    closed INTEGER NOT NULL DEFAULT 0,
    first_pushed_at TEXT,
    last_pushed_at TEXT,
    lease_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telegram_push_messages (
    chat_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    notification_id TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_telegram_push_messages_notification
    ON telegram_push_messages(notification_id);
CREATE TABLE IF NOT EXISTS telegram_updates (
    update_id TEXT PRIMARY KEY,
    notification_id TEXT,
    outcome TEXT NOT NULL,
    claimed_at TEXT NOT NULL
);
"""

LEASE_SECONDS = 120
MAX_FIRST_PUSH_FAILURES = 5


def init_schema(conn) -> None:
    conn.executescript(SCHEMA)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _row(row) -> dict[str, Any] | None:
    return dict(row) if row else None


def get_push(store, notification_id: str) -> dict[str, Any] | None:
    with store._conn() as conn:
        return _row(
            conn.execute(
                "SELECT * FROM telegram_push WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
        )


def claim_first_push(
    store,
    notification_id: str,
    *,
    realm_id: str,
    chat_id: str,
    now: datetime,
) -> dict[str, Any] | None:
    """Reserve the right to send the first push; None if sent, leased or exhausted."""
    lease = _iso(now + timedelta(seconds=LEASE_SECONDS))
    with store._conn() as conn:
        row = conn.execute(
            "SELECT * FROM telegram_push WHERE notification_id=?", (notification_id,)
        ).fetchone()
        if row is None:
            token = ""
            for _ in range(20):
                token = secrets.token_hex(3)
                if not conn.execute(
                    "SELECT 1 FROM telegram_push WHERE token=?", (token,)
                ).fetchone():
                    break
            conn.execute(
                "INSERT INTO telegram_push (notification_id, realm_id, chat_id, token,"
                " lease_until, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (notification_id, realm_id, chat_id, token, lease, _iso(now)),
            )
        else:
            if (
                int(row["push_count"]) > 0
                or int(row["failures"]) >= MAX_FIRST_PUSH_FAILURES
                or (row["lease_until"] and row["lease_until"] > _iso(now))
            ):
                return None
            conn.execute(
                "UPDATE telegram_push SET lease_until=?, chat_id=? WHERE notification_id=?",
                (lease, chat_id, notification_id),
            )
        return _row(
            conn.execute(
                "SELECT * FROM telegram_push WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
        )


def claim_reminder(
    store,
    notification_id: str,
    *,
    now: datetime,
    interval: timedelta,
    max_reminders: int,
) -> dict[str, Any] | None:
    """Reserve one reminder send if one is due and the cap is not reached."""
    lease = _iso(now + timedelta(seconds=LEASE_SECONDS))
    due = _iso(now - interval)
    with store._conn() as conn:
        cursor = conn.execute(
            "UPDATE telegram_push SET lease_until=? WHERE notification_id=?"
            " AND closed=0 AND push_count >= 1 AND push_count <= ? AND last_pushed_at <= ?"
            " AND (lease_until IS NULL OR lease_until <= ?)",
            (lease, notification_id, max_reminders, due, _iso(now)),
        )
        if cursor.rowcount != 1:
            return None
        return _row(
            conn.execute(
                "SELECT * FROM telegram_push WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
        )


def record_sent(
    store,
    notification_id: str,
    *,
    chat_id: str,
    message_id: str | None,
    now: datetime,
) -> None:
    with store._conn() as conn:
        conn.execute(
            "UPDATE telegram_push SET push_count=push_count+1, failures=0,"
            " first_pushed_at=COALESCE(first_pushed_at, ?), last_pushed_at=?,"
            " lease_until=NULL, last_error=NULL WHERE notification_id=?",
            (_iso(now), _iso(now), notification_id),
        )
        if message_id:
            conn.execute(
                "INSERT OR REPLACE INTO telegram_push_messages"
                " (chat_id, message_id, notification_id, sent_at) VALUES (?, ?, ?, ?)",
                (chat_id, str(message_id), notification_id, _iso(now)),
            )


def record_failure(
    store, notification_id: str, *, error: str, now: datetime, backoff_seconds: int = 60
) -> None:
    with store._conn() as conn:
        conn.execute(
            "UPDATE telegram_push SET failures=failures+1, last_error=?, lease_until=?"
            " WHERE notification_id=?",
            (error[:300], _iso(now + timedelta(seconds=backoff_seconds)), notification_id),
        )


def notification_for_message(store, chat_id: str, message_id: str) -> str | None:
    with store._conn() as conn:
        row = conn.execute(
            "SELECT notification_id FROM telegram_push_messages"
            " WHERE chat_id=? AND message_id=?",
            (str(chat_id), str(message_id)),
        ).fetchone()
    return str(row["notification_id"]) if row else None


def notification_for_token(store, chat_id: str, token: str) -> str | None:
    with store._conn() as conn:
        row = conn.execute(
            "SELECT notification_id FROM telegram_push WHERE token=? AND chat_id=?"
            " AND push_count > 0",
            (token.lower(), str(chat_id)),
        ).fetchone()
    return str(row["notification_id"]) if row else None


def reminder_candidates(
    store, *, now: datetime, interval: timedelta, max_reminders: int, limit: int = 200
) -> list[str]:
    """Pushed notifications whose next reminder is due (and not yet capped)."""
    with store._conn() as conn:
        rows = conn.execute(
            "SELECT notification_id FROM telegram_push WHERE closed=0"
            " AND push_count >= 1 AND push_count <= ? AND last_pushed_at <= ?"
            " ORDER BY last_pushed_at LIMIT ?",
            (max_reminders, _iso(now - interval), limit),
        ).fetchall()
    return [str(row["notification_id"]) for row in rows]


def close_push(store, notification_id: str) -> None:
    """Stop reminding: the notification was answered, dismissed or expired."""
    with store._conn() as conn:
        conn.execute(
            "UPDATE telegram_push SET closed=1 WHERE notification_id=?", (notification_id,)
        )


def claim_update(store, update_id: str, *, now: datetime) -> bool:
    """True exactly once per Telegram update id."""
    with store._conn() as conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO telegram_updates (update_id, outcome, claimed_at)"
            " VALUES (?, 'claimed', ?)",
            (str(update_id), _iso(now)),
        )
        return cursor.rowcount == 1


def finish_update(
    store, update_id: str, *, notification_id: str | None, outcome: str
) -> None:
    with store._conn() as conn:
        conn.execute(
            "UPDATE telegram_updates SET notification_id=?, outcome=? WHERE update_id=?",
            (notification_id, outcome[:100], str(update_id)),
        )


def prune(store, *, older_than: timedelta, now: datetime) -> None:
    cutoff = _iso(now - older_than)
    with store._conn() as conn:
        conn.execute("DELETE FROM telegram_updates WHERE claimed_at < ?", (cutoff,))
        conn.execute(
            "DELETE FROM telegram_push_messages WHERE sent_at < ?"
            " AND notification_id NOT IN (SELECT notification_id FROM telegram_push"
            " WHERE last_pushed_at >= ?)",
            (cutoff, cutoff),
        )
