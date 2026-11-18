"""Immutable attachment revisions, projected from canonical card events."""
from datetime import UTC, datetime

from pa.domain.card_limits import CARD_ATTACHMENT_BYTES
from pa.domain.models import CardAttachment, CardEvent, EventType


class AttachmentRevisionConflict(ValueError):
    pass


def init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS card_attachment_revisions (
        realm_id TEXT NOT NULL, card_id TEXT NOT NULL, attachment_id TEXT NOT NULL,
        revision INTEGER NOT NULL, sha256 TEXT NOT NULL, payload TEXT NOT NULL,
        PRIMARY KEY (realm_id, card_id, attachment_id, revision))""")


def record_revision(conn, attachment):
    conn.execute("INSERT OR IGNORE INTO card_attachment_revisions VALUES (?, ?, ?, ?, ?, ?)",
                 (attachment.realm_id, attachment.card_id, attachment.attachment_id,
                  attachment.revision, attachment.sha256, attachment.model_dump_json()))


def historical_attachment(store, card_id, realm_id, attachment_id, *, revision=None, sha256=None):
    with store._conn() as conn:
        row = conn.execute("""SELECT payload FROM card_attachment_revisions
            WHERE realm_id=? AND card_id=? AND attachment_id=?
            AND (? IS NULL OR revision=?) AND (? IS NULL OR sha256=?)
            ORDER BY revision DESC LIMIT 1""",
            (realm_id, card_id, attachment_id, revision, revision, sha256, sha256)).fetchone()
    return CardAttachment.model_validate_json(row[0]) if row else None


def revise(store, card_id, attachment_id, *, realm_id, changes, expected_revision,
           expected_version, principal_id, instance_id, idempotency_key, request_fingerprint):
    from pa.domain.projection import CardVersionConflict
    card = store.get_card(card_id, realm_id=realm_id)
    current = next((a for a in card.attachments if a.attachment_id == attachment_id), None) if card else None
    if current is None:
        return None
    if expected_version is not None and card.updated_at != expected_version:
        raise CardVersionConflict(card_id, expected_version, card.updated_at)
    if current.revision != expected_revision:
        raise AttachmentRevisionConflict("Attachment changed. Refresh its details before saving again.")
    # Only this allowlist can change. Ownership, visibility and identity are inherited.
    allowed = {"name", "description", "resource_url", "filename", "media_type", "size", "sha256", "blob_ref"}
    if set(changes) - allowed:
        raise ValueError("Unsupported attachment changes")
    now = datetime.now(UTC)
    revised = CardAttachment.model_validate({**current.model_dump(), **changes,
        "revision": current.revision + 1, "modified_at": now,
        "modified_by_principal": principal_id, "modified_by_instance": instance_id})
    if sum(a.size for a in card.attachments) - current.size + revised.size > CARD_ATTACHMENT_BYTES:
        raise ValueError("Card attachments exceed the 100 MiB total limit")
    payload = revised.model_dump(mode="json")
    payload.update(card_updated_at=now.isoformat(), previous=current.model_dump(mode="json"))
    result = {"attachment": revised.model_dump(mode="json"), "updated_at": now.isoformat(),
              "message": "Saved attachment revision. Earlier content remains available in material history."}
    store.commit_event(CardEvent(type=EventType.ATTACHMENT_UPDATED, realm_id=realm_id,
        card_id=card_id, author_principal=principal_id, author_instance=instance_id,
        payload=payload, causal_card_version=card.updated_at.isoformat(),
        source_operation="card.attachment.update", idempotency_key=idempotency_key,
        request_fingerprint=request_fingerprint, operation_result=result))
    return result
