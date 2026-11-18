"""Substantive card results: immutable, replayable, and separate from acceptance."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pa.domain.models import CardAttachment, CardEvent, EventType


class ResultSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    heading: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1, max_length=32_000)

    @field_validator("heading", "content")
    @classmethod
    def substantive(cls, value):
        if not value.strip():
            raise ValueError("Section headings and content cannot be blank")
        return value


class ResultReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["attachment", "external", "completion_record"]
    label: str = Field(min_length=1, max_length=300)
    attachment_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9-]{1,80}$")
    sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    url: str | None = Field(default=None, max_length=2048)
    record_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9-]{1,80}$")

    @model_validator(mode="after")
    def target(self):
        supplied = {k for k in ("attachment_id", "sha256", "url", "record_id") if getattr(self, k) is not None}
        expected = {"attachment": {"attachment_id", "sha256"}, "external": {"url"}, "completion_record": {"record_id"}}[self.kind]
        if supplied != expected or not self.label.strip():
            raise ValueError("Reference requires a label and exactly the fields for its kind")
        if self.url:
            parsed = urlsplit(self.url)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or any(ord(c) < 33 for c in self.url):
                raise ValueError("External references require an HTTPS URL without credentials or whitespace")
        return self


class CompletionRecordInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1]
    title: str = Field(min_length=1, max_length=200)
    summary: str = Field(min_length=1, max_length=2000)
    sections: list[ResultSection] = Field(min_length=1, max_length=32)
    references: list[ResultReference] = Field(default_factory=list, max_length=64)
    supersedes_record_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9-]{1,80}$")

    @model_validator(mode="after")
    def bounded(self):
        if not self.title.strip() or not self.summary.strip():
            raise ValueError("Title and summary cannot be blank")
        if len(json.dumps(self.model_dump(mode="json", include=set(CompletionRecordInput.model_fields)), ensure_ascii=False, separators=(",", ":")).encode()) > 65_536:
            raise ValueError("Completion record exceeds 64 KiB; publish larger evidence as attachments")
        return self


class CompletionRecord(CompletionRecordInput):
    record_id: str
    card_id: str
    realm_id: str
    created_at: datetime
    author_principal: str
    author_instance: str
    source_session_id: str | None = None
    source_dispatch_id: str | None = None
    # PA snapshots only validated same-card immutable attachment metadata.
    retained_attachments: list[CardAttachment] = Field(default_factory=list)


def init_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS completion_records (
            realm_id TEXT NOT NULL, card_id TEXT NOT NULL, record_id TEXT NOT NULL,
            created_at TEXT NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(realm_id, card_id, record_id)
        );
        CREATE INDEX IF NOT EXISTS idx_completion_records_order
          ON completion_records(realm_id, card_id, created_at DESC, record_id DESC);
    """)


def apply_record(store, event):
    record = CompletionRecord.model_validate(event.payload)
    if (record.realm_id, record.card_id, record.author_principal, record.author_instance) != (
        event.realm_id, event.card_id, event.author_principal, event.author_instance
    ):
        raise ValueError("Completion record event provenance mismatch")
    with store._conn() as conn:
        old = conn.execute("SELECT payload FROM completion_records WHERE realm_id=? AND card_id=? AND record_id=?",
                           (record.realm_id, record.card_id, record.record_id)).fetchone()
        if old and json.loads(old["payload"]) != record.model_dump(mode="json"):
            raise ValueError("Immutable completion record conflict")
        conn.execute("INSERT OR IGNORE INTO completion_records VALUES (?, ?, ?, ?, ?)",
                     (record.realm_id, record.card_id, record.record_id, record.created_at.isoformat(), record.model_dump_json()))


def get_record(store, card_id, record_id, realm_id):
    with store._conn() as conn:
        row = conn.execute("SELECT payload FROM completion_records WHERE realm_id=? AND card_id=? AND record_id=?",
                           (realm_id, card_id, record_id)).fetchone()
    return CompletionRecord.model_validate_json(row["payload"]) if row else None


def list_records(store, card_id, realm_id, *, limit=20, offset=0):
    with store._conn() as conn:
        total = conn.execute("SELECT count(*) FROM completion_records WHERE realm_id=? AND card_id=?", (realm_id, card_id)).fetchone()[0]
        rows = conn.execute("SELECT json_remove(payload, '$.sections', '$.references', '$.retained_attachments') AS summary FROM completion_records WHERE realm_id=? AND card_id=? ORDER BY created_at DESC, record_id DESC LIMIT ? OFFSET ?",
                            (realm_id, card_id, min(max(limit, 1), 50), max(offset, 0))).fetchall()
    # Full content is retrieved only when a user/agent opens a record.
    items = [json.loads(row["summary"]) for row in rows]
    return {"items": items, "total": total, "next_offset": offset + len(items) if offset + len(items) < total else None}


def append_record(store, card_id, realm_id, data, *, principal_id, instance_id,
                  session_id=None, dispatch_id=None, idempotency_key, request_fingerprint):
    """Called with the canonical projection's serialized mutation lock held."""
    card = store.get_card(card_id, realm_id=realm_id)
    if not card:
        raise ValueError("Card not found")
    if data.supersedes_record_id and not get_record(store, card_id, data.supersedes_record_id, realm_id):
        raise ValueError("Superseded record must already exist on this card")
    retained = {}
    for reference in data.references:
        if reference.kind == "attachment":
            attachment = next((a for a in card.attachments if a.attachment_id == reference.attachment_id and a.sha256 == reference.sha256), None)
            # A revision can retain an attachment already removed from the card.
            attachment = attachment or retained_attachment(store, card_id, realm_id, reference.attachment_id, reference.sha256)
            if not attachment:
                raise ValueError("Attachment ID/hash must identify durable evidence on this card")
            retained[attachment.attachment_id] = attachment
        elif reference.kind == "completion_record" and not get_record(store, card_id, reference.record_id, realm_id):
            raise ValueError("Referenced completion record must exist on this card")
    record = CompletionRecord(**data.model_dump(), record_id=str(uuid4()), card_id=card_id, realm_id=realm_id,
                              created_at=datetime.now(UTC), author_principal=principal_id, author_instance=instance_id,
                              source_session_id=session_id, source_dispatch_id=dispatch_id, retained_attachments=list(retained.values()))
    payload = record.model_dump(mode="json")
    store.commit_event(CardEvent(type=EventType.COMPLETION_RECORD_CREATED, card_id=card_id, realm_id=realm_id,
                                 author_principal=principal_id, author_instance=instance_id, payload=payload,
                                 source_operation="card.completion_record.create", idempotency_key=idempotency_key,
                                 request_fingerprint=request_fingerprint, operation_result=payload))
    return payload


def retained_attachment(store, card_id, realm_id, attachment_id, sha256=None):
    with store._conn() as conn:
        rows = conn.execute("SELECT payload FROM completion_records WHERE realm_id=? AND card_id=? AND EXISTS (SELECT 1 FROM json_each(payload, '$.retained_attachments') WHERE json_extract(value, '$.attachment_id')=?) LIMIT 1",
                            (realm_id, card_id, attachment_id)).fetchall()
    for row in rows:
        for item in json.loads(row["payload"]).get("retained_attachments", []):
            if item["attachment_id"] == attachment_id and (sha256 is None or item["sha256"] == sha256):
                return CardAttachment.model_validate(item)
    return None


def results_url(card_id, realm_id, record_id=None):
    return "/results?" + urlencode({"card_id": card_id, "realm": realm_id, **({"record": record_id} if record_id else {})})
