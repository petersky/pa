"""Bounded, instance-local discussion notes and durable start-selected receipts.

Notes never create cards or authorize execution. Admission freezes selected notes
before any side effect; card and dispatch ledgers own their individual effects.
This state is separate from provider-owned session configuration and transcripts.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class IssueConflict(ValueError):
    pass


class Issue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_-]+$")
    title: str = Field(min_length=1, max_length=300)
    detail: str = Field(default="", max_length=4000)
    group: str = Field(default="", max_length=80)
    deferred: bool = False

    @model_validator(mode="before")
    @classmethod
    def receipt_is_read_only(cls, value):
        # Public issues can be round-tripped by tools. The durable notebook,
        # never the caller, owns this one known receipt field. All other extra
        # fields still fail strict validation, and edit() restores frozen IDs.
        if isinstance(value, dict):
            return {k: v for k, v in value.items() if k != "request_id"}
        return value

    @field_validator("title")
    @classmethod
    def title_nonempty(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Issue title cannot be empty")
        return value

    @field_validator("group")
    @classmethod
    def group_trimmed(cls, value):
        return value.strip()


class IssueEdit(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0)
    issues: list[Issue] = Field(max_length=100)

    @model_validator(mode="after")
    def unique_ids(self):
        if len({i.id for i in self.issues}) != len(self.issues):
            raise ValueError("Issue IDs must be unique")
        return self


class StartIssues(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=8, max_length=80, pattern=r"^[a-zA-Z0-9_-]+$")
    revision: int = Field(ge=0)
    issue_ids: list[str] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def unique_ids(self):
        if len(set(self.issue_ids)) != len(self.issue_ids):
            raise ValueError("Select each issue once")
        return self


def fingerprint(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class ConversationIssues:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _read(conn, session_id):
        row = conn.execute("SELECT payload FROM conversation_issues WHERE session_id=?", (session_id,)).fetchone()
        return json.loads(row[0]) if row else {"revision": 0, "issues": [], "requests": []}

    @staticmethod
    def _save(conn, session_id, state):
        conn.execute("INSERT INTO conversation_issues VALUES (?, ?) ON CONFLICT(session_id) DO UPDATE SET payload=excluded.payload",
                     (session_id, json.dumps(state)))
        return state

    def read(self, session_id):
        with self.store._conn() as conn:
            return self._read(conn, session_id)

    def edit(self, session_id, body: IssueEdit):
        with self.store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._read(conn, session_id)
            incoming = [i.model_dump() for i in body.issues]
            old = {i["id"]: i for i in state["issues"]}
            by_id = {i["id"]: i for i in incoming}
            for issue in old.values():
                if issue.get("request_id"):
                    candidate = by_id.get(issue["id"])
                    if candidate != {k: v for k, v in issue.items() if k != "request_id"}:
                        raise IssueConflict("Started issues are frozen; make further changes on their card")
                    candidate["request_id"] = issue["request_id"]
            # Exact retries are harmless even after the revision increment.
            if incoming == state["issues"]:
                return state
            if state["revision"] != body.revision:
                raise IssueConflict("Issue list changed; refresh before saving")
            state.update(revision=state["revision"] + 1, issues=incoming)
            return self._save(conn, session_id, state)

    def admit(self, session_id, body: StartIssues, context: dict):
        digest = fingerprint(body.model_dump())
        with self.store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._read(conn, session_id)
            previous = next((r for r in state["requests"] if r["request_id"] == body.request_id), None)
            if previous:
                if previous["fingerprint"] != digest:
                    raise IssueConflict("Request ID was already used for another selection")
                return state
            if state["revision"] != body.revision:
                raise IssueConflict("Issue list changed; refresh before starting")
            if len(state["requests"]) >= 100:
                raise IssueConflict("This conversation has reached its work-request limit; start a new thread")
            selected = [i for i in state["issues"] if i["id"] in body.issue_ids]
            if len(selected) != len(body.issue_ids) or any(i["deferred"] or i.get("request_id") for i in selected):
                raise IssueConflict("Select only current, unstarted, non-deferred issues")
            groups = {}
            for issue in selected:
                key = "group:" + issue["group"] if issue["group"] else "issue:" + issue["id"]
                group = groups.setdefault(key, {"title": issue["group"] or issue["title"], "issues": [], "state": "accepted"})
                group["issues"].append(dict(issue))
                issue["request_id"] = body.request_id
            state["requests"].append({
                "request_id": body.request_id, "fingerprint": digest,
                "submission": body.model_dump(), "context": context,
                "accepted_at": datetime.now(UTC).isoformat(), "state": "accepted",
                "groups": list(groups.values()), "timings_ms": {},
            })
            state["revision"] += 1
            return self._save(conn, session_id, state)

    def update_request(self, session_id, request_id, **changes):
        with self.store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._read(conn, session_id)
            record = next(r for r in state["requests"] if r["request_id"] == request_id)
            record.update(changes)
            return self._save(conn, session_id, state)

    @staticmethod
    def public(state):
        # Execution inputs remain durable but are not repeated in compact reads.
        return {"revision": state["revision"], "issues": state["issues"],
                "editable_issues": [{k: v for k, v in i.items() if k != "request_id"} for i in state["issues"]], "requests": [
            {k: v for k, v in r.items() if k not in {"context", "fingerprint"}} | {
                "groups": [{k: v for k, v in g.items() if k not in {"issues", "dispatch_inputs"}} for g in r["groups"]]
            } for r in state["requests"]
        ]}
