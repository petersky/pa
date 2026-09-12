"""Bounded wire contracts. Evidence is data, never execution authority."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pa.prompts.registry import redact_value

MAX_PAGE = 32
MAX_BYTES = 256 * 1024


def sanitized(value):
    value = redact_value(value)
    if isinstance(value, dict):
        return {k: sanitized(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitized(v) for v in value]
    if isinstance(value, str):
        value = re.sub(r'-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----', '[REDACTED]', value)
        value = re.sub(r'(?i)(?:https?://)[^\s/@]+:[^\s/@]+@', 'https://[REDACTED]@', value)
        value = re.sub(r'(?i)\b(?:authorization|cookie|set-cookie)\s*[:=][^\n]*', '[REDACTED]', value)
        value = re.sub(r'\bsk-[A-Za-z0-9_-]{12,}\b', '[REDACTED]', value)
        return ''.join(c for c in value if c in '\n\t' or ord(c) >= 32)
    return value


def encode(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def digest(value) -> str:
    return hashlib.sha256(encode(value).encode()).hexdigest()


class Strict(BaseModel):
    model_config = ConfigDict(extra='forbid')


class Observation(Strict):
    subsystem: str = Field(min_length=1, max_length=80, pattern=r'^[a-zA-Z0-9_.-]+$')
    error_code: str = Field(min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_.-]+$')
    summary: str = Field(min_length=1, max_length=1000)
    symptom: str = Field(default='', max_length=2000)
    expected: str = Field(default='', max_length=1000)
    actual: str = Field(default='', max_length=2000)
    reproduction: str = Field(default='', max_length=2000)
    evidence: list[str] = Field(default_factory=list, max_length=8)
    correlation_ids: list[str] = Field(default_factory=list, max_length=8)
    occurrence_key: str = Field(min_length=1, max_length=160)
    recurrence_after_acceptance: str | None = Field(default=None, max_length=200)
    report_id: str | None = Field(default=None, max_length=80)
    realm: str | None = Field(default=None, max_length=80)

    @field_validator('evidence', 'correlation_ids')
    @classmethod
    def bounded_strings(cls, value):
        if any(len(v) > 1000 for v in value):
            raise ValueError('Evidence/correlation entry exceeds 1000 characters')
        return value


class Policy(Strict):
    authority_id: str = Field(default='', max_length=80)
    epoch: int = Field(default=1, ge=1)
    version: int = Field(default=1, ge=1)
    enabled: bool = False
    paused: bool = False
    interval_seconds: int = Field(default=60, ge=30, le=3600)
    project_id: str = Field(default='', max_length=80)
    principal_id: str = Field(default='', max_length=100)
    max_sources: int = Field(default=16, ge=1, le=64)


class Assessment(Strict):
    expected_version: int = Field(ge=1)
    disposition: Literal['no_fix', 'duplicate', 'needs_input', 'reproduced', 'linked', 'in_progress', 'merged', 'deployed_verified', 'reopened']
    reason: str = Field(min_length=1, max_length=2000)
    card_id: str | None = Field(default=None, max_length=80)
    duplicate_of: str | None = Field(default=None, max_length=80)
    commit: str | None = Field(default=None, max_length=64, pattern=r'^[a-f0-9]{7,64}$')
    pr_url: str | None = Field(default=None, max_length=300, pattern=r'^https://github.com/[^/]+/[^/]+/pull/[0-9]+$')
    acceptance_reference: str | None = Field(default=None, max_length=200)
    accepted_subject_revision: str | None = Field(default=None, max_length=160)
    acceptance_scenario: str | None = Field(default=None, max_length=200)
    accepted_instances: list[Annotated[str, Field(max_length=80)]] = Field(default_factory=list, max_length=32)


class Revision(Strict):
    source_instance_id: str = Field(min_length=1, max_length=80)
    incarnation: str = Field(min_length=1, max_length=80)
    report_id: str = Field(min_length=1, max_length=80)
    revision: int = Field(ge=1, le=2_147_483_647)
    previous_hash: str | None = Field(default=None, pattern=r'^[a-f0-9]{64}$')
    realm: str = Field(min_length=1, max_length=80)
    principal: str = Field(min_length=1, max_length=160)
    context: dict
    created_at: str = Field(max_length=40)
    updated_at: str = Field(max_length=40)
    recurrence_count: int = Field(ge=1, le=2_147_483_647)
    observation: Observation

    @field_validator('context')
    @classmethod
    def bounded_context(cls, value):
        if set(value) - {'session_id', 'dispatch_id', 'card_id', 'project_id', 'repository', 'runtime_build', 'startup_id'}:
            raise ValueError('Unknown journal context field')
        if len(encode(value)) > 2000:
            raise ValueError('Journal context too large')
        return value

    @field_validator('created_at', 'updated_at')
    @classmethod
    def timestamp(cls, value):
        from datetime import datetime
        if datetime.fromisoformat(value).tzinfo is None:
            raise ValueError('Journal timestamp must be timezone-aware')
        return value
