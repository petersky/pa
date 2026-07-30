from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


class MetricQuality(StrEnum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"


class Metric(BaseModel):
    value: float | int | None = None
    unit: str
    quality: MetricQuality
    source: str
    detail: str | None = None


class TelemetrySample(BaseModel):
    schema_version: int = 1
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    collection_duration_ms: float = 0
    instance_id: str
    instance_name: str
    scope_type: Literal["instance", "session"]
    scope_id: str
    restart_id: str
    provider_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    realm_id: str | None = None
    principal_id: str | None = None
    root_pid: int | None = None
    ownership: str | None = None
    metrics: dict[str, Metric] = Field(default_factory=dict)

    def public_dict(self, *, include_principal: bool = False) -> dict:
        data = self.model_dump(mode="json")
        if not include_principal:
            data.pop("principal_id", None)
        # PID is ownership evidence for the local collector, not useful process
        # metadata for reports and diagnostic exports.
        data.pop("root_pid", None)
        return data


class TelemetryQuery(BaseModel):
    start: datetime
    end: datetime
    scope_type: Literal["instance", "session"] | None = None
    scope_ids: list[str] = Field(default_factory=list)
    instance_ids: list[str] = Field(default_factory=list)
    provider_ids: list[str] = Field(default_factory=list)
    card_ids: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    bucket_seconds: int = Field(default=60, ge=1, le=86400)
    visible_principal_id: str | None = None


def unavailable(unit: str, source: str, detail: str) -> Metric:
    return Metric(
        value=None,
        unit=unit,
        quality=MetricQuality.UNAVAILABLE,
        source=source,
        detail=detail,
    )


def unsupported(unit: str, source: str, detail: str) -> Metric:
    return Metric(
        value=None,
        unit=unit,
        quality=MetricQuality.UNSUPPORTED,
        source=source,
        detail=detail,
    )
