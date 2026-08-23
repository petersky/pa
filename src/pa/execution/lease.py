"""Execution leases on cards."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pa.domain.models import CardEvent, EventType
from pa.domain.store import Store
from pa.sync.event_log import EventLog

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pa.cloud import CloudCoordinator


class LeaseManager:
    DEFAULT_TTL_SECONDS = 300

    def __init__(
        self,
        store: Store,
        event_log: EventLog,
        instance_id: str,
        *,
        cloud: CloudCoordinator | None = None,
    ) -> None:
        self.store = store
        self.log = event_log
        self.instance_id = instance_id
        self.cloud = cloud

    def grant(
        self,
        card_id: str,
        realm_id: str,
        *,
        holder_instance: str,
        holder_principal: str,
        ttl_seconds: int | None = None,
    ) -> bool:
        card = self.store.get_card(card_id, realm_id=realm_id)
        if not card:
            return False
        if card.lease_holder_instance and card.lease_expires_at:
            if card.lease_expires_at > datetime.now(UTC):
                if card.lease_holder_instance != holder_instance:
                    return False
        ttl = ttl_seconds or self.DEFAULT_TTL_SECONDS
        expires = datetime.now(UTC) + timedelta(seconds=ttl)
        if self.cloud is not None:
            from pa.cloud import CloudLeaseResult

            result = self.cloud.acquire_lease(
                {
                    "card_id": card_id,
                    "realm_id": realm_id,
                    "holder_instance": holder_instance,
                    "holder_principal": holder_principal,
                    "expires_at": expires.isoformat(),
                }
            )
            if result == CloudLeaseResult.DENIED:
                return False
            if result == CloudLeaseResult.UNAVAILABLE and not self.cloud.fail_open:
                return False
        event = CardEvent(
            type=EventType.LEASE_GRANTED,
            realm_id=realm_id,
            card_id=card_id,
            author_principal=holder_principal,
            author_instance=holder_instance,
            payload={
                "holder_instance": holder_instance,
                "holder_principal": holder_principal,
                "expires_at": expires.isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )
        self.store.commit_event(event)
        return True

    def release(
        self,
        card_id: str,
        realm_id: str,
        *,
        principal_id: str,
        holder_instance: str | None = None,
    ) -> bool:
        card = self.store.get_card(card_id, realm_id=realm_id)
        if not card or not card.lease_holder_instance:
            return False
        expected = holder_instance or self.instance_id
        if card.lease_holder_instance != expected:
            return False
        if card.lease_expires_at and card.lease_expires_at < datetime.now(UTC):
            return False
        if self.cloud is not None:
            from pa.cloud import CloudLeaseResult

            result = self.cloud.release_lease(
                {
                    "card_id": card_id,
                    "realm_id": realm_id,
                    "holder_instance": expected,
                    "principal_id": principal_id,
                }
            )
            if result == CloudLeaseResult.DENIED:
                return False
            if result == CloudLeaseResult.UNAVAILABLE and not self.cloud.fail_open:
                return False
        event = CardEvent(
            type=EventType.LEASE_RELEASED,
            realm_id=realm_id,
            card_id=card_id,
            author_principal=principal_id,
            author_instance=self.instance_id,
            payload={"updated_at": datetime.now(UTC).isoformat()},
        )
        self.store.commit_event(event)
        return True

    def is_holder(self, card_id: str, realm_id: str, instance_id: str) -> bool:
        card = self.store.get_card(card_id, realm_id=realm_id)
        if not card or not card.lease_holder_instance:
            return True
        if card.lease_expires_at and card.lease_expires_at < datetime.now(UTC):
            return True
        return card.lease_holder_instance == instance_id
