"""Realm membership store."""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.models import Membership, PrincipalType, Realm, RealmInvite, RealmRole
from pa.core.io import atomic_write_json


class MembershipStore:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "memberships.json"
        self.invites_path = data_dir / "realm_invites.json"
        self._memberships: list[Membership] = []
        self._realms: dict[str, Realm] = {}
        self._invites: dict[str, RealmInvite] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                for item in data.get("memberships", []):
                    self._memberships.append(Membership.model_validate(item))
                for item in data.get("realms", []):
                    realm = Realm.model_validate(item)
                    self._realms[realm.id] = realm
            except (json.JSONDecodeError, ValueError):
                pass
        if self.invites_path.exists():
            try:
                data = json.loads(self.invites_path.read_text())
                for item in data.get("invites", []):
                    inv = RealmInvite.model_validate(item)
                    if not inv.accepted:
                        self._invites[inv.token] = inv
            except (json.JSONDecodeError, ValueError):
                pass

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "realms": [r.model_dump() for r in self._realms.values()],
            "memberships": [m.model_dump(mode="json") for m in self._memberships],
        }
        atomic_write_json(self.path, payload)

    def _save_invites(self) -> None:
        payload = {"invites": [i.model_dump(mode="json") for i in self._invites.values()]}
        atomic_write_json(self.invites_path, payload)

    def ensure_realm(self, realm_id: str, name: str = "") -> Realm:
        if realm_id not in self._realms:
            self._realms[realm_id] = Realm(id=realm_id, name=name or realm_id)
            self._save()
        return self._realms[realm_id]

    def ensure_owner_membership(
        self,
        realm_id: str,
        principal_id: str,
        *,
        fleet_id: str | None = None,
    ) -> Membership:
        self.ensure_realm(realm_id)
        for m in self._memberships:
            if (
                m.realm_id == realm_id
                and m.principal_type == PrincipalType.USER
                and m.principal_id == principal_id
            ):
                return m
        membership = Membership(
            realm_id=realm_id,
            principal_type=PrincipalType.USER,
            principal_id=principal_id,
            role=RealmRole.ADMIN,
            fleet_id=fleet_id,
        )
        self._memberships.append(membership)
        self._save()
        return membership

    def add_membership(self, membership: Membership) -> Membership:
        self.ensure_realm(membership.realm_id)
        self._memberships.append(membership)
        self._save()
        return membership

    def list_realms(self) -> list[Realm]:
        return list(self._realms.values())

    def list_memberships(self, realm_id: str | None = None) -> list[Membership]:
        if realm_id:
            return [m for m in self._memberships if m.realm_id == realm_id]
        return list(self._memberships)

    def has_role(
        self,
        realm_id: str,
        principal_id: str,
        *,
        min_role: RealmRole = RealmRole.VIEWER,
    ) -> bool:
        role_order = {
            RealmRole.VIEWER: 0,
            RealmRole.RELAY: 1,
            RealmRole.EDITOR: 2,
            RealmRole.ADMIN: 3,
        }
        required = role_order[min_role]
        for m in self._memberships:
            if m.realm_id != realm_id:
                continue
            pid = m.principal_id
            if m.principal_type == PrincipalType.USER and pid == principal_id:
                if role_order.get(m.role, 0) >= required:
                    return True
            if m.principal_type == PrincipalType.FLEET:
                if role_order.get(m.role, 0) >= required:
                    return True
        # Default: no matching membership means no access
        return False

    def create_invite(
        self,
        realm_id: str,
        role: RealmRole = RealmRole.EDITOR,
        *,
        ttl_hours: int = 72,
        created_by: str = "",
    ) -> RealmInvite:
        token = secrets.token_urlsafe(24)
        invite = RealmInvite(
            realm_id=realm_id,
            role=role,
            token=token,
            expires_at=datetime.now(UTC) + timedelta(hours=ttl_hours),
            created_by=created_by,
        )
        self._invites[token] = invite
        self._save_invites()
        return invite

    def accept_invite(self, token: str, principal_id: str, fleet_id: str | None = None) -> Membership | None:
        invite = self._invites.get(token)
        if not invite or invite.accepted:
            return None
        if invite.expires_at and invite.expires_at < datetime.now(UTC):
            return None
        invite.accepted = True
        membership = Membership(
            realm_id=invite.realm_id,
            principal_type=PrincipalType.USER,
            principal_id=principal_id,
            role=invite.role,
            fleet_id=fleet_id,
        )
        self._memberships.append(membership)
        self._save()
        self._save_invites()
        return membership
