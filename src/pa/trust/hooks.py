"""Trust tier T2+ hooks — OIDC, grants, encryption."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pa.domain.models import RealmGrant
from pa.core.io import atomic_write_json

logger = logging.getLogger(__name__)


class GrantStore:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "realm_grants.json"
        self._grants: list[RealmGrant] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
            self._grants = [RealmGrant.model_validate(g) for g in data.get("grants", [])]
        except (json.JSONDecodeError, ValueError):
            pass

    def _save(self) -> None:
        payload = {"grants": [g.model_dump(mode="json") for g in self._grants]}
        atomic_write_json(self.path, payload)

    def add(self, grant: RealmGrant) -> RealmGrant:
        self._grants.append(grant)
        self._save()
        return grant

    def list_for_realm(self, realm_id: str) -> list[RealmGrant]:
        return [g for g in self._grants if g.target_realm_id == realm_id or g.source_realm_id == realm_id]


class OIDCConfig:
    """Stub for T2+ OIDC SSO integration."""

    def __init__(self, issuer: str = "", client_id: str = "", client_secret: str = "") -> None:
        self.issuer = issuer
        self.client_id = client_id
        self.client_secret = client_secret

    @property
    def enabled(self) -> bool:
        return bool(self.issuer and self.client_id)

    def authorize_url(self, redirect_uri: str) -> str:
        if not self.enabled:
            raise RuntimeError("OIDC not configured")
        return f"{self.issuer}/authorize?client_id={self.client_id}&redirect_uri={redirect_uri}"


class RealmEncryption:
    """Hook for per-realm encryption at rest (T2+)."""

    def __init__(self, data_dir: Path) -> None:
        self.keys_path = data_dir / "realm_keys.json"

    def encrypt(self, realm_id: str, data: bytes) -> bytes:
        return data  # T1: plaintext

    def decrypt(self, realm_id: str, data: bytes) -> bytes:
        return data


class CommitSigner:
    """Hook for signed commits (T3+)."""

    def sign(self, commit_hash: str) -> str | None:
        return None

    def verify(self, commit_hash: str, signature: str) -> bool:
        return True


class FederationHooks:
    """Open-federation extension points (T4)."""

    def verify_capability_cert(self, cert: str) -> bool:
        return False

    def reputation_score(self, instance_id: str) -> float:
        return 1.0
