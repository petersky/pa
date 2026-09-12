"""Validated instance-local policy. Authentication never supplies a scope grant."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pa.pr_supervisor.models import canonical_repository_name

META = "pa_supervision_scope"


class PolicyLoadError(ValueError):
    def __init__(self, status: str):
        self.status = status
        self.code = "scope_config_invalid" if status == "invalid" else "scope_config_unavailable"
        super().__init__("GitHub scope configuration cannot be read safely. Restore a valid local policy and review its exact repository scope in Settings.")


@dataclass(frozen=True)
class ScopePolicy:
    mode: Literal["none", "allowlist", "unrestricted"]
    repositories: tuple[str, ...]
    revision: str
    source: str

    def public(self) -> dict:
        return {"scope_mode": self.mode, "allowed_repositories": list(self.repositories),
                "revision": self.revision, "policy_source": self.source, "configuration_status": "valid"}


def read_document(data_dir: Path) -> dict:
    try:
        payload = json.loads((data_dir / "integrations" / "github.json").read_text())
    except FileNotFoundError:
        raise PolicyLoadError("missing") from None
    except (OSError, UnicodeError):
        raise PolicyLoadError("unreadable") from None
    except ValueError:
        raise PolicyLoadError("invalid") from None
    if not isinstance(payload, dict):
        raise PolicyLoadError("invalid")
    return payload


def parse_policy(payload: dict) -> ScopePolicy:
    try:
        if not isinstance(payload, dict) or "allowed_repositories" not in payload:
            raise ValueError
        values = payload["allowed_repositories"]
        if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
            raise ValueError
        repositories = tuple(sorted({canonical_repository_name(v) for v in values}))
        meta = payload.get(META, {})
        if not isinstance(meta, dict):
            raise ValueError
        if "schema_version" in meta and (type(meta["schema_version"]) is not int or meta["schema_version"] != 1):
            raise ValueError
        if "revision" in meta and (not isinstance(meta["revision"], str) or not meta["revision"] or len(meta["revision"]) > 100):
            raise ValueError
        if "audit" in meta and (not isinstance(meta["audit"], list) or any(not isinstance(x, dict) for x in meta["audit"])):
            raise ValueError
        if "receipts" in meta and (not isinstance(meta["receipts"], dict) or any(
            not isinstance(v, dict) or not isinstance(v.get("fingerprint"), str) or not isinstance(v.get("result"), dict)
            for v in meta["receipts"].values())):
            raise ValueError
        for receipt in meta.get("receipts", {}).values():
            result = receipt["result"]
            if (not isinstance(result.get("revision"), str) or not result["revision"]
                    or result.get("scope_mode") not in ("explicit", "allowlist")
                    or not isinstance(result.get("allowed_repositories"), list)
                    or not result["allowed_repositories"]):
                raise ValueError
            for repository in result["allowed_repositories"]:
                canonical_repository_name(repository)
        if "mode" in meta:
            mode = meta["mode"]
            if mode not in ("none", "allowlist", "unrestricted") or (bool(repositories) != (mode == "allowlist")):
                raise ValueError
            source = "configured"
        else:
            if "schema_version" in meta:
                raise ValueError
            mode = "allowlist" if repositories else "unrestricted"
            source = "legacy_explicit" if repositories else "legacy_unrestricted"
        revision_basis = list(repositories) if source.startswith("legacy_") else {"mode": mode, "repositories": list(repositories)}
        revision = meta.get("revision") or hashlib.sha256(json.dumps(revision_basis, sort_keys=True).encode()).hexdigest()
        return ScopePolicy(mode, repositories, revision, source)
    except (ValueError, TypeError, AttributeError):
        raise PolicyLoadError("invalid") from None
