"""Realm-authorized immutable card attachment blobs and dispatch materialization."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from pa.core.io import atomic_write_json
from pa.domain.models import CardAttachment

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_CARD_BYTES = 100 * 1024 * 1024
MAX_CARD_ATTACHMENTS = 10
CHUNK_BYTES = 1024 * 1024


class AttachmentError(RuntimeError):
    def __init__(self, code: str, message: str, *, recoverable: bool = False):
        super().__init__(message)
        self.code = code
        self.recoverable = recoverable

    def detail(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": str(self),
            "recoverable": self.recoverable,
        }


def safe_filename(value: str, index: int = 1) -> str:
    basename = (value or f"attachment-{index}").replace("\\", "/").split("/")[-1]
    cleaned = re.sub(r"[^\w.() -]+", "_", basename).strip(" .")
    if not cleaned:
        cleaned = f"attachment-{index}"
    suffix = Path(cleaned).suffix[:20]
    if len(cleaned) > 160:
        cleaned = cleaned[: 160 - len(suffix)].rstrip(" .") + suffix
    return cleaned


def manifest_digest(manifest: Iterable[CardAttachment | dict]) -> str:
    records = [
        item.model_dump(mode="json") if isinstance(item, CardAttachment) else dict(item)
        for item in manifest
    ]
    canonical = __import__("json").dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class AttachmentStore:
    """Content-addressed storage kept outside the global sync ObjectStore."""

    def __init__(self, data_dir: Path):
        self.root = Path(data_dir) / "attachments-v1"
        self.blobs = self.root / "blobs" / "sha256"
        self.uploads = self.root / "uploads"
        self.dispatches = self.root / "dispatches"

    def blob_path(self, sha256: str) -> Path:
        if not SHA256_RE.fullmatch(sha256):
            raise AttachmentError("invalid_digest", "Invalid SHA-256 digest")
        return self.blobs / sha256[:2] / sha256

    def has_verified_blob(self, sha256: str, size: int) -> bool:
        path = self.blob_path(sha256)
        if not path.is_file() or path.stat().st_size != size:
            return False
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(CHUNK_BYTES):
                digest.update(chunk)
        return digest.hexdigest() == sha256

    def ingest(
        self, source: BinaryIO, *, expected_size: int | None = None
    ) -> tuple[str, int]:
        self.uploads.mkdir(parents=True, exist_ok=True)
        temporary = self.uploads / f"{uuid4().hex}.part"
        digest = hashlib.sha256()
        size = 0
        try:
            source.seek(0)
            with temporary.open("xb") as output:
                while chunk := source.read(CHUNK_BYTES):
                    size += len(chunk)
                    if size > MAX_ATTACHMENT_BYTES:
                        raise AttachmentError(
                            "attachment_too_large", "Attachment exceeds the 25 MB limit"
                        )
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if expected_size is not None and size != expected_size:
                raise AttachmentError(
                    "size_mismatch",
                    "Attachment size does not match the manifest",
                    recoverable=True,
                )
            sha256 = digest.hexdigest()
            destination = self.blob_path(sha256)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if not self.has_verified_blob(sha256, size):
                    raise AttachmentError(
                        "blob_corrupt",
                        "Existing content-addressed blob is corrupt",
                        recoverable=True,
                    )
                temporary.unlink()
            else:
                os.replace(temporary, destination)
            return sha256, size
        finally:
            temporary.unlink(missing_ok=True)

    def authorize_transfer(
        self,
        dispatch_id: str,
        realm_id: str,
        card_id: str,
        manifest: list[CardAttachment],
    ) -> None:
        self.uploads.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.uploads / f"dispatch-{safe_filename(dispatch_id)}.json",
            {
                "realm_id": realm_id,
                "card_id": card_id,
                "digest": manifest_digest(manifest),
                "attachments": [item.model_dump(mode="json") for item in manifest],
            },
        )

    def authorized_attachment(
        self, dispatch_id: str, realm_id: str, card_id: str, sha256: str, size: int
    ) -> bool:
        try:
            payload = __import__("json").loads(
                (
                    self.uploads / f"dispatch-{safe_filename(dispatch_id)}.json"
                ).read_text()
            )
        except OSError, ValueError:
            return False
        return (
            payload.get("realm_id") == realm_id
            and payload.get("card_id") == card_id
            and any(
                item.get("sha256") == sha256 and item.get("size") == size
                for item in payload.get("attachments", [])
            )
        )

    def ingest_chunks(
        self, chunks: Iterable[bytes], *, expected_sha256: str, expected_size: int
    ) -> Path:
        self.uploads.mkdir(parents=True, exist_ok=True)
        temporary = self.uploads / f"{uuid4().hex}.part"
        digest = hashlib.sha256()
        size = 0
        try:
            with temporary.open("xb") as output:
                for chunk in chunks:
                    size += len(chunk)
                    if size > MAX_ATTACHMENT_BYTES or size > expected_size:
                        raise AttachmentError(
                            "transfer_limit", "Attachment exceeds declared limits"
                        )
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if size != expected_size or digest.hexdigest() != expected_sha256:
                raise AttachmentError(
                    "hash_mismatch",
                    "Fetched attachment failed manifest verification",
                    recoverable=True,
                )
            destination = self.blob_path(expected_sha256)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if not self.has_verified_blob(expected_sha256, expected_size):
                    raise AttachmentError(
                        "blob_corrupt",
                        "Existing content-addressed blob is corrupt",
                        recoverable=True,
                    )
            else:
                os.replace(temporary, destination)
            return destination
        finally:
            temporary.unlink(missing_ok=True)

    def partial_size(self, dispatch_id: str, sha256: str) -> int:
        path = self.uploads / f"dispatch-{safe_filename(dispatch_id)}-{sha256}.part"
        return path.stat().st_size if path.exists() else 0

    def append_chunk(
        self,
        dispatch_id: str,
        sha256: str,
        *,
        offset: int,
        data: bytes,
        total_size: int,
    ) -> int:
        if (
            total_size > MAX_ATTACHMENT_BYTES
            or offset < 0
            or offset + len(data) > total_size
        ):
            raise AttachmentError(
                "transfer_limit", "Chunk exceeds declared attachment limits"
            )
        self.uploads.mkdir(parents=True, exist_ok=True)
        path = self.uploads / f"dispatch-{safe_filename(dispatch_id)}-{sha256}.part"
        actual = path.stat().st_size if path.exists() else 0
        if actual != offset:
            raise AttachmentError(
                "offset_mismatch", f"Resume at byte {actual}", recoverable=True
            )
        with path.open("ab") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        return offset + len(data)

    def finalize_partial(self, dispatch_id: str, sha256: str, size: int) -> Path:
        partial = self.uploads / f"dispatch-{safe_filename(dispatch_id)}-{sha256}.part"
        if not partial.is_file() or partial.stat().st_size != size:
            raise AttachmentError(
                "blob_incomplete", "Attachment transfer is incomplete", recoverable=True
            )
        digest = hashlib.sha256()
        with partial.open("rb") as source:
            while chunk := source.read(CHUNK_BYTES):
                digest.update(chunk)
        if digest.hexdigest() != sha256:
            partial.unlink(missing_ok=True)
            raise AttachmentError(
                "hash_mismatch",
                "Transferred attachment failed SHA-256 verification",
                recoverable=True,
            )
        destination = self.blob_path(sha256)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if not self.has_verified_blob(sha256, size):
                raise AttachmentError(
                    "blob_corrupt",
                    "Existing content-addressed blob is corrupt",
                    recoverable=True,
                )
            partial.unlink()
        else:
            os.replace(partial, destination)
        return destination

    def materialize(self, dispatch_id: str, manifest: list[CardAttachment]) -> dict:
        if (
            len(manifest) > MAX_CARD_ATTACHMENTS
            or sum(item.size for item in manifest) > MAX_CARD_BYTES
        ):
            raise AttachmentError(
                "card_quota_exceeded", "Attachment manifest exceeds card limits"
            )
        missing = [
            item.sha256
            for item in manifest
            if not self.has_verified_blob(item.sha256, item.size)
        ]
        if missing:
            raise AttachmentError(
                "required_blob_missing",
                "One or more required attachments are missing",
                recoverable=True,
            )
        final = self.dispatches / safe_filename(dispatch_id)
        temporary = self.dispatches / f".{safe_filename(dispatch_id)}-{uuid4().hex}.tmp"
        self.dispatches.mkdir(parents=True, exist_ok=True)
        temporary.mkdir(parents=True, exist_ok=False)
        evidence: list[dict[str, object]] = []
        used: set[str] = set()
        try:
            for index, item in enumerate(manifest, 1):
                name = safe_filename(item.filename, index)
                stem, suffix = Path(name).stem, Path(name).suffix
                candidate = name
                counter = 2
                while candidate.casefold() in used:
                    candidate = f"{stem}-{counter}{suffix}"
                    counter += 1
                used.add(candidate.casefold())
                target = temporary / candidate
                os.link(self.blob_path(item.sha256), target)
                target.chmod(0o444)
                evidence.append(
                    {
                        **item.model_dump(mode="json"),
                        "local_path": str((final / candidate).resolve()),
                    }
                )
            atomic_write_json(
                temporary / "manifest.json",
                {"digest": manifest_digest(manifest), "attachments": evidence},
            )
            if final.exists():
                existing = final / "manifest.json"
                if existing.is_file() and __import__("json").loads(
                    existing.read_text()
                ).get("digest") == manifest_digest(manifest):
                    shutil.rmtree(temporary)
                else:
                    raise AttachmentError(
                        "materialization_conflict",
                        "Dispatch already has a different attachment manifest",
                    )
            else:
                final.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temporary, final)
            return {
                "digest": manifest_digest(manifest),
                "attachments": evidence,
                "root": str(final.resolve()),
                "verified": True,
            }
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    def garbage_collect(
        self,
        referenced: Iterable[CardAttachment],
        *,
        minimum_age_seconds: float = 86400.0,
    ) -> dict[str, int]:
        """Collect unreferenced blobs/uploads while hard-linked dispatch pins remain live."""
        import time

        keep = {item.sha256 for item in referenced}
        now = time.time()
        removed_blobs = 0
        removed_uploads = 0
        if self.blobs.exists():
            for path in self.blobs.glob("*/*"):
                if (
                    path.is_file()
                    and path.name not in keep
                    and path.stat().st_nlink == 1
                    and now - path.stat().st_mtime >= minimum_age_seconds
                ):
                    path.unlink()
                    removed_blobs += 1
        if self.uploads.exists():
            for path in self.uploads.iterdir():
                if (
                    path.is_file()
                    and now - path.stat().st_mtime >= minimum_age_seconds
                    and (path.suffix == ".part" or path.name.startswith("dispatch-"))
                ):
                    path.unlink()
                    removed_uploads += 1
        return {"removed_blobs": removed_blobs, "removed_uploads": removed_uploads}
