from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from pa.attachments import (
    AttachmentError,
    AttachmentStore,
    manifest_digest,
    safe_filename,
)
from pa.domain.models import CardAttachment


class AttachmentStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = AttachmentStore(Path(self.tmp.name))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def attachment(self, content: bytes, *, name: str = "proof.txt") -> CardAttachment:
        digest = hashlib.sha256(content).hexdigest()
        return CardAttachment(
            card_id="card-1",
            realm_id="realm-1",
            filename=name,
            media_type="text/plain",
            size=len(content),
            sha256=digest,
            blob_ref=f"sha256:{digest}",
            created_by_principal="user:one",
            created_by_instance="instance-one",
        )

    def test_ingest_deduplicates_by_verified_content_hash(self) -> None:
        content = b"byte identical"
        first = self.store.ingest(io.BytesIO(content))
        second = self.store.ingest(io.BytesIO(content))
        self.assertEqual(first, second)
        self.assertEqual(len(list(self.store.blobs.rglob(first[0]))), 1)
        self.assertTrue(self.store.has_verified_blob(*first))

    def test_interrupted_transfer_resumes_and_rejects_wrong_offset(self) -> None:
        content = b"0123456789"
        item = self.attachment(content)
        self.store.authorize_transfer("dispatch-1", item.realm_id, item.card_id, [item])
        self.assertEqual(
            self.store.append_chunk(
                "dispatch-1",
                item.sha256,
                offset=0,
                data=content[:4],
                total_size=len(content),
            ),
            4,
        )
        with self.assertRaises(AttachmentError) as raised:
            self.store.append_chunk(
                "dispatch-1", item.sha256, offset=0, data=b"x", total_size=len(content)
            )
        self.assertEqual(raised.exception.code, "offset_mismatch")
        self.store.append_chunk(
            "dispatch-1",
            item.sha256,
            offset=4,
            data=content[4:],
            total_size=len(content),
        )
        self.store.finalize_partial("dispatch-1", item.sha256, item.size)
        self.assertTrue(self.store.has_verified_blob(item.sha256, item.size))

    def test_integrity_failure_never_finalizes_or_materializes(self) -> None:
        item = self.attachment(b"expected")
        self.store.authorize_transfer("dispatch-2", item.realm_id, item.card_id, [item])
        self.store.append_chunk(
            "dispatch-2", item.sha256, offset=0, data=b"corrupt!", total_size=item.size
        )
        with self.assertRaises(AttachmentError) as raised:
            self.store.finalize_partial("dispatch-2", item.sha256, item.size)
        self.assertEqual(raised.exception.code, "hash_mismatch")
        with self.assertRaises(AttachmentError) as missing:
            self.store.materialize("dispatch-2", [item])
        self.assertEqual(missing.exception.code, "required_blob_missing")

    def test_materialization_is_read_only_atomic_and_idempotent(self) -> None:
        content = b"dispatch evidence"
        item = self.attachment(content, name="../unsafe.txt")
        self.store.ingest(io.BytesIO(content))
        first = self.store.materialize("dispatch-3", [item])
        second = self.store.materialize("dispatch-3", [item])
        self.assertEqual(first["digest"], manifest_digest([item]))
        self.assertEqual(second["digest"], first["digest"])
        local = Path(first["attachments"][0]["local_path"])
        self.assertEqual(local.name, safe_filename(item.filename))
        self.assertEqual(local.read_bytes(), content)
        self.assertEqual(local.stat().st_mode & 0o222, 0)
        evidence = json.loads((Path(first["root"]) / "manifest.json").read_text())
        self.assertEqual(evidence["digest"], first["digest"])

    def test_garbage_collection_respects_card_references_and_dispatch_pins(
        self,
    ) -> None:
        kept = self.attachment(b"kept")
        pinned = self.attachment(b"pinned", name="pinned.txt")
        orphan = self.attachment(b"orphan", name="orphan.txt")
        for content in (b"kept", b"pinned", b"orphan"):
            self.store.ingest(io.BytesIO(content))
        self.store.materialize("dispatch-pin", [pinned])
        result = self.store.garbage_collect([kept], minimum_age_seconds=0)
        self.assertEqual(result["removed_blobs"], 1)
        self.assertTrue(self.store.blob_path(kept.sha256).exists())
        self.assertTrue(self.store.blob_path(pinned.sha256).exists())
        self.assertFalse(self.store.blob_path(orphan.sha256).exists())

    def test_transfer_grant_prevents_cross_realm_or_card_substitution(self) -> None:
        item = self.attachment(b"secret")
        self.store.authorize_transfer("dispatch-4", item.realm_id, item.card_id, [item])
        self.assertTrue(
            self.store.authorized_attachment(
                "dispatch-4", "realm-1", "card-1", item.sha256, item.size
            )
        )
        self.assertFalse(
            self.store.authorized_attachment(
                "dispatch-4", "realm-2", "card-1", item.sha256, item.size
            )
        )
        self.assertFalse(
            self.store.authorized_attachment(
                "dispatch-4", "realm-1", "card-2", item.sha256, item.size
            )
        )


if __name__ == "__main__":
    unittest.main()
