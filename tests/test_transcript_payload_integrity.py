"""Cold reads verify both independent identities with one digest of the bytes."""
import hashlib
import json
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from pa.domain.transcript_storage import TranscriptStorage


class TranscriptPayloadIntegrityTests(unittest.TestCase):
    def test_cold_read_hashes_full_bytes_once_and_checks_both_identities(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = TranscriptStorage(Path(tmp) / "transcripts.db")
            payload = {"text": "verified bytes " * 1000}
            raw = json.dumps(payload).encode()
            original = hashlib.sha256
            digest = original(raw).hexdigest()
            cases = [
                (digest, digest, payload),
                (digest, "0" * 64, {"availability": "corrupt", "cold_hash": digest}),
                ("1" * 64, digest, {"availability": "corrupt", "cold_hash": "1" * 64}),
            ]
            for cold_hash, payload_hash, expected in cases:
                with self.subTest(cold_hash=cold_hash, payload_hash=payload_hash):
                    path = store._object_path(cold_hash)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(zlib.compress(raw))
                    row = {"hot_payload": "{}", "cold_hash": cold_hash, "payload_hash": payload_hash}
                    with patch("pa.domain.transcript_storage.hashlib.sha256", wraps=original) as hashing:
                        self.assertEqual(store._payload(row), expected)
                    self.assertEqual(hashing.call_count, 1)
                    self.assertEqual(hashing.call_args.args, (raw,))

    def test_matching_digest_does_not_skip_json_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = TranscriptStorage(Path(tmp) / "transcripts.db")
            raw = b"not valid JSON"
            digest = hashlib.sha256(raw).hexdigest()
            path = store._object_path(digest)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(zlib.compress(raw))
            with self.assertRaises(json.JSONDecodeError):
                store._payload({"hot_payload": "{}", "cold_hash": digest, "payload_hash": digest})
