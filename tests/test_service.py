"""Tests for host service installation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pa.cli import service


class InstallPlistTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.plist = Path(self._tmp.name) / service.PLIST_NAME
        self.settings = MagicMock()
        self.pa_bin = Path("/usr/local/bin/pa")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _install(self, content: bytes) -> Path:
        with (
            patch.object(service, "_is_darwin", return_value=True),
            patch.object(service, "_launch_agents_dir", return_value=self.plist.parent),
            patch.object(service, "_plist_path", return_value=self.plist),
            patch.object(service, "render_plist", return_value=content),
        ):
            return service.install_plist(self.settings, self.pa_bin)

    def test_does_not_rewrite_unchanged_plist(self) -> None:
        content = b"existing launch agent"
        self.plist.write_bytes(content)
        original_mtime = self.plist.stat().st_mtime_ns

        result = self._install(content)

        self.assertEqual(result, self.plist)
        self.assertEqual(self.plist.stat().st_mtime_ns, original_mtime)

    def test_writes_changed_plist(self) -> None:
        self.plist.write_bytes(b"old launch agent")

        result = self._install(b"updated launch agent")

        self.assertEqual(result, self.plist)
        self.assertEqual(self.plist.read_bytes(), b"updated launch agent")


if __name__ == "__main__":
    unittest.main()
