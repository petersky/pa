"""uv executable resolution and invocation tests."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from pa.install.runner import install_from_path


class UvInstallInvocationTests(unittest.TestCase):
    def test_install_uses_resolved_uv_path(self) -> None:
        with (
            patch(
                "pa.install.runner.resolve_uv_binary",
                return_value="/home/alice/.local/bin/uv",
            ),
            patch("pa.install.runner._run") as run,
            patch("pa.install.runner.svc.find_pa_binary", return_value=None),
            self.assertRaisesRegex(RuntimeError, "pa binary not found"),
        ):
            install_from_path(start_service=False)

        run.assert_called_once_with(
            ["/home/alice/.local/bin/uv", "tool", "install", "--force", "pa"]
        )
