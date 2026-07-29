import os
import tempfile
import unittest
from pathlib import Path

from pa.core.assets import compute_asset_version


class AssetVersionTests(unittest.TestCase):
    def test_fingerprint_is_independent_of_install_mtimes(self) -> None:
        with (
            tempfile.TemporaryDirectory() as first,
            tempfile.TemporaryDirectory() as second,
        ):
            roots = (Path(first), Path(second))
            for root in roots:
                (root / "css").mkdir()
                (root / "css" / "app.css").write_text("body { color: black; }\n")
                (root / "app.js").write_text("console.log('PA');\n")

            for path in roots[1].rglob("*"):
                if path.is_file():
                    os.utime(path, (1_900_000_000, 1_900_000_000))

            self.assertEqual(
                compute_asset_version(roots[0]),
                compute_asset_version(roots[1]),
            )

    def test_fingerprint_changes_with_content_or_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            asset = root / "app.js"
            asset.write_text("first")
            initial = compute_asset_version(root)

            asset.write_text("second")
            changed_content = compute_asset_version(root)
            self.assertNotEqual(initial, changed_content)

            asset.rename(root / "renamed.js")
            self.assertNotEqual(changed_content, compute_asset_version(root))
