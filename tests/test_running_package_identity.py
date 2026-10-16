import base64
import hashlib
from types import SimpleNamespace

import pytest

from pa.install import metadata


@pytest.fixture
def wheel(tmp_path, monkeypatch):
    files = {'pa/__init__.py':b'version bytes', 'pa/install/metadata.py':b'loaded module bytes'}
    rows = []
    for name, content in files.items():
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip('=')
        rows.append(f'{name},sha256={encoded},{len(content)}')
    dist = SimpleNamespace(version=metadata.__version__, locate_file=lambda name:tmp_path/name,
        read_text=lambda name:'\n'.join(rows) if name == 'RECORD' else '{"archive_info":{}}')
    monkeypatch.setattr(metadata, 'distribution', lambda name:dist)
    monkeypatch.setattr(metadata, '__file__', str(tmp_path/'pa/install/metadata.py'))
    return tmp_path, rows, dist


def test_normal_wheel_has_exact_package_identity_without_vcs_claim(wheel):
    assert metadata.running_source_revision() is None
    identity = metadata.running_package_identity()
    assert identity.startswith('sha256:') and len(identity) == 71
    assert metadata.running_package_identity() == identity


@pytest.mark.parametrize('change', ['bytes','unhashed','missing','version','different_import','empty'])
def test_package_identity_fails_closed_for_unproved_loaded_bytes(wheel, monkeypatch, change):
    root, rows, dist = wheel
    if change == 'bytes': (root/'pa/__init__.py').write_bytes(b'changed bytes')
    elif change == 'unhashed': rows[0] = 'pa/__init__.py,,13'
    elif change == 'missing': dist.locate_file = lambda name:root/'missing'
    elif change == 'version': dist.version = 'different'
    elif change == 'different_import': monkeypatch.setattr(metadata, '__file__', str(root/'other/import.py'))
    elif change == 'empty': rows.clear()
    assert metadata.running_package_identity() is None
