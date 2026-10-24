"""Rendered user-facing server policy, without installing or changing a job."""
import plistlib
from pathlib import Path
from pa.cli.service import render_plist
from pa.config import Settings


def test_user_requested_http_service_uses_interactive_resource_policy(tmp_path):
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces')
    value = plistlib.loads(render_plist(settings, Path('/fixture/pa')))
    assert value['ProcessType'] == 'Interactive'
    assert value['KeepAlive'] is True
    assert value['RunAtLoad'] is True
    assert value['ExitTimeOut'] == 300
    assert value['ThrottleInterval'] == 10
    assert value['LimitLoadToSessionType'] == ['Aqua', 'Background']
    assert 'Nice' not in value and 'LegacyTimers' not in value
    assert 'LowPriorityIO' not in value
