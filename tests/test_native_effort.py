import shutil
import subprocess
from pathlib import Path

import pytest


def test_native_effort_evidence():
    node=shutil.which('node')
    if not node: pytest.skip('node unavailable')
    root=Path(__file__).parents[1]
    result=subprocess.run([node,str(root/'tests/native_effort_node_harness.js'),str(root/'src/pa/server/static/js/native-effort.js')],capture_output=True,text=True)
    assert result.returncode==0,result.stderr
