"""Every setting the code reads must exist on Settings.

A setting read through ``settings.<name>`` but never declared raises
AttributeError at run time; here it left a reconciler failing every pass.
"""

import re
from pathlib import Path

from pa.config import Settings
from pa.domain.instance_config import InstanceConfig

SRC = Path(__file__).resolve().parents[1] / "src" / "pa"
# Attribute reads on objects that are not Settings but share the name.
NOT_SETTINGS = {"html", "render"}


def test_every_referenced_setting_is_declared():
    known = set(Settings.model_fields) | set(dir(Settings))
    missing: dict[str, str] = {}
    for path in SRC.rglob("*.py"):
        for match in re.finditer(r"\bsettings\.([a-z_][a-z0-9_]*)", path.read_text()):
            name = match.group(1)
            if name not in known and name not in NOT_SETTINGS:
                missing.setdefault(name, str(path.relative_to(SRC)))
    assert missing == {}


def test_reconciliation_session_unavailable_setting_is_declared_everywhere():
    name = "reconciliation_session_unavailable_seconds"
    assert Settings.model_fields[name].default == 3600
    assert InstanceConfig.model_fields[name].default == 3600
