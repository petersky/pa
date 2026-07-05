"""Version reading and semver bump helpers."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PYPROJECT = ROOT / "pyproject.toml"
INIT_PY = ROOT / "src" / "pa" / "__init__.py"
CHANNELS_JSON = ROOT / "channels.json"

_STABLE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_PRERELEASE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)-(alpha|beta|rc)\.(\d+)$",
    re.IGNORECASE,
)


def read_version() -> str:
    text = INIT_PY.read_text()
    match = re.search(r'__version__\s*=\s*"([^"]+)"', text)
    if not match:
        raise RuntimeError("Could not read __version__ from src/pa/__init__.py")
    return match.group(1)


def write_version(version: str) -> None:
    init_text = INIT_PY.read_text()
    init_text = re.sub(
        r'(__version__\s*=\s*")[^"]+(")',
        rf"\g<1>{version}\g<2>",
        init_text,
        count=1,
    )
    INIT_PY.write_text(init_text)

    pyproject = PYPROJECT.read_text()
    pyproject = re.sub(
        r'(^version\s*=\s*")[^"]+(")',
        rf"\g<1>{version}\g<2>",
        pyproject,
        count=1,
        flags=re.MULTILINE,
    )
    PYPROJECT.write_text(pyproject)


def bump_patch(version: str) -> str:
    major, minor, patch = _parse_stable_base(version)
    return f"{major}.{minor}.{patch + 1}"


def bump_minor(version: str) -> str:
    major, minor, _patch = _parse_stable_base(version)
    return f"{major}.{minor + 1}.0"


def bump_major(version: str) -> str:
    major, _minor, _patch = _parse_stable_base(version)
    return f"{major + 1}.0.0"


def bump_prerelease(version: str, label: str = "beta") -> str:
    label = label.lower()
    if label not in {"alpha", "beta", "rc"}:
        raise ValueError("Prerelease label must be alpha, beta, or rc")

    match = _PRERELEASE.match(version)
    if match and match.group(4).lower() == label:
        major, minor, patch, _lbl, pre_num = match.groups()
        return f"{major}.{minor}.{patch}-{label}.{int(pre_num) + 1}"

    major, minor, patch = _parse_stable_base(version)
    return f"{major}.{minor}.{patch + 1}-{label}.1"


def _parse_stable_base(version: str) -> tuple[int, int, int]:
    match = _STABLE.match(version)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))

    match = _PRERELEASE.match(version)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))

    raise ValueError(f"Unsupported version format: {version}")


def tag_for_version(version: str) -> str:
    return f"v{version}"


def is_prerelease_version(version: str) -> bool:
    return bool(_PRERELEASE.match(version))


def track_for_version(version: str) -> str:
    if is_prerelease_version(version):
        lower = version.lower()
        if "-alpha." in lower:
            return "alpha"
        if "-beta." in lower or "-rc." in lower:
            return "beta"
    return "release"
