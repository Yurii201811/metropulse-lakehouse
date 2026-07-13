from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

from metropulse import __version__

ROOT = Path(__file__).resolve().parents[1]


def test_release_versions_are_in_sync() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dashboard = json.loads(
        (ROOT / "apps" / "dashboard" / "package.json").read_text(encoding="utf-8")
    )
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    released_versions = re.findall(r"^## (\d+\.\d+\.\d+)\b", changelog, re.MULTILINE)

    assert pyproject["project"]["version"] == __version__
    assert dashboard["version"] == __version__
    assert released_versions[0] == __version__
