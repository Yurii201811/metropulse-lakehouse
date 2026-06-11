from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4


def isolated_project_root(label: str) -> Path:
    root = Path(".test-runs") / f"{label}-{uuid4().hex[:8]}"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    return root.resolve()
