from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """Filesystem layout used by the pipeline and API."""

    root: Path
    raw_dir: Path
    warehouse_dir: Path
    db_path: Path

    @classmethod
    def from_root(cls, root: Path | str | None = None) -> ProjectPaths:
        project_root = Path(root or os.getenv("METROPULSE_PROJECT_ROOT", ".")).resolve()
        default_db_path = project_root / "data" / "warehouse" / "metropulse.duckdb"
        db_path = Path(os.getenv("METROPULSE_DB_PATH", default_db_path))
        if not db_path.is_absolute():
            db_path = project_root / db_path

        return cls(
            root=project_root,
            raw_dir=project_root / "data" / "raw",
            warehouse_dir=project_root / "data" / "warehouse",
            db_path=db_path,
        )

    def ensure(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)


def resolve_db_path(
    db_path: Path | str | None = None,
    project_root: Path | str | None = None,
) -> Path:
    if db_path is not None:
        return Path(db_path).resolve()
    return ProjectPaths.from_root(project_root).db_path
