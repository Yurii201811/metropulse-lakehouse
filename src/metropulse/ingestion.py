from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import duckdb

from metropulse.generator import RawBatch


@dataclass(frozen=True)
class IngestFile:
    dataset_name: str
    source_file: str
    file_sha256: str
    file_bytes: int
    row_count: int


def load_bronze(
    con: duckdb.DuckDBPyConnection,
    raw: RawBatch,
    *,
    run_id: str,
    source_root: Path,
) -> tuple[dict[str, int], list[IngestFile]]:
    """Load raw CSV sources into bronze tables with source-file lineage."""

    mappings = {
        "trips": raw.trips,
        "payments": raw.payments,
        "stations": raw.stations,
        "weather": raw.weather,
    }
    counts: dict[str, int] = {}
    manifests: list[IngestFile] = []
    for name, path in mappings.items():
        source_file = path.resolve().relative_to(source_root.resolve()).as_posix()
        _load_csv(
            con,
            f"bronze.{name}",
            path,
            run_id=run_id,
            source_file=source_file,
        )
        counts[name] = int(con.execute(f"SELECT count(*) FROM bronze.{name}").fetchone()[0])
        manifests.append(
            IngestFile(
                dataset_name=name,
                source_file=source_file,
                file_sha256=_sha256(path),
                file_bytes=path.stat().st_size,
                row_count=counts[name],
            )
        )
    return counts, manifests


def _load_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    path: Path,
    *,
    run_id: str,
    source_file: str,
) -> None:
    escaped_source_file = source_file.replace("'", "''")
    csv_path = str(path.resolve()).replace("'", "''")
    escaped_run_id = run_id.replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            current_timestamp AS loaded_at,
            '{escaped_source_file}' AS source_file,
            '{escaped_run_id}' AS source_run_id
        FROM read_csv_auto('{csv_path}', header=true)
        """
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
