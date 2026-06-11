from __future__ import annotations

from pathlib import Path

import duckdb

from metropulse.generator import RawBatch


def load_bronze(con: duckdb.DuckDBPyConnection, raw: RawBatch) -> dict[str, int]:
    """Load raw CSV sources into bronze tables with source-file lineage."""

    mappings = {
        "trips": raw.trips,
        "payments": raw.payments,
        "stations": raw.stations,
        "weather": raw.weather,
    }
    counts: dict[str, int] = {}
    for name, path in mappings.items():
        _load_csv(con, f"bronze.{name}", path)
        counts[name] = int(con.execute(f"SELECT count(*) FROM bronze.{name}").fetchone()[0])
    return counts


def _load_csv(con: duckdb.DuckDBPyConnection, table_name: str, path: Path) -> None:
    source_file = str(path.resolve()).replace("'", "''")
    csv_path = str(path.resolve()).replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *, current_timestamp AS loaded_at, '{source_file}' AS source_file
        FROM read_csv_auto('{csv_path}', header=true)
        """
    )
