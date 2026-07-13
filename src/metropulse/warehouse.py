from __future__ import annotations

from pathlib import Path

import duckdb


def connect(db_path: Path | str, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def init_schemas(con: duckdb.DuckDBPyConnection) -> None:
    for schema in ("ops", "bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP,
            status VARCHAR NOT NULL,
            days_requested INTEGER NOT NULL,
            seed INTEGER NOT NULL,
            raw_trips INTEGER,
            silver_trips INTEGER,
            gold_hourly_rows INTEGER,
            quality_passed INTEGER,
            quality_failed INTEGER,
            data_interval_start DATE,
            data_interval_end DATE,
            source_mode VARCHAR,
            replay_of_run_id VARCHAR,
            parent_run_id VARCHAR,
            code_version VARCHAR,
            contract_version VARCHAR,
            config_sha256 VARCHAR,
            input_set_sha256 VARCHAR,
            output_set_sha256 VARCHAR,
            error_message VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.quality_results (
            run_id VARCHAR NOT NULL,
            check_name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            observed_value DOUBLE,
            threshold VARCHAR,
            details VARCHAR,
            checked_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.ingest_files (
            run_id VARCHAR NOT NULL,
            dataset_name VARCHAR NOT NULL,
            source_file VARCHAR NOT NULL,
            file_sha256 VARCHAR NOT NULL,
            file_bytes BIGINT NOT NULL,
            row_count BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (run_id, dataset_name)
        )
        """
    )

    con.execute(
        """
        ALTER TABLE ops.pipeline_runs
        ADD COLUMN IF NOT EXISTS rejected_trips INTEGER
        """
    )
    con.execute(
        """
        ALTER TABLE ops.pipeline_runs
        ADD COLUMN IF NOT EXISTS published_at TIMESTAMP
        """
    )
    for column_name, column_type in (
        ("data_interval_start", "DATE"),
        ("data_interval_end", "DATE"),
        ("source_mode", "VARCHAR"),
        ("replay_of_run_id", "VARCHAR"),
        ("parent_run_id", "VARCHAR"),
        ("code_version", "VARCHAR"),
        ("contract_version", "VARCHAR"),
        ("config_sha256", "VARCHAR"),
        ("input_set_sha256", "VARCHAR"),
        ("output_set_sha256", "VARCHAR"),
    ):
        con.execute(
            f"ALTER TABLE ops.pipeline_runs ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.dataset_profiles (
            run_id VARCHAR NOT NULL,
            metric_name VARCHAR NOT NULL,
            metric_value DOUBLE,
            unit VARCHAR NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (run_id, metric_name)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.relation_fingerprints (
            run_id VARCHAR NOT NULL,
            relation_name VARCHAR NOT NULL,
            row_count BIGINT NOT NULL,
            fingerprint_sha256 VARCHAR NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (run_id, relation_name)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.drift_results (
            run_id VARCHAR NOT NULL,
            baseline_run_id VARCHAR NOT NULL,
            metric_name VARCHAR NOT NULL,
            current_value DOUBLE,
            baseline_value DOUBLE,
            delta DOUBLE,
            delta_kind VARCHAR NOT NULL,
            threshold DOUBLE NOT NULL,
            status VARCHAR NOT NULL,
            checked_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (run_id, metric_name)
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS quality_results_run_check_idx
        ON ops.quality_results (run_id, check_name)
        """
    )


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])


def single_value(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    return con.execute(sql).fetchone()[0]
