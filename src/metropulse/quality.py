from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class QualityCheck:
    name: str
    status: str
    observed_value: float
    threshold: str
    details: str


def run_quality_checks(con: duckdb.DuckDBPyConnection, run_id: str) -> list[QualityCheck]:
    checks = [
        _min_count(con, "raw_trips_loaded", "bronze.trips", 1),
        _min_count(con, "silver_trips_loaded", "silver.trips", 1),
        _equals_zero(
            con,
            "duplicate_trip_ids",
            """
            SELECT count(*)
            FROM (
                SELECT trip_id
                FROM silver.trips
                GROUP BY 1
                HAVING count(*) > 1
            )
            """,
        ),
        _equals_zero(
            con,
            "invalid_trip_timestamps",
            "SELECT count(*) FROM silver.trips WHERE ended_at <= started_at",
        ),
        _at_least(
            con,
            "payment_match_rate",
            """
            SELECT coalesce(avg(CASE WHEN has_payment THEN 1.0 ELSE 0.0 END), 0)
            FROM silver.trip_enriched
            """,
            0.995,
        ),
        _equals_zero(
            con,
            "missing_station_references",
            """
            SELECT count(*)
            FROM silver.trip_enriched
            WHERE start_station_name IS NULL OR end_station_name IS NULL
            """,
        ),
        _at_least(
            con,
            "dashboard_summary_ready",
            "SELECT total_trips FROM gold.dashboard_summary",
            1,
        ),
        _at_most(
            con,
            "latest_trip_freshness_days",
            "SELECT date_diff('day', max(trip_date), current_date) FROM silver.trip_enriched",
            5,
        ),
    ]

    con.executemany(
        """
        INSERT INTO ops.quality_results (
            run_id, check_name, status, observed_value, threshold, details
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(run_id, c.name, c.status, c.observed_value, c.threshold, c.details) for c in checks],
    )
    return checks


def _min_count(
    con: duckdb.DuckDBPyConnection, name: str, table_name: str, minimum: int
) -> QualityCheck:
    observed = float(con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])
    status = "pass" if observed >= minimum else "fail"
    return QualityCheck(name, status, observed, f">= {minimum}", f"{table_name} row count")


def _equals_zero(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> QualityCheck:
    observed = float(con.execute(sql).fetchone()[0])
    status = "pass" if observed == 0 else "fail"
    return QualityCheck(name, status, observed, "= 0", "Data defect count")


def _at_least(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    threshold: float,
) -> QualityCheck:
    observed = float(con.execute(sql).fetchone()[0])
    status = "pass" if observed >= threshold else "fail"
    return QualityCheck(name, status, observed, f">= {threshold}", "Minimum acceptable value")


def _at_most(con: duckdb.DuckDBPyConnection, name: str, sql: str, threshold: float) -> QualityCheck:
    observed = float(con.execute(sql).fetchone()[0])
    status = "pass" if observed <= threshold else "fail"
    return QualityCheck(name, status, observed, f"<= {threshold}", "Maximum acceptable value")
