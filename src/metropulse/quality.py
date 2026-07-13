from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol

import duckdb


@dataclass(frozen=True)
class QualityCheck:
    name: str
    status: str
    observed_value: float | None
    threshold: str
    details: str


class DriftOutcome(Protocol):
    status: str


def run_quality_checks(
    con: duckdb.DuckDBPyConnection,
    *,
    expected_end_date: date,
    drift_results: Sequence[DriftOutcome] | None = None,
) -> list[QualityCheck]:
    checks = [
        _min_count(con, "raw_trips_loaded", "bronze.trips", 1),
        _min_count(con, "silver_trips_loaded", "silver.trips", 1),
        _equals_zero(
            con,
            "trip_contract_rejections",
            "SELECT count(*) FROM silver.trip_rejections",
        ),
        _equals_zero(
            con,
            "payment_contract_rejections",
            "SELECT count(*) FROM silver.payment_rejections",
        ),
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
        _equals_zero(
            con,
            "enriched_trip_cardinality",
            """
            SELECT abs(
                (SELECT count(*) FROM silver.trips)
                - (SELECT count(*) FROM silver.trip_enriched)
            )
            """,
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
        _equals_zero(
            con,
            "payment_amount_reconciliation",
            """
            SELECT count(*)
            FROM silver.payments
            WHERE abs(total_amount - (fare_amount - discount_amount + tax_amount)) > 0.02
            """,
        ),
        _at_least(
            con,
            "dashboard_summary_ready",
            "SELECT total_trips FROM gold.dashboard_summary",
            1,
        ),
        _interval_end_check(con, expected_end_date),
        _drift_check(drift_results),
    ]

    return checks


def persist_quality_checks(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    checks: list[QualityCheck],
) -> None:
    checked_at = datetime.now(UTC)
    con.execute("DELETE FROM ops.quality_results WHERE run_id = ?", [run_id])
    con.executemany(
        """
        INSERT INTO ops.quality_results (
            run_id, check_name, status, observed_value, threshold, details, checked_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                c.name,
                c.status,
                c.observed_value,
                c.threshold,
                c.details,
                checked_at,
            )
            for c in checks
        ],
    )


def _min_count(
    con: duckdb.DuckDBPyConnection, name: str, table_name: str, minimum: int
) -> QualityCheck:
    observed = _observed(con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])
    status = "pass" if observed is not None and observed >= minimum else "fail"
    return QualityCheck(name, status, observed, f">= {minimum}", f"{table_name} row count")


def _equals_zero(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> QualityCheck:
    observed = _observed(con.execute(sql).fetchone()[0])
    status = "pass" if observed is not None and observed == 0 else "fail"
    return QualityCheck(name, status, observed, "= 0", "Data defect count")


def _at_least(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    threshold: float,
) -> QualityCheck:
    observed = _observed(con.execute(sql).fetchone()[0])
    status = "pass" if observed is not None and observed >= threshold else "fail"
    return QualityCheck(name, status, observed, f">= {threshold}", "Minimum acceptable value")


def _at_most(con: duckdb.DuckDBPyConnection, name: str, sql: str, threshold: float) -> QualityCheck:
    observed = _observed(con.execute(sql).fetchone()[0])
    status = "pass" if observed is not None and observed <= threshold else "fail"
    return QualityCheck(name, status, observed, f"<= {threshold}", "Maximum acceptable value")


def _interval_end_check(
    con: duckdb.DuckDBPyConnection,
    expected_end_date: date,
) -> QualityCheck:
    observed = _observed(
        con.execute(
            "SELECT date_diff('day', max(trip_date), ?::DATE) FROM silver.trip_enriched",
            [expected_end_date],
        ).fetchone()[0]
    )
    status = "pass" if observed == 0 else "fail"
    return QualityCheck(
        "data_interval_end_gap_days",
        status,
        observed,
        "= 0",
        f"Latest accepted trip date must equal the declared interval end {expected_end_date}",
    )


def _drift_check(drift_results: Sequence[DriftOutcome] | None) -> QualityCheck:
    if drift_results is None:
        return QualityCheck(
            "cross_snapshot_drift",
            "pass",
            0.0,
            "= 0 breached metrics",
            "No compatible published baseline exists yet",
        )
    failed_count = sum(result.status != "pass" for result in drift_results)
    return QualityCheck(
        "cross_snapshot_drift",
        "pass" if failed_count == 0 else "fail",
        float(failed_count),
        "= 0 breached metrics",
        f"Compared {len(drift_results)} profiled metrics with the compatible baseline",
    )


def _observed(value: object) -> float | None:
    return None if value is None else float(value)
