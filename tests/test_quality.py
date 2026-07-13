from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metropulse import orchestration
from metropulse.generator import RawBatch
from metropulse.orchestration import run_pipeline
from metropulse.warehouse import connect


def test_quality_results_are_persisted_with_run_id(tmp_path: Path) -> None:
    result = run_pipeline(project_root=tmp_path, days=4, seed=77)

    con = connect(result.db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT check_name, status
            FROM ops.quality_results
            WHERE run_id = ?
            """,
            [result.run_id],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) >= 12
    assert {status for _, status in rows} == {"pass"}


def test_duplicate_payment_is_quarantined_without_multiplying_fact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_generate = orchestration.generate_raw_batch

    def generate_duplicate_payment(*args: object, **kwargs: object) -> RawBatch:
        batch = original_generate(*args, **kwargs)
        payments = pd.read_csv(batch.payments)
        payments = pd.concat([payments, payments.iloc[[0]]], ignore_index=True)
        payments.to_csv(batch.payments, index=False)
        return batch

    monkeypatch.setattr(orchestration, "generate_raw_batch", generate_duplicate_payment)
    result = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=81,
        fail_on_quality=False,
    )

    assert "payment_contract_rejections" in {check.name for check in result.failed_checks}
    con = connect(result.db_path, read_only=True)
    try:
        trips, enriched, rejected = con.execute(
            """
            SELECT
                (SELECT count(*) FROM silver.trips),
                (SELECT count(*) FROM silver.trip_enriched),
                (SELECT count(*) FROM silver.payment_rejections)
            """
        ).fetchone()
    finally:
        con.close()

    assert enriched == trips
    assert rejected == 2


def test_non_finite_numeric_values_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_generate = orchestration.generate_raw_batch

    def generate_non_finite_values(*args: object, **kwargs: object) -> RawBatch:
        batch = original_generate(*args, **kwargs)
        trips = pd.read_csv(batch.trips)
        payments = pd.read_csv(batch.payments)
        trips.loc[0, "distance_km"] = float("inf")
        trips.loc[1, "duration_min"] = float("-inf")
        payments.loc[2, "fare_amount"] = float("inf")
        trips.to_csv(batch.trips, index=False)
        payments.to_csv(batch.payments, index=False)
        return batch

    monkeypatch.setattr(orchestration, "generate_raw_batch", generate_non_finite_values)
    result = run_pipeline(
        project_root=tmp_path,
        days=3,
        seed=83,
        fail_on_quality=False,
    )

    assert {check.name for check in result.failed_checks} >= {
        "trip_contract_rejections",
        "payment_contract_rejections",
    }
    con = connect(result.db_path, read_only=True)
    try:
        trip_reasons = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT rejection_reason FROM silver.trip_rejections"
            ).fetchall()
        }
        payment_reasons = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT rejection_reason FROM silver.payment_rejections"
            ).fetchall()
        }
    finally:
        con.close()

    assert {"invalid_distance", "invalid_duration"} <= trip_reasons
    assert "invalid_amount" in payment_reasons


def test_all_rejected_batch_records_failed_quality_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_generate = orchestration.generate_raw_batch

    def generate_all_invalid(*args: object, **kwargs: object) -> RawBatch:
        batch = original_generate(*args, **kwargs)
        trips = pd.read_csv(batch.trips)
        trips["ended_at"] = trips["started_at"]
        trips.to_csv(batch.trips, index=False)
        return batch

    monkeypatch.setattr(orchestration, "generate_raw_batch", generate_all_invalid)
    with pytest.raises(RuntimeError, match="Quality checks failed"):
        run_pipeline(project_root=tmp_path, days=3, seed=82)

    con = connect(tmp_path / "data" / "warehouse" / "metropulse.duckdb", read_only=True)
    try:
        run = con.execute(
            """
            SELECT run_id, status, quality_passed, quality_failed
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        checks = con.execute(
            """
            SELECT check_name, status, observed_value
            FROM ops.quality_results
            WHERE run_id = ?
            """,
            [run[0]],
        ).fetchall()
    finally:
        con.close()

    freshness = next(check for check in checks if check[0] == "data_interval_end_gap_days")
    assert run[1] == "failed_quality"
    assert run[2] + run[3] == len(checks) >= 13
    assert freshness == ("data_interval_end_gap_days", "fail", None)
