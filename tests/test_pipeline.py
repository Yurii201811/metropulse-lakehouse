from __future__ import annotations

from metropulse.orchestration import run_pipeline
from metropulse.warehouse import connect
from tests.helpers import isolated_project_root


def test_pipeline_builds_expected_layers() -> None:
    result = run_pipeline(project_root=isolated_project_root("layers"), days=7, seed=1234)

    assert result.db_path.exists()
    assert result.total_trips > 0
    assert result.gold_hourly_rows > 0
    assert not result.failed_checks

    con = connect(result.db_path, read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                """
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops')
                """
            ).fetchall()
        }
        assert "bronze.trips" in tables
        assert "silver.trip_enriched" in tables
        assert "gold.hourly_mobility" in tables
        assert "ops.quality_results" in tables
    finally:
        con.close()


def test_pipeline_is_deterministic_for_same_seed() -> None:
    first = run_pipeline(project_root=isolated_project_root("deterministic-a"), days=5, seed=2026)
    second = run_pipeline(project_root=isolated_project_root("deterministic-b"), days=5, seed=2026)

    assert first.total_trips == second.total_trips
    assert first.gold_hourly_rows == second.gold_hourly_rows
