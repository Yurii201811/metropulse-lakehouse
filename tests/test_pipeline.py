from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from metropulse import orchestration
from metropulse.api import create_app
from metropulse.generator import RawBatch
from metropulse.orchestration import run_pipeline
from metropulse.warehouse import connect


def test_pipeline_builds_expected_layers(tmp_path: Path) -> None:
    result = run_pipeline(project_root=tmp_path, days=7, seed=1234)

    assert result.db_path.exists()
    assert result.total_trips > 0
    assert result.rejected_trips == 0
    assert result.gold_hourly_rows > 0
    assert not result.failed_checks
    assert len(result.ingest_files) == 4
    assert all(
        item.source_file.startswith(f"runs/{result.run_id}/") for item in result.ingest_files
    )
    assert all(
        (tmp_path / "data" / "raw" / item.source_file).is_file()
        for item in result.ingest_files
    )

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
        assert "silver.trip_rejections" in tables
        assert "silver.payment_rejections" in tables
        assert "gold.hourly_mobility" in tables
        assert "ops.ingest_files" in tables
        assert "ops.quality_results" in tables
        timing = con.execute(
            """
            SELECT
                ended_at > started_at,
                published_at = ended_at,
                (SELECT max(checked_at) FROM ops.quality_results WHERE run_id = ?) < ended_at,
                (SELECT max(loaded_at) FROM ops.ingest_files WHERE run_id = ?) < ended_at
            FROM ops.pipeline_runs
            WHERE run_id = ?
            """,
            [result.run_id, result.run_id, result.run_id],
        ).fetchone()
        assert timing == (True, True, True, True)
    finally:
        con.close()


def test_pipeline_is_deterministic_for_same_seed(tmp_path: Path) -> None:
    first = run_pipeline(project_root=tmp_path / "a", days=5, seed=2026)
    second = run_pipeline(project_root=tmp_path / "b", days=5, seed=2026)

    assert first.total_trips == second.total_trips
    assert first.gold_hourly_rows == second.gold_hourly_rows


def test_corrupt_published_database_does_not_leave_candidate(tmp_path: Path) -> None:
    warehouse_dir = tmp_path / "data" / "warehouse"
    warehouse_dir.mkdir(parents=True)
    published = warehouse_dir / "metropulse.duckdb"
    published.write_text("not a DuckDB database", encoding="utf-8")

    with pytest.raises(duckdb.Error):
        run_pipeline(project_root=tmp_path, days=2, seed=500)

    assert published.read_text(encoding="utf-8") == "not a DuckDB database"
    assert not list(warehouse_dir.glob(".*.candidate.duckdb"))


def test_candidate_build_keeps_published_file_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = run_pipeline(project_root=tmp_path, days=4, seed=501)
    entered_candidate = Event()
    release_candidate = Event()
    candidate_paths: list[Path] = []
    original_build_gold = orchestration.build_gold

    def build_gold_with_pause(con: duckdb.DuckDBPyConnection) -> None:
        database_path = Path(con.execute("PRAGMA database_list").fetchone()[2])
        candidate_paths.append(database_path)
        original_build_gold(con)
        entered_candidate.set()
        if not release_candidate.wait(timeout=10):
            raise TimeoutError("Test did not release candidate publication.")

    monkeypatch.setattr(orchestration, "build_gold", build_gold_with_pause)
    client = TestClient(create_app(published.db_path))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_pipeline, project_root=tmp_path, days=5, seed=502)
        assert entered_candidate.wait(timeout=10)
        try:
            during_build = client.get("/api/summary")
            assert during_build.status_code == 200
            assert during_build.json()["snapshot_run_id"] == published.run_id
            assert candidate_paths[0] != published.db_path
            assert candidate_paths[0].exists()
            with pytest.raises(
                RuntimeError,
                match="Another MetroPulse pipeline run is already active",
            ):
                run_pipeline(project_root=tmp_path, days=2, seed=503)
        finally:
            release_candidate.set()
        replacement = future.result(timeout=10)

    after_publish = client.get("/api/summary")
    assert after_publish.status_code == 200
    assert after_publish.json()["snapshot_run_id"] == replacement.run_id
    assert not candidate_paths[0].exists()


def test_failed_run_keeps_last_published_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    good = run_pipeline(project_root=tmp_path, days=5, seed=44)
    good_trip_manifest = next(item for item in good.ingest_files if item.dataset_name == "trips")
    good_trip_path = tmp_path / "data" / "raw" / good_trip_manifest.source_file
    original_generate = orchestration.generate_raw_batch

    def generate_invalid_batch(*args: object, **kwargs: object) -> RawBatch:
        batch = original_generate(*args, **kwargs)
        trips = pd.read_csv(batch.trips)
        trips.loc[0, "ended_at"] = trips.loc[0, "started_at"]
        trips.to_csv(batch.trips, index=False)
        return batch

    monkeypatch.setattr(orchestration, "generate_raw_batch", generate_invalid_batch)
    with pytest.raises(RuntimeError, match="trip_contract_rejections"):
        run_pipeline(project_root=tmp_path, days=4, seed=45)

    con = connect(good.db_path, read_only=True)
    try:
        snapshot = con.execute(
            "SELECT total_trips FROM gold.dashboard_summary"
        ).fetchone()
        latest_published = con.execute(
            """
            SELECT run_id
            FROM ops.pipeline_runs
            WHERE published_at IS NOT NULL
            ORDER BY published_at DESC
            LIMIT 1
            """
        ).fetchone()
        failed_run = con.execute(
            """
            SELECT run_id, status, published_at
            FROM ops.pipeline_runs
            WHERE status = 'failed_quality'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        rejected_check = con.execute(
            """
            SELECT observed_value
            FROM ops.quality_results
            WHERE run_id = ? AND check_name = 'trip_contract_rejections'
            """,
            [failed_run[0]],
        ).fetchone()
    finally:
        con.close()

    assert snapshot[0] == good.total_trips
    assert latest_published[0] == good.run_id
    assert failed_run[1:] == ("failed_quality", None)
    assert rejected_check[0] == 1
    assert good_trip_path.is_file()
    assert hashlib.sha256(good_trip_path.read_bytes()).hexdigest() == good_trip_manifest.file_sha256
    assert good_trip_path.parent.name == good.run_id
