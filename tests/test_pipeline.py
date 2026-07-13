from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from threading import Event

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from metropulse import orchestration
from metropulse.api import create_app
from metropulse.generator import RawBatch
from metropulse.orchestration import replay_pipeline, run_pipeline
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
    as_of_date = date(2026, 6, 30)
    first = run_pipeline(
        project_root=tmp_path / "a",
        days=5,
        seed=2026,
        as_of_date=as_of_date,
    )
    second = run_pipeline(
        project_root=tmp_path / "b",
        days=5,
        seed=2026,
        as_of_date=as_of_date,
    )

    assert first.total_trips == second.total_trips
    assert first.gold_hourly_rows == second.gold_hourly_rows
    assert first.input_set_sha256 == second.input_set_sha256
    assert first.output_set_sha256 == second.output_set_sha256
    assert {
        item.dataset_name: item.file_sha256 for item in first.ingest_files
    } == {item.dataset_name: item.file_sha256 for item in second.ingest_files}


def test_replay_verifies_sources_and_reproduces_snapshot(tmp_path: Path) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=5,
        seed=2027,
        as_of_date=date(2026, 7, 1),
    )

    replay = replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)

    assert replay.source_mode == "replay"
    assert replay.replay_of_run_id == original.run_id
    assert replay.data_interval_start == original.data_interval_start
    assert replay.data_interval_end == original.data_interval_end
    assert replay.input_set_sha256 == original.input_set_sha256
    assert replay.output_set_sha256 == original.output_set_sha256
    assert all(
        item.source_file.startswith(f"runs/{replay.run_id}/")
        for item in replay.ingest_files
    )
    assert {item.fingerprint_sha256 for item in replay.fingerprints} == {
        item.fingerprint_sha256 for item in original.fingerprints
    }
    assert replay.drift_results
    assert {item.status for item in replay.drift_results} == {"pass"}

    con = connect(replay.db_path, read_only=True)
    try:
        metadata = con.execute(
            """
            SELECT source_mode, replay_of_run_id, parent_run_id, input_set_sha256,
                   output_set_sha256, contract_version
            FROM ops.pipeline_runs
            WHERE run_id = ?
            """,
            [replay.run_id],
        ).fetchone()
    finally:
        con.close()

    assert metadata[:5] == (
        "replay",
        original.run_id,
        original.run_id,
        original.input_set_sha256,
        original.output_set_sha256,
    )
    assert metadata[5] == "snapshot-v1"


def test_replay_stages_verified_bytes_before_using_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=2032,
        as_of_date=date(2026, 7, 5),
    )
    trips_manifest = next(
        item for item in original.ingest_files if item.dataset_name == "trips"
    )
    source_trips = tmp_path / "data" / "raw" / trips_manifest.source_file
    original_verify = orchestration._verified_replay_batch

    def mutate_source_after_staging(*args: object, **kwargs: object) -> RawBatch:
        batch = original_verify(*args, **kwargs)
        source_trips.write_bytes(source_trips.read_bytes() + b"\n")
        return batch

    monkeypatch.setattr(
        orchestration,
        "_verified_replay_batch",
        mutate_source_after_staging,
    )
    replay = replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)

    assert replay.input_set_sha256 == original.input_set_sha256
    assert replay.output_set_sha256 == original.output_set_sha256
    assert all(
        item.source_file.startswith(f"runs/{replay.run_id}/")
        for item in replay.ingest_files
    )


def test_staged_replay_input_change_is_refused_without_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=2033,
        as_of_date=date(2026, 7, 6),
    )
    original_verify = orchestration._verified_replay_batch

    def tamper_with_staged_input(*args: object, **kwargs: object) -> RawBatch:
        batch = original_verify(*args, **kwargs)
        batch.trips.write_bytes(batch.trips.read_bytes() + b"\n")
        return batch

    monkeypatch.setattr(
        orchestration,
        "_verified_replay_batch",
        tamper_with_staged_input,
    )
    with pytest.raises(RuntimeError, match="Replay input fingerprint mismatch"):
        replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)

    con = connect(original.db_path, read_only=True)
    try:
        latest_published = con.execute(
            """
            SELECT run_id FROM ops.pipeline_runs
            WHERE published_at IS NOT NULL
            ORDER BY published_at DESC LIMIT 1
            """
        ).fetchone()[0]
        failed = con.execute(
            """
            SELECT status, replay_of_run_id, published_at, input_set_sha256,
                   output_set_sha256
            FROM ops.pipeline_runs
            ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    assert latest_published == original.run_id
    assert failed[:3] == ("failed", original.run_id, None)
    assert failed[3] != original.input_set_sha256
    assert failed[4] is None


def test_replay_output_change_is_refused_without_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=2034,
        as_of_date=date(2026, 7, 7),
    )
    original_build_gold = orchestration.build_gold

    def build_changed_gold(con: duckdb.DuckDBPyConnection) -> None:
        original_build_gold(con)
        con.execute(
            """
            UPDATE gold.revenue_by_zone
            SET revenue = revenue + 0.01
            WHERE zone_id = (SELECT min(zone_id) FROM gold.revenue_by_zone)
            """
        )

    monkeypatch.setattr(orchestration, "build_gold", build_changed_gold)
    with pytest.raises(RuntimeError, match="Replay output fingerprint mismatch"):
        replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)

    con = connect(original.db_path, read_only=True)
    try:
        latest_published = con.execute(
            """
            SELECT run_id FROM ops.pipeline_runs
            WHERE published_at IS NOT NULL
            ORDER BY published_at DESC LIMIT 1
            """
        ).fetchone()[0]
        failed = con.execute(
            """
            SELECT run_id, status, replay_of_run_id, published_at,
                   input_set_sha256, output_set_sha256
            FROM ops.pipeline_runs
            ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()
        evidence_count = con.execute(
            """
            SELECT count(*) FROM ops.relation_fingerprints WHERE run_id = ?
            """,
            [failed[0]],
        ).fetchone()[0]
    finally:
        con.close()

    assert latest_published == original.run_id
    assert failed[1:4] == ("failed", original.run_id, None)
    assert failed[4] == original.input_set_sha256
    assert failed[5] != original.output_set_sha256
    assert evidence_count == 6


def test_tampered_replay_source_is_refused_without_replacing_snapshot(tmp_path: Path) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=2028,
        as_of_date=date(2026, 7, 2),
    )
    trips_manifest = next(
        item for item in original.ingest_files if item.dataset_name == "trips"
    )
    trips_path = tmp_path / "data" / "raw" / trips_manifest.source_file
    trips_path.write_bytes(trips_path.read_bytes() + b"\n")

    with pytest.raises(RuntimeError, match="Replay source size mismatch"):
        replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)

    con = connect(original.db_path, read_only=True)
    try:
        latest_published = con.execute(
            """
            SELECT run_id
            FROM ops.pipeline_runs
            WHERE published_at IS NOT NULL
            ORDER BY published_at DESC
            LIMIT 1
            """
        ).fetchone()[0]
        failed = con.execute(
            """
            SELECT status, replay_of_run_id, published_at, error_message
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    assert latest_published == original.run_id
    assert failed[:3] == ("failed", original.run_id, None)
    assert "size mismatch" in failed[3]


def test_profile_drift_blocks_publication_and_preserves_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=2029,
        as_of_date=date(2026, 7, 3),
    )
    original_generate = orchestration.generate_raw_batch

    def generate_shifted_rider_mix(*args: object, **kwargs: object) -> RawBatch:
        batch = original_generate(*args, **kwargs)
        trips = pd.read_csv(batch.trips)
        trips["rider_type"] = "member"
        trips.to_csv(batch.trips, index=False)
        return batch

    monkeypatch.setattr(orchestration, "generate_raw_batch", generate_shifted_rider_mix)
    with pytest.raises(RuntimeError, match="cross_snapshot_drift"):
        run_pipeline(
            project_root=tmp_path,
            days=4,
            seed=2030,
            as_of_date=date(2026, 7, 4),
        )

    con = connect(baseline.db_path, read_only=True)
    try:
        latest_published = con.execute(
            """
            SELECT run_id FROM ops.pipeline_runs
            WHERE published_at IS NOT NULL
            ORDER BY published_at DESC LIMIT 1
            """
        ).fetchone()[0]
        failed_run = con.execute(
            """
            SELECT run_id FROM ops.pipeline_runs
            WHERE status = 'failed_quality'
            ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()[0]
        failed_metrics = con.execute(
            """
            SELECT metric_name
            FROM ops.drift_results
            WHERE run_id = ? AND status = 'fail'
            ORDER BY metric_name
            """,
            [failed_run],
        ).fetchall()
        drift_gate = con.execute(
            """
            SELECT status, observed_value
            FROM ops.quality_results
            WHERE run_id = ? AND check_name = 'cross_snapshot_drift'
            """,
            [failed_run],
        ).fetchone()
    finally:
        con.close()

    assert latest_published == baseline.run_id
    assert {row[0] for row in failed_metrics} >= {"member_share", "casual_share"}
    assert drift_gate[0] == "fail"
    assert drift_gate[1] >= 2


def test_future_snapshot_date_is_rejected_without_mutating_warehouse(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot be later than yesterday"):
        run_pipeline(
            project_root=tmp_path,
            days=2,
            seed=2031,
            as_of_date=date.today() + timedelta(days=1),
        )

    assert not (tmp_path / "data" / "warehouse" / "metropulse.duckdb").exists()


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
