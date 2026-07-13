from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from metropulse import api as api_module
from metropulse.api import _failure_summary, create_app
from metropulse.orchestration import replay_pipeline, run_pipeline
from metropulse.warehouse import connect


def test_api_serves_dashboard_payloads(tmp_path: Path) -> None:
    result = run_pipeline(project_root=tmp_path, days=6, seed=99)
    client = TestClient(create_app(result.db_path))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["warehouse_found"] is True

    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["snapshot_run_id"] == result.run_id

    summary = client.get("/api/summary")
    assert summary.status_code == 200
    assert summary.json()["total_trips"] == result.total_trips
    assert summary.json()["snapshot_run_id"] == result.run_id

    quality = client.get("/api/quality")
    assert quality.status_code == 200
    assert all(check["status"] == "pass" for check in quality.json())

    stations = client.get("/api/stations?limit=5")
    assert stations.status_code == 200
    assert len(stations.json()) == 5

    lineage = client.get("/api/lineage")
    assert lineage.status_code == 200
    assert any(edge["target_node"] == "FastAPI + dashboard" for edge in lineage.json())

    manifests = client.get("/api/ingest-files")
    assert manifests.status_code == 200
    assert {item["dataset_name"] for item in manifests.json()} == {
        "payments",
        "stations",
        "trips",
        "weather",
    }
    assert all(len(item["file_sha256"]) == 64 for item in manifests.json())

    drift = client.get("/api/drift")
    assert drift.status_code == 200
    assert drift.json()["status"] == "no_baseline"
    assert drift.json()["run_id"] == result.run_id

    detail = client.get(f"/api/pipeline-runs/{result.run_id}")
    assert detail.status_code == 200
    detail_data = detail.json()
    assert detail_data["run"]["is_current_snapshot"] is True
    assert "error_message" not in detail_data["run"]
    assert detail_data["run"]["failure_summary"] is None
    assert detail_data["run"]["data_interval_end"]
    assert len(detail_data["profiles"]) == 12
    assert len(detail_data["fingerprints"]) == 6


def test_api_filters_and_bounds_queries(tmp_path: Path) -> None:
    result = run_pipeline(project_root=tmp_path, days=6, seed=123)
    client = TestClient(create_app(result.db_path))

    filters = client.get("/api/filters")
    assert filters.status_code == 200
    filter_data = filters.json()
    assert len(filter_data["zones"]) >= 2

    total = client.get("/api/summary").json()["total_trips"]
    zone_id = filter_data["zones"][0]["zone_id"]
    filtered = client.get(
        "/api/summary",
        params={"zone_id": zone_id, "rider_type": "member"},
    )
    assert filtered.status_code == 200
    assert 0 < filtered.json()["total_trips"] < total

    series = client.get("/api/timeseries", params={"zone_id": zone_id})
    assert series.status_code == 200
    assert series.json()

    assert client.get("/api/stations?limit=0").status_code == 422
    assert client.get("/api/stations?limit=51").status_code == 422
    assert client.get("/api/summary?zone_id=bad").status_code == 422
    reversed_window = client.get(
        "/api/summary",
        params={"start_date": filter_data["end_date"], "end_date": filter_data["start_date"]},
    )
    assert reversed_window.status_code == 422


def test_api_exposes_replay_provenance_and_stable_drift(tmp_path: Path) -> None:
    original = run_pipeline(
        project_root=tmp_path,
        days=4,
        seed=504,
        as_of_date=date(2026, 7, 5),
    )
    replay = replay_pipeline(project_root=tmp_path, replay_run_id=original.run_id)
    client = TestClient(create_app(replay.db_path))

    runs = client.get("/api/pipeline-runs")
    assert runs.status_code == 200
    assert runs.json()[0]["source_mode"] == "replay"
    assert runs.json()[0]["replay_of_run_id"] == original.run_id

    drift = client.get("/api/drift")
    assert drift.status_code == 200
    assert drift.json()["status"] == "stable"
    assert drift.json()["baseline_run_id"] == original.run_id
    assert drift.json()["checked_metrics"] == 9
    assert drift.json()["failed_metrics"] == 0

    detail = client.get(f"/api/pipeline-runs/{replay.run_id}")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["run"]["previous_published_run_id"] == original.run_id
    assert payload["run"]["input_set_sha256"] == original.input_set_sha256
    assert payload["run"]["output_set_sha256"] == original.output_set_sha256
    assert len(payload["quality"]) == 13
    assert len(payload["manifests"]) == 4
    assert len(payload["drift"]) == 9

    con = connect(replay.db_path)
    try:
        con.execute(
            """
            UPDATE ops.drift_results
            SET status = 'fail'
            WHERE run_id = ? AND metric_name = 'active_stations'
            """,
            [replay.run_id],
        )
    finally:
        con.close()
    breached_drift = client.get("/api/drift").json()
    assert breached_drift["results"][0]["metric_name"] == "active_stations"
    assert breached_drift["results"][0]["status"] == "fail"
    breached_detail = client.get(f"/api/pipeline-runs/{replay.run_id}").json()
    assert breached_detail["drift"][0]["metric_name"] == "active_stations"
    assert breached_detail["drift"][0]["status"] == "fail"

    historical_quality = client.get("/api/quality", params={"run_id": original.run_id})
    assert historical_quality.status_code == 200
    assert len(historical_quality.json()) == 13
    assert client.get("/api/pipeline-runs/not-a-run").status_code == 422
    assert client.get("/api/pipeline-runs/20260705120000-deadbeef").status_code == 404


@pytest.mark.parametrize(
    ("status", "error_message", "expected"),
    [
        ("failed_quality", "Quality checks failed: drift", "Quality gate failure"),
        (
            "failed",
            "Replay source SHA-256 mismatch: /private/raw/trips.csv",
            "Replay source or input integrity failure",
        ),
        (
            "failed",
            "Replay output fingerprint mismatch; not equivalent to source",
            "Replay output equivalence failure",
        ),
        ("failed", "Permission denied: /private/warehouse", "Run failed; review operator logs"),
    ],
)
def test_failure_summary_classifies_without_exposing_operator_details(
    status: str,
    error_message: str,
    expected: str,
) -> None:
    summary = _failure_summary(status, error_message)

    assert summary == expected
    assert "/private/" not in summary


def test_failed_run_api_exposes_only_safe_failure_summary(tmp_path: Path) -> None:
    result = run_pipeline(project_root=tmp_path, days=3, seed=701)
    con = connect(result.db_path)
    try:
        con.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = 'failed',
                published_at = NULL,
                error_message = 'Replay source file is missing: /private/raw/trips.csv'
            WHERE run_id = ?
            """,
            [result.run_id],
        )
    finally:
        con.close()
    client = TestClient(create_app(result.db_path))

    listed = client.get("/api/pipeline-runs").json()[0]
    detail = client.get(f"/api/pipeline-runs/{result.run_id}").json()["run"]

    for run in (listed, detail):
        assert run["failure_summary"] == "Replay source or input integrity failure"
        assert "error_message" not in run
        assert "/private/" not in str(run)


def test_readiness_fails_without_published_warehouse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = TestClient(create_app(tmp_path / "missing.duckdb"))

    assert client.get("/health").status_code == 200
    assert client.get("/health").json()["warehouse_found"] is False
    readiness = client.get("/ready")
    assert readiness.status_code == 503
    assert readiness.json()["status"] == "not_ready"
    assert client.get("/api/summary").status_code == 503

    corrupt_db = tmp_path / "corrupt.duckdb"
    corrupt_db.write_text("not a DuckDB database", encoding="utf-8")
    corrupt_client = TestClient(create_app(corrupt_db))
    corrupt_readiness = corrupt_client.get("/ready")
    assert corrupt_readiness.status_code == 503
    assert corrupt_readiness.json()["detail"] == "Warehouse inspection failed."

    directory_db = tmp_path / "directory.duckdb"
    directory_db.mkdir()
    directory_client = TestClient(create_app(directory_db))
    assert directory_client.get("/ready").status_code == 503
    assert directory_client.get("/api/summary").status_code == 503

    def raise_permission_error(*args: object, **kwargs: object) -> None:
        raise PermissionError("simulated permission failure")

    monkeypatch.setattr(api_module, "connect", raise_permission_error)
    permission_client = TestClient(create_app(corrupt_db))
    assert permission_client.get("/ready").status_code == 503
    assert permission_client.get("/api/summary").status_code == 503


def test_cors_origins_are_configurable_and_validated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_origin = "https://dashboard.example.com:8443"
    monkeypatch.setenv("METROPULSE_CORS_ORIGINS", allowed_origin)
    client = TestClient(create_app(tmp_path / "missing.duckdb"))

    preflight = client.options(
        "/health",
        headers={
            "Origin": allowed_origin,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == allowed_origin

    monkeypatch.setenv(
        "METROPULSE_CORS_ORIGINS",
        "https://user:secret@dashboard.example.com/private",
    )
    with pytest.raises(ValueError, match="Invalid CORS origin"):
        create_app(tmp_path / "missing.duckdb")
