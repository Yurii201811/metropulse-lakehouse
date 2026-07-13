from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from metropulse import api as api_module
from metropulse.api import create_app
from metropulse.orchestration import run_pipeline


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
