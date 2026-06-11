from __future__ import annotations

from fastapi.testclient import TestClient

from metropulse.api import create_app
from metropulse.orchestration import run_pipeline
from tests.helpers import isolated_project_root


def test_api_serves_dashboard_payloads() -> None:
    result = run_pipeline(project_root=isolated_project_root("api"), days=6, seed=99)
    client = TestClient(create_app(result.db_path))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["db_exists"] is True

    summary = client.get("/api/summary")
    assert summary.status_code == 200
    assert summary.json()["total_trips"] == result.total_trips

    quality = client.get("/api/quality")
    assert quality.status_code == 200
    assert all(check["status"] == "pass" for check in quality.json())

    stations = client.get("/api/stations?limit=5")
    assert stations.status_code == 200
    assert len(stations.json()) == 5

    lineage = client.get("/api/lineage")
    assert lineage.status_code == 200
    assert any(edge["target_node"] == "FastAPI + dashboard" for edge in lineage.json())
