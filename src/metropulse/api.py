from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from metropulse.config import resolve_db_path
from metropulse.warehouse import connect


def create_app(db_path: Path | str | None = None) -> FastAPI:
    resolved_db_path = resolve_db_path(db_path)
    app = FastAPI(
        title="MetroPulse Lakehouse API",
        version="0.1.0",
        description="Portfolio API serving DuckDB gold marts produced by the MetroPulse pipeline.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "db_path": str(resolved_db_path),
            "db_exists": resolved_db_path.exists(),
        }

    @app.get("/api/summary")
    def summary() -> dict[str, Any]:
        return _single_row(
            resolved_db_path,
            """
            SELECT
                total_trips,
                total_revenue,
                active_stations,
                first_trip_date,
                last_trip_date,
                avg_duration_min,
                payment_match_rate,
                (
                    SELECT quality_passed * 1.0 / nullif(quality_passed + quality_failed, 0)
                    FROM ops.pipeline_runs
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS validation_rate,
                (
                    SELECT date_diff('second', started_at, ended_at)
                    FROM ops.pipeline_runs
                    WHERE ended_at IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                ) AS latest_runtime_seconds
            FROM gold.dashboard_summary
            """,
        )

    @app.get("/api/timeseries")
    def timeseries() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT
                trip_hour,
                sum(trips)::INTEGER AS trips,
                round(sum(revenue), 2) AS revenue
            FROM gold.hourly_mobility
            GROUP BY 1
            ORDER BY 1
            """,
        )

    @app.get("/api/zones")
    def zones() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT zone_id, zone_name, trips, revenue, revenue_per_trip, avg_duration_min
            FROM gold.revenue_by_zone
            ORDER BY revenue DESC
            """,
        )

    @app.get("/api/stations")
    def stations(limit: int = 12) -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT
                station_id,
                station_name,
                zone_name,
                sum(departures)::INTEGER AS departures,
                round(sum(revenue), 2) AS revenue,
                round(avg(member_trip_share), 4) AS member_trip_share
            FROM gold.daily_station_performance
            GROUP BY 1, 2, 3
            ORDER BY departures DESC
            LIMIT ?
            """,
            [limit],
        )

    @app.get("/api/quality")
    def quality() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT check_name, status, observed_value, threshold, details, checked_at
            FROM ops.quality_results
            WHERE run_id = (SELECT run_id FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 1)
            ORDER BY check_name
            """,
        )

    @app.get("/api/pipeline-runs")
    def pipeline_runs() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT
                run_id,
                started_at,
                ended_at,
                status,
                days_requested,
                raw_trips,
                silver_trips,
                gold_hourly_rows,
                quality_passed,
                quality_failed
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 8
            """,
        )

    @app.get("/api/lineage")
    def lineage() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT source_node, target_node, transform_type
            FROM gold.lineage_edges
            """,
        )

    return app


def _rows(db_path: Path, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    if not db_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Warehouse not found. Run `metropulse run` first.",
        )
    con = connect(db_path, read_only=True)
    try:
        cursor = con.execute(sql, params or [])
        columns = [description[0] for description in cursor.description]
        return [_serialize(dict(zip(columns, row, strict=True))) for row in cursor.fetchall()]
    finally:
        con.close()


def _single_row(db_path: Path, sql: str) -> dict[str, Any]:
    rows = _rows(db_path, sql)
    if not rows:
        raise HTTPException(status_code=404, detail="No rows found. Run `metropulse run` first.")
    return rows[0]


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    serialized = {}
    for key, value in row.items():
        serialized[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return serialized
