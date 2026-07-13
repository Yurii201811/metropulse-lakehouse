from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi import Path as PathParam
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from metropulse import __version__
from metropulse.config import resolve_db_path
from metropulse.warehouse import connect

RiderType = Literal["member", "casual", "corporate"]
REQUIRED_TABLES = {
    "gold.dashboard_summary",
    "gold.hourly_mobility",
    "gold.lineage_edges",
    "ops.ingest_files",
    "ops.dataset_profiles",
    "ops.drift_results",
    "ops.pipeline_runs",
    "ops.quality_results",
    "ops.relation_fingerprints",
    "silver.trip_enriched",
}


def create_app(db_path: Path | str | None = None) -> FastAPI:
    resolved_db_path = resolve_db_path(db_path)
    app = FastAPI(
        title="MetroPulse Lakehouse API",
        version=__version__,
        description="Operational API for the published MetroPulse DuckDB snapshot.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins(),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["Content-Type"],
    )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "metropulse-api",
            "version": __version__,
            "warehouse_found": resolved_db_path.exists(),
        }

    @app.get("/ready", response_model=None)
    def ready() -> Any:
        state = _readiness(resolved_db_path)
        if state["status"] != "ready":
            return JSONResponse(status_code=503, content=state)
        return state

    @app.get("/api/summary")
    def summary(
        start_date: Annotated[date | None, Query()] = None,
        end_date: Annotated[date | None, Query()] = None,
        zone_id: Annotated[str | None, Query(pattern=r"^Z\d{2}$")] = None,
        rider_type: Annotated[RiderType | None, Query()] = None,
    ) -> dict[str, Any]:
        where_sql, params = _analytics_filter(start_date, end_date, zone_id, rider_type)
        return _single_row(
            resolved_db_path,
            f"""
            SELECT
                count(*)::INTEGER AS total_trips,
                coalesce(round(sum(total_amount), 2), 0) AS total_revenue,
                count(DISTINCT start_station_id)::INTEGER AS active_stations,
                min(trip_date) AS first_trip_date,
                max(trip_date) AS last_trip_date,
                round(avg(duration_min), 2) AS avg_duration_min,
                round(avg(CASE WHEN has_payment THEN 1.0 ELSE 0.0 END), 4)
                    AS payment_match_rate,
                (SELECT count(*)::INTEGER FROM silver.trip_rejections) AS rejected_trips,
                (SELECT count(*)::INTEGER FROM silver.payment_rejections)
                    AS rejected_payments,
                (
                    SELECT quality_passed * 1.0 / nullif(quality_passed + quality_failed, 0)
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                ) AS validation_rate,
                (
                    SELECT round(
                        date_diff('millisecond', started_at, ended_at) / 1000.0,
                        3
                    )
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL AND ended_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                ) AS latest_runtime_seconds,
                (
                    SELECT run_id
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                ) AS snapshot_run_id,
                (
                    SELECT published_at
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                ) AS snapshot_published_at
            FROM silver.trip_enriched
            WHERE {where_sql}
            """,
            params,
        )

    @app.get("/api/timeseries")
    def timeseries(
        start_date: Annotated[date | None, Query()] = None,
        end_date: Annotated[date | None, Query()] = None,
        zone_id: Annotated[str | None, Query(pattern=r"^Z\d{2}$")] = None,
        rider_type: Annotated[RiderType | None, Query()] = None,
    ) -> list[dict[str, Any]]:
        where_sql, params = _analytics_filter(start_date, end_date, zone_id, rider_type)
        return _rows(
            resolved_db_path,
            f"""
            SELECT
                trip_hour,
                sum(trips)::INTEGER AS trips,
                round(sum(revenue), 2) AS revenue
            FROM (
                SELECT
                    date_trunc('hour', started_at) AS trip_hour,
                    count(*)::INTEGER AS trips,
                    sum(total_amount) AS revenue
                FROM silver.trip_enriched
                WHERE {where_sql}
                GROUP BY 1
            )
            GROUP BY 1
            ORDER BY 1
            """,
            params,
        )

    @app.get("/api/zones")
    def zones(
        start_date: Annotated[date | None, Query()] = None,
        end_date: Annotated[date | None, Query()] = None,
        zone_id: Annotated[str | None, Query(pattern=r"^Z\d{2}$")] = None,
        rider_type: Annotated[RiderType | None, Query()] = None,
    ) -> list[dict[str, Any]]:
        where_sql, params = _analytics_filter(start_date, end_date, zone_id, rider_type)
        return _rows(
            resolved_db_path,
            f"""
            SELECT
                start_zone_id AS zone_id,
                start_zone_name AS zone_name,
                count(*)::INTEGER AS trips,
                round(sum(total_amount), 2) AS revenue,
                round(avg(total_amount), 2) AS revenue_per_trip,
                round(avg(duration_min), 2) AS avg_duration_min
            FROM silver.trip_enriched
            WHERE {where_sql}
            GROUP BY 1, 2
            ORDER BY revenue DESC
            """,
            params,
        )

    @app.get("/api/stations")
    def stations(
        limit: Annotated[int, Query(ge=1, le=50)] = 12,
        start_date: Annotated[date | None, Query()] = None,
        end_date: Annotated[date | None, Query()] = None,
        zone_id: Annotated[str | None, Query(pattern=r"^Z\d{2}$")] = None,
        rider_type: Annotated[RiderType | None, Query()] = None,
    ) -> list[dict[str, Any]]:
        where_sql, params = _analytics_filter(start_date, end_date, zone_id, rider_type)
        return _rows(
            resolved_db_path,
            f"""
            SELECT
                start_station_id AS station_id,
                start_station_name AS station_name,
                start_zone_name AS zone_name,
                count(*)::INTEGER AS departures,
                round(sum(total_amount), 2) AS revenue,
                round(avg(CASE WHEN rider_type = 'member' THEN 1.0 ELSE 0.0 END), 4)
                    AS member_trip_share
            FROM silver.trip_enriched
            WHERE {where_sql}
            GROUP BY 1, 2, 3
            ORDER BY departures DESC
            LIMIT ?
            """,
            [*params, limit],
        )

    @app.get("/api/quality")
    def quality(
        run_id: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    ) -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT check_name, status, observed_value, threshold, details, checked_at
            FROM ops.quality_results
            WHERE run_id = coalesce(
                ?,
                (
                    SELECT run_id
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                )
            )
            ORDER BY check_name
            """,
            [run_id],
        )

    @app.get("/api/pipeline-runs")
    def pipeline_runs() -> list[dict[str, Any]]:
        runs = _rows(
            resolved_db_path,
            """
            SELECT
                run_id,
                started_at,
                ended_at,
                status,
                days_requested,
                seed,
                data_interval_start,
                data_interval_end,
                source_mode,
                replay_of_run_id,
                parent_run_id,
                raw_trips,
                silver_trips,
                rejected_trips,
                gold_hourly_rows,
                quality_passed,
                quality_failed,
                published_at,
                contract_version,
                input_set_sha256,
                output_set_sha256,
                error_message
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 8
            """,
        )
        return [_public_run(run) for run in runs]

    @app.get("/api/pipeline-runs/{run_id}")
    def pipeline_run_detail(
        run_id: Annotated[
            str,
            PathParam(pattern=r"^\d{14}-[0-9a-f]{8}$"),
        ],
    ) -> dict[str, Any]:
        return _pipeline_run_detail(resolved_db_path, run_id)

    @app.get("/api/drift")
    def drift(
        run_id: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    ) -> dict[str, Any]:
        selected = _single_row(
            resolved_db_path,
            """
            SELECT run_id, contract_version
            FROM ops.pipeline_runs
            WHERE run_id = coalesce(
                ?,
                (
                    SELECT run_id
                    FROM ops.pipeline_runs
                    ORDER BY started_at DESC
                    LIMIT 1
                )
            )
            """,
            [run_id],
        )
        results = _rows(
            resolved_db_path,
            """
            SELECT
                run_id,
                baseline_run_id,
                metric_name,
                current_value,
                baseline_value,
                delta,
                delta_kind,
                threshold,
                status,
                checked_at
            FROM ops.drift_results
            WHERE run_id = ?
            ORDER BY CASE WHEN status <> 'pass' THEN 0 ELSE 1 END, metric_name
            """,
            [selected["run_id"]],
        )
        failed_metrics = sum(item["status"] != "pass" for item in results)
        return {
            "run_id": selected["run_id"],
            "baseline_run_id": results[0]["baseline_run_id"] if results else None,
            "contract_version": selected["contract_version"],
            "status": "no_baseline" if not results else "breached" if failed_metrics else "stable",
            "checked_metrics": len(results),
            "failed_metrics": failed_metrics,
            "results": results,
        }

    @app.get("/api/lineage")
    def lineage() -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT source_node, target_node, transform_type
            FROM gold.lineage_edges
            """,
        )

    @app.get("/api/filters")
    def filters() -> dict[str, Any]:
        window = _single_row(
            resolved_db_path,
            """
            SELECT min(trip_date) AS start_date, max(trip_date) AS end_date
            FROM silver.trip_enriched
            """,
        )
        zones = _rows(
            resolved_db_path,
            """
            SELECT DISTINCT start_zone_id AS zone_id, start_zone_name AS zone_name
            FROM silver.trip_enriched
            ORDER BY zone_name
            """,
        )
        return {
            **window,
            "zones": zones,
            "rider_types": ["member", "casual", "corporate"],
        }

    @app.get("/api/ingest-files")
    def ingest_files(
        run_id: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    ) -> list[dict[str, Any]]:
        return _rows(
            resolved_db_path,
            """
            SELECT
                run_id,
                dataset_name,
                source_file,
                file_sha256,
                file_bytes,
                row_count,
                loaded_at
            FROM ops.ingest_files
            WHERE run_id = coalesce(
                ?,
                (
                    SELECT run_id
                    FROM ops.pipeline_runs
                    WHERE published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT 1
                )
            )
            ORDER BY dataset_name
            """,
            [run_id],
        )

    return app


def _pipeline_run_detail(db_path: Path, run_id: str) -> dict[str, Any]:
    run_rows = _rows(
        db_path,
        """
        WITH selected AS (
            SELECT *
            FROM ops.pipeline_runs
            WHERE run_id = ?
        ), previous AS (
            SELECT candidate.*
            FROM ops.pipeline_runs candidate, selected current_run
            WHERE candidate.published_at IS NOT NULL
              AND candidate.started_at < current_run.started_at
            ORDER BY candidate.published_at DESC
            LIMIT 1
        ), current_manifest AS (
            SELECT coalesce(sum(row_count), 0) AS source_rows,
                   coalesce(sum(file_bytes), 0) AS source_bytes
            FROM ops.ingest_files
            WHERE run_id = ?
        ), previous_manifest AS (
            SELECT coalesce(sum(row_count), 0) AS source_rows,
                   coalesce(sum(file_bytes), 0) AS source_bytes
            FROM ops.ingest_files
            WHERE run_id = (SELECT run_id FROM previous)
        )
        SELECT
            selected.*,
            round(date_diff('millisecond', selected.started_at, selected.ended_at) / 1000.0, 3)
                AS runtime_seconds,
            selected.run_id = (
                SELECT run_id
                FROM ops.pipeline_runs
                WHERE published_at IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 1
            ) AS is_current_snapshot,
            previous.run_id AS previous_published_run_id,
            current_manifest.source_rows,
            current_manifest.source_bytes,
            current_manifest.source_rows - previous_manifest.source_rows AS source_rows_delta,
            current_manifest.source_bytes - previous_manifest.source_bytes AS source_bytes_delta,
            selected.silver_trips - previous.silver_trips AS silver_trips_delta,
            selected.rejected_trips - previous.rejected_trips AS rejected_trips_delta,
            selected.gold_hourly_rows - previous.gold_hourly_rows AS gold_hourly_rows_delta,
            round(
                (selected.silver_trips - previous.silver_trips) * 100.0
                    / nullif(previous.silver_trips, 0),
                2
            ) AS silver_trips_delta_percent
        FROM selected
        CROSS JOIN current_manifest
        CROSS JOIN previous_manifest
        LEFT JOIN previous ON true
        """,
        [run_id, run_id],
    )
    if not run_rows:
        raise HTTPException(status_code=404, detail=f"Pipeline run not found: {run_id}")

    quality = _rows(
        db_path,
        """
        SELECT check_name, status, observed_value, threshold, details, checked_at
        FROM ops.quality_results
        WHERE run_id = ?
        ORDER BY check_name
        """,
        [run_id],
    )
    manifests = _rows(
        db_path,
        """
        SELECT dataset_name, source_file, file_sha256, file_bytes, row_count, loaded_at
        FROM ops.ingest_files
        WHERE run_id = ?
        ORDER BY dataset_name
        """,
        [run_id],
    )
    profiles = _rows(
        db_path,
        """
        SELECT metric_name, metric_value, unit, recorded_at
        FROM ops.dataset_profiles
        WHERE run_id = ?
        ORDER BY metric_name
        """,
        [run_id],
    )
    fingerprints = _rows(
        db_path,
        """
        SELECT relation_name, row_count, fingerprint_sha256, recorded_at
        FROM ops.relation_fingerprints
        WHERE run_id = ?
        ORDER BY relation_name
        """,
        [run_id],
    )
    drift = _rows(
        db_path,
        """
        SELECT
            baseline_run_id,
            metric_name,
            current_value,
            baseline_value,
            delta,
            delta_kind,
            threshold,
            status,
            checked_at
        FROM ops.drift_results
        WHERE run_id = ?
        ORDER BY CASE WHEN status <> 'pass' THEN 0 ELSE 1 END, metric_name
        """,
        [run_id],
    )
    run = _public_run(run_rows[0])
    return {
        "run": run,
        "quality": quality,
        "manifests": manifests,
        "profiles": profiles,
        "fingerprints": fingerprints,
        "drift": drift,
    }


def _public_run(run: dict[str, Any]) -> dict[str, Any]:
    public_run = dict(run)
    error_message = public_run.pop("error_message", None)
    public_run["failure_summary"] = _failure_summary(
        status=public_run.get("status"),
        error_message=error_message,
    )
    return public_run


def _failure_summary(status: Any, error_message: Any) -> str | None:
    normalized_status = str(status or "").lower()
    normalized_error = str(error_message or "").lower()
    if normalized_status == "failed_quality" or normalized_error.startswith(
        "quality checks failed:"
    ):
        return "Quality gate failure"
    if "replay output fingerprint mismatch" in normalized_error or (
        "replay" in normalized_error and "not equivalent" in normalized_error
    ):
        return "Replay output equivalence failure"
    if normalized_error and (
        "replay" in normalized_error
        or "recorded input manifest fingerprint" in normalized_error
        or "recorded configuration hash" in normalized_error
    ):
        return "Replay source or input integrity failure"
    if normalized_status == "failed" or normalized_error:
        return "Run failed; review operator logs"
    return None


def _cors_origins() -> list[str]:
    raw_origins = os.getenv(
        "METROPULSE_CORS_ORIGINS",
        "http://127.0.0.1:5173,http://localhost:5173",
    )
    origins: list[str] = []
    for raw_origin in raw_origins.split(","):
        origin = raw_origin.strip()
        if not origin:
            continue
        parsed = urlsplit(origin)
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"Invalid CORS origin: {origin}") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"Invalid CORS origin: {origin}")
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        origins.append(f"{parsed.scheme}://{host}{f':{port}' if port else ''}")
    if not origins:
        raise ValueError("METROPULSE_CORS_ORIGINS must contain at least one origin.")
    return origins


def _analytics_filter(
    start_date: date | None,
    end_date: date | None,
    zone_id: str | None,
    rider_type: RiderType | None,
) -> tuple[str, list[Any]]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422,
            detail="start_date must be on or before end_date.",
        )

    clauses = ["1 = 1"]
    params: list[Any] = []
    if start_date:
        clauses.append("trip_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("trip_date <= ?")
        params.append(end_date)
    if zone_id:
        clauses.append("start_zone_id = ?")
        params.append(zone_id)
    if rider_type:
        clauses.append("rider_type = ?")
        params.append(rider_type)
    return " AND ".join(clauses), params


def _readiness(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {
            "status": "not_ready",
            "warehouse_found": False,
            "missing_tables": sorted(REQUIRED_TABLES),
            "snapshot_run_id": None,
        }

    try:
        con = connect(db_path, read_only=True)
    except (duckdb.Error, OSError):
        return {
            "status": "not_ready",
            "warehouse_found": True,
            "missing_tables": sorted(REQUIRED_TABLES),
            "snapshot_run_id": None,
            "detail": "Warehouse inspection failed.",
        }
    try:
        rows = con.execute(
            """
            SELECT table_schema || '.' || table_name
            FROM information_schema.tables
            """
        ).fetchall()
        present = {row[0] for row in rows}
        missing = sorted(REQUIRED_TABLES - present)
        snapshot = None
        if "ops.pipeline_runs" in present:
            snapshot = con.execute(
                """
                SELECT run_id
                FROM ops.pipeline_runs
                WHERE published_at IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 1
                """
            ).fetchone()
        return {
            "status": "ready" if not missing and snapshot else "not_ready",
            "warehouse_found": True,
            "missing_tables": missing,
            "snapshot_run_id": snapshot[0] if snapshot else None,
        }
    except (duckdb.Error, OSError):
        return {
            "status": "not_ready",
            "warehouse_found": True,
            "missing_tables": sorted(REQUIRED_TABLES),
            "snapshot_run_id": None,
            "detail": "Warehouse inspection failed.",
        }
    finally:
        con.close()


def _rows(db_path: Path, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    if not db_path.exists():
        raise HTTPException(
            status_code=503,
            detail="Warehouse not found. Run `metropulse run` first.",
        )
    try:
        con = connect(db_path, read_only=True)
    except (duckdb.Error, OSError) as exc:
        raise HTTPException(status_code=503, detail="Warehouse is unavailable.") from exc
    try:
        cursor = con.execute(sql, params or [])
        columns = [description[0] for description in cursor.description]
        return [_serialize(dict(zip(columns, row, strict=True))) for row in cursor.fetchall()]
    except (duckdb.Error, OSError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Published warehouse snapshot is not ready.",
        ) from exc
    finally:
        con.close()


def _single_row(
    db_path: Path,
    sql: str,
    params: list[Any] | None = None,
) -> dict[str, Any]:
    rows = _rows(db_path, sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail="No rows found. Run `metropulse run` first.")
    return rows[0]


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    serialized = {}
    for key, value in row.items():
        serialized[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return serialized
