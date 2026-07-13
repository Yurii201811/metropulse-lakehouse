from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import duckdb

CONTRACT_VERSION = "snapshot-v1"


@dataclass(frozen=True, slots=True)
class DatasetProfile:
    metric_name: str
    metric_value: float | None
    unit: str


@dataclass(frozen=True, slots=True)
class RelationFingerprint:
    relation_name: str
    row_count: int
    fingerprint_sha256: str


@dataclass(frozen=True, slots=True)
class DriftResult:
    run_id: str
    baseline_run_id: str
    metric_name: str
    current_value: float | None
    baseline_value: float | None
    delta: float | None
    delta_kind: Literal["relative", "absolute"]
    threshold: float
    status: Literal["pass", "fail"]


@dataclass(frozen=True, slots=True)
class _DriftRule:
    delta_kind: Literal["relative", "absolute"]
    threshold: float


_DRIFT_RULES = {
    "daily_trip_rate": _DriftRule("relative", 0.25),
    "revenue_per_trip": _DriftRule("relative", 0.20),
    "avg_duration_min": _DriftRule("relative", 0.20),
    "avg_distance_km": _DriftRule("relative", 0.20),
    "payment_match_rate": _DriftRule("absolute", 0.02),
    "member_share": _DriftRule("absolute", 0.10),
    "casual_share": _DriftRule("absolute", 0.10),
    "corporate_share": _DriftRule("absolute", 0.05),
    "active_stations": _DriftRule("relative", 0.10),
}

_FINGERPRINT_SPECS = {
    "silver.trips": (
        "trip_id, started_at, ended_at, start_station_id, end_station_id, "
        "rider_type, vehicle_type, distance_km, duration_min"
    ),
    "silver.payments": (
        "payment_id, trip_id, fare_amount, discount_amount, tax_amount, total_amount, "
        "payment_method, paid_at"
    ),
    "silver.trip_enriched": (
        "trip_id, started_at, ended_at, trip_hour, trip_date, start_station_id, "
        "start_station_name, start_zone_id, start_zone_name, start_zone_type, "
        "end_station_id, end_station_name, end_zone_id, end_zone_name, rider_type, "
        "vehicle_type, distance_km, duration_min, fare_amount, discount_amount, "
        "tax_amount, total_amount, payment_method, temp_c, rain_mm, wind_kph, condition, "
        "has_payment"
    ),
    "gold.hourly_mobility": (
        "trip_hour, zone_id, zone_name, rider_type, trips, revenue, avg_duration_min, "
        "avg_distance_km, avg_temp_c, rainy_trip_share"
    ),
    "gold.daily_station_performance": (
        "trip_date, station_id, station_name, zone_name, departures, revenue, "
        "avg_duration_min, avg_distance_km, member_trip_share"
    ),
    "gold.revenue_by_zone": (
        "zone_id, zone_name, trips, revenue, revenue_per_trip, avg_duration_min"
    ),
}


def build_dataset_profiles(
    con: duckdb.DuckDBPyConnection,
    days: int,
) -> list[DatasetProfile]:
    if days <= 0:
        raise ValueError("days must be greater than zero")

    (
        total_trips,
        total_revenue,
        revenue_per_trip,
        avg_duration_min,
        avg_distance_km,
        payment_match_rate,
        member_share,
        casual_share,
        corporate_share,
    ) = con.execute(
        """
        SELECT
            count(*)::BIGINT AS total_trips,
            coalesce(sum(total_amount), 0)::DOUBLE AS total_revenue,
            coalesce(sum(total_amount), 0)::DOUBLE / nullif(count(*), 0)
                AS revenue_per_trip,
            avg(duration_min)::DOUBLE AS avg_duration_min,
            avg(distance_km)::DOUBLE AS avg_distance_km,
            avg(CASE WHEN has_payment THEN 1.0 ELSE 0.0 END)::DOUBLE
                AS payment_match_rate,
            avg(CASE WHEN rider_type = 'member' THEN 1.0 ELSE 0.0 END)::DOUBLE
                AS member_share,
            avg(CASE WHEN rider_type = 'casual' THEN 1.0 ELSE 0.0 END)::DOUBLE
                AS casual_share,
            avg(CASE WHEN rider_type = 'corporate' THEN 1.0 ELSE 0.0 END)::DOUBLE
                AS corporate_share
        FROM silver.trip_enriched
        """
    ).fetchone()
    active_stations = con.execute(
        """
        SELECT count(DISTINCT start_station_id)::BIGINT
        FROM silver.trips
        """
    ).fetchone()[0]
    gold_hourly_rows = con.execute("SELECT count(*)::BIGINT FROM gold.hourly_mobility").fetchone()[
        0
    ]

    total_trips_value = float(total_trips)
    return [
        DatasetProfile("total_trips", total_trips_value, "trips"),
        DatasetProfile("daily_trip_rate", total_trips_value / days, "trips_per_day"),
        DatasetProfile("total_revenue", _as_optional_float(total_revenue), "currency"),
        DatasetProfile(
            "revenue_per_trip", _as_optional_float(revenue_per_trip), "currency_per_trip"
        ),
        DatasetProfile("avg_duration_min", _as_optional_float(avg_duration_min), "minutes"),
        DatasetProfile("avg_distance_km", _as_optional_float(avg_distance_km), "km"),
        DatasetProfile("payment_match_rate", _as_optional_float(payment_match_rate), "ratio"),
        DatasetProfile("member_share", _as_optional_float(member_share), "ratio"),
        DatasetProfile("casual_share", _as_optional_float(casual_share), "ratio"),
        DatasetProfile("corporate_share", _as_optional_float(corporate_share), "ratio"),
        DatasetProfile("active_stations", float(active_stations), "stations"),
        DatasetProfile("gold_hourly_rows", float(gold_hourly_rows), "rows"),
    ]


def build_relation_fingerprints(
    con: duckdb.DuckDBPyConnection,
) -> list[RelationFingerprint]:
    fingerprints = []
    for relation_name, columns in _FINGERPRINT_SPECS.items():
        rows = con.execute(f"SELECT {columns} FROM {relation_name} ORDER BY {columns}").fetchall()
        serialized = json.dumps(
            [[_canonical_value(value) for value in row] for row in rows],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        fingerprints.append(
            RelationFingerprint(
                relation_name=relation_name,
                row_count=len(rows),
                fingerprint_sha256=hashlib.sha256(serialized).hexdigest(),
            )
        )
    return fingerprints


def compatible_baseline_run_id(
    con: duckdb.DuckDBPyConnection,
    current_run_id: str,
    days: int,
    contract_version: str,
) -> str | None:
    row = con.execute(
        """
        SELECT run_id
        FROM ops.pipeline_runs
        WHERE run_id <> ?
          AND days_requested = ?
          AND contract_version = ?
          AND published_at IS NOT NULL
        ORDER BY published_at DESC, ended_at DESC, started_at DESC
        LIMIT 1
        """,
        [current_run_id, days, contract_version],
    ).fetchone()
    return None if row is None else str(row[0])


def compare_profiles(
    run_id: str,
    baseline_run_id: str,
    current: list[DatasetProfile],
    baseline: list[DatasetProfile],
) -> list[DriftResult]:
    current_by_name = {profile.metric_name: profile.metric_value for profile in current}
    baseline_by_name = {profile.metric_name: profile.metric_value for profile in baseline}
    results = []

    for metric_name, rule in _DRIFT_RULES.items():
        current_value = current_by_name.get(metric_name)
        baseline_value = baseline_by_name.get(metric_name)
        delta = _calculate_delta(current_value, baseline_value, rule.delta_kind)
        results.append(
            DriftResult(
                run_id=run_id,
                baseline_run_id=baseline_run_id,
                metric_name=metric_name,
                current_value=current_value,
                baseline_value=baseline_value,
                delta=delta,
                delta_kind=rule.delta_kind,
                threshold=rule.threshold,
                status="pass" if delta is not None and abs(delta) <= rule.threshold else "fail",
            )
        )
    return results


def persist_observability(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    profiles: list[DatasetProfile],
    fingerprints: list[RelationFingerprint],
    drift_results: list[DriftResult],
) -> None:
    persisted_at = datetime.now(UTC)
    con.execute("DELETE FROM ops.dataset_profiles WHERE run_id = ?", [run_id])
    if profiles:
        con.executemany(
            """
            INSERT INTO ops.dataset_profiles (
                run_id, metric_name, metric_value, unit, recorded_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    profile.metric_name,
                    profile.metric_value,
                    profile.unit,
                    persisted_at,
                )
                for profile in profiles
            ],
        )

    con.execute("DELETE FROM ops.relation_fingerprints WHERE run_id = ?", [run_id])
    if fingerprints:
        con.executemany(
            """
            INSERT INTO ops.relation_fingerprints (
                run_id, relation_name, row_count, fingerprint_sha256, recorded_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    fingerprint.relation_name,
                    fingerprint.row_count,
                    fingerprint.fingerprint_sha256,
                    persisted_at,
                )
                for fingerprint in fingerprints
            ],
        )

    con.execute("DELETE FROM ops.drift_results WHERE run_id = ?", [run_id])
    if drift_results:
        con.executemany(
            """
            INSERT INTO ops.drift_results (
                run_id, baseline_run_id, metric_name, current_value, baseline_value,
                delta, delta_kind, threshold, status, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    result.run_id,
                    result.baseline_run_id,
                    result.metric_name,
                    result.current_value,
                    result.baseline_value,
                    result.delta,
                    result.delta_kind,
                    result.threshold,
                    result.status,
                    persisted_at,
                )
                for result in drift_results
            ],
        )


def output_set_sha256(fingerprints: list[RelationFingerprint]) -> str:
    payload = "\n".join(
        f"{item.relation_name}:{item.row_count}:{item.fingerprint_sha256}"
        for item in sorted(fingerprints, key=lambda item: item.relation_name)
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _as_optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _calculate_delta(
    current_value: float | None,
    baseline_value: float | None,
    delta_kind: Literal["relative", "absolute"],
) -> float | None:
    if current_value is None or baseline_value is None:
        return None
    if not math.isfinite(current_value) or not math.isfinite(baseline_value):
        return None
    if delta_kind == "absolute":
        return current_value - baseline_value
    if baseline_value == 0:
        return 0.0 if current_value == 0 else None
    return (current_value - baseline_value) / abs(baseline_value)


def _canonical_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        if value == 0:
            return 0.0
    return value
