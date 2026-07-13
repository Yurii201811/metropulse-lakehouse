from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from metropulse.observability import (
    CONTRACT_VERSION,
    DatasetProfile,
    DriftResult,
    RelationFingerprint,
    build_dataset_profiles,
    build_relation_fingerprints,
    compare_profiles,
    compatible_baseline_run_id,
    output_set_sha256,
    persist_observability,
)


def test_build_dataset_profiles_computes_operational_metrics() -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE SCHEMA silver; CREATE SCHEMA gold")
        con.execute(
            """
            CREATE TABLE silver.trips AS
            SELECT * FROM (VALUES
                ('t1', 's1'),
                ('t2', 's2'),
                ('t3', 's1')
            ) trips(trip_id, start_station_id);

            CREATE TABLE silver.trip_enriched AS
            SELECT * FROM (VALUES
                ('t1', 'member', 10.0, 10.0, 2.0, true),
                ('t2', 'casual', NULL, 20.0, 4.0, false),
                ('t3', 'corporate', 20.0, 30.0, 6.0, true)
            ) enriched(
                trip_id, rider_type, total_amount, duration_min, distance_km, has_payment
            );

            CREATE TABLE gold.hourly_mobility AS
            SELECT * FROM (VALUES (1), (2)) hourly(row_id);
            """
        )

        profiles = build_dataset_profiles(con, days=2)
    finally:
        con.close()

    assert [profile.metric_name for profile in profiles] == [
        "total_trips",
        "daily_trip_rate",
        "total_revenue",
        "revenue_per_trip",
        "avg_duration_min",
        "avg_distance_km",
        "payment_match_rate",
        "member_share",
        "casual_share",
        "corporate_share",
        "active_stations",
        "gold_hourly_rows",
    ]
    values = {profile.metric_name: profile.metric_value for profile in profiles}
    assert values == pytest.approx(
        {
            "total_trips": 3.0,
            "daily_trip_rate": 1.5,
            "total_revenue": 30.0,
            "revenue_per_trip": 10.0,
            "avg_duration_min": 20.0,
            "avg_distance_km": 4.0,
            "payment_match_rate": 2 / 3,
            "member_share": 1 / 3,
            "casual_share": 1 / 3,
            "corporate_share": 1 / 3,
            "active_stations": 2.0,
            "gold_hourly_rows": 2.0,
        }
    )


def test_build_dataset_profiles_rejects_non_positive_window() -> None:
    con = duckdb.connect()
    try:
        with pytest.raises(ValueError, match="days must be greater than zero"):
            build_dataset_profiles(con, days=0)
    finally:
        con.close()


def test_relation_fingerprints_ignore_ingestion_metadata_and_detect_business_changes() -> None:
    con = duckdb.connect()
    try:
        _create_fingerprint_relations(con)
        initial = build_relation_fingerprints(con)

        con.execute(
            """
            UPDATE silver.trips
            SET loaded_at = TIMESTAMP '2030-01-01', source_run_id = 'replay';
            UPDATE silver.payments
            SET loaded_at = TIMESTAMP '2030-01-01', source_run_id = 'replay';
            """
        )
        replay = build_relation_fingerprints(con)
        assert replay == initial

        con.execute("UPDATE silver.trips SET distance_km = distance_km + 1")
        changed = build_relation_fingerprints(con)
    finally:
        con.close()

    assert len(initial) == 6
    initial_by_name = {item.relation_name: item for item in initial}
    changed_by_name = {item.relation_name: item for item in changed}
    assert (
        changed_by_name["silver.trips"].fingerprint_sha256
        != initial_by_name["silver.trips"].fingerprint_sha256
    )
    assert changed_by_name["gold.hourly_mobility"] == initial_by_name["gold.hourly_mobility"]
    assert output_set_sha256(initial) == output_set_sha256(list(reversed(initial)))


def test_compatible_baseline_selects_latest_published_matching_contract() -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE SCHEMA ops")
        con.execute(
            """
            CREATE TABLE ops.pipeline_runs (
                run_id VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                published_at TIMESTAMP,
                days_requested INTEGER,
                contract_version VARCHAR
            )
            """
        )
        con.executemany(
            "INSERT INTO ops.pipeline_runs VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    "old",
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 1),
                    7,
                    CONTRACT_VERSION,
                ),
                (
                    "latest",
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 2),
                    7,
                    CONTRACT_VERSION,
                ),
                (
                    "wrong-days",
                    datetime(2026, 1, 3),
                    datetime(2026, 1, 3),
                    datetime(2026, 1, 3),
                    8,
                    CONTRACT_VERSION,
                ),
                (
                    "wrong-contract",
                    datetime(2026, 1, 4),
                    datetime(2026, 1, 4),
                    datetime(2026, 1, 4),
                    7,
                    "snapshot-v0",
                ),
                (
                    "unpublished",
                    datetime(2026, 1, 5),
                    datetime(2026, 1, 5),
                    None,
                    7,
                    CONTRACT_VERSION,
                ),
                ("current", datetime(2026, 1, 6), None, None, 7, CONTRACT_VERSION),
            ],
        )

        baseline = compatible_baseline_run_id(con, "current", 7, CONTRACT_VERSION)
        no_baseline = compatible_baseline_run_id(con, "current", 30, CONTRACT_VERSION)
    finally:
        con.close()

    assert baseline == "latest"
    assert no_baseline is None


def test_compare_profiles_applies_relative_and_absolute_thresholds() -> None:
    baseline = _drift_profiles()
    current_values = {
        "daily_trip_rate": 126.0,
        "revenue_per_trip": 12.0,
        "avg_duration_min": 15.0,
        "avg_distance_km": None,
        "payment_match_rate": 0.94,
        "member_share": 0.60,
        "casual_share": 0.31,
        "corporate_share": 0.11,
        "active_stations": 89.0,
    }
    current = [
        DatasetProfile(profile.metric_name, current_values[profile.metric_name], profile.unit)
        for profile in baseline
    ]

    results = compare_profiles("current", "baseline", current, baseline)
    by_name = {result.metric_name: result for result in results}

    assert len(results) == 9
    assert by_name["revenue_per_trip"].delta == pytest.approx(0.20)
    assert by_name["revenue_per_trip"].status == "pass"
    assert by_name["daily_trip_rate"].delta == pytest.approx(0.26)
    assert by_name["daily_trip_rate"].status == "fail"
    assert by_name["payment_match_rate"].delta_kind == "absolute"
    assert by_name["payment_match_rate"].delta == pytest.approx(-0.04)
    assert by_name["payment_match_rate"].status == "fail"
    assert by_name["avg_distance_km"].delta is None
    assert by_name["avg_distance_km"].status == "fail"
    assert by_name["active_stations"].delta == pytest.approx(-0.11)
    assert by_name["active_stations"].status == "fail"


def test_persist_observability_replaces_run_evidence_and_allows_empty_drift() -> None:
    con = duckdb.connect()
    try:
        _create_observability_tables(con)
        profiles = [DatasetProfile("total_trips", 3.0, "trips")]
        fingerprints = [RelationFingerprint("silver.trips", 3, "a" * 64)]
        drift = [
            DriftResult(
                run_id="run-1",
                baseline_run_id="run-0",
                metric_name="daily_trip_rate",
                current_value=3.0,
                baseline_value=2.0,
                delta=0.5,
                delta_kind="relative",
                threshold=0.25,
                status="fail",
            )
        ]

        persist_observability(con, "run-1", profiles, fingerprints, drift)
        first_timestamps = con.execute(
            """
            SELECT
                (SELECT recorded_at FROM ops.dataset_profiles WHERE run_id = 'run-1'),
                (SELECT recorded_at FROM ops.relation_fingerprints WHERE run_id = 'run-1'),
                (SELECT checked_at FROM ops.drift_results WHERE run_id = 'run-1')
            """
        ).fetchone()
        persist_observability(
            con,
            "run-1",
            [DatasetProfile("total_trips", 4.0, "trips")],
            [RelationFingerprint("silver.trips", 4, "b" * 64)],
            [],
        )
        evidence = con.execute(
            """
            SELECT
                (SELECT metric_value FROM ops.dataset_profiles WHERE run_id = 'run-1'),
                (SELECT row_count FROM ops.relation_fingerprints WHERE run_id = 'run-1'),
                (SELECT count(*) FROM ops.drift_results WHERE run_id = 'run-1')
            """
        ).fetchone()
    finally:
        con.close()

    assert evidence == (4.0, 4, 0)
    assert first_timestamps[0] == first_timestamps[1] == first_timestamps[2]
    assert first_timestamps[0] != datetime(2000, 1, 1)


def _drift_profiles() -> list[DatasetProfile]:
    return [
        DatasetProfile("daily_trip_rate", 100.0, "trips_per_day"),
        DatasetProfile("revenue_per_trip", 10.0, "currency_per_trip"),
        DatasetProfile("avg_duration_min", 15.0, "minutes"),
        DatasetProfile("avg_distance_km", 4.0, "km"),
        DatasetProfile("payment_match_rate", 0.98, "ratio"),
        DatasetProfile("member_share", 0.50, "ratio"),
        DatasetProfile("casual_share", 0.40, "ratio"),
        DatasetProfile("corporate_share", 0.10, "ratio"),
        DatasetProfile("active_stations", 100.0, "stations"),
    ]


def _create_observability_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE SCHEMA ops;
        CREATE TABLE ops.dataset_profiles (
            run_id VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            unit VARCHAR,
            recorded_at TIMESTAMP DEFAULT TIMESTAMP '2000-01-01'
        );
        CREATE TABLE ops.relation_fingerprints (
            run_id VARCHAR,
            relation_name VARCHAR,
            row_count BIGINT,
            fingerprint_sha256 VARCHAR,
            recorded_at TIMESTAMP DEFAULT TIMESTAMP '2000-01-01'
        );
        CREATE TABLE ops.drift_results (
            run_id VARCHAR,
            baseline_run_id VARCHAR,
            metric_name VARCHAR,
            current_value DOUBLE,
            baseline_value DOUBLE,
            delta DOUBLE,
            delta_kind VARCHAR,
            threshold DOUBLE,
            status VARCHAR,
            checked_at TIMESTAMP DEFAULT TIMESTAMP '2000-01-01'
        );
        """
    )


def _create_fingerprint_relations(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE SCHEMA silver;
        CREATE SCHEMA gold;

        CREATE TABLE silver.trips AS SELECT
            't1'::VARCHAR AS trip_id,
            TIMESTAMP '2026-01-01 08:00:00' AS started_at,
            TIMESTAMP '2026-01-01 08:10:00' AS ended_at,
            's1'::VARCHAR AS start_station_id,
            's2'::VARCHAR AS end_station_id,
            'member'::VARCHAR AS rider_type,
            'e-bike'::VARCHAR AS vehicle_type,
            2.0::DOUBLE AS distance_km,
            10.0::DOUBLE AS duration_min,
            TIMESTAMP '2026-01-01 09:00:00' AS loaded_at,
            'trips.csv'::VARCHAR AS source_file,
            'run-1'::VARCHAR AS source_run_id;

        CREATE TABLE silver.payments AS SELECT
            'p1'::VARCHAR AS payment_id,
            't1'::VARCHAR AS trip_id,
            8.0::DOUBLE AS fare_amount,
            0.0::DOUBLE AS discount_amount,
            2.0::DOUBLE AS tax_amount,
            10.0::DOUBLE AS total_amount,
            'card'::VARCHAR AS payment_method,
            TIMESTAMP '2026-01-01 08:11:00' AS paid_at,
            TIMESTAMP '2026-01-01 09:00:00' AS loaded_at,
            'payments.csv'::VARCHAR AS source_file,
            'run-1'::VARCHAR AS source_run_id;

        CREATE TABLE silver.trip_enriched AS SELECT
            't1'::VARCHAR AS trip_id,
            TIMESTAMP '2026-01-01 08:00:00' AS started_at,
            TIMESTAMP '2026-01-01 08:10:00' AS ended_at,
            TIMESTAMP '2026-01-01 08:00:00' AS trip_hour,
            DATE '2026-01-01' AS trip_date,
            's1'::VARCHAR AS start_station_id,
            'Central'::VARCHAR AS start_station_name,
            'z1'::VARCHAR AS start_zone_id,
            'Core'::VARCHAR AS start_zone_name,
            'urban'::VARCHAR AS start_zone_type,
            's2'::VARCHAR AS end_station_id,
            'North'::VARCHAR AS end_station_name,
            'z2'::VARCHAR AS end_zone_id,
            'North'::VARCHAR AS end_zone_name,
            'member'::VARCHAR AS rider_type,
            'e-bike'::VARCHAR AS vehicle_type,
            2.0::DOUBLE AS distance_km,
            10.0::DOUBLE AS duration_min,
            8.0::DOUBLE AS fare_amount,
            0.0::DOUBLE AS discount_amount,
            2.0::DOUBLE AS tax_amount,
            10.0::DOUBLE AS total_amount,
            'card'::VARCHAR AS payment_method,
            18.0::DOUBLE AS temp_c,
            0.0::DOUBLE AS rain_mm,
            5.0::DOUBLE AS wind_kph,
            'clear'::VARCHAR AS condition,
            true AS has_payment;

        CREATE TABLE gold.hourly_mobility AS SELECT
            TIMESTAMP '2026-01-01 08:00:00' AS trip_hour,
            'z1'::VARCHAR AS zone_id,
            'Core'::VARCHAR AS zone_name,
            'member'::VARCHAR AS rider_type,
            1::INTEGER AS trips,
            10.0::DOUBLE AS revenue,
            10.0::DOUBLE AS avg_duration_min,
            2.0::DOUBLE AS avg_distance_km,
            18.0::DOUBLE AS avg_temp_c,
            0.0::DOUBLE AS rainy_trip_share;

        CREATE TABLE gold.daily_station_performance AS SELECT
            DATE '2026-01-01' AS trip_date,
            's1'::VARCHAR AS station_id,
            'Central'::VARCHAR AS station_name,
            'Core'::VARCHAR AS zone_name,
            1::INTEGER AS departures,
            10.0::DOUBLE AS revenue,
            10.0::DOUBLE AS avg_duration_min,
            2.0::DOUBLE AS avg_distance_km,
            1.0::DOUBLE AS member_trip_share;

        CREATE TABLE gold.revenue_by_zone AS SELECT
            'z1'::VARCHAR AS zone_id,
            'Core'::VARCHAR AS zone_name,
            1::INTEGER AS trips,
            10.0::DOUBLE AS revenue,
            10.0::DOUBLE AS revenue_per_trip,
            10.0::DOUBLE AS avg_duration_min;
        """
    )
