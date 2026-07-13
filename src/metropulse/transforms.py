from __future__ import annotations

import duckdb


def build_silver(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE silver.stations AS
        SELECT
            station_id::VARCHAR AS station_id,
            station_name::VARCHAR AS station_name,
            zone_id::VARCHAR AS zone_id,
            zone_name::VARCHAR AS zone_name,
            zone_type::VARCHAR AS zone_type,
            latitude::DOUBLE AS latitude,
            longitude::DOUBLE AS longitude,
            capacity::INTEGER AS capacity,
            CAST(opened_at AS DATE) AS opened_at,
            loaded_at,
            source_file,
            source_run_id
        FROM bronze.stations
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.weather_hourly AS
        SELECT
            CAST(observed_at AS TIMESTAMP) AS observed_at,
            temp_c::DOUBLE AS temp_c,
            rain_mm::DOUBLE AS rain_mm,
            wind_kph::DOUBLE AS wind_kph,
            condition::VARCHAR AS condition,
            loaded_at,
            source_file,
            source_run_id
        FROM bronze.weather
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE trip_contract_stage AS
        WITH typed AS (
            SELECT
                nullif(trim(trip_id::VARCHAR), '') AS trip_id,
                try_cast(started_at AS TIMESTAMP) AS started_at,
                try_cast(ended_at AS TIMESTAMP) AS ended_at,
                nullif(trim(start_station_id::VARCHAR), '') AS start_station_id,
                nullif(trim(end_station_id::VARCHAR), '') AS end_station_id,
                lower(trim(rider_type::VARCHAR)) AS rider_type,
                lower(trim(vehicle_type::VARCHAR)) AS vehicle_type,
                try_cast(distance_km AS DOUBLE) AS distance_km,
                try_cast(duration_min AS DOUBLE) AS duration_min,
                loaded_at,
                source_file,
                source_run_id
            FROM bronze.trips
        )
        SELECT
            *,
            CASE
                WHEN trip_id IS NULL THEN 'missing_trip_id'
                WHEN started_at IS NULL OR ended_at IS NULL THEN 'invalid_timestamp'
                WHEN ended_at <= started_at THEN 'non_positive_trip_window'
                WHEN start_station_id IS NULL OR end_station_id IS NULL
                    THEN 'missing_station_id'
                WHEN rider_type NOT IN ('member', 'casual', 'corporate')
                    THEN 'invalid_rider_type'
                WHEN vehicle_type NOT IN ('e-bike', 'classic-bike', 'scooter')
                    THEN 'invalid_vehicle_type'
                WHEN distance_km IS NULL OR NOT isfinite(distance_km) OR distance_km <= 0
                    THEN 'invalid_distance'
                WHEN duration_min IS NULL OR NOT isfinite(duration_min) OR duration_min <= 0
                    THEN 'invalid_duration'
                ELSE NULL
            END AS rejection_reason
        FROM typed
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.trip_rejections AS
        SELECT
            trip_id,
            started_at,
            ended_at,
            start_station_id,
            end_station_id,
            rider_type,
            vehicle_type,
            distance_km,
            duration_min,
            rejection_reason,
            loaded_at,
            source_file,
            source_run_id
        FROM trip_contract_stage
        WHERE rejection_reason IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.trips AS
        SELECT
            trip_id,
            started_at,
            ended_at,
            start_station_id,
            end_station_id,
            rider_type,
            vehicle_type,
            distance_km,
            duration_min,
            loaded_at,
            source_file,
            source_run_id
        FROM trip_contract_stage
        WHERE rejection_reason IS NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE payment_contract_stage AS
        WITH typed AS (
            SELECT
                nullif(trim(payment_id::VARCHAR), '') AS payment_id,
                nullif(trim(trip_id::VARCHAR), '') AS trip_id,
                try_cast(fare_amount AS DOUBLE) AS fare_amount,
                try_cast(discount_amount AS DOUBLE) AS discount_amount,
                try_cast(tax_amount AS DOUBLE) AS tax_amount,
                try_cast(total_amount AS DOUBLE) AS total_amount,
                lower(trim(payment_method::VARCHAR)) AS payment_method,
                try_cast(paid_at AS TIMESTAMP) AS paid_at,
                loaded_at,
                source_file,
                source_run_id
            FROM bronze.payments
        ), counted AS (
            SELECT
                *,
                count(*) OVER (PARTITION BY payment_id) AS payment_id_count,
                count(*) OVER (PARTITION BY trip_id) AS trip_id_count
            FROM typed
        )
        SELECT
            *,
            CASE
                WHEN payment_id IS NULL THEN 'missing_payment_id'
                WHEN trip_id IS NULL THEN 'missing_trip_id'
                WHEN payment_id_count > 1 THEN 'duplicate_payment_id'
                WHEN trip_id_count > 1 THEN 'duplicate_trip_payment'
                WHEN fare_amount IS NULL OR discount_amount IS NULL
                    OR tax_amount IS NULL OR total_amount IS NULL
                    THEN 'invalid_amount'
                WHEN NOT isfinite(fare_amount) OR NOT isfinite(discount_amount)
                    OR NOT isfinite(tax_amount) OR NOT isfinite(total_amount)
                    THEN 'invalid_amount'
                WHEN fare_amount < 0 OR discount_amount < 0 OR tax_amount < 0
                    OR total_amount < 0 THEN 'negative_amount'
                WHEN abs(total_amount - (fare_amount - discount_amount + tax_amount)) > 0.02
                    THEN 'amount_reconciliation_failed'
                WHEN payment_method NOT IN ('card', 'wallet', 'invoice')
                    THEN 'invalid_payment_method'
                WHEN paid_at IS NULL THEN 'invalid_paid_at'
                ELSE NULL
            END AS rejection_reason
        FROM counted
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.payment_rejections AS
        SELECT
            payment_id,
            trip_id,
            fare_amount,
            discount_amount,
            tax_amount,
            total_amount,
            payment_method,
            paid_at,
            rejection_reason,
            loaded_at,
            source_file,
            source_run_id
        FROM payment_contract_stage
        WHERE rejection_reason IS NOT NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.payments AS
        SELECT
            payment_id,
            trip_id,
            fare_amount,
            discount_amount,
            tax_amount,
            total_amount,
            payment_method,
            paid_at,
            loaded_at,
            source_file,
            source_run_id
        FROM payment_contract_stage
        WHERE rejection_reason IS NULL
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.trip_enriched AS
        SELECT
            t.trip_id,
            t.started_at,
            t.ended_at,
            date_trunc('hour', t.started_at) AS trip_hour,
            CAST(t.started_at AS DATE) AS trip_date,
            t.start_station_id,
            start_station.station_name AS start_station_name,
            start_station.zone_id AS start_zone_id,
            start_station.zone_name AS start_zone_name,
            start_station.zone_type AS start_zone_type,
            t.end_station_id,
            end_station.station_name AS end_station_name,
            end_station.zone_id AS end_zone_id,
            end_station.zone_name AS end_zone_name,
            t.rider_type,
            t.vehicle_type,
            t.distance_km,
            t.duration_min,
            p.fare_amount,
            p.discount_amount,
            p.tax_amount,
            p.total_amount,
            p.payment_method,
            w.temp_c,
            w.rain_mm,
            w.wind_kph,
            w.condition,
            CASE WHEN p.payment_id IS NOT NULL THEN true ELSE false END AS has_payment
        FROM silver.trips t
        LEFT JOIN silver.stations start_station
            ON t.start_station_id = start_station.station_id
        LEFT JOIN silver.stations end_station
            ON t.end_station_id = end_station.station_id
        LEFT JOIN silver.payments p
            ON t.trip_id = p.trip_id
        LEFT JOIN silver.weather_hourly w
            ON date_trunc('hour', t.started_at) = w.observed_at
        """
    )


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE gold.hourly_mobility AS
        SELECT
            trip_hour,
            start_zone_id AS zone_id,
            start_zone_name AS zone_name,
            rider_type,
            count(*)::INTEGER AS trips,
            round(sum(total_amount), 2) AS revenue,
            round(avg(duration_min), 2) AS avg_duration_min,
            round(avg(distance_km), 2) AS avg_distance_km,
            round(avg(temp_c), 2) AS avg_temp_c,
            round(sum(CASE WHEN condition = 'rain' THEN 1 ELSE 0 END) * 1.0 / count(*), 4)
                AS rainy_trip_share
        FROM silver.trip_enriched
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 4
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.daily_station_performance AS
        SELECT
            trip_date,
            start_station_id AS station_id,
            start_station_name AS station_name,
            start_zone_name AS zone_name,
            count(*)::INTEGER AS departures,
            round(sum(total_amount), 2) AS revenue,
            round(avg(duration_min), 2) AS avg_duration_min,
            round(avg(distance_km), 2) AS avg_distance_km,
            round(sum(CASE WHEN rider_type = 'member' THEN 1 ELSE 0 END) * 1.0 / count(*), 4)
                AS member_trip_share
        FROM silver.trip_enriched
        GROUP BY 1, 2, 3, 4
        ORDER BY 1 DESC, departures DESC
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.revenue_by_zone AS
        SELECT
            start_zone_id AS zone_id,
            start_zone_name AS zone_name,
            count(*)::INTEGER AS trips,
            round(sum(total_amount), 2) AS revenue,
            round(avg(total_amount), 2) AS revenue_per_trip,
            round(avg(duration_min), 2) AS avg_duration_min
        FROM silver.trip_enriched
        GROUP BY 1, 2
        ORDER BY revenue DESC
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.dashboard_summary AS
        SELECT
            count(*)::INTEGER AS total_trips,
            round(sum(total_amount), 2) AS total_revenue,
            count(DISTINCT start_station_id)::INTEGER AS active_stations,
            min(trip_date) AS first_trip_date,
            max(trip_date) AS last_trip_date,
            round(avg(duration_min), 2) AS avg_duration_min,
            round(sum(CASE WHEN has_payment THEN 1 ELSE 0 END) * 1.0 / count(*), 4)
                AS payment_match_rate,
            (SELECT count(*)::INTEGER FROM silver.trip_rejections) AS rejected_trips,
            (SELECT count(*)::INTEGER FROM silver.payment_rejections) AS rejected_payments
        FROM silver.trip_enriched
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gold.lineage_edges AS
        SELECT * FROM (
            VALUES
                ('raw.trips.csv', 'bronze.trips', 'ingest_csv'),
                ('raw.payments.csv', 'bronze.payments', 'ingest_csv'),
                ('raw.stations.csv', 'bronze.stations', 'ingest_csv'),
                ('raw.weather.csv', 'bronze.weather', 'ingest_csv'),
                ('bronze.*', 'silver.trip_enriched', 'clean_join_cast'),
                ('silver.trip_enriched', 'gold.hourly_mobility', 'aggregate'),
                ('silver.trip_enriched', 'gold.daily_station_performance', 'aggregate'),
                ('silver.trip_enriched', 'gold.revenue_by_zone', 'aggregate'),
                ('gold.*', 'FastAPI + dashboard', 'serve')
        ) AS t(source_node, target_node, transform_type)
        """
    )
