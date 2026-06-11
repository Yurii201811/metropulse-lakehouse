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
            source_file
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
            source_file
        FROM bronze.weather
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.trips AS
        SELECT
            trip_id::VARCHAR AS trip_id,
            CAST(started_at AS TIMESTAMP) AS started_at,
            CAST(ended_at AS TIMESTAMP) AS ended_at,
            start_station_id::VARCHAR AS start_station_id,
            end_station_id::VARCHAR AS end_station_id,
            rider_type::VARCHAR AS rider_type,
            vehicle_type::VARCHAR AS vehicle_type,
            distance_km::DOUBLE AS distance_km,
            duration_min::DOUBLE AS duration_min,
            loaded_at,
            source_file
        FROM bronze.trips
        WHERE
            trip_id IS NOT NULL
            AND CAST(ended_at AS TIMESTAMP) > CAST(started_at AS TIMESTAMP)
            AND distance_km::DOUBLE > 0
            AND duration_min::DOUBLE > 0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE silver.payments AS
        SELECT
            payment_id::VARCHAR AS payment_id,
            trip_id::VARCHAR AS trip_id,
            fare_amount::DOUBLE AS fare_amount,
            discount_amount::DOUBLE AS discount_amount,
            tax_amount::DOUBLE AS tax_amount,
            total_amount::DOUBLE AS total_amount,
            payment_method::VARCHAR AS payment_method,
            CAST(paid_at AS TIMESTAMP) AS paid_at,
            loaded_at,
            source_file
        FROM bronze.payments
        WHERE total_amount::DOUBLE >= 0
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
                AS payment_match_rate
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
