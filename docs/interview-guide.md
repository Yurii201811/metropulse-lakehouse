# Interview Guide

## One-Minute Pitch

MetroPulse Lakehouse is a local end-to-end data platform for urban mobility analytics. It ingests raw trip, payment, station, and weather feeds; builds bronze, silver, and gold warehouse layers in DuckDB; runs quality gates; exposes analytics through FastAPI; and visualizes the final marts in a dependency-free web dashboard.

## What To Demo

1. Run `metropulse run --days 45`.
2. Show the generated raw files in `data/raw/`.
3. Open DuckDB and show `bronze`, `silver`, `gold`, and `ops` schemas.
4. Run `pytest -q`.
5. Start FastAPI and open `/docs`.
6. Start the dashboard and show the KPI cards, time series, quality checks, table, and lineage map.

## Interview Talking Points

- Bronze stores source-shaped data and lineage metadata.
- Silver applies type casting, invalid-row filtering, and joins.
- Gold is organized around use cases: time-series analytics, station performance, zone revenue, and dashboard KPIs.
- Quality checks are persisted so they can be audited, trended, and exposed to downstream users.
- The dashboard does not read CSVs directly; it consumes a stable API backed by warehouse tables.
- The project is deterministic for tests, but realistic enough to explain operational tradeoffs.

## Possible Extensions

- Add dbt models and dbt tests.
- Replace the CLI with Dagster or Airflow.
- Add incremental loads with partition tracking.
- Deploy the API and dashboard.
- Add Docker Compose.
- Add a Great Expectations or Soda quality suite.
- Add OpenTelemetry-style runtime metrics.
