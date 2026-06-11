# Architecture Notes

## Design Goal

MetroPulse Lakehouse is intentionally compact but production-shaped. The goal is to show that the author understands the lifecycle of analytical data: source generation, ingestion, cleaning, modeling, validation, serving, and presentation.

## Data Flow

1. `metropulse run` creates deterministic raw CSV files for trips, payments, stations, and weather.
2. The ingestion step loads each file into a DuckDB bronze table and adds source metadata.
3. Silver transformations type-cast fields, remove invalid rows, and join dimensions.
4. Gold transformations build analytics-ready marts for dashboard and API use cases.
5. Quality checks run after the marts are built and persist check results in `ops.quality_results`.
6. FastAPI exposes gold and ops tables as stable JSON endpoints.
7. The dependency-free web dashboard renders metrics from those endpoints.

## Tooling Choices

- DuckDB gives a fast local analytical warehouse without requiring cloud credentials.
- Python keeps ingestion and orchestration easy to read.
- SQL handles transformations where Data Engineers are expected to be strongest.
- FastAPI provides a realistic serving layer and a clean contract for the dashboard.
- A dependency-free HTML/CSS/JS dashboard makes the final output demo-friendly without hiding the data work.
- Pytest verifies the pipeline and API end to end.

## How This Would Scale

The same project structure can map to a larger stack:

- Raw CSVs -> object storage such as S3 or GCS
- DuckDB -> Snowflake, BigQuery, Redshift, Databricks, or Postgres
- Python CLI -> Airflow, Dagster, or Prefect DAGs
- SQL modules -> dbt models and dbt tests
- Local quality checks -> Great Expectations, Soda, or dbt-expectations
- Local API/dashboard -> deployed data product or semantic layer

## Reliability Notes

The project avoids depending on external APIs so that interview demos and CI runs are stable. The generated data is deterministic by seed, but still realistic enough to contain daily, hourly, rider-type, weather, and zone-level variation.
