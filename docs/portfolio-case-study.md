# Portfolio Case Study

## Project

MetroPulse Lakehouse is a self-contained urban mobility data platform. It turns raw trip, payment, station, and weather feeds into analytics-ready DuckDB marts, validates the data, exposes it through FastAPI, and renders a dashboard for operations and revenue monitoring.

## Problem Framing

Mobility teams need to know whether demand, revenue, station usage, and data quality are healthy. The project models that workflow end to end:

- Source feeds arrive as raw CSV files.
- The warehouse preserves raw lineage in bronze tables.
- Silver tables clean and type the data.
- A joined trip fact table combines stations, payments, and weather.
- Gold marts power API and dashboard use cases.
- Quality results are stored as auditable operational data.

## Data Engineering Highlights

- Deterministic source generation for repeatable local runs and tests
- DuckDB warehouse with `bronze`, `silver`, `gold`, and `ops` schemas
- SQL transformations for dimensional and fact modeling
- Persistent pipeline run metadata in `ops.pipeline_runs`
- Quality checks persisted in `ops.quality_results`
- FastAPI endpoints over gold marts
- Dependency-free dashboard that consumes the API
- Automated pytest coverage for pipeline, API, and quality persistence
- CI workflow that runs the same verification path as a developer laptop

## Demo Script

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
bash scripts/verify.sh
```

Then start the two local services:

```bash
metropulse serve-api --host 127.0.0.1 --port 8000
npm --prefix apps/dashboard run dev
```

Open `http://127.0.0.1:5173`.

## Interview Narrative

The most important design choice is separating concerns. Raw source fidelity lives in bronze. Silver models enforce types, valid timestamps, station references, and payment joins. Gold models are shaped around concrete consumers: hourly mobility trends, station performance, zone revenue, and dashboard KPIs.

The project also treats quality as data. Checks are not just terminal output; each run records pass/fail status and observed values, which makes quality auditable and visible to downstream users.

## What I Would Add In Production

- Object storage for raw files
- Airflow, Dagster, or Prefect orchestration
- dbt models and dbt tests
- Great Expectations or Soda checks
- Incremental loading and backfill controls
- Cloud warehouse deployment
- Observability metrics and alerting
