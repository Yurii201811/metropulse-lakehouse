# Portfolio case study

## The problem

An urban mobility team needs trustworthy answers about demand, revenue, station use, and pipeline health. A chart is not enough: operators also need to know which source bytes produced the numbers, which rows were rejected, which checks authorized publication, and whether a new run replaced a healthy snapshot.

MetroPulse models that system end to end on a laptop.

## The solution

The project generates four realistic feeds and stores each run under a unique source directory. DuckDB loads a copy-on-write bronze/silver/gold candidate inside one transaction. Trip and payment contracts split accepted and rejected records; 12 checks then decide whether the candidate can publish. An atomic file replacement keeps FastAPI readers on the prior snapshot during candidate work. The API exposes the published snapshot, operational evidence, filters, and lineage, while a responsive console consumes those products independently.

## Engineering decisions

### Protect readers from failed runs

The pipeline writes an isolated candidate rather than locking the published file. A concurrency test proves API readers can still query the prior run during transformation; failed-quality coverage proves gold KPIs and `published_at` stay on the successful run while failed-run checks and manifests remain auditable.

### Make invalid rows visible

Safe casts and explicit rejection reasons replace silent filtering. Payment duplicate detection happens before the join, so a duplicate cannot multiply facts or revenue.

### Make inputs verifiable

`ops.ingest_files` records the run-scoped path, full SHA-256, byte size, and row count. Tests verify that a later failed run cannot invalidate the manifest of the published snapshot.

### Separate liveness from readiness

`/health` answers whether the API process is alive. `/ready` verifies required warehouse tables and a published run. Data endpoints return a controlled `503` when the snapshot is unavailable.

### Tell the truth in the UI

Trips and revenue share a time domain but have different units and axes. Every returned hour is rendered, and a keyboard-accessible table exposes the values. The lineage section renders source-to-target edges and transformation types from the API rather than a hard-coded diagram.

## Verified outcome

On 2026-07-13, the 45-day reference run produced 12,034 accepted trips, $64,194.47 matched revenue, 28 active stations, four source manifests, 7,731 gold hourly rows, and 12 passing quality checks. The full automated suite includes 13 Python tests and 14 frontend/server tests, plus Ruff and an isolated end-to-end run.

## Demo

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make verify
make pipeline
```

Then start the API and console in separate terminals:

```bash
make api
make dashboard
```

Open `http://127.0.0.1:5173`, filter by zone and rider, open the command menu with `⌘K`/`Ctrl+K`, inspect source hashes and lineage, and run `metropulse status` in the terminal.

## How it scales

- Run-scoped local files become versioned object-storage prefixes.
- The DuckDB transaction becomes a staging schema plus atomic warehouse swap.
- The CLI becomes an Airflow, Dagster, or Prefect asset graph.
- SQL transformations become dbt models with contract tests.
- `ops` evidence feeds alerting, SLAs, and an observability warehouse.
- FastAPI and the console gain authentication, deployment, and telemetry.

The important part is that those production substitutions preserve boundaries already demonstrated here: source identity, row contracts, publication policy, consumer models, and operational evidence.
