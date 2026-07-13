# MetroPulse Lakehouse

[![CI](https://github.com/Yurii201811/metropulse-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Yurii201811/metropulse-lakehouse/actions/workflows/ci.yml)
[![Python 3.11–3.14](https://img.shields.io/badge/Python-3.11%E2%80%933.14-2563eb)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-2563eb.svg)](LICENSE)

MetroPulse is a compact, production-shaped data platform for an urban mobility operator. It generates deterministic trip, payment, station, and weather feeds; records file-level provenance; builds bronze, silver, and gold models in DuckDB; enforces row-level contracts and publication gates; exposes the published snapshot through FastAPI; and renders it in a dependency-free operations console.

![MetroPulse operations console](docs/dashboard-screenshot.png)

## What makes it production-shaped

- **Non-blocking atomic publication:** each run builds a copy-on-write DuckDB candidate under a single-publisher lock, commits internally, then atomically swaps the file. API readers stay on the previous file until that swap.
- **Explicit contracts:** invalid trips and payments receive a rejection reason instead of disappearing during a filter.
- **Verifiable inputs:** every run writes source files to `data/raw/runs/<run_id>/` and records path, SHA-256, byte size, and row count in `ops.ingest_files`.
- **Cardinality protection:** duplicate trip payments are quarantined before the fact join, preventing revenue and row multiplication.
- **Operational API:** separate liveness and readiness endpoints, bounded parameters, typed filters, snapshot metadata, and safe 503 responses.
- **Resilient console:** filters, truthful hourly axes, accessible data tables, actual lineage edges, a command menu, responsive station cards, and partial-failure recovery.
- **Repeatable verification:** 13 Python tests, 14 frontend/server tests, Ruff, an isolated 45-day pipeline run, and Python 3.11–3.14 CI coverage.

## Architecture

```mermaid
flowchart LR
  RAW["Run-scoped CSV sources"] --> MANIFEST["SHA-256 source manifest"]
  RAW --> CANDIDATE["Copy-on-write DuckDB candidate"]
  CANDIDATE --> BRONZE["Bronze source-shaped tables"]
  BRONZE --> CONTRACTS["Trip and payment contracts"]
  CONTRACTS --> REJECTS["Typed rejection tables"]
  CONTRACTS --> SILVER["Silver accepted and enriched facts"]
  SILVER --> GOLD["Gold analytics marts"]
  GOLD --> QUALITY["12 publication checks"]
  QUALITY -->|"pass"| COMMIT["Commit candidate transaction"]
  QUALITY -->|"fail"| ROLLBACK["Rollback candidate snapshot"]
  COMMIT --> SWAP["Atomic published-file swap"]
  ROLLBACK --> SWAP
  SWAP --> API["FastAPI"]
  API --> UI["Operations console"]
  MANIFEST --> OPS["Ops evidence"]
  ROLLBACK --> OPS
```

The generated source files live outside the DuckDB transaction so failed-run evidence remains inspectable. Warehouse work happens in a separate candidate file. After success, or after rollback plus failed-run evidence persistence, one atomic file replacement publishes the new state; short-lived API readers continue opening the prior file until that replacement.

## Quick start

Requirements:

- macOS or Linux (publication uses POSIX file-lock and atomic-rename semantics)
- Python 3.11–3.14
- Node.js 24 LTS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
metropulse run --days 45 --seed 20260611
metropulse status
npm --prefix apps/dashboard run build
```

Start the two local services in separate terminals:

```bash
metropulse serve-api --host 127.0.0.1 --port 8000
```

```bash
npm --prefix apps/dashboard run dev
```

Open [http://127.0.0.1:5173](http://127.0.0.1:5173). API documentation is available at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

The dashboard has no runtime packages or bundler. Node runs the build, tests, and local static server.

## Reference run

The 45-day run verified on **2026-07-13** with seed `20260611` produced:

| Evidence | Result |
| --- | ---: |
| Accepted trips | 12,034 |
| Matched revenue | $64,194.47 |
| Active stations | 28 |
| Weather hours | 1,080 |
| Gold hourly rows | 7,731 |
| Source manifests | 4 |
| Quality checks | 12 / 12 passing |
| Trip / payment rejects | 0 / 0 |

Because the generator ends at yesterday, dates move with the day a run is executed; for a fixed execution date and window, the seed is deterministic.

## Commands

```bash
make setup      # install the editable package and build the dashboard
make pipeline   # publish a deterministic 45-day snapshot
make status     # show the latest run and source hashes
make test       # run Python tests
make lint       # run Ruff
make verify     # isolated pipeline + CLI + Python + frontend verification
make api        # serve FastAPI on 127.0.0.1:8000
make dashboard  # build and serve the console on 127.0.0.1:5173
```

`bash scripts/verify.sh` uses a temporary project root by default, so verification does not overwrite your working warehouse. Set `METROPULSE_VERIFY_ROOT` to preserve its generated evidence.

## Data model

| Layer | Main relations | Responsibility |
| --- | --- | --- |
| Bronze | `bronze.trips`, `payments`, `stations`, `weather` | Source-shaped CSV loads plus `loaded_at`, `source_file`, and `source_run_id` |
| Silver | `silver.trips`, `payments`, `stations`, `weather_hourly`, `trip_enriched` | Typed accepted records and analytical joins |
| Rejections | `silver.trip_rejections`, `payment_rejections` | Contract failures with explicit rejection reasons |
| Gold | `hourly_mobility`, `daily_station_performance`, `revenue_by_zone`, `dashboard_summary`, `lineage_edges` | Consumer-shaped aggregates and lineage |
| Ops | `pipeline_runs`, `quality_results`, `ingest_files` | Run state, publication timestamps, gates, errors, and source evidence |

The complete contracts and rejection reasons are documented in [docs/data-contracts.md](docs/data-contracts.md).

## API surface

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Process liveness; does not expose local filesystem paths |
| `GET /ready` | Published-warehouse readiness and missing-table diagnostics |
| `GET /api/summary` | Filtered KPIs plus snapshot and rejection metadata |
| `GET /api/timeseries` | Filtered hourly trips and revenue |
| `GET /api/stations`, `/api/zones` | Filtered station and zone aggregates |
| `GET /api/filters` | Available date, zone, and rider dimensions |
| `GET /api/pipeline-runs`, `/api/quality` | Operational run and gate history |
| `GET /api/ingest-files`, `/api/lineage` | Source manifests and source-to-target edges |

Analytics endpoints accept `start_date`, `end_date`, `zone_id`, and `rider_type`. Station results additionally enforce `1 <= limit <= 50`.

## Configuration

Copy `.env.example` if you want a shell environment file, then load it explicitly:

```bash
cp .env.example .env
set -a; source .env; set +a
```

- `METROPULSE_PROJECT_ROOT` controls the data root.
- `METROPULSE_DB_PATH` overrides the DuckDB path.
- `METROPULSE_API_BASE_URL` is embedded in `dist/config.js` by the dashboard build.
- `METROPULSE_CORS_ORIGINS` is a comma-separated list of exact dashboard origins allowed by the API; paths, credentials, queries, and fragments are rejected.

The application intentionally does not parse `.env` files automatically.

## Repository layout

```text
.
├── .github/               # CI and dependency-update policy
├── apps/dashboard/        # HTML, token-driven CSS, ES modules, tests, local server
├── data/raw/runs/         # Generated run-scoped source files (ignored)
├── data/warehouse/        # Generated DuckDB snapshot (ignored)
├── docs/                  # Architecture, contracts, screenshots, interview material
├── scripts/verify.sh      # Isolated end-to-end verifier
├── src/metropulse/        # Generator, ingestion, SQL models, quality, API, CLI
├── tests/                 # Pipeline, rollback, quality, and API tests
└── tokens.css             # Hallmark Cobalt design tokens
```

## Further reading

- [Architecture and failure semantics](docs/architecture.md)
- [Data contracts](docs/data-contracts.md)
- [Portfolio case study](docs/portfolio-case-study.md)
- [Interview guide](docs/interview-guide.md)
- [Mobile console screenshot](docs/dashboard-mobile-screenshot.png)
- [Original dashboard concept](docs/dashboard-concept.png)

## License

[MIT](LICENSE) © Yurii Bakurov
