# Portfolio case study

## The problem

An urban mobility team needs trustworthy answers about demand, revenue, station use, and pipeline health. A chart is not enough: operators also need to know which source bytes produced the numbers, which rows were rejected, which checks authorized publication, and whether a new run replaced a healthy snapshot.

MetroPulse models that system end to end on a laptop, including the harder question of whether a later snapshot is equivalent, expectedly different, or unsafe to publish.

## The solution

The project generates four realistic feeds for an explicit inclusive end date and stores each run under a unique source directory. It can also replay a prior run after verifying its exact four-file manifest. DuckDB loads a copy-on-write bronze/silver/gold candidate inside one transaction. Trip and payment contracts split accepted and rejected records; 12 profiles, six output-relation fingerprints, nine drift comparisons, and 13 checks then decide whether the candidate can publish. An atomic file replacement keeps FastAPI readers on the prior snapshot during candidate work. The API exposes the published snapshot and historical run evidence, while a responsive console consumes those products independently.

## Engineering decisions

### Protect readers from failed runs

The pipeline writes an isolated candidate rather than locking the published file. A concurrency test proves API readers can still query the prior run during transformation; failed-quality coverage proves gold KPIs and `published_at` stay on the successful run while failed-run checks and manifests remain auditable.

### Make invalid rows visible

Safe casts and explicit rejection reasons replace silent filtering. Payment duplicate detection happens before the join, so a duplicate cannot multiply facts or revenue.

### Make inputs verifiable

`ops.ingest_files` records the run-scoped path, full SHA-256, byte size, and row count. Tests verify that a later failed run cannot invalidate the manifest of the published snapshot.

### Make snapshots replayable

`metropulse run --as-of` removes the execution date from generated-data identity. `metropulse replay --run-id` inherits the source run's interval and `snapshot-v1` configuration, validates all four manifest paths, sizes, and SHA-256 digests, and stages private copies before loading. It then requires the post-ingest input fingerprint and rebuilt output fingerprint to exactly match the source run. A changed, missing, or non-equivalent replay fails safely and leaves the prior snapshot published.

### Detect plausible but dangerous change

Row contracts catch invalid values, but valid-looking data can still drift. MetroPulse profiles 12 snapshot characteristics and thresholds nine volume, economics, coverage, rider-mix, and station metrics against the latest compatible published run. `cross_snapshot_drift` turns those comparisons into a publication decision, while the underlying per-metric evidence remains available for investigation.

### Prove business equivalence

Six relation fingerprints cover accepted silver facts and consumer gold marts. They hash ordered business columns while ignoring load timestamps, source paths, and run IDs. An exact replay can therefore have a new operational identity and still reproduce the original output fingerprint.

### Separate liveness from readiness

`/health` answers whether the API process is alive. `/ready` verifies required warehouse tables and a published run. Data endpoints return a controlled `503` when the snapshot is unavailable. Run-detail and drift endpoints expose successful and failed historical evidence without pretending that a failed candidate became the live analytics snapshot.

### Tell the truth in the UI

Trips and revenue share a time domain but have different units and axes. Every returned hour is rendered, and a keyboard-accessible table exposes the values. The lineage section renders source-to-target edges and transformation types from the API rather than a hard-coded diagram. Drift and run-investigation views expose current/baseline comparisons, replay lineage, manifests, checks, profiles, and fingerprints.

## Verified outcome

On 2026-07-13, the 45-day reference snapshot ending 2026-07-12 produced 12,034 accepted trips, $64,194.47 matched revenue, 28 active stations, four source manifests, 7,731 gold hourly rows, and 13 passing quality checks. The full automated suite includes 35 Python tests and 17 frontend/server tests, plus Ruff and an isolated end-to-end run.

## Demo

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make verify
metropulse run --days 45 --seed 20260611 --as-of 2026-07-12
```

Then start the API and console in separate terminals:

```bash
make api
make dashboard
```

Open `http://127.0.0.1:5173`, filter by zone and rider, open the command menu with `⌘K`/`Ctrl+K`, and inspect drift, a run's evidence, source hashes, and lineage. In the terminal, use `metropulse status`, then demonstrate equivalence with `metropulse replay --run-id RUN_ID`.

## How it scales

- Run-scoped local files become versioned object-storage prefixes.
- The DuckDB transaction becomes a staging schema plus atomic warehouse swap.
- The CLI becomes an Airflow, Dagster, or Prefect asset graph.
- SQL transformations become dbt models with contract tests.
- `ops` evidence feeds alerting, SLAs, and an observability warehouse.
- FastAPI and the console gain authentication, deployment, and telemetry.

The important part is that those production substitutions preserve boundaries already demonstrated here: source identity, row contracts, publication policy, consumer models, and operational evidence.

## Honest scope

MetroPulse 0.3.0 is still a synthetic full-refresh system on a local DuckDB file. Drift thresholds are illustrative, replay is limited to the same `snapshot-v1` contract, and there is no incremental ingestion, scheduler/orchestrator, container image, cloud deployment, authentication, or alert delivery.
