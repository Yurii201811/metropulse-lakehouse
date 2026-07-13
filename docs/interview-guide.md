# Interview guide

## One-minute pitch

MetroPulse is a local urban-mobility data product built to demonstrate more than a successful SQL query. Each run gets verifiable source files, builds a candidate bronze/silver/gold snapshot in DuckDB, enforces trip and payment contracts, and publishes atomically only after 12 quality gates. FastAPI exposes the published data and operational evidence, while a dependency-free console shows truthful hourly trends, filters, run history, source hashes, and actual lineage edges.

## Five-minute demo

1. Run `metropulse status` and point out `run_id`, `published_at`, accepted/rejected counts, and the four SHA-256 manifest entries.
2. Open `data/raw/runs/<run_id>/` to show that source identity is run-scoped rather than a mutable filename.
3. Show `silver.trip_rejections`, `silver.payment_rejections`, and the accepted `silver.trip_enriched` fact.
4. Open `ops.pipeline_runs`, `ops.quality_results`, and `ops.ingest_files` to explain publication evidence.
5. Start FastAPI and contrast `/health` with `/ready` in `/docs`.
6. Open the console, filter one zone/rider, toggle the hourly table, and inspect the run, quality, manifest, and lineage sections.
7. Run `make verify` to show the same isolated path CI executes.

## Strong talking points

### Why atomic publication?

A multi-table analytical snapshot is only useful if all of its layers agree. MetroPulse builds replacements in a copy-on-write DuckDB candidate under a single-publisher lock. API requests keep opening the prior published file during transformation; after completion, an atomic file replacement exposes the whole candidate at once. On a failed gate, rollback preserves prior gold and silver tables while publishing only the failed-run evidence.

### Why keep failed-run evidence outside the transaction?

Rollback should protect readers, not erase the investigation trail. The run record, observed check values, source paths, and hashes persist after rollback. Run-scoped files ensure a failed candidate also cannot overwrite the inputs referenced by the successful manifest.

### Why rejection tables instead of `WHERE valid`?

Silent filters hide source problems and make reconciliation difficult. Each invalid trip or payment receives a reason. The default policy still blocks publication when rejects exist, but the defect becomes measurable and testable.

### Why detect payment duplicates before the join?

Joining a one-to-many payment defect into trips would inflate fact rows and revenue. The payment contract rejects duplicate payment IDs and duplicate trip-payment relationships first, and a cardinality check verifies `silver.trips == silver.trip_enriched`.

### Why separate liveness and readiness?

An API process can be healthy while its warehouse is missing or incomplete. `/health` stays cheap and process-oriented; `/ready` checks required relations plus a published run. That distinction supports correct orchestration and clearer UI messaging.

### Why does the chart use two panels?

Trips and revenue share timestamps but not units. Separate aligned axes avoid the false comparison created by normalizing both series onto one unlabeled scale. All 1,080 hours in the reference window are rendered and also available as a table.

## Failure scenarios to discuss

- All trip rows fail their contract: freshness becomes `NULL`, the check fails safely, and all 12 outcomes remain recorded.
- One duplicated payment row produces two rejected duplicate rows but does not multiply the trip fact.
- A candidate fails quality after a successful run: `published_at` and dashboard KPIs remain on the successful snapshot.
- The API goes down during a dashboard filter: analytical panels show errors, but already-loaded run and quality evidence remains usable; Retry restores the query.
- The API is live without a warehouse: `/health` is `200`, `/ready` and data endpoints are `503`.

## Honest current limits

- Loads are full-snapshot rather than incremental.
- The generator uses the local execution date, so exact date windows shift over time.
- Ops evidence is local DuckDB metadata, not an external immutable audit store.
- Atomic publication relies on same-filesystem POSIX file replacement and `flock`; the local implementation targets macOS/Linux, while a production deployment would use a warehouse-native lock and swap.
- There is no authentication, deployment, scheduler, or alert delivery.
- Failed-row contents from a rolled-back candidate are reconstructed from its run-scoped source files; the live rejection tables describe the published snapshot.

## Production evolution

- Object storage with versioned prefixes and retention policy
- Airflow/Dagster/Prefect orchestration with backfills and retries
- dbt models, contracts, and environment promotion
- Incremental/partition-aware loading
- Cloud warehouse staging plus atomic schema/table swaps
- OpenTelemetry metrics, alert routing, and data SLAs
- Authenticated API and deployed operations console
