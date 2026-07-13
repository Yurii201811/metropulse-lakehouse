# Architecture

## Design goal

MetroPulse is intentionally local and compact, but its boundaries mirror a larger analytical platform: dated and replayable inputs, source-shaped landing tables, explicit contracts, consumer-oriented marts, drift-aware publication gates, an operational API, and a UI that can degrade by data product instead of failing as one page.

## Run lifecycle

```mermaid
sequenceDiagram
  participant CLI as "metropulse run / replay"
  participant FS as "Run-scoped CSV directory"
  participant DB as "Copy-on-write DuckDB candidate"
  participant OPS as "Ops evidence"
  participant PUB as "Published DuckDB path"
  participant API as "Read-only API requests"

  alt generated snapshot
    CLI->>FS: Generate four feeds for an inclusive --as-of date
  else replay snapshot
    CLI->>OPS: Resolve source run and snapshot-v1 configuration
    CLI->>FS: Verify four paths, sizes, and SHA-256 digests
  end
  CLI->>DB: Begin transaction
  CLI->>DB: Replace bronze candidate tables
  CLI->>DB: Build contracts, silver facts, gold marts
  CLI->>DB: Build 12 profiles and 6 relation fingerprints
  CLI->>OPS: Compare 9 drift metrics with compatible baseline
  CLI->>DB: Evaluate 13 quality checks
  API->>PUB: Continue reading prior file during candidate work
  alt all gates pass
    CLI->>DB: Persist manifests, checks, profiles, fingerprints, and drift
    CLI->>DB: Commit
    CLI->>PUB: Atomic file replacement
    API->>PUB: Open new snapshot
  else a gate fails
    CLI->>DB: Roll back candidate tables
    CLI->>OPS: Persist failed run, checks, manifests, profiles, hashes, and drift
    CLI->>PUB: Atomic evidence-only file replacement
    API->>PUB: Open prior snapshot plus new failure evidence
  end
```

Raw generation precedes the database transaction, and each generated run receives a unique `data/raw/runs/<run_id>/` directory. A replay verifies that the original manifest contains exactly `trips`, `payments`, `stations`, and `weather`, that every path stays inside the raw-data root, and that aggregate, per-file size, and SHA-256 fingerprints still match. It stages those bytes under the new run ID, then rechecks the aggregate input fingerprint after ingestion. A non-blocking publisher lock permits one candidate at a time. The candidate starts as a copy of the published database (or a new database for the first run), so failed inputs remain inspectable and a failed or tampered replay cannot replace a healthy snapshot.

Every run records its inclusive data interval, `generated` or `replay` source mode, latest-published parent, optional direct replay source, package version, `snapshot-v1` contract version, and configuration/input/output SHA-256 values. Generated identity is derived from days, seed, interval end, and contract. Replay inherits those fields from the source run, rejects older or different contracts, and publishes only when both rebuilt input and output fingerprints are exact matches.

## Warehouse responsibilities

### Bronze

Bronze tables mirror each CSV and add `loaded_at`, `source_file`, and `source_run_id`. They are replaced only inside the candidate transaction.

### Silver and contracts

Trip and payment fields use safe casts. Invalid rows receive explicit reasons in `silver.trip_rejections` or `silver.payment_rejections`; accepted rows proceed to `silver.trips` and `silver.payments`. Duplicate payments are removed from the accepted set before `silver.trip_enriched` is built, preserving fact cardinality.

Station and weather dimensions are typed separately. The enriched fact joins start/end stations, one accepted payment, and hourly weather. Cardinality, payment coverage, missing dimensions, and amount reconciliation are publication gates.

### Gold

- `gold.hourly_mobility`: hour × start zone × rider aggregates
- `gold.daily_station_performance`: daily station departures, revenue, and member share
- `gold.revenue_by_zone`: zone revenue and trip economics
- `gold.dashboard_summary`: current snapshot KPIs and published rejection counts
- `gold.lineage_edges`: source, target, and transform relationships rendered by the console

### Ops

- `ops.pipeline_runs`: lifecycle status, interval, source/replay lineage, versions, counts, hashes, errors, and `published_at`
- `ops.quality_results`: observed values and thresholds for every evaluated gate
- `ops.ingest_files`: run-scoped path, SHA-256, bytes, rows, and load time
- `ops.dataset_profiles`: 12 snapshot metrics with units
- `ops.relation_fingerprints`: six row counts and canonical SHA-256 values
- `ops.drift_results`: nine current/baseline comparisons with delta type, threshold, and status

## Snapshot identity and drift

The input-set hash summarizes the sorted four-file manifests. The output-set hash summarizes six relation fingerprints: `silver.trips`, `silver.payments`, `silver.trip_enriched`, `gold.hourly_mobility`, `gold.daily_station_performance`, and `gold.revenue_by_zone`. Each relation fingerprint serializes only ordered business columns; ingestion timestamps, source paths, and `source_run_id` are deliberately excluded so a replay can prove business equivalence under a new run ID.

The 12 profiles cover volume, trip rate, revenue, trip economics, duration, distance, payment coverage, rider mix, active stations, and hourly-mart rows. Nine of those metrics have illustrative relative or absolute thresholds. A candidate is compared with the latest published run having the same requested days and contract. The first compatible snapshot has no baseline and passes `cross_snapshot_drift` explicitly; subsequent candidates fail that gate when any threshold is breached.

## Read consistency

FastAPI opens short-lived read-only DuckDB connections to the published path, while the pipeline writes only its isolated candidate file. The closed candidate is atomically renamed onto the published path after it is complete, so an API request sees either the prior file or the complete replacement without contending with the writer. `/health` reports process liveness; `/ready` separately confirms that required relations and a published run exist.

Analytics filters are applied to `silver.trip_enriched`, not to cached dashboard JSON. Summary, time series, stations, and zones therefore share the same date/zone/rider predicate. Operational endpoints accept explicit run IDs where historical evidence is meaningful: `/api/quality`, `/api/ingest-files`, and `/api/drift`; `/api/pipeline-runs/{run_id}` returns one investigation bundle with metadata, checks, manifests, profiles, fingerprints, and drift.

## Console behavior

The browser requests data products independently. A failure in one endpoint replaces only that panel and raises one concise partial-data notice; successful operational sections remain available. A retry re-requests the failed state without requiring a page reload. The operations area shows drift status and lets an operator select a run to investigate its source/replay lineage and evidence without changing the currently published analytics snapshot.

Trips and revenue use aligned hourly domains but separate labeled scales. The console renders every returned hour, exposes the same values as a table, renders actual lineage edges and transform types, and changes station tables into labeled records at phone widths.

## Tool choices

- **DuckDB:** transactional analytical SQL without cloud credentials
- **Python + Typer:** readable orchestration and a demo-friendly CLI
- **SQL:** visible contract, dimensional, fact, and aggregate logic
- **FastAPI:** typed query validation, OpenAPI, liveness, and readiness
- **HTML/CSS/ES modules:** a transparent UI with no package runtime or bundler
- **Pytest + Node test runner + Ruff:** deterministic data, API, UI-core, and static-server verification

## Production evolution

The boundaries map directly to a larger deployment:

| Local component | Production analogue |
| --- | --- |
| Run-scoped CSV directory | Versioned S3/GCS landing prefix |
| Copy-on-write DuckDB plus file swap | Warehouse staging schema plus atomic swap |
| Python CLI | Dagster, Airflow, or Prefect asset graph |
| SQL functions | dbt models and tests |
| In-process gates | dbt, Soda, or Great Expectations policy |
| Profiles, fingerprints, and `ops` tables | Orchestrator metadata and observability warehouse |
| FastAPI | Versioned data-product or semantic API |
| Local console | Authenticated deployed operations UI |

See [data-contracts.md](data-contracts.md) for row rules, rejection reasons, and thresholds.
