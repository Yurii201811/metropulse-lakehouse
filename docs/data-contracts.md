# Data contracts

MetroPulse separates **candidate data** from the **published snapshot**. Each run generates source files under `data/raw/runs/<run_id>/`, copies the published DuckDB file into an isolated candidate, evaluates that candidate inside one transaction, and atomically replaces the published file only after the run state is complete.

## Publication policy

- `fail_on_quality=true` is the default.
- A non-blocking publisher lock prevents two candidate files from racing to replace the published path.
- Any failed check rolls back candidate bronze, silver, gold, rejection, manifest, and quality-table changes.
- Failed-run metadata, check outcomes, and source manifests are then persisted in `ops` outside the rolled-back transaction.
- The previously published file remains available to API readers throughout candidate work.
- Run-scoped raw files are never reused by a later run, so a manifest path and hash continue to identify the bytes that were evaluated.
- `fail_on_quality=false` is an explicit local override. It publishes the candidate with status `failed_quality`; the API and console surface that degraded status.

The live `silver.*_rejections` tables therefore describe the latest **published** snapshot. For a rolled-back candidate, use its `ops.quality_results`, `ops.ingest_files`, and run-scoped source files for investigation.

## Trip contract

Accepted trip fields are typed into `silver.trips`. A row enters `silver.trip_rejections` with the first matching `rejection_reason` when any rule fails.

| Rule | Accepted value | Rejection reason |
| --- | --- | --- |
| Trip identity | Non-empty `trip_id` | `missing_trip_id` |
| Timestamps | `started_at` and `ended_at` cast to timestamps | `invalid_timestamp` |
| Trip window | `ended_at > started_at` | `non_positive_trip_window` |
| Station identity | Non-empty start and end station IDs | `missing_station_id` |
| Rider type | `member`, `casual`, or `corporate` | `invalid_rider_type` |
| Vehicle type | `e-bike`, `classic-bike`, or `scooter` | `invalid_vehicle_type` |
| Distance | Numeric and greater than zero | `invalid_distance` |
| Duration | Numeric and greater than zero | `invalid_duration` |

Duplicate trip IDs are evaluated as a separate publication check. This keeps the row contract focused on field validity while still preventing a duplicate-key snapshot from publishing.

## Payment contract

Accepted payment fields are typed into `silver.payments`. Duplicate detection happens before joining payments to trips, so one-to-many payment defects cannot multiply the analytical fact table.

| Rule | Accepted value | Rejection reason |
| --- | --- | --- |
| Payment identity | Non-empty `payment_id` | `missing_payment_id` |
| Trip identity | Non-empty `trip_id` | `missing_trip_id` |
| Payment uniqueness | One row per payment ID | `duplicate_payment_id` |
| Trip-payment cardinality | At most one payment row per trip | `duplicate_trip_payment` |
| Amount typing | Fare, discount, tax, and total cast to numbers | `invalid_amount` |
| Amount sign | All amount fields are non-negative | `negative_amount` |
| Reconciliation | `abs(total - (fare - discount + tax)) <= 0.02` | `amount_reconciliation_failed` |
| Method | `card`, `wallet`, or `invoice` | `invalid_payment_method` |
| Paid timestamp | `paid_at` casts to a timestamp | `invalid_paid_at` |

## Publication checks

The current candidate runs 12 checks. Every outcome records its observed value and threshold in `ops.quality_results`; a missing observation is stored as `NULL` and fails safely.

| Check | Gate |
| --- | --- |
| `raw_trips_loaded` | Bronze trip count `>= 1` |
| `silver_trips_loaded` | Accepted trip count `>= 1` |
| `trip_contract_rejections` | Rejection count `= 0` |
| `payment_contract_rejections` | Rejection count `= 0` |
| `duplicate_trip_ids` | Duplicate-key groups `= 0` |
| `invalid_trip_timestamps` | Non-positive accepted windows `= 0` |
| `enriched_trip_cardinality` | `abs(trips - trip_enriched) = 0` |
| `payment_match_rate` | Matched fact share `>= 0.995` |
| `missing_station_references` | Missing start/end dimensions `= 0` |
| `payment_amount_reconciliation` | Accepted amount defects `= 0` |
| `dashboard_summary_ready` | Dashboard trip count `>= 1` |
| `latest_trip_freshness_days` | Latest trip age `<= 5` days |

## Source manifest contract

`ops.ingest_files` has one record per dataset and run:

| Field | Meaning |
| --- | --- |
| `run_id` | Candidate/published run identifier |
| `dataset_name` | `trips`, `payments`, `stations`, or `weather` |
| `source_file` | Path relative to `data/raw/`, including the run directory |
| `file_sha256` | Full SHA-256 digest of the evaluated bytes |
| `file_bytes` | File size at ingestion |
| `row_count` | Bronze rows loaded from the file |
| `loaded_at` | Manifest persistence timestamp |

## API validation

- Date filters use ISO `YYYY-MM-DD`; `start_date` must not be after `end_date`.
- `zone_id` must match `Z` plus two digits.
- `rider_type` is limited to the trip contract enum.
- Station `limit` is bounded from 1 through 50.
- A live process with no complete published warehouse returns `503` from `/ready` and data endpoints while `/health` remains `200`.
