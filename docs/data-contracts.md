# Data contracts

MetroPulse separates **candidate data** from the **published snapshot**. A generated run writes source files under `data/raw/runs/<run_id>/`; a replay verifies one prior run's immutable files and stages the same bytes under its own run ID. Both copy the published DuckDB file into an isolated candidate, evaluate that candidate inside one transaction, and atomically replace the published file only after the run state is complete.

## Publication policy

- `fail_on_quality=true` is the default.
- A non-blocking publisher lock prevents two candidate files from racing to replace the published path.
- Any failed check, including cross-snapshot drift, rolls back candidate bronze, silver, gold, rejection, manifest, profile, fingerprint, drift, and quality-table changes.
- Failed-run metadata and all evidence produced before the failure are then persisted in `ops` outside the rolled-back transaction.
- The previously published file remains available to API readers throughout candidate work.
- Generated run-scoped raw files are treated as immutable and are never overwritten. A replay may intentionally load those same bytes only after revalidating the complete manifest, paths, sizes, and SHA-256 values, then copying them into a private run directory. The replay records its own manifest against the staged sources.
- `fail_on_quality=false` is an explicit local override. It publishes the candidate with status `failed_quality`; the API and console surface that degraded status.
- A replay manifest failure occurs before the data load. It records a failed run and error while leaving the published snapshot and the original source run's manifest untouched.

The live `silver.*_rejections` tables therefore describe the latest **published** snapshot. For a rolled-back candidate, use its run-detail API response or `ops.quality_results`, `ops.ingest_files`, `ops.dataset_profiles`, `ops.relation_fingerprints`, `ops.drift_results`, and run-scoped source files for investigation.

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

## Snapshot and replay contract

`metropulse run --as-of YYYY-MM-DD` declares the inclusive interval end; omitting it uses yesterday, and a future value is rejected. With days, seed, interval end, and `snapshot-v1` fixed, generated source bytes are reproducible across execution dates.

`metropulse replay --run-id RUN_ID` inherits the original run's days, seed, interval, configuration hash, and contract. Before loading, replay requires exactly four manifest datasets (`trips`, `payments`, `stations`, and `weather`), verifies that their aggregate manifest matches the recorded input fingerprint, confines every resolved path to `data/raw/`, validates each file's recorded byte size and SHA-256, and stages verified copies under the new run ID. It recomputes the input fingerprint after ingestion and requires the rebuilt output fingerprint to exactly match the source run before publication. A run created before the replay contract or under a different contract version is not eligible.

`ops.pipeline_runs` records:

| Field | Meaning |
| --- | --- |
| `data_interval_start`, `data_interval_end` | Inclusive snapshot window |
| `source_mode` | `generated` or `replay` |
| `parent_run_id` | Published snapshot that was current when this candidate began |
| `replay_of_run_id` | Direct source run for a replay; otherwise `NULL` |
| `code_version`, `contract_version` | MetroPulse package and snapshot contract versions |
| `config_sha256` | Hash of days, seed, interval end, and contract |
| `input_set_sha256` | Hash summarizing the sorted source manifests |
| `output_set_sha256` | Hash summarizing the six relation fingerprints |

## Publication checks

The current candidate runs 13 checks. Every outcome records its observed value and threshold in `ops.quality_results`; a missing observation is stored as `NULL` and fails safely.

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
| `data_interval_end_gap_days` | Latest accepted trip date equals the declared interval end (`= 0` days) |
| `cross_snapshot_drift` | Threshold breaches across nine compatible-baseline comparisons `= 0` |

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

The input-set hash is computed from the four sorted manifest records, including dataset, SHA-256, byte size, and row count.

## Profiles, fingerprints, and drift

Each evaluated snapshot records 12 profiles in `ops.dataset_profiles`: `total_trips`, `daily_trip_rate`, `total_revenue`, `revenue_per_trip`, `avg_duration_min`, `avg_distance_km`, `payment_match_rate`, `member_share`, `casual_share`, `corporate_share`, `active_stations`, and `gold_hourly_rows`.

Six fingerprints in `ops.relation_fingerprints` cover `silver.trips`, `silver.payments`, `silver.trip_enriched`, `gold.hourly_mobility`, `gold.daily_station_performance`, and `gold.revenue_by_zone`. Their ordered serialization contains business columns only, excluding ingestion metadata so an exact replay has the same output fingerprint despite a different run ID or load timestamp.

The latest published run with the same requested days and `snapshot-v1` contract is the compatible baseline. Nine profile metrics are thresholded:

| Metric | Delta | Maximum absolute change |
| --- | --- | ---: |
| `daily_trip_rate` | Relative | 25% |
| `revenue_per_trip` | Relative | 20% |
| `avg_duration_min` | Relative | 20% |
| `avg_distance_km` | Relative | 20% |
| `payment_match_rate` | Absolute | 0.02 |
| `member_share` | Absolute | 0.10 |
| `casual_share` | Absolute | 0.10 |
| `corporate_share` | Absolute | 0.05 |
| `active_stations` | Relative | 10% |

With no compatible baseline, no drift rows are emitted and `cross_snapshot_drift` passes with a `no baseline` explanation. The thresholds are illustrative portfolio policy, not calibrated production SLAs.

## API validation

- Date filters use ISO `YYYY-MM-DD`; `start_date` must not be after `end_date`.
- `zone_id` must match `Z` plus two digits.
- `rider_type` is limited to the trip contract enum.
- Station `limit` is bounded from 1 through 50.
- `/api/quality`, `/api/ingest-files`, and `/api/drift` accept `run_id` for historical evidence.
- `/api/pipeline-runs/{run_id}` requires the MetroPulse run-ID format and returns metadata, checks, manifests, profiles, fingerprints, and drift for that run.
- A live process with no complete published warehouse returns `503` from `/ready` and data endpoints while `/health` remains `200`.
