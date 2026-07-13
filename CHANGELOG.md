# Changelog

All notable project changes are documented here.

## 0.3.0 — 2026-07-13

### Added

- Explicit snapshot dating with `metropulse run --as-of YYYY-MM-DD`
- Manifest-verified replay with `metropulse replay --run-id RUN_ID`, run-private source staging, and mandatory input/output fingerprint equivalence
- Run metadata for data intervals, generated/replay mode, parent and replay links, package and contract versions, and configuration/input/output SHA-256 values
- Twelve dataset profiles, six business-column relation fingerprints, and nine thresholded cross-snapshot drift comparisons
- `data_interval_end_gap_days` and `cross_snapshot_drift` publication gates, bringing the candidate policy to 13 checks
- `GET /api/drift` and `GET /api/pipeline-runs/{run_id}`, plus historical `run_id` filters on quality and ingest-file endpoints
- Console drift status and run-investigation views for manifests, profiles, fingerprints, checks, and replay lineage
- Release-version parity checks plus CI build, metadata validation, clean-wheel installation, and fixed-date pipeline smoke coverage

### Changed

- Package, API, and dashboard version advanced to `0.3.0`
- Deterministic generation now uses days, seed, contract, and an inclusive data-interval end instead of relying on the execution date
- Replay reuses the original run's interval and configuration only after validating a complete four-file manifest, file sizes, and SHA-256 digests
- Relation hashes exclude ingestion-only metadata so equivalent business snapshots remain comparable across run IDs
- Failed quality or drift candidates retain prior published silver/gold data while recording their operational evidence
- Package licensing uses current PEP 639 metadata

### Fixed

- Historical snapshots and replays no longer fail a wall-clock freshness rule; the declared interval end is checked against the latest accepted trip date
- Missing, modified, resized, path-escaping, or non-equivalent replay inputs and outputs are rejected and cannot replace the published snapshot

## 0.2.0 — 2026-07-13

### Added

- Copy-on-write candidate publication, a single-publisher lock, and atomic file replacement that keeps the published DuckDB readable during builds
- Run-scoped raw source directories and SHA-256 manifests in `ops.ingest_files`
- Trip and payment rejection models with explicit contract reasons
- Duplicate-payment and fact-cardinality protection
- Liveness/readiness separation, typed analytics filters, bounded station limits, and manifest/filter API endpoints
- `metropulse status` for run and source evidence
- Workbench-style responsive operations console with command menu, accessible hourly tables, actual lineage edges, source hashes, and partial-error retry
- Frontend core/static-server tests and Python rollback/contract regression coverage
- Python 3.11–3.14 CI matrix, Node 24 LTS integration job, least-privilege workflow permissions, concurrency control, and GitHub Actions Dependabot
- Bounded Python dependency ranges and weekly Dependabot coverage for both Python and GitHub Actions
- Full commit-SHA pinning for every third-party GitHub Action
- Architecture, contract, case-study, interview, configuration, and reference-run documentation

### Changed

- Package and API version advanced to `0.2.0`
- Verification now uses an isolated project root and runs both Python and frontend checks
- Dashboard API configuration now uses `METROPULSE_API_BASE_URL` at build time
- API CORS origins are configurable through a validated `METROPULSE_CORS_ORIGINS` list
- Hourly charts render the complete API series with separate labeled scales

### Fixed

- Invalid rows no longer disappear through silent filtering
- Duplicate payments no longer multiply enriched facts or revenue
- Empty/all-rejected batches record failed checks instead of crashing on a null freshness value
- Completion, manifest, and quality timestamps now record the actual event rather than DuckDB transaction start
- Failed candidates no longer overwrite source files referenced by the published manifest
- The local static server rejects directories and traversal without an uncaught stream error
- Superseded dashboard requests can no longer overwrite newer filter, reset, or retry results
- Refreshed snapshot metadata reconciles filter options, and empty/error views clear stale chart and quality state
- Dashboard builds reject API URLs containing credentials, queries, fragments, or non-HTTP protocols
- Mobile navigation is inert while closed and the 320–768px layouts avoid horizontal overflow
