# Changelog

All notable project changes are documented here.

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
