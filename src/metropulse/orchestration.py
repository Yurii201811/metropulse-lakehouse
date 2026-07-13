from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import copy2
from uuid import uuid4

import duckdb

try:
    import fcntl
except ImportError:  # pragma: no cover - the pipeline is intentionally POSIX-only
    fcntl = None  # type: ignore[assignment]

from metropulse import __version__
from metropulse.config import ProjectPaths
from metropulse.generator import RawBatch, generate_raw_batch
from metropulse.ingestion import IngestFile, load_bronze
from metropulse.observability import (
    CONTRACT_VERSION,
    DatasetProfile,
    DriftResult,
    RelationFingerprint,
    build_dataset_profiles,
    build_relation_fingerprints,
    compare_profiles,
    compatible_baseline_run_id,
    output_set_sha256,
    persist_observability,
)
from metropulse.quality import QualityCheck, persist_quality_checks, run_quality_checks
from metropulse.transforms import build_gold, build_silver
from metropulse.warehouse import connect, init_schemas, table_count

DATASETS = ("payments", "stations", "trips", "weather")


@dataclass(frozen=True)
class RunSpec:
    days: int
    seed: int
    data_interval_start: date
    data_interval_end: date
    source_mode: str
    replay_of_run_id: str | None
    parent_run_id: str | None
    config_sha256: str
    expected_input_set_sha256: str | None
    expected_output_set_sha256: str | None


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    db_path: Path
    raw_counts: dict[str, int]
    quality_checks: list[QualityCheck]
    ingest_files: list[IngestFile]
    total_trips: int
    rejected_trips: int
    gold_hourly_rows: int
    source_mode: str
    replay_of_run_id: str | None
    data_interval_start: date
    data_interval_end: date
    input_set_sha256: str
    output_set_sha256: str
    profiles: list[DatasetProfile]
    fingerprints: list[RelationFingerprint]
    drift_results: list[DriftResult]

    @property
    def failed_checks(self) -> list[QualityCheck]:
        return [check for check in self.quality_checks if check.status != "pass"]


def run_pipeline(
    *,
    project_root: Path | str | None = None,
    days: int = 45,
    seed: int = 20260611,
    as_of_date: date | None = None,
    fail_on_quality: bool = True,
) -> PipelineResult:
    """Build and atomically publish a deterministic snapshot."""

    paths = ProjectPaths.from_root(project_root)
    paths.ensure()
    with _publisher_lock(paths.db_path):
        return _run_pipeline_locked(
            paths=paths,
            days=days,
            seed=seed,
            as_of_date=as_of_date,
            replay_run_id=None,
            fail_on_quality=fail_on_quality,
        )


def replay_pipeline(
    *,
    replay_run_id: str,
    project_root: Path | str | None = None,
    fail_on_quality: bool = True,
) -> PipelineResult:
    """Rebuild a snapshot from a prior run's verified immutable source files."""

    if not replay_run_id.strip():
        raise ValueError("replay_run_id must not be empty")
    paths = ProjectPaths.from_root(project_root)
    paths.ensure()
    with _publisher_lock(paths.db_path):
        return _run_pipeline_locked(
            paths=paths,
            days=None,
            seed=None,
            as_of_date=None,
            replay_run_id=replay_run_id,
            fail_on_quality=fail_on_quality,
        )


def _run_pipeline_locked(
    *,
    paths: ProjectPaths,
    days: int | None,
    seed: int | None,
    as_of_date: date | None,
    replay_run_id: str | None,
    fail_on_quality: bool,
) -> PipelineResult:
    run_id = f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    candidate_path = paths.db_path.with_name(
        f".{paths.db_path.stem}.{run_id}.candidate.duckdb"
    )
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if paths.db_path.exists():
            copy2(paths.db_path, candidate_path)
        con = connect(candidate_path)
    except Exception:
        candidate_path.unlink(missing_ok=True)
        raise

    transaction_open = False
    run_row_inserted = False
    run_status_recorded = False
    candidate_ready = False
    pending_error: Exception | None = None
    result: PipelineResult | None = None
    spec: RunSpec | None = None
    raw_counts: dict[str, int] = {}
    manifests: list[IngestFile] = []
    checks: list[QualityCheck] = []
    profiles: list[DatasetProfile] = []
    fingerprints: list[RelationFingerprint] = []
    drift_results: list[DriftResult] = []
    total_trips = 0
    rejected_trips = 0
    gold_hourly_rows = 0
    input_hash: str | None = None
    output_hash: str | None = None
    try:
        init_schemas(con)
        spec = _resolve_run_spec(
            con,
            days=days,
            seed=seed,
            as_of_date=as_of_date,
            replay_run_id=replay_run_id,
        )
        con.execute(
            """
            INSERT INTO ops.pipeline_runs (
                run_id,
                started_at,
                status,
                days_requested,
                seed,
                data_interval_start,
                data_interval_end,
                source_mode,
                replay_of_run_id,
                parent_run_id,
                code_version,
                contract_version,
                config_sha256
            )
            VALUES (?, current_timestamp, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                spec.days,
                spec.seed,
                spec.data_interval_start,
                spec.data_interval_end,
                spec.source_mode,
                spec.replay_of_run_id,
                spec.parent_run_id,
                __version__,
                CONTRACT_VERSION,
                spec.config_sha256,
            ],
        )
        run_row_inserted = True

        if spec.source_mode == "replay":
            if spec.replay_of_run_id is None:  # pragma: no cover - RunSpec invariant
                raise RuntimeError("Replay source run was not resolved.")
            if spec.expected_input_set_sha256 is None:  # pragma: no cover - invariant
                raise RuntimeError("Replay input fingerprint was not resolved.")
            raw_batch = _verified_replay_batch(
                con,
                paths,
                spec.replay_of_run_id,
                run_id=run_id,
                expected_input_set_sha256=spec.expected_input_set_sha256,
            )
        else:
            run_raw_dir = paths.raw_dir / "runs" / run_id
            raw_batch = generate_raw_batch(
                run_raw_dir,
                days=spec.days,
                seed=spec.seed,
                as_of_date=spec.data_interval_end,
            )

        con.execute("BEGIN TRANSACTION")
        transaction_open = True
        raw_counts, manifests = load_bronze(
            con,
            raw_batch,
            run_id=run_id,
            source_root=paths.raw_dir,
        )
        input_hash = _input_set_sha256(manifests)
        if (
            spec.expected_input_set_sha256 is not None
            and input_hash != spec.expected_input_set_sha256
        ):
            raise RuntimeError(
                "Replay input fingerprint mismatch after staging; source bytes changed "
                "before ingestion completed."
            )
        build_silver(con)
        build_gold(con)
        profiles = build_dataset_profiles(con, days=spec.days)
        fingerprints = build_relation_fingerprints(con)
        output_hash = output_set_sha256(fingerprints)
        if (
            spec.expected_output_set_sha256 is not None
            and output_hash != spec.expected_output_set_sha256
        ):
            raise RuntimeError(
                "Replay output fingerprint mismatch; the rebuilt snapshot is not "
                "equivalent to its source run."
            )
        baseline_run_id = compatible_baseline_run_id(
            con,
            current_run_id=run_id,
            days=spec.days,
            contract_version=CONTRACT_VERSION,
        )
        if baseline_run_id is not None:
            baseline_profiles = _load_profiles(con, baseline_run_id)
            drift_results = compare_profiles(
                run_id=run_id,
                baseline_run_id=baseline_run_id,
                current=profiles,
                baseline=baseline_profiles,
            )
        checks = run_quality_checks(
            con,
            expected_end_date=spec.data_interval_end,
            drift_results=drift_results if baseline_run_id is not None else None,
        )
        failed = [check for check in checks if check.status != "pass"]

        total_trips = table_count(con, "silver.trips")
        rejected_trips = table_count(con, "silver.trip_rejections")
        gold_hourly_rows = table_count(con, "gold.hourly_mobility")

        if failed and fail_on_quality:
            con.execute("ROLLBACK")
            transaction_open = False
            _persist_run_evidence(
                con,
                run_id,
                manifests,
                checks,
                profiles,
                fingerprints,
                drift_results,
            )
            _finish_run(
                con,
                run_id,
                status="failed_quality",
                raw_counts=raw_counts,
                total_trips=total_trips,
                rejected_trips=rejected_trips,
                gold_hourly_rows=gold_hourly_rows,
                checks=checks,
                published=False,
                input_set_sha256=input_hash,
                output_set_sha256=output_hash,
                error_message=_quality_error(failed),
            )
            run_status_recorded = True
            candidate_ready = True
            pending_error = RuntimeError(_quality_error(failed))
        else:
            persist_quality_checks(con, run_id, checks)
            _persist_ingest_files(con, run_id, manifests)
            persist_observability(
                con,
                run_id=run_id,
                profiles=profiles,
                fingerprints=fingerprints,
                drift_results=drift_results,
            )
            _finish_run(
                con,
                run_id,
                status="failed_quality" if failed else "success",
                raw_counts=raw_counts,
                total_trips=total_trips,
                rejected_trips=rejected_trips,
                gold_hourly_rows=gold_hourly_rows,
                checks=checks,
                published=True,
                input_set_sha256=input_hash,
                output_set_sha256=output_hash,
            )
            con.execute("COMMIT")
            transaction_open = False
            candidate_ready = True

            result = PipelineResult(
                run_id=run_id,
                db_path=paths.db_path,
                raw_counts=raw_counts,
                quality_checks=checks,
                ingest_files=manifests,
                total_trips=total_trips,
                rejected_trips=rejected_trips,
                gold_hourly_rows=gold_hourly_rows,
                source_mode=spec.source_mode,
                replay_of_run_id=spec.replay_of_run_id,
                data_interval_start=spec.data_interval_start,
                data_interval_end=spec.data_interval_end,
                input_set_sha256=input_hash,
                output_set_sha256=output_hash,
                profiles=profiles,
                fingerprints=fingerprints,
                drift_results=drift_results,
            )
    except Exception as exc:
        pending_error = exc
        if transaction_open:
            con.execute("ROLLBACK")
            transaction_open = False
        if run_row_inserted and not run_status_recorded:
            _persist_run_evidence(
                con,
                run_id,
                manifests,
                checks,
                profiles,
                fingerprints,
                drift_results,
            )
            _finish_run(
                con,
                run_id,
                status="failed",
                raw_counts=raw_counts,
                total_trips=total_trips,
                rejected_trips=rejected_trips,
                gold_hourly_rows=gold_hourly_rows,
                checks=checks,
                published=False,
                input_set_sha256=input_hash,
                output_set_sha256=output_hash,
                error_message=str(exc),
            )
            candidate_ready = True
    finally:
        try:
            con.close()
        except Exception:
            candidate_ready = False
            raise
        finally:
            if not candidate_ready:
                candidate_path.unlink(missing_ok=True)

    if candidate_ready:
        try:
            candidate_path.replace(paths.db_path)
        except Exception:
            candidate_path.unlink(missing_ok=True)
            raise

    if pending_error is not None:
        raise pending_error
    if result is None:
        raise RuntimeError("Pipeline completed without a result.")
    return result


def _resolve_run_spec(
    con: duckdb.DuckDBPyConnection,
    *,
    days: int | None,
    seed: int | None,
    as_of_date: date | None,
    replay_run_id: str | None,
) -> RunSpec:
    parent_row = con.execute(
        """
        SELECT run_id
        FROM ops.pipeline_runs
        WHERE published_at IS NOT NULL
        ORDER BY published_at DESC
        LIMIT 1
        """
    ).fetchone()
    parent_run_id = parent_row[0] if parent_row else None

    if replay_run_id is not None:
        source = con.execute(
            """
            SELECT
                days_requested,
                seed,
                data_interval_start,
                data_interval_end,
                contract_version,
                config_sha256,
                input_set_sha256,
                output_set_sha256
            FROM ops.pipeline_runs
            WHERE run_id = ?
            """,
            [replay_run_id],
        ).fetchone()
        if source is None:
            raise ValueError(f"Replay source run does not exist: {replay_run_id}")
        if any(value is None for value in source):
            raise ValueError(
                f"Run {replay_run_id} predates the replay contract and cannot be replayed."
            )
        if source[4] != CONTRACT_VERSION:
            raise ValueError(
                f"Run {replay_run_id} uses contract {source[4]}, not {CONTRACT_VERSION}."
            )
        resolved_days = int(source[0])
        resolved_seed = int(source[1])
        interval_start = source[2]
        interval_end = source[3]
        config_hash = _config_sha256(resolved_days, resolved_seed, interval_end)
        if config_hash != source[5]:
            raise RuntimeError(f"Recorded configuration hash is invalid for run {replay_run_id}.")
        return RunSpec(
            days=resolved_days,
            seed=resolved_seed,
            data_interval_start=interval_start,
            data_interval_end=interval_end,
            source_mode="replay",
            replay_of_run_id=replay_run_id,
            parent_run_id=parent_run_id,
            config_sha256=config_hash,
            expected_input_set_sha256=source[6],
            expected_output_set_sha256=source[7],
        )

    if days is None or seed is None:  # pragma: no cover - public API invariant
        raise ValueError("days and seed are required for generated runs")
    if days < 2:
        raise ValueError("days must be at least 2")
    interval_end = as_of_date or date.today() - timedelta(days=1)
    if interval_end > date.today() - timedelta(days=1):
        raise ValueError("as_of_date cannot be later than yesterday")
    interval_start = interval_end - timedelta(days=days - 1)
    return RunSpec(
        days=days,
        seed=seed,
        data_interval_start=interval_start,
        data_interval_end=interval_end,
        source_mode="generated",
        replay_of_run_id=None,
        parent_run_id=parent_run_id,
        config_sha256=_config_sha256(days, seed, interval_end),
        expected_input_set_sha256=None,
        expected_output_set_sha256=None,
    )


def _config_sha256(days: int, seed: int, as_of_date: date) -> str:
    payload = json.dumps(
        {
            "as_of_date": as_of_date.isoformat(),
            "contract_version": CONTRACT_VERSION,
            "days": days,
            "seed": seed,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _verified_replay_batch(
    con: duckdb.DuckDBPyConnection,
    paths: ProjectPaths,
    replay_run_id: str,
    *,
    run_id: str,
    expected_input_set_sha256: str,
) -> RawBatch:
    rows = con.execute(
        """
        SELECT dataset_name, source_file, file_sha256, file_bytes, row_count
        FROM ops.ingest_files
        WHERE run_id = ?
        ORDER BY dataset_name
        """,
        [replay_run_id],
    ).fetchall()
    manifests = {row[0]: row[1:] for row in rows}
    if set(manifests) != set(DATASETS):
        raise RuntimeError(f"Run {replay_run_id} does not have a complete four-file manifest.")

    recorded_manifests = [
        IngestFile(
            dataset_name=dataset_name,
            source_file=manifest[0],
            file_sha256=manifest[1],
            file_bytes=manifest[2],
            row_count=manifest[3],
        )
        for dataset_name, manifest in manifests.items()
    ]
    if _input_set_sha256(recorded_manifests) != expected_input_set_sha256:
        raise RuntimeError(
            f"Recorded input manifest fingerprint is invalid for run {replay_run_id}."
        )

    raw_root = paths.raw_dir.resolve()
    replay_raw_dir = paths.raw_dir / "runs" / run_id
    replay_raw_dir.mkdir(parents=True, exist_ok=False)
    files: dict[str, Path] = {}
    for dataset_name in DATASETS:
        source_file, expected_sha256, expected_bytes, _ = manifests[dataset_name]
        source_path = (raw_root / source_file).resolve()
        try:
            source_path.relative_to(raw_root)
        except ValueError as exc:
            raise RuntimeError(
                f"Replay manifest escapes the raw-data root: {source_file}"
            ) from exc
        if not source_path.is_file():
            raise RuntimeError(f"Replay source file is missing: {source_file}")
        if source_path.stat().st_size != expected_bytes:
            raise RuntimeError(f"Replay source size mismatch: {source_file}")
        if _sha256_file(source_path) != expected_sha256:
            raise RuntimeError(f"Replay source SHA-256 mismatch: {source_file}")
        staged_path = replay_raw_dir / f"{dataset_name}.csv"
        copy2(source_path, staged_path)
        if staged_path.stat().st_size != expected_bytes:
            raise RuntimeError(f"Replay staged source size mismatch: {source_file}")
        if _sha256_file(staged_path) != expected_sha256:
            raise RuntimeError(f"Replay staged source SHA-256 mismatch: {source_file}")
        files[dataset_name] = staged_path

    return RawBatch(
        trips=files["trips"],
        payments=files["payments"],
        stations=files["stations"],
        weather=files["weather"],
    )


def _load_profiles(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
) -> list[DatasetProfile]:
    return [
        DatasetProfile(metric_name=row[0], metric_value=row[1], unit=row[2])
        for row in con.execute(
            """
            SELECT metric_name, metric_value, unit
            FROM ops.dataset_profiles
            WHERE run_id = ?
            ORDER BY metric_name
            """,
            [run_id],
        ).fetchall()
    ]


def _input_set_sha256(manifests: list[IngestFile]) -> str:
    payload = json.dumps(
        [
            {
                "dataset_name": item.dataset_name,
                "file_bytes": item.file_bytes,
                "file_sha256": item.file_sha256,
                "row_count": item.row_count,
            }
            for item in sorted(manifests, key=lambda item: item.dataset_name)
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _publisher_lock(db_path: Path) -> Iterator[None]:
    if fcntl is None:
        raise RuntimeError(
            "MetroPulse pipeline publication requires macOS or Linux POSIX file locking."
        )
    lock_path = db_path.with_name(f"{db_path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("Another MetroPulse pipeline run is already active.") from exc
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _quality_error(failed: list[QualityCheck]) -> str:
    failed_names = ", ".join(check.name for check in failed)
    return f"Quality checks failed: {failed_names}"


def _persist_run_evidence(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    manifests: list[IngestFile],
    checks: list[QualityCheck],
    profiles: list[DatasetProfile],
    fingerprints: list[RelationFingerprint],
    drift_results: list[DriftResult],
) -> None:
    if manifests:
        _persist_ingest_files(con, run_id, manifests)
    if checks:
        persist_quality_checks(con, run_id, checks)
    if profiles or fingerprints or drift_results:
        persist_observability(
            con,
            run_id=run_id,
            profiles=profiles,
            fingerprints=fingerprints,
            drift_results=drift_results,
        )


def _persist_ingest_files(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    manifests: list[IngestFile],
) -> None:
    loaded_at = datetime.now(UTC)
    con.execute("DELETE FROM ops.ingest_files WHERE run_id = ?", [run_id])
    con.executemany(
        """
        INSERT INTO ops.ingest_files (
            run_id, dataset_name, source_file, file_sha256, file_bytes, row_count, loaded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.dataset_name,
                item.source_file,
                item.file_sha256,
                item.file_bytes,
                item.row_count,
                loaded_at,
            )
            for item in manifests
        ],
    )


def _finish_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    status: str,
    raw_counts: dict[str, int],
    total_trips: int,
    rejected_trips: int,
    gold_hourly_rows: int,
    checks: list[QualityCheck],
    published: bool,
    input_set_sha256: str | None,
    output_set_sha256: str | None,
    error_message: str | None = None,
) -> None:
    finished_at = datetime.now(UTC)
    failed_count = sum(check.status != "pass" for check in checks)
    con.execute(
        """
        UPDATE ops.pipeline_runs
        SET
            ended_at = ?,
            status = ?,
            raw_trips = ?,
            silver_trips = ?,
            rejected_trips = ?,
            gold_hourly_rows = ?,
            quality_passed = ?,
            quality_failed = ?,
            input_set_sha256 = ?,
            output_set_sha256 = ?,
            published_at = CASE WHEN ? THEN ? ELSE published_at END,
            error_message = ?
        WHERE run_id = ?
        """,
        [
            finished_at,
            status,
            raw_counts.get("trips"),
            total_trips,
            rejected_trips,
            gold_hourly_rows,
            len(checks) - failed_count,
            failed_count,
            input_set_sha256,
            output_set_sha256,
            published,
            finished_at,
            error_message,
            run_id,
        ],
    )
