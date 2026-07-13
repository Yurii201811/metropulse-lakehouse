from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2
from uuid import uuid4

import duckdb

try:
    import fcntl
except ImportError:  # pragma: no cover - the pipeline is intentionally POSIX-only
    fcntl = None  # type: ignore[assignment]

from metropulse.config import ProjectPaths
from metropulse.generator import generate_raw_batch
from metropulse.ingestion import IngestFile, load_bronze
from metropulse.quality import QualityCheck, persist_quality_checks, run_quality_checks
from metropulse.transforms import build_gold, build_silver
from metropulse.warehouse import connect, init_schemas, table_count


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

    @property
    def failed_checks(self) -> list[QualityCheck]:
        return [check for check in self.quality_checks if check.status != "pass"]


def run_pipeline(
    *,
    project_root: Path | str | None = None,
    days: int = 45,
    seed: int = 20260611,
    fail_on_quality: bool = True,
) -> PipelineResult:
    paths = ProjectPaths.from_root(project_root)
    paths.ensure()
    with _publisher_lock(paths.db_path):
        return _run_pipeline_locked(
            paths=paths,
            days=days,
            seed=seed,
            fail_on_quality=fail_on_quality,
        )


def _run_pipeline_locked(
    *,
    paths: ProjectPaths,
    days: int,
    seed: int,
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
    run_status_recorded = False
    candidate_ready = False
    pending_error: Exception | None = None
    result: PipelineResult | None = None
    raw_counts: dict[str, int] = {}
    manifests: list[IngestFile] = []
    checks: list[QualityCheck] = []
    total_trips = 0
    rejected_trips = 0
    gold_hourly_rows = 0
    try:
        init_schemas(con)
        con.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at, status, days_requested, seed)
            VALUES (?, current_timestamp, 'running', ?, ?)
            """,
            [run_id, days, seed],
        )

        run_raw_dir = paths.raw_dir / "runs" / run_id
        raw_batch = generate_raw_batch(run_raw_dir, days=days, seed=seed)
        con.execute("BEGIN TRANSACTION")
        transaction_open = True
        raw_counts, manifests = load_bronze(
            con,
            raw_batch,
            run_id=run_id,
            source_root=paths.raw_dir,
        )
        build_silver(con)
        build_gold(con)
        checks = run_quality_checks(con)
        failed = [check for check in checks if check.status != "pass"]

        total_trips = table_count(con, "silver.trips")
        rejected_trips = table_count(con, "silver.trip_rejections")
        gold_hourly_rows = table_count(con, "gold.hourly_mobility")

        if failed and fail_on_quality:
            con.execute("ROLLBACK")
            transaction_open = False
            _persist_run_evidence(con, run_id, manifests, checks)
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
                error_message=_quality_error(failed),
            )
            run_status_recorded = True
            candidate_ready = True
            pending_error = RuntimeError(_quality_error(failed))
        else:
            persist_quality_checks(con, run_id, checks)
            _persist_ingest_files(con, run_id, manifests)
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
            )
    except Exception as exc:
        pending_error = exc
        if transaction_open:
            con.execute("ROLLBACK")
            transaction_open = False
        if not run_status_recorded:
            _persist_run_evidence(con, run_id, manifests, checks)
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
) -> None:
    if manifests:
        _persist_ingest_files(con, run_id, manifests)
    if checks:
        persist_quality_checks(con, run_id, checks)


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
            published,
            finished_at,
            error_message,
            run_id,
        ],
    )
