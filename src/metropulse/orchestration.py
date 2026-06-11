from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from metropulse.config import ProjectPaths
from metropulse.generator import generate_raw_batch
from metropulse.ingestion import load_bronze
from metropulse.quality import QualityCheck, run_quality_checks
from metropulse.transforms import build_gold, build_silver
from metropulse.warehouse import connect, init_schemas, table_count


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    db_path: Path
    raw_counts: dict[str, int]
    quality_checks: list[QualityCheck]
    total_trips: int
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
    run_id = f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    con = connect(paths.db_path)
    try:
        init_schemas(con)
        con.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at, status, days_requested, seed)
            VALUES (?, current_timestamp, 'running', ?, ?)
            """,
            [run_id, days, seed],
        )

        raw_batch = generate_raw_batch(paths.raw_dir, days=days, seed=seed)
        raw_counts = load_bronze(con, raw_batch)
        build_silver(con)
        build_gold(con)
        checks = run_quality_checks(con, run_id)
        failed = [check for check in checks if check.status != "pass"]

        total_trips = table_count(con, "silver.trips")
        gold_hourly_rows = table_count(con, "gold.hourly_mobility")
        con.execute(
            """
            UPDATE ops.pipeline_runs
            SET
                ended_at = current_timestamp,
                status = ?,
                raw_trips = ?,
                silver_trips = ?,
                gold_hourly_rows = ?,
                quality_passed = ?,
                quality_failed = ?
            WHERE run_id = ?
            """,
            [
                "failed_quality" if failed else "success",
                raw_counts["trips"],
                total_trips,
                gold_hourly_rows,
                len(checks) - len(failed),
                len(failed),
                run_id,
            ],
        )

        result = PipelineResult(
            run_id=run_id,
            db_path=paths.db_path,
            raw_counts=raw_counts,
            quality_checks=checks,
            total_trips=total_trips,
            gold_hourly_rows=gold_hourly_rows,
        )
        if failed and fail_on_quality:
            failed_names = ", ".join(check.name for check in failed)
            raise RuntimeError(f"Quality checks failed: {failed_names}")
        return result
    except Exception as exc:
        con.execute(
            """
            UPDATE ops.pipeline_runs
            SET ended_at = current_timestamp, status = 'failed', error_message = ?
            WHERE run_id = ?
            """,
            [str(exc), run_id],
        )
        raise
    finally:
        con.close()
