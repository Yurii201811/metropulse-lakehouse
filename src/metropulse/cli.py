from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from metropulse.api import create_app
from metropulse.config import ProjectPaths
from metropulse.orchestration import run_pipeline
from metropulse.warehouse import connect

app = typer.Typer(help="MetroPulse Lakehouse data engineering portfolio project.")
console = Console()


@app.command()
def run(
    project_root: Annotated[
        Path, typer.Option(help="Project root containing data/ folders.")
    ] = Path("."),
    days: Annotated[
        int, typer.Option(min=2, help="Number of synthetic raw days to generate.")
    ] = 45,
    seed: Annotated[int, typer.Option(help="Deterministic source-data seed.")] = 20260611,
    fail_on_quality: Annotated[
        bool, typer.Option(help="Exit non-zero when a quality check fails.")
    ] = True,
) -> None:
    """Generate raw data, load DuckDB, transform marts, and run quality gates."""

    result = run_pipeline(
        project_root=project_root,
        days=days,
        seed=seed,
        fail_on_quality=fail_on_quality,
    )
    if result.failed_checks:
        console.print(
            f"[bold yellow]Pipeline published with failed quality gates[/] run_id={result.run_id}"
        )
    else:
        console.print(f"[bold green]Pipeline succeeded[/] run_id={result.run_id}")
    console.print(f"Warehouse: [bold]{result.db_path}[/]")
    console.print(f"Silver trips: [bold]{result.total_trips:,}[/]")
    console.print(f"Rejected trips: [bold]{result.rejected_trips:,}[/]")
    console.print(f"Source manifests: [bold]{len(result.ingest_files)}[/]")
    console.print(f"Gold hourly rows: [bold]{result.gold_hourly_rows:,}[/]")

    table = Table(title="Quality Checks")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Observed", justify="right")
    table.add_column("Threshold")
    for check in result.quality_checks:
        status = "[green]pass[/]" if check.status == "pass" else "[red]fail[/]"
        observed = "null" if check.observed_value is None else f"{check.observed_value:.4g}"
        table.add_row(check.name, status, observed, check.threshold)
    console.print(table)


@app.command("show-summary")
def show_summary(
    project_root: Annotated[
        Path, typer.Option(help="Project root containing the warehouse.")
    ] = Path("."),
) -> None:
    """Print a compact summary from the current gold tables."""

    paths = ProjectPaths.from_root(project_root)
    con = connect(paths.db_path, read_only=True)
    try:
        summary = con.execute("SELECT * FROM gold.dashboard_summary").fetchone()
        if summary is None:
            raise typer.BadParameter("No dashboard summary found. Run `metropulse run` first.")
        columns = [description[0] for description in con.description]
        table = Table(title="MetroPulse Dashboard Summary")
        table.add_column("Metric")
        table.add_column("Value", justify="right")
        for name, value in zip(columns, summary, strict=True):
            table.add_row(name, str(value))
        console.print(table)
    finally:
        con.close()


@app.command()
def status(
    project_root: Annotated[
        Path, typer.Option(help="Project root containing the warehouse.")
    ] = Path("."),
) -> None:
    """Show the latest run, quality outcome, and ingested source files."""

    paths = ProjectPaths.from_root(project_root)
    if not paths.db_path.exists():
        raise typer.BadParameter("Warehouse not found. Run `metropulse run` first.")

    con = connect(paths.db_path, read_only=True)
    try:
        run = con.execute(
            """
            SELECT
                run_id,
                status,
                started_at,
                ended_at,
                published_at,
                raw_trips,
                silver_trips,
                rejected_trips,
                quality_passed,
                quality_failed
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        if run is None:
            raise typer.BadParameter("No pipeline runs found. Run `metropulse run` first.")

        columns = [description[0] for description in con.description]
        summary = Table(title="Latest Pipeline Run")
        summary.add_column("Field")
        summary.add_column("Value")
        for name, value in zip(columns, run, strict=True):
            summary.add_row(name, str(value))
        console.print(summary)

        manifests = con.execute(
            """
            SELECT dataset_name, source_file, row_count, left(file_sha256, 12)
            FROM ops.ingest_files
            WHERE run_id = ?
            ORDER BY dataset_name
            """,
            [run[0]],
        ).fetchall()
        files = Table(title="Source Manifest")
        files.add_column("Dataset")
        files.add_column("File")
        files.add_column("Rows", justify="right")
        files.add_column("SHA-256")
        for manifest in manifests:
            files.add_row(*(str(value) for value in manifest))
        console.print(files)
    finally:
        con.close()


@app.command("serve-api")
def serve_api(
    project_root: Annotated[
        Path, typer.Option(help="Project root containing the warehouse.")
    ] = Path("."),
    host: Annotated[str, typer.Option(help="API host.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="API port.")] = 8000,
) -> None:
    """Serve the dashboard API over FastAPI."""

    paths = ProjectPaths.from_root(project_root)
    api = create_app(paths.db_path)
    uvicorn.run(api, host=host, port=port)


if __name__ == "__main__":
    app()
