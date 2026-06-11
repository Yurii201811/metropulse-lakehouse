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
    console.print(f"[bold green]Pipeline succeeded[/] run_id={result.run_id}")
    console.print(f"Warehouse: [bold]{result.db_path}[/]")
    console.print(f"Silver trips: [bold]{result.total_trips:,}[/]")
    console.print(f"Gold hourly rows: [bold]{result.gold_hourly_rows:,}[/]")

    table = Table(title="Quality Checks")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Observed", justify="right")
    table.add_column("Threshold")
    for check in result.quality_checks:
        status = "[green]pass[/]" if check.status == "pass" else "[red]fail[/]"
        table.add_row(check.name, status, f"{check.observed_value:.4g}", check.threshold)
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
