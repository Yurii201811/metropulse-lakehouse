from __future__ import annotations

from metropulse.orchestration import run_pipeline
from metropulse.warehouse import connect
from tests.helpers import isolated_project_root


def test_quality_results_are_persisted_with_run_id() -> None:
    result = run_pipeline(project_root=isolated_project_root("quality"), days=4, seed=77)

    con = connect(result.db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT check_name, status
            FROM ops.quality_results
            WHERE run_id = ?
            """,
            [result.run_id],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) >= 8
    assert {status for _, status in rows} == {"pass"}
