from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from metropulse.cli import app
from metropulse.exporter import (
    MANIFEST_FILENAME,
    _neutralize_csv_cell,
    export_snapshot,
    verify_snapshot_bundle,
)
from metropulse.orchestration import run_pipeline

runner = CliRunner()


def test_export_snapshot_and_verify_bundle_roundtrip(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    result = run_pipeline(
        project_root=project_root,
        days=7,
        seed=20260611,
        fail_on_quality=False,
    )
    bundle_dir = tmp_path / "bundle"
    summary = export_snapshot(result.db_path, bundle_dir)

    assert summary.run_id == result.run_id
    assert (bundle_dir / MANIFEST_FILENAME).is_file()
    assert set(summary.files) == {
        "dashboard_summary.json",
        "quality_and_drift.json",
        "revenue_by_zone.csv",
        "run_metadata.json",
    }

    verification = verify_snapshot_bundle(bundle_dir)
    assert verification.valid is True
    assert verification.run_id == result.run_id
    assert verification.checked_files == 4
    assert verification.errors == []


def test_verify_bundle_rejects_tampered_and_unexpected_files(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    result = run_pipeline(
        project_root=project_root,
        days=7,
        seed=20260611,
        fail_on_quality=False,
    )
    bundle_dir = tmp_path / "bundle"
    export_snapshot(result.db_path, bundle_dir)

    # Unexpected file fails closed
    extra = bundle_dir / "extra.txt"
    extra.write_text("rogue", encoding="utf-8")
    res_extra = verify_snapshot_bundle(bundle_dir)
    assert res_extra.valid is False
    assert any("Unexpected file" in e for e in res_extra.errors)
    extra.unlink()

    # Tampered file fails closed
    target = bundle_dir / "dashboard_summary.json"
    target.write_text(target.read_text(encoding="utf-8") + " ", encoding="utf-8")
    res_tampered = verify_snapshot_bundle(bundle_dir)
    assert res_tampered.valid is False
    assert any("mismatch" in e for e in res_tampered.errors)


def test_export_rejects_nonempty_or_symlink_or_unknown_run(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    result = run_pipeline(
        project_root=project_root,
        days=7,
        seed=20260611,
        fail_on_quality=False,
    )

    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    (nonempty / "keep.txt").write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="must be empty"):
        export_snapshot(result.db_path, nonempty)

    symlink_target = tmp_path / "real_dir"
    symlink_target.mkdir()
    symlink_out = tmp_path / "link_dir"
    symlink_out.symlink_to(symlink_target)
    with pytest.raises(ValueError, match="symlinked"):
        export_snapshot(result.db_path, symlink_out)

    with pytest.raises(ValueError, match="not found"):
        export_snapshot(result.db_path, tmp_path / "missing_run", run_id="does-not-exist")


def test_csv_formula_neutralization() -> None:
    assert _neutralize_csv_cell("=SUM(A1:A2)") == "'=SUM(A1:A2)"
    assert _neutralize_csv_cell("+cmd") == "'+cmd"
    assert _neutralize_csv_cell("-1") == "'-1"
    assert _neutralize_csv_cell("@evil") == "'@evil"
    assert _neutralize_csv_cell("Central") == "Central"
    assert _neutralize_csv_cell(None) == ""


def test_cli_export_and_verify_commands(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    result = run_pipeline(
        project_root=project_root,
        days=7,
        seed=20260611,
        fail_on_quality=False,
    )
    bundle_dir = tmp_path / "cli_bundle"

    export_res = runner.invoke(
        app,
        [
            "export-snapshot",
            "--project-root",
            str(project_root),
            "--output",
            str(bundle_dir),
            "--run-id",
            result.run_id,
        ],
    )
    assert export_res.exit_code == 0, export_res.output

    manifest = json.loads((bundle_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert manifest["run_id"] == result.run_id

    verify_res = runner.invoke(app, ["verify-bundle", "--bundle", str(bundle_dir)])
    assert verify_res.exit_code == 0, verify_res.output
