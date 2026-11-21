from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from metropulse import __version__
from metropulse.warehouse import connect

MANIFEST_SCHEMA_VERSION = 1
MANIFEST_FILENAME = "bundle_manifest.json"
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


@dataclass(frozen=True)
class SnapshotBundleSummary:
    run_id: str
    output_dir: Path
    manifest_path: Path
    files: dict[str, str]


@dataclass(frozen=True)
class BundleVerificationResult:
    valid: bool
    run_id: str | None
    checked_files: int
    errors: list[str]


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _neutralize_csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        text = value.isoformat()
    else:
        text = str(value)
    if text.startswith(_FORMULA_PREFIXES):
        return f"'{text}"
    return text


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fetch_dicts(con: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cursor = con.execute(sql, params or [])
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _rows_to_csv_bytes(columns: list[str], rows: list[tuple[Any, ...]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_neutralize_csv_cell(val) for val in row])
    return buf.getvalue().encode("utf-8")


def _validate_output_dir(output_dir: Path, db_path: Path) -> Path:
    if output_dir.is_symlink():
        raise ValueError("Refusing to export into a symlinked output directory.")
    for parent in output_dir.parents:
        if parent.is_symlink():
            break
    resolved_out = output_dir.resolve()
    resolved_db_dir = db_path.resolve().parent
    if resolved_out == resolved_db_dir or resolved_db_dir in resolved_out.parents:
        raise ValueError("Output directory must not overlap with the warehouse directory.")
    if resolved_out.exists():
        if not resolved_out.is_dir():
            raise ValueError("Output path exists and is not a directory.")
        if any(resolved_out.iterdir()):
            raise ValueError("Output directory must be empty before exporting a snapshot bundle.")
    return resolved_out


def export_snapshot(
    db_path: Path,
    output_dir: Path,
    *,
    run_id: str | None = None,
) -> SnapshotBundleSummary:
    """Export a self-contained, SHA-256-verified snapshot bundle from the warehouse."""

    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse not found at {db_path}. Run `metropulse run` first.")

    target_dir = _validate_output_dir(output_dir, db_path)
    con = connect(db_path, read_only=True)
    try:
        if run_id is not None:
            runs = _fetch_dicts(
                con,
                "SELECT * FROM ops.pipeline_runs WHERE run_id = ?",
                [run_id],
            )
            if not runs:
                raise ValueError(f"Pipeline run `{run_id}` was not found in ops.pipeline_runs.")
        else:
            runs = _fetch_dicts(
                con,
                """
                SELECT * FROM ops.pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
                """,
            )
            if not runs:
                raise ValueError("No pipeline runs found. Run `metropulse run` first.")

        run_record = runs[0]
        resolved_run_id = str(run_record["run_id"])

        quality = _fetch_dicts(
            con,
            """
            SELECT check_name, status, observed_value, threshold, details, checked_at
            FROM ops.quality_results
            WHERE run_id = ?
            ORDER BY check_name
            """,
            [resolved_run_id],
        )
        manifests = _fetch_dicts(
            con,
            """
            SELECT dataset_name, source_file, file_sha256, file_bytes, row_count, loaded_at
            FROM ops.ingest_files
            WHERE run_id = ?
            ORDER BY dataset_name
            """,
            [resolved_run_id],
        )
        profiles = _fetch_dicts(
            con,
            """
            SELECT metric_name, metric_value, unit, recorded_at
            FROM ops.dataset_profiles
            WHERE run_id = ?
            ORDER BY metric_name
            """,
            [resolved_run_id],
        )
        fingerprints = _fetch_dicts(
            con,
            """
            SELECT relation_name, row_count, fingerprint_sha256, recorded_at
            FROM ops.relation_fingerprints
            WHERE run_id = ?
            ORDER BY relation_name
            """,
            [resolved_run_id],
        )
        drift = _fetch_dicts(
            con,
            """
            SELECT baseline_run_id, metric_name, current_value, baseline_value,
                   delta, delta_kind, threshold, status, checked_at
            FROM ops.drift_results
            WHERE run_id = ?
            ORDER BY metric_name
            """,
            [resolved_run_id],
        )
        summary_rows = _fetch_dicts(con, "SELECT * FROM gold.dashboard_summary")
        revenue_cursor = con.execute("SELECT * FROM gold.revenue_by_zone ORDER BY 1")
        revenue_cols = [d[0] for d in revenue_cursor.description]
        revenue_rows = revenue_cursor.fetchall()
    finally:
        con.close()

    artifacts: dict[str, bytes] = {
        "run_metadata.json": (
            json.dumps(
                {
                    "run": run_record,
                    "source_manifests": manifests,
                    "dataset_profiles": profiles,
                    "relation_fingerprints": fingerprints,
                },
                indent=2,
                sort_keys=True,
                default=_json_default,
            )
            + "\n"
        ).encode("utf-8"),
        "quality_and_drift.json": (
            json.dumps(
                {
                    "run_id": resolved_run_id,
                    "quality_results": quality,
                    "drift_results": drift,
                },
                indent=2,
                sort_keys=True,
                default=_json_default,
            )
            + "\n"
        ).encode("utf-8"),
        "dashboard_summary.json": (
            json.dumps(
                {
                    "run_id": resolved_run_id,
                    "summary": summary_rows[0] if summary_rows else None,
                },
                indent=2,
                sort_keys=True,
                default=_json_default,
            )
            + "\n"
        ).encode("utf-8"),
        "revenue_by_zone.csv": _rows_to_csv_bytes(revenue_cols, revenue_rows),
    }

    target_dir.mkdir(parents=True, exist_ok=True)
    file_entries: list[dict[str, Any]] = []
    file_hashes: dict[str, str] = {}
    for filename in sorted(artifacts):
        payload = artifacts[filename]
        file_path = target_dir / filename
        file_path.write_bytes(payload)
        digest = _sha256_bytes(payload)
        file_hashes[filename] = digest
        file_entries.append(
            {
                "path": filename,
                "bytes": len(payload),
                "sha256": digest,
            }
        )

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "package_version": __version__,
        "run_id": resolved_run_id,
        "input_set_sha256": run_record.get("input_set_sha256"),
        "output_set_sha256": run_record.get("output_set_sha256"),
        "files": file_entries,
    }
    manifest_path = target_dir / MANIFEST_FILENAME
    manifest_text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    manifest_path.write_text(manifest_text, encoding="utf-8")

    return SnapshotBundleSummary(
        run_id=resolved_run_id,
        output_dir=target_dir,
        manifest_path=manifest_path,
        files=file_hashes,
    )


def verify_snapshot_bundle(bundle_dir: Path) -> BundleVerificationResult:
    """Fail-closed verification of a snapshot bundle directory against its SHA-256 manifest."""

    errors: list[str] = []
    if bundle_dir.is_symlink() or not bundle_dir.is_dir():
        return BundleVerificationResult(
            valid=False,
            run_id=None,
            checked_files=0,
            errors=["Bundle path must be an existing non-symlink directory."],
        )

    manifest_path = bundle_dir / MANIFEST_FILENAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        return BundleVerificationResult(
            valid=False,
            run_id=None,
            checked_files=0,
            errors=[f"Missing {MANIFEST_FILENAME}."],
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return BundleVerificationResult(
            valid=False,
            run_id=None,
            checked_files=0,
            errors=[f"Invalid manifest JSON: {exc}"],
        )

    if not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        return BundleVerificationResult(
            valid=False,
            run_id=None,
            checked_files=0,
            errors=["Unsupported or missing manifest schema_version."],
        )

    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        return BundleVerificationResult(
            valid=False,
            run_id=manifest.get("run_id"),
            checked_files=0,
            errors=["Manifest declares no files."],
        )

    expected_names: set[str] = {MANIFEST_FILENAME}
    checked = 0
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("Manifest contains a non-object file entry.")
            continue
        rel = entry.get("path")
        expected_bytes = entry.get("bytes")
        expected_sha = entry.get("sha256")
        if (
            not isinstance(rel, str)
            or "/" in rel
            or "\\" in rel
            or rel in {"", ".", "..", MANIFEST_FILENAME}
        ):
            errors.append(f"Unsafe manifest path entry: {rel!r}")
            continue
        expected_names.add(rel)
        target = bundle_dir / rel
        if target.is_symlink():
            errors.append(f"Bundle file must not be a symlink: {rel}")
            continue
        if not target.is_file():
            errors.append(f"Missing bundle file: {rel}")
            continue
        data = target.read_bytes()
        if len(data) != expected_bytes:
            errors.append(f"Size mismatch for {rel}: expected {expected_bytes}, got {len(data)}")
            continue
        actual_sha = _sha256_bytes(data)
        if actual_sha != expected_sha:
            errors.append(f"SHA-256 mismatch for {rel}: expected {expected_sha}, got {actual_sha}")
            continue
        checked += 1

    actual_names = {item.name for item in bundle_dir.iterdir()}
    unexpected = sorted(actual_names - expected_names)
    if unexpected:
        errors.append(f"Unexpected file(s) present in bundle: {', '.join(unexpected)}")

    return BundleVerificationResult(
        valid=not errors,
        run_id=manifest.get("run_id") if isinstance(manifest.get("run_id"), str) else None,
        checked_files=checked,
        errors=errors,
    )
