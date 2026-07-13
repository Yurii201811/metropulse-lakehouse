#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif [[ -n "${PYTHON_BIN:-}" ]]; then
  :
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python 3.11 or newer is required." >&2
  exit 1
fi

if [[ -n "${METROPULSE_VERIFY_ROOT:-}" ]]; then
  VERIFY_ROOT="${METROPULSE_VERIFY_ROOT}"
  mkdir -p "${VERIFY_ROOT}"
else
  VERIFY_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/metropulse-verify.XXXXXX")"
  trap 'rm -rf "${VERIFY_ROOT}"' EXIT
fi

VERIFY_DB="${VERIFY_ROOT}/data/warehouse/metropulse.duckdb"
METROPULSE_DB_PATH="${VERIFY_DB}" "${PYTHON_BIN}" -m metropulse.cli run \
  --project-root "${VERIFY_ROOT}" \
  --days "${METROPULSE_DAYS:-45}" \
  --seed "${METROPULSE_SEED:-20260611}"
METROPULSE_DB_PATH="${VERIFY_DB}" "${PYTHON_BIN}" -m metropulse.cli status \
  --project-root "${VERIFY_ROOT}"
"${PYTHON_BIN}" -m pytest -q
"${PYTHON_BIN}" -m ruff check src tests
npm --prefix apps/dashboard run check
npm --prefix apps/dashboard run build
