#!/usr/bin/env bash
set -euo pipefail

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python}"
fi

"${PYTHON_BIN}" -m metropulse.cli run --days "${METROPULSE_DAYS:-45}" --seed "${METROPULSE_SEED:-20260611}"
"${PYTHON_BIN}" -m pytest -q
"${PYTHON_BIN}" -m ruff check src tests
npm --prefix apps/dashboard run build
