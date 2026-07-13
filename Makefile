.PHONY: setup pipeline status test lint api dashboard verify clean

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
METROPULSE = $(PYTHON) -m metropulse.cli

setup:
	$(PYTHON) -m pip install -e ".[dev]"
	npm --prefix apps/dashboard run build

pipeline:
	$(METROPULSE) run --days 45 --seed 20260611

status:
	$(METROPULSE) status

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check src tests

api:
	$(METROPULSE) serve-api --host 127.0.0.1 --port 8000

dashboard:
	npm --prefix apps/dashboard run dev

verify:
	bash scripts/verify.sh

clean:
	rm -f data/raw/*.csv data/warehouse/*.duckdb data/warehouse/*.duckdb.wal data/warehouse/*.duckdb.lock
	rm -rf data/raw/runs
	rm -rf .pytest_cache .ruff_cache apps/dashboard/dist
