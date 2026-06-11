.PHONY: setup pipeline test lint api dashboard verify clean

setup:
	python -m pip install -e ".[dev]"
	cd apps/dashboard && npm run build

pipeline:
	metropulse run --days 45 --seed 20260611

test:
	pytest -q

lint:
	ruff check src tests

api:
	metropulse serve-api --host 127.0.0.1 --port 8000

dashboard:
	cd apps/dashboard && npm run dev

verify: pipeline test lint
	cd apps/dashboard && npm run build

clean:
	rm -f data/raw/*.csv data/warehouse/*.duckdb data/warehouse/*.duckdb.wal
	rm -rf .pytest_cache .ruff_cache apps/dashboard/dist
