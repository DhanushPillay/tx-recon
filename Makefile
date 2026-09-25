.PHONY: up down install clean format lint test coverage typecheck integration bench accuracy regression pipeline

accuracy:
	python tests/performance/quick_perf.py

regression:
	python tests/performance/quick_perf.py
	python tests/performance/check_regression.py --results tests/performance/results_accuracy.json --baseline tests/performance/baseline_real.json

pipeline:
	python -m src.pipeline --date 2026-09-04

install:
	python -m venv .venv
	.venv/Scripts/pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down -v

clean:
	python -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__')]; [p.unlink(missing_ok=True) for p in pathlib.Path('.').rglob('*.pyc')]"

format:
	ruff format src/ tests/ dags/

lint:
	ruff check src/ tests/ dags/

typecheck:
	mypy src/ --ignore-missing-imports

test:
	pytest tests/ -m "not integration" -n auto --benchmark-disable

coverage:
	pytest tests/ -m "not integration" -n auto --benchmark-disable --cov=src --cov-branch --cov-report=term-missing:skip-covered --cov-report=xml --cov-fail-under=90

integration:
	pytest tests/ -m "integration" --no-cov --benchmark-disable

bench:
	python tests/performance/run_benchmarks.py --suite iceberg
	python tests/performance/kafka_producer_benchmark.py --count 5000 --acks all --compression lz4
	python tests/performance/pandas_validation_benchmark.py
