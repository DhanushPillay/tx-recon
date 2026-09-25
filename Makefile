.PHONY: up down install clean format lint test bench accuracy pipeline

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
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

format:
	ruff format src/ tests/ dags/

lint:
	ruff check src/ tests/ dags/

test:
	pytest tests/ -m "not integration" -n auto --benchmark-disable

bench:
	python tests/performance/run_benchmarks.py --suite iceberg
	python tests/performance/kafka_producer_benchmark.py --count 5000 --acks all --compression lz4
	python tests/performance/pandas_validation_benchmark.py
