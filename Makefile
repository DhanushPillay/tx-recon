.PHONY: up down install clean format lint test bench demo pipeline-demo

demo:
	python tests/performance/quick_perf.py

pipeline-demo:
	python -m src.pipeline --date 2026-09-04 --demo

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
	python tests/performance/run_benchmarks.py --suite iceberg --scale 100000
	python tests/performance/kafka_producer_benchmark.py --count 5000 --acks all --compression lz4
	python tests/performance/pandas_validation_benchmark.py
