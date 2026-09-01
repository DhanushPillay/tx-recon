.PHONY: up down install clean format lint test bench-kafka bench-pandera bench-all dbt-run dbt-test

# Setup virtual environment and install dependencies
install:
	python -m venv .venv
	.venv/bin/pip install -r requirements.txt

# Start infrastructure
up:
	docker compose up -d

# Stop infrastructure
down:
	docker compose down -v

# Clean compiled python files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Format code
format:
	black src/ tests/ dags/

# Lint code
lint:
	ruff check src/ tests/ dags/

# Run unit tests
test:
	pytest tests/ -m "not integration" --benchmark-disable -v

# Run integration tests (requires docker compose up -d)
test-integration:
	pytest tests/ -m "integration" -v

# Run Kafka producer benchmark
bench-kafka:
	python tests/performance/kafka_producer_benchmark.py --count 1000000

# Run Pandera validation benchmark
bench-pandera:
	python tests/performance/pandas_validation_benchmark.py --rows 1000000

# Run all benchmarks
bench-all:
	python tests/performance/run_benchmarks.py --suite all

# Run dbt models
dbt-run:
	cd dbt_recon && dbt run

# Run dbt tests
dbt-test:
	cd dbt_recon && dbt test
