.PHONY: up down install clean format bench-kafka bench-pandera bench-all

# Setup virtual environment and install dependencies
install:
	python -m venv .venv
	.venv\Scripts\pip install -r requirements.txt

# Start infrastructure
up:
	docker-compose up -d

# Stop infrastructure
down:
	docker-compose down -v

# Clean compiled python files
clean:
	Get-ChildItem -Path . -Include __pycache__ -Recurse -Directory | Remove-Item -Recurse -Force
	Get-ChildItem -Path . -Include *.pyc -Recurse -File | Remove-Item -Force

# Format code
format:
	.venv\Scripts\black src/ tests/ dags/

# Run Kafka producer benchmark (default: 1M messages, acks=1, lz4)
bench-kafka:
	python tests/performance/kafka_producer_benchmark.py --count 1000000

# Run Pandera validation benchmark (default: 1M rows)
bench-pandera:
	python tests/performance/pandas_validation_benchmark.py --rows 1000000

# Run all benchmarks (requires docker-compose up -d for kafka/pyspark/iceberg)
bench-all:
	python tests/performance/run_benchmarks.py --suite all
