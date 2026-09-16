.PHONY: up down install clean format lint test demo pipeline-demo

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
