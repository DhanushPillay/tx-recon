.PHONY: up down install clean format lint test

install:
	python -m venv .venv
	.venv/bin/pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

format:
	black src/ tests/ dags/

lint:
	ruff check src/ tests/ dags/

test:
	pytest tests/ -v
