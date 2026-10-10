.PHONY: help up down wait install clean format lint typecheck test coverage integration bench accuracy regression pipeline

DATE ?= $(shell python -c "import datetime;print(datetime.date.today())")

help:
	@echo "up/down         - start/stop docker services (redpanda, minio, nessie, trino)"
	@echo "install         - create .venv and install package (run from an activated env)"
	@echo "lint/format     - ruff check/format over src/ tests/ dags/"
	@echo "typecheck       - mypy over src/"
	@echo "test            - unit tests (skip integration)"
	@echo "coverage        - unit tests with 90% branch gate"
	@echo "integration     - live Spark + docker tests (needs 'make up')"
	@echo "accuracy        - real-data accuracy gate (>=85% on 10k sample)"
	@echo "regression      - accuracy gate vs checked-in baseline"
	@echo "bench           - full real-data bench (accuracy + kafka/pandera/iceberg/pyspark)"
	@echo "pipeline        - run daily recon (DATE=YYYY-MM-DD, default today)"
	@echo "clean           - remove pycache/pyc (portable, no Unix find)"

accuracy:
	python tests/performance/quick_perf.py

regression:
	python tests/performance/quick_perf.py
	python tests/performance/check_regression.py --results tests/performance/results_accuracy.json --baseline tests/performance/baseline_real.json

pipeline:
	python -m src.pipeline --date $(DATE)

install:
	python -m venv .venv
	python -m pip install -e ".[dev]"

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

wait:
	python -c "import time,urllib.request;end=time.time()+300\nfor name,url,ok in [('minio','http://localhost:9000/minio/health/live',lambda b:True),('nessie','http://localhost:19120/api/v2/config',lambda b:True),('trino','http://localhost:8080/v1/info',lambda b:b'\"starting\":false' in b)]:\n while True:\n  try:\n   body=urllib.request.urlopen(url,timeout=5).read()\n   assert ok(body),name+' not ready yet';print(name,'ready');break\n  except Exception as e:\n   assert time.time()<end,f'{name} never became ready: {e}';time.sleep(5)"

integration: wait
	pytest tests/ -m "integration" --no-cov --benchmark-disable

bench: accuracy
	python tests/performance/run_benchmarks.py --suite all
