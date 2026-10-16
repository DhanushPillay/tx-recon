.PHONY: help up down wait install lock clean format lint typecheck test coverage integration bench accuracy regression regression-full pipeline

DATE ?= $(shell python -c "import datetime;print(datetime.date.today())")

help:
	@echo "up/down         - start/stop docker services (redpanda, minio, nessie, trino)"
	@echo "install         - create/update .venv from uv.lock (make lock to refresh it)"
	@echo "lock            - refresh uv.lock from pyproject.toml"
	@echo "lint/format     - ruff check/format over src/ tests/ dags/"
	@echo "typecheck       - mypy over src/"
	@echo "test            - unit tests (skip integration)"
	@echo "coverage        - unit tests with 90% branch gate"
	@echo "integration     - live Spark + docker tests (needs 'make up')"
	@echo "accuracy        - sample-mix check over the 10k sample (not accuracy)"
	@echo "regression      - mix gate vs baseline (mix only)"
	@echo "regression-full - gate kafka + latency + iceberg too (needs 'make bench')"
	@echo "bench           - full real-data bench (mix + kafka/pandera/iceberg/pyspark)"
	@echo "pipeline        - run daily recon (DATE=YYYY-MM-DD, default today)"
	@echo "clean           - remove pycache/pyc (portable, no Unix find)"

accuracy:
	python tests/performance/quick_perf.py

# Mix only. --require says so out loud: the mix harness does not
# produce throughput or latency numbers, and the gate now fails rather than
# warning when a required family is missing from the run.
regression: accuracy
	python tests/performance/check_regression.py --results tests/performance/results_accuracy.json --baseline tests/performance/baseline_real.json --require mix

# Full set. Run `make bench` first so results_real.json has all four suites.
# Comparing against a partially populated artifact fails instead of skipping.
regression-full:
	python tests/performance/check_regression.py --results tests/performance/results_real.json --baseline tests/performance/baseline_real.json --require mix,kafka,kafka-latency,iceberg --hardware-policy fail

pipeline:
	python -m src.pipeline --date $(DATE)

# uv.lock is the record of the resolved set, so install from it rather than
# letting a fresh resolve decide what the tests run against.
lock:
	uv lock

install:
	uv sync --extra dev

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
