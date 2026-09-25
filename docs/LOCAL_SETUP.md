# Local Setup

How to run tx-recon on your machine.

## Prerequisites

- Python 3.11 (matches CI; 3.10 works but 3.11 is tested)
- Docker and Docker Compose
- Java 17 (JDK) for PySpark

## 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set these three values (S3 writes fail without the access keys):

```bash
MINIO_ROOT_PASSWORD=password
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
```

`MINIO_ACCESS_KEY/SECRET_KEY` must match `MINIO_ROOT_USER/PASSWORD` on a fresh
stack. All other variables have sensible defaults for local development. See `src/common/settings.py` for the full list.

## 2. Start infrastructure

```bash
docker compose up -d
```

Verify all services are healthy:

```bash
docker ps
```

You should see: `redpanda`, `minio`, `nessie`, `trino`, `metabase`, and `minio-createbucket`.

## 3. Create virtual environment

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"
# macOS/Linux: .venv/bin/pip install -e ".[dev]"
```

## 4. Run the accuracy gate

No Docker needed for this step:

```bash
make accuracy
# or: python tests/performance/quick_perf.py
```

This runs the real-data accuracy gate (10k sample, match_rate >= 85%) and writes `tests/performance/results_accuracy.json`.

## 5. Run the full pipeline

```bash
# Real PG file (fail-closed, no synthesis):
python -m src.pipeline --date 2026-09-04
```

The pipeline refuses to run without a real settlement CSV (`FileNotFoundError`); nothing is ever synthesized. A fresh stack with no webhooks table lands every row in `EXCEPTION_MISSING_WEBHOOK`. The validated file is written as `curated_*.csv`; reruns merge from curated, never `quarantine_*`. To fetch a provider FAQ-real file use `data/settlement_YYYYMMDD.csv`.

Additional fail-closed switches (see `src/common/settings.py` and RUNBOOK):

- `REQUIRE_KAFKA_SASL=1` — ingestion refuses `PLAINTEXT` brokers (otherwise forged records are trusted).
- `REQUIRE_SCHEMA_REGISTRY=1` — unreachable registry fails ingestion instead of warn-only drift check.
- `strict_slo=true` (default) — sub-SLO match rate, stale mart, >50% volume drop, >48h-old file fail the batch.
- Secrets via `*_FILE` (`MINIO_SECRET_KEY_FILE`, `WEBHOOK_SECRET_FILE`): mounted file wins over empty env, explicit env wins over file.
- Table layout: `PARTITIONED BY bucket(16, transaction_id)` (new tables only) with `WRITE ORDERED BY transaction_id` so MERGE prunes to file groups.

This validates the settlement (PAN Luhn scan + `sha256` file registry), writes `curated_*.csv` / `quarantine_*.csv` / `.processed_files.json`, and runs the Iceberg MERGE reconciliation (provider batch windows from `config/providers.yaml`, per-provider `late_sla_days`, optional bank third leg via `reconcile_bank_leg`).

## 6. Run tests

```bash
# Unit tests (no Docker required; Spark skipped only when py missing, not on Windows host)
pytest tests/ -m "not integration" -v

# Integration tests (Docker required: MinIO+Nessie+Redpanda)
pytest tests/ -m integration -v

# One date's file registry / PAN guard in isolation:
pytest tests/validation/test_file_registry.py tests/validation/test_pan_guard.py -v
# WAP branch lifecycle (requires Docker for live table ops):
pytest tests/processing/test_wap.py -v
```

## Running benchmarks

```bash
# Accuracy (no infra)
python tests/performance/quick_perf.py

# Pandera validation
python tests/performance/pandas_validation_benchmark.py

# Kafka producer (needs Redpanda)
python tests/performance/kafka_producer_benchmark.py --count 5000

# Iceberg MERGE (needs MinIO + Nessie; scales: 10000 sample, 1000000, 5000000, 12000000)
python tests/performance/run_benchmarks.py --suite iceberg
```

Full benchmark details: [BENCHMARKS.md](BENCHMARKS.md).

## Querying data

Once the pipeline has run, you can query the Iceberg tables via Trino:

```sql
-- Via Metabase (http://localhost:3001) or Trino directly (http://localhost:8080)
SELECT reconciliation_status, COUNT(*)
FROM nessie.db.webhooks
GROUP BY reconciliation_status;
```

Or via PySpark:

```python
from src.common.config import get_spark_session

spark = get_spark_session("Query")
spark.sql("SELECT * FROM nessie.db.webhooks WHERE reconciliation_status != 'MATCHED'").show()
```
