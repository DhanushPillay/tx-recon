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

Nessie is the one without a volume, so the Iceberg catalog it serves does not outlive
its container. After `docker compose down` and up again, MinIO still holds every data
file, but Spark reports `Table not found` until ingest recreates the DDL
(`python -m src.ingestion.ingest_webhooks`) and reconcile repopulates the tables for a
date. `docker compose down -v` also wipes MinIO: that is the reset to reach for when
you want to start from nothing.

## 3. Create virtual environment

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"
# macOS/Linux: .venv/bin/pip install -e ".[dev]"
```

## 4. Run the sample-mix gate (the target is still named `accuracy`)

No Docker needed for this step:

```bash
make accuracy
# or: python tests/performance/quick_perf.py
```

This measures the mix of the checked-in 10k sample (`match_rate >= 0.85`) and writes `tests/performance/results_accuracy.json`. It is not an accuracy gate: nothing in the sample is labelled independently, so it can show the match mix moving and nothing more. The hand-labelled answer key is `tests/fixtures/recon_cases.yaml`.

## 5. Run the full pipeline

```bash
# Settlement file must already exist (fail-closed, no synthesis):
python -m src.pipeline --date 2026-09-04
```

The pipeline refuses to run without a settlement CSV (`FileNotFoundError`); nothing is ever synthesized. On a fresh stack the `webhooks` table does not exist yet and the pipeline fails at WAP setup — seed it first (creates the table; opt-in because it deletes overlapping ids):

```bash
ALLOW_DESTRUCTIVE_SEED=1 python -c "
from src.adapters.real_data import seed_real_webhooks
from src.common.config import get_spark_session
from src.common.settings import get_settings
s = get_settings()
n = seed_real_webhooks(get_spark_session('Seed'), s.webhook_table, 'data/samples/real_10k_hooks.csv', '2026-09-04')
print('seeded', n)"
```

The validated file is written as `curated_*.csv`; reruns merge from curated, never `quarantine_*`. A provider file goes in as `data/settlement_YYYYMMDD.csv`, named for the date the run should read.

Additional fail-closed switches (see `src/common/settings.py` and RUNBOOK):

- `REQUIRE_KAFKA_SASL=1` — ingestion refuses `PLAINTEXT` brokers (otherwise forged records are trusted).
- `REQUIRE_SCHEMA_REGISTRY=1` — an unreachable registry fails ingestion. Schema-id drift itself always raises, since the MERGE would silently drop new producer fields.
- `strict_slo=true` (default) — sub-SLO match rate, stale mart, >50% volume drop fail the batch. A >48h-old settlement file warns only, so backfills are possible.
- Secrets via `*_FILE` (`MINIO_SECRET_KEY_FILE`, `WEBHOOK_SECRET_FILE`, `KAFKA_SASL_PASSWORD_FILE`): mounted file wins over empty env, explicit env wins over file. `WEBHOOK_SECRET` is inert in this repo; see `SECURITY.md`.
- Table layout: `PARTITIONED BY bucket(16, transaction_id)` (new tables only), which colocates the MERGE join keys on both sides.

This validates the settlement (PAN Luhn scan + `sha256` file registry), writes `curated_*.csv` / `quarantine_*.csv` / `.processed_files.json`, and runs the Iceberg MERGE reconciliation on a WAP branch, plus the bank third leg from `data/statement_<YYYYMMDD>.sta` when present. Batch windows come from `config/providers.yaml`; the late threshold is `LATE_SLA_DAYS`. Narrow the run with `WIRED_LEGS=bank` (comma list) to skip legs you do not have inputs for.

## 6. Run tests

```bash
# Unit tests. With the venv active (step 3): a global interpreter starts Spark
# workers in a different Python and they die on the socket, not on your code.
pytest tests/ -m "not integration" -v

# Integration tests (Docker required: MinIO+Nessie+Redpanda)
pytest tests/ -m integration -v

# One date's file registry / PAN guard in isolation:
pytest tests/validation/test_validation_extra.py -k "registry or pan" -v
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
