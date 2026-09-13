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

Edit `.env` and set `MINIO_ROOT_PASSWORD`. Docker Compose will refuse to start without it (the `:?` syntax in `docker-compose.yml` fails fast).

```bash
MINIO_ROOT_PASSWORD=password
```

All other variables have sensible defaults for local development. See `src/common/settings.py` for the full list.

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
make demo
# or: python tests/performance/quick_perf.py
```

This runs the sealed accuracy harness (2000 rows x 3 seeds) and writes `tests/performance/results_accuracy.json`.

## 5. Run the full pipeline

```bash
python -m src.pipeline --date 2026-09-04
```

This generates synthetic webhooks and a settlement file, validates the settlement, and runs the Iceberg MERGE reconciliation.

## 6. Run tests

```bash
# Unit tests (no Docker required)
pytest tests/ -m "not integration" -v

# Integration tests (Docker required)
pytest tests/ -m integration -v
```

## Local multi-node Spark cluster

For running Iceberg benchmarks at scale, a Docker-based Spark cluster is available:

```bash
# Start the cluster (adds spark-master + 2 workers on top of base services)
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d

# Run benchmarks in cluster mode
SPARK_MODE=cluster python -m tests.performance.reconciliation_benchmark
```

The cluster uses `Dockerfile.spark` (Apache Spark 3.5.1 + Python 3.11 via `uv`). The driver runs on the host, workers run in Docker. The host must resolve `spark-master`, `minio`, `nessie`, and `redpanda` to `127.0.0.1` (add to `/etc/hosts` or `C:\Windows\System32\drivers\etc\hosts`).

Spark master UI: `http://localhost:8082` (Trino owns port 8080).

## Running benchmarks

```bash
# Accuracy (no infra)
python tests/performance/quick_perf.py

# Pandera validation
python tests/performance/pandas_validation_benchmark.py

# Kafka producer (needs Redpanda)
python tests/performance/kafka_producer_benchmark.py --count 5000

# Iceberg MERGE (needs MinIO + Nessie)
python tests/performance/run_benchmarks.py --suite iceberg --scale 100000
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
