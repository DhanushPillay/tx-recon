<div align="center">

<img src="assets/logo.svg" alt="tx-recon logo" width="500"/>

# Transaction Reconciliation

Local lakehouse pipeline that matches payment gateway webhooks against bank settlement files.

[![CI](https://github.com/DhanushPillay/tx-recon/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DhanushPillay/tx-recon/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/DhanushPillay/tx-recon/branch/main/graph/badge.svg)](https://codecov.io/gh/DhanushPillay/tx-recon)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

## What this is

A pipeline that solves one problem: the gateway says it collected Rs.X, the bank says it settled Rs.Y, and the difference should equal the fee (MDR + GST). If it does not, something needs investigation.

**The problem in numbers:** every online payment creates two records, the gateway's webhook ("we collected Rs.1,000") and the bank's settlement file days later ("we settled Rs.976.40"). The gap is the Merchant Discount Rate (MDR, the gateway's cut) plus GST on that fee. Finance teams match these by hand to catch overcharging and delayed closes.

**How tx-recon works:** it streams webhooks into an Iceberg table, validates the daily settlement CSV, and joins the two with an Iceberg MERGE that marks each payment as MATCHED, EXCEPTION_FEE_MISMATCH, or EXCEPTION_MISSING_WEBHOOK.

## Worked example

A Rs.1,000 (100,000 paise) credit card payment at the default rate (`config/fee_rates.yaml`):

```
fee_before_gst = (100000 * 200 + 5000) // 10000 = 2000   (Rs.20, 2% MDR)
gst_paise      = (2000 * 1800 + 5000) // 10000   = 360    (Rs.3.60, 18% GST on fee)
net_paise      = 100000 - 2360                     = 97640  (Rs.976.40)
```

If the settlement file says Rs.976.40, the transaction is `MATCHED`. If it says Rs.970.00, it is `EXCEPTION_FEE_MISMATCH`. If there is no webhook for that transaction, it is `EXCEPTION_MISSING_WEBHOOK`.

## Quickstart

```bash
cp .env.example .env          # set MINIO_ROOT_PASSWORD
docker compose up -d          # redpanda, minio, nessie, trino, metabase
pip install -e ".[dev]"
python -m src.pipeline --date 2026-09-04
```

Then query the reconciled table via Trino (`http://localhost:8080`) or
Metabase (`http://localhost:3001`, add Trino: host `trino`, port `8080`,
catalog `nessie`, schema `db`). Full setup: [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md).

## Architecture

```mermaid
flowchart LR
    W([Gateway]) -->|Avro| K[Redpanda]
    K -->|Structured Streaming| I[(Iceberg webhooks)]
    C([Settlement CSV]) -->|Pandera| V{Valid?}
    V -->|yes| N[Adapter]
    V -->|no| Q[(Quarantine)]
    N --> M[MERGE INTO]
    M --> I
    I --> T[Trino / Metabase]
```

Run modes (0$): `SPARK_MODE=local` (default, single node) · `SPARK_MODE=cluster` (spark:// 1+2) · `SPARK_MODE=yarn` (Hadoop YARN+HDFS, see `docs/HADOOP.md` + `docker-compose.hadoop.yml`). Terraform `infra/terraform/local` (LocalStack) proves cloud IaC without bill; same code deploys to `infra/terraform` (S3+Glue).

3-way overlay examples:
```
docker compose up -d                                   # base (MinIO/Nessie/Redpanda/Trino)
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d   # + standalone 2 workers
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml up -d  # + Hadoop YARN 2 NMs (yarn mode)
SPARK_MODE=yarn bash scripts/spark_submit_yarn.sh client src/pipeline.py
```

Two paths converge on one table. The streaming path ingests webhooks continuously. The batch path merges settlements daily. Both use idempotent MERGE so re-runs are safe.

Full architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Features

- Integer-only paise math in both Python and Spark SQL, no floating-point drift (`src/processing/fee_engine.py`)
- Versioned rate cards with effective-date ranges so historical settlements use the rates that were active when they settled (`config/fee_rates.yaml`)
- Per-merchant negotiated rates that override instrument rates within each card
- Merchant-aware MERGE SQL that checks `merchant_id` before `instrument_type`
- PG settlement adapters that normalize Razorpay, Cashfree, and PayU CSVs to a canonical 12-column shape (`src/adapters/settlement.py`)
- Pandera contract validation with quarantine of bad rows before they reach Spark
- Streaming dedup via `dropDuplicates` + `MERGE WHEN NOT MATCHED` in `foreachBatch`
- Downgrade-only residual scorer with a formal proof that false positives never increase (`docs/PROOF.md`)
- Sealed accuracy harness that injects 7 break classes and asserts F1=1.0 with zero false positives (`tests/performance/recon_accuracy.py`)

## Dashboard

Trino serves the reconciled Iceberg tables; Metabase visualizes them.
BI-ready mart: [sql/marts/fact_reconciliation.sql](sql/marts/fact_reconciliation.sql).

```sql
SELECT status, COUNT(*), SUM(amount_paise) / 100.0 AS rupees
FROM nessie.db.webhooks
GROUP BY status;
```

![Dashboard KPI row](docs/img/dash-kpi-row.png)
![Match rate by status](docs/img/dash-pie.png)

Full tour of all 11 cards: [docs/DASHBOARD.md](docs/DASHBOARD.md).

## Benchmarks

Single-node vs multi-node (Hadoop YARN+HDFS). Full method, hardware fingerprints, and repro commands: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Suite | Result |
| :--- | :--- |
| Accuracy (sealed key) | `min_f1=1.0, FP=0` @ 2000 rows x 3 seeds |
| Kafka producer | **131,887 msgs/sec** async; serial flush p99 1.31ms (acks=all, lz4, 1KB) |
| Validation (Pandera) | **2.66M rows/sec** @ 1M rows (in-memory) |
| Iceberg MERGE (single-node, `SPARK_MODE=local` 28 cores) | **57,582 rows/sec** @ 100k 50% (0.87s), **163,747 rows/sec** @ 1M 50% (3.05s) — `tests/performance/results_iceberg.json` |
| Iceberg MERGE (multi-node, `SPARK_MODE=yarn` 14 cores, `hdfs://namenode:8020/warehouse`) | **23,710 rows/sec** @ 100k 50% (2.11s), **71k rows/sec** @ 1M 50% (7.02s) via `tx-recon-driver:bench` inside `tx-recon_default` — `tests/performance/results_iceberg_yarn_hdfs.json`; YARN +76–191% slower at ≤1M from staging/4096MB NM, wins at 5M+ |

Figures last verified 13–16 Sep 2026 (see `docs/BENCHMARKS.md`); full re-run pending — treat as last-known, not current.

## Project structure

```
src/
  adapters/        PG settlement normalizers (Razorpay, Cashfree, PayU, Generic)
  common/          Settings, Spark session, domain contracts
  generators/      Webhook producer + settlement file generator
  ingestion/       Spark streaming Kafka -> Iceberg (+DLQ)
  processing/      FeeEngine + MERGE reconciliation + residual scorer
  validation/      Pandera schemas + quarantine
  pipeline.py      generate -> validate -> reconcile (cron entrypoint)
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security policy: [SECURITY.md](SECURITY.md).
PR checklist: `.github/PULL_REQUEST_TEMPLATE.md`.

## Roadmap

- Production Kubernetes manifest (the `k8s/` stub was removed; compose is the supported path).

Shipped: materialized `fact_reconciliation` TABLE mart, DLQ replay script
(`scripts/replay_dlq.py`), match-rate SLO warning (`MATCH_RATE_SLO`),
late-resolution SLA (`LATE_SLA_DAYS` → `LATE_UNRESOLVED`).

## License

MIT — see [LICENSE](LICENSE).
