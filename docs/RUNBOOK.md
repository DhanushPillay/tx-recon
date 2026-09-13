# Runbook

Operations, error handling, and troubleshooting for tx-recon.

## Error handling

The pipeline separates bad data from good data rather than failing the entire process.

### Webhook dead-letter queue (DLQ)

The ingestion path (`ingest_webhooks.py`) splits each micro-batch into valid and invalid rows. Invalid rows (corrupt Avro decode, `amount_paise <= 0`, `transaction_id IS NULL`) are appended to the DLQ table (`nessie.db.webhooks_dlq`).

**Querying the DLQ:**

```sql
SELECT * FROM nessie.db.webhooks_dlq ORDER BY ingested_at DESC LIMIT 100;
```

Common causes: schema registry mismatch, producer emitting garbage, Kafka tombstone messages.

### Settlement quarantine

Pandera validates settlement CSVs before they reach Spark. Rows that fail (negative amounts, duplicate `transaction_id`, bad dates, wrong instrument type) are written to `quarantine_YYYYMMDD.csv` and the pipeline raises `SettlementValidationError`.

**To reprocess quarantined rows:** fix the source CSV, delete the quarantine file, and re-run the pipeline for that date.

### MERGE errors

The MERGE wraps all SQL in a `RuntimeError`. Common causes:

- **Duplicate `transaction_id` in the Iceberg target:** the MERGE `ON` clause sees ambiguity. Fix: ensure the ingestion path deduplicates (it does via `dropDuplicates`).
- **Schema mismatch between settlement CSV and Spark schema:** the `bank_schema` in `reconcile.py` defines 12 columns. Extra or missing columns cause parse errors.
- **Nessie/MinIO unreachable:** Spark throws `AnalysisException`. Check `docker ps` and service health.

## Batch drift

`pipeline.py` logs a warning when `settlement_rows_deduped != sum(batch_*)`. This means the MERGE missed rows (NULL guards) or double-counted. Treat as P1: check the dedup window and `WHERE s.transaction_id IS NOT NULL` clause in `reconcile.py`.

The `batch_*` keys in the counts dict scope the MERGE to this batch's settlement IDs (joined back to the target), so a stale cumulative `MATCHED` count cannot mask a failing batch.

## Failure injection (testing)

These scenarios are fixture-driven. No code path in production randomly fails.

### Duplicate webhook

Replay the same `transaction_id` twice in a Kafka micro-batch. Streaming `dropDuplicates` + `MERGE WHEN NOT MATCHED` keeps one row; the duplicate is a no-op. Verify `EXCEPTION_DUPLICATE_WEBHOOK` is never spuriously emitted.

### Duplicate settlement

Add two rows with the same `transaction_id` and different `settlement_date` in one CSV. Pandera quarantines intra-file duplicates via `unique=True`; pre-MERGE `WINDOW row_number() ORDER BY settlement_date DESC` keeps the latest. Check `count(batch_*) == settlement_rows_deduped`.

### Late correction

Generate a day-T file, run MERGE, then re-run with an amendment file carrying the same `transaction_id` with a newer `settlement_date`. The second MERGE updates `bank_ref_id` and status in place. Re-running again is idempotent.

### Fee mismatch

Mutate one settlement `settled_amount_paise` by +500 paise. Expect `batch_EXCEPTION_FEE_MISMATCH` increments by one, `batch_MATCHED` drops by one, `FP==0` preserved.

### Missing webhook (orphan)

Add a settlement `transaction_id` with no matching webhook row. Expect `EXCEPTION_MISSING_WEBHOOK` placeholder inserted. Second run preserves its status (first `WHEN MATCHED` clause) and only refreshes `bank_ref_id`.

Run with: `pytest tests/ -m "not integration" -k "chaos or failure or drift"`

## Replaying data

The pipeline is idempotent. Re-running over the same settlement files converges to identical state.

- **Midway failure:** re-run the script. Successfully processed rows update in place; missed rows insert.
- **Late corrections:** place the updated CSV in `data/` and re-run. MERGE overwrites based on `transaction_id`.

## Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| `docker compose up` fails with `MINIO_ROOT_PASSWORD` | Missing from `.env` | Set `MINIO_ROOT_PASSWORD=password` in `.env` |
| `FileNotFoundError` in pipeline | No settlement CSV in `data/` | Generate one: `python src/generators/settlement_generator.py` |
| `AnalysisException: Table not found` | Nessie namespace/table missing | Run `python -m src.ingestion.ingest_webhooks` to create DDL |
| `AnalysisException: UNRESOLVED_COLUMN s.settlement_date` | Old `build_fee_case_sql` without `settlement_date_col` | Pull latest `main` |
| `ImportError: cannot import name UTC` | Python 3.10 missing `datetime.UTC` | Upgrade to Python 3.11 or set `SPARK_PYTHON` to 3.11 |
| Spark `UnknownHostException spark-master` | Hosts file missing entry | Add `127.0.0.1 spark-master` to hosts file |
| `pyarrow` not found | PySpark CSV read needs Arrow | `pip install pyarrow` |
| Integration tests hang on Ivy | First run downloading jars | Wait; subsequent runs use cached jars |

## Querying data

### Via PySpark

```python
from src.common.config import get_spark_session

spark = get_spark_session("Query")

# Status distribution
spark.sql(
    "SELECT reconciliation_status, COUNT(*) FROM nessie.db.webhooks GROUP BY reconciliation_status"
).show()

# All mismatches
spark.sql(
    "SELECT * FROM nessie.db.webhooks WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'"
).show()

# Orphans (settlement without webhook)
spark.sql(
    "SELECT * FROM nessie.db.webhooks WHERE reconciliation_status = 'EXCEPTION_MISSING_WEBHOOK'"
).show()
```

### Via Trino

```sql
-- Connect to Trino at localhost:8080
SELECT reconciliation_status, COUNT(*)
FROM nessie.db.webhooks
GROUP BY reconciliation_status;
```

### Via Metabase

Open `http://localhost:3001` and connect to Trino (host: `trino`, port: `8080`, catalog: `nessie`, schema: `db`).
