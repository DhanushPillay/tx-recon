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

Pandera validates settlement CSVs before they reach Spark. Rows that fail (negative amounts, bad dates, unknown instrument `UNKNOWN` → `isin` quarantine, non-INR currency, Luhn PAN hit) are written to `quarantine_YYYYMMDD.csv` and the pipeline raises `SettlementValidationError`. Duplicate `transaction_id` rows pass validation by design (`unique=False`, otherwise whole files would quarantine) and are collapsed pre-MERGE by `WINDOW row_number() ORDER BY settlement_date DESC, bank_ref_id/settlement_id ASC` (deterministic tiebreak), counted as `duplicate_settlement_rows`. Content-hash registry records every file's `sha256` in `.processed_files.json` before validation — redelivery warns, it never double-counts.

Validated rows are persisted as `curated_<basename>.csv` (PG-normalized: INR→paise, ISO dates, canonical instruments + `provider` stamp). Reconcile merges from the curated file when present, falling back to raw; `_settlement_source` globs only `settlement_*.csv`/`curated_*.csv`, never `quarantine_*.csv`. When debugging a MERGE, inspect the curated file — it is what Spark actually read.

**To reprocess quarantined rows:** fix the source CSV, delete the quarantine file, and re-run the pipeline for that date.

### MERGE errors

The MERGE wraps all SQL in a `RuntimeError`. Common causes:

- **Duplicate `transaction_id` in the Iceberg target:** the MERGE `ON` clause sees ambiguity. Fix: ensure the ingestion path deduplicates (it does via `dropDuplicates`).
- **Schema mismatch between settlement CSV and Spark schema:** the `bank_schema` in `reconcile.py` defines 12 columns. Extra or missing columns cause parse errors.
- **Nessie/MinIO unreachable:** Spark throws `AnalysisException`. Check `docker ps` and service health.

## Batch drift

`check_batch_drift` raises `RuntimeError` when `settlement_rows_deduped != sum(batch_*)` (excluding the `batch_EXCEPTION_DUPLICATE_SETTLEMENT` diagnostic and float gauges like `batch_match_rate`). This means the MERGE missed rows (NULL guards) or double-counted. `missing_within_lag` is intentionally not `batch_`-prefixed so it never enters the tile. Treat as P1: check the dedup window and `WHERE s.transaction_id IS NOT NULL` clause in `reconcile.py`. Both cron (`run_daily`) and Airflow enforce it, so drift pages instead of logging green.

## Strict mode (fail-closed switches)

Defaults are strict (`strict_slo=true`): a sub-SLO match rate, a stale mart,
a >50% volume drop, or a >48h-old settlement file fails the batch instead of
warning. Real-data conveniences are opt-in and never inherited:

| Env | Effect |
| --- | ------ |
| `ALLOW_DESTRUCTIVE_SEED=1` | real-data webhook seeders may MERGE-DELETE existing rows |
| `REQUIRE_KAFKA_SASL=1` | ingestion refuses `PLAINTEXT` brokers (forged records would be trusted) |
| `REQUIRE_SCHEMA_REGISTRY=1` | unreachable registry fails ingestion instead of disabling drift check |
| `MATCH_RATE_SLO`, `LATE_SLA_DAYS`, `MAINTAIN_RETAIN_LAST` | SLO knobs; malformed values fall back to defaults with a warning |

Secrets follow the `*_FILE` convention (`MINIO_SECRET_KEY_FILE`,
`WEBHOOK_SECRET_FILE`, `KAFKA_SASL_PASSWORD_FILE`): mounted file contents win
over empty env, explicit env values win over files. Unknown PG instrument
strings normalize to `UNKNOWN` and quarantine via the schema `isin` check —
they are never silently priced as `CREDIT_CARD`.

The `batch_*` keys in the counts dict scope the MERGE to this batch's settlement IDs (joined back to the target), so a stale cumulative `MATCHED` count cannot mask a failing batch.

## Failure injection (testing)

These scenarios are fixture-driven. No code path in production randomly fails.

### Duplicate webhook

Replay the same `transaction_id` twice in a Kafka micro-batch. Streaming `dropDuplicates` + `MERGE WHEN NOT MATCHED` keeps one row; the duplicate is a no-op. Verify `EXCEPTION_DUPLICATE_WEBHOOK` is never spuriously emitted.

### Duplicate settlement

Add two rows with the same `transaction_id` and different `settlement_date` in one CSV. Pandera quarantines schema-invalid rows; intra-file duplicates pass validation (`unique=False` by design — uniqueness would quarantine whole files) and pre-MERGE `WINDOW row_number() ORDER BY settlement_date DESC` keeps the latest. Check `count(batch_*) == settlement_rows_deduped`.

### Late correction

Generate a day-T file, run MERGE, then re-run with an amendment file carrying the same `transaction_id` with a newer `settlement_date`. The second MERGE updates `bank_ref_id` and status in place. Re-running again is idempotent.

### Fee mismatch

Mutate one settlement `settled_amount_paise` by +500 paise. Expect `batch_EXCEPTION_FEE_MISMATCH` increments by one, `batch_MATCHED` drops by one, `FP==0` preserved.

### Missing webhook (orphan)

Add a settlement `transaction_id` with no matching webhook row. Expect `EXCEPTION_MISSING_WEBHOOK` placeholder inserted. Second run preserves its status (first `WHEN MATCHED` clause) and only refreshes `bank_ref_id`. A late-arriving webhook heals the placeholder via the ingestion MERGE (`WHEN MATCHED AND status=MISSING THEN UPDATE` amount/status back to the gateway status).

Run with: `pytest tests/ -m "not integration" -k "chaos or failure or drift"`

## Replaying data

The pipeline is idempotent. Re-running over the same settlement files converges to identical state.

- **DLQ replay:** `python scripts/replay_dlq.py --reason "TICKET-123 fix" --approved-by alice [--delete]` replays `webhooks_dlq`
  rows back through validation into the target via MERGE. `--reason` and `--approved-by` are required (maker-checker) and journaled to `data/corrections.jsonl`; `--delete` purges only after a successful replay and journals the purge. Re-run without `--delete` first to verify counts.
- **Quarantine replay:** fix the offending rows in `data/`, delete the
  `quarantine_*.csv` marker, and re-run the pipeline; only validated rows reach
  the MERGE (Write-Audit-Publish). `curated_*.csv` is what Spark actually read — inspect it when debugging a MERGE.
- **Bank leg re-run:** `reconcile_bank_leg(spark, table, bank_df)` is idempotent (`MERGE ... WHEN MATCHED AND status=MATCHED`). Re-run with the correct MT940 + `lag_days` from `config/providers.yaml`; orphans are counted, not statused.
- **SLO/SLA knobs:** `MATCH_RATE_SLO` (default `0.95`) fails the batch in `strict_slo` mode; `LATE_SLA_DAYS` env overrides `config/providers.yaml` per-provider `late_sla_days` (default 7) and flips stale `MISSING_WEBHOOK` placeholders to `LATE_UNRESOLVED`, which the batch MERGE preserves across re-runs. Late marking emits `late_unresolved_marked` + `missing_within_lag` gauges before the MERGE consumes MISSING rows.

## Oncall

Signals come from one JSON line per batch: `metrics run_date=<ds> {"batch_match_rate": ...,
"dlq_depth": ..., "late_unresolved_marked": ..., "late_unresolved_total": ..., "missing_within_lag": ..., "bank_missing": ...}`.
Batch gauges are `batch_*` (scoped to this settlement's ids); `missing_within_lag` is *not* `batch_`-prefixed because it overlaps MISSING and must not enter the drift tile.

- **`batch_match_rate < MATCH_RATE_SLO`** (tolerance breach): in `strict_slo` this raises `RuntimeError` and fails the batch; otherwise it warns. Owner is whoever changed `config/fee_rates.yaml` last (`git log -- config/fee_rates.yaml`). Do NOT widen `tolerance_paise` to silence it — first check whether a PG changed MDR without notice (compare `EXCEPTION_FEE_MISMATCH` rows' instruments against the active card). Escalate to the payments owner if the mismatch persists across two batches.
- **`dlq_depth` growing across batches**: upstream is publishing corrupt rows.
  Inspect `SELECT * FROM nessie.db.webhooks_dlq ORDER BY ingested_at DESC LIMIT 20`,
  fix the producer, then replay with `python scripts/replay_dlq.py --reason ... --approved-by ... --delete`.
  Replay when the cause is fixed; never `--delete` before verifying the replayed rows merge (re-run without the flag first and compare counts).
- **`late_unresolved_marked > 0`**: counterparty webhooks never arrived past the per-provider `late_sla_days` window (`config/providers.yaml: razorpay/cashfree/payu/upi_npci`, env `LATE_SLA_DAYS` overrides all). Page the gateway integrator; these rows are terminal for the batch MERGE and heal only via a late webhook through ingestion (ingestion MERGE heals `MISSING -> gateway_status`). `missing_within_lag` tells whether the MISSING was still within `lag_days` (expected delay) vs truly missing.
- **`bank_missing > 0` / `EXCEPTION_MISSING_BANK_STATEMENT` growing**: gateway pair matched but no bank credit found. Confirm the MT940 was fetched, `lag_days` matches the rail, and `link_tx` extraction ( `TXN\d{6,}` ) isn't missing due to narration truncation.
- **`schema-id drift` warning in ingestion**: writer ids vs registry latest (`gateway_webhooks-value`) differ — producer evolved the Avro schema without updating the pinned `WEBHOOK_AVRO_SCHEMA`. Diff the registry subject against `src/common/schemas.py`; expect a DLQ flood if the change breaks the parse. With `REQUIRE_SCHEMA_REGISTRY=1`, registry unreachable fails ingestion instead of warn-only.
- **`volume shift` / `stale settlement file` warning in validation**: the PG drop shrank >50% vs the previous curated batch (strict mode fails), or the file is >48h old. Confirm the drop is complete before trusting the batch counts; re-pull the file if not.
- **PAN hit**: `validate_and_quarantine` raises with `found[col]=n` — purge the source file, do not hand-scrub; the lake never receives card numbers (last-4 + issuer only).
- **`redelivery` warning**: `file_registry` saw the same `sha256` under a different name — idempotent re-merge, not double count.
- **Backfill (whole day re-run)**: only when the settlement file itself was wrong.
  Replace `data/settlement_<date>.csv`, delete its `quarantine_*.csv` marker if any,
  re-run the pipeline. MERGE converges; do not hand-edit the Iceberg table.
- **Maintenance `status: failed`** in the `Maintenance complete` log: batch is unaffected (maintenance never raises) but `entry["error"]` records why (expired snapshots / orphan removal / binpack / manifests) and file debt grows. Re-run `maintain_tables()` standalone; if binpack keeps failing, check MinIO disk before the next batch. Order is load-bearing: `expire_snapshots -> remove_orphan_files (minAge 3d) -> rewrite_data_files(binpack) -> rewrite_manifests`.

## Maintenance

`maintain_tables()` runs `expire_snapshots(retain_last=7)` → `remove_orphan_files` (minAge 3d, never sweeps in-flight WAP) → `rewrite_data_files(binpack)` → `rewrite_manifests` after each MERGE (see `src/processing/reconcile.py: maintain_tables` for the exact order). If MERGE latency creeps up week over week, check `files_before/after` in the counts log — a growing delta means maintenance is disabled (`MAINTAIN_AFTER_MERGE=0`) or failing. Tune retention with `MAINTAIN_RETAIN_LAST` (lower = faster, less time-travel); malformed values fall back to 7 with a warning. Never-raises does not mean never-seen: each table's `entry["status"]="failed"` + `entry["error"]` is the oncall signal.

- **Midway failure:** re-run the script. Successfully processed rows update in place; missed rows insert.
- **Late corrections:** place the updated CSV in `data/` and re-run. MERGE overwrites based on `transaction_id`; add a WAP branch (`src/processing/wap.py`) when auditing before publish.
- **Orphan surprise:** `remove_orphan_files` without `minAge 3d` would delete in-flight WAP files. The default retains active writes; warn-only with `error` when it fails.

## Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| `docker compose up` fails with `MINIO_ROOT_PASSWORD` | Missing from `.env` | Set `MINIO_ROOT_PASSWORD=password` in `.env` |
| `FileNotFoundError` in pipeline | No settlement CSV in `data/` | Place the real PG file at `data/settlement_YYYYMMDD.csv` (pipeline never synthesizes input) |
| `AnalysisException: Table not found` | Nessie namespace/table missing | Run `python -m src.ingestion.ingest_webhooks` to create DDL |
| `ValueError: run_reconciliation requires date_str` | Date-less `run_daily`/`run_reconciliation` called (would re-merge all history) | Pass `--date YYYY-MM-DD` (single source; `pipeline.py: _resolve_batch_date`) |
| `EXCEPTION_MISSING_BANK_STATEMENT` spiking | MT940 not fetched or `lag_days` wrong | Check `config/providers.yaml` lag, bank file dates, `link_tx` regex |
| `redelivery: ... sha256 matches known` | Same file re-dropped under new name | Expected: idempotent re-merge via file registry |
| `AnalysisException: UNRESOLVED_COLUMN s.settlement_date` | Old `build_fee_case_sql` without `settlement_date_col` | Pull latest `main` |
| `ImportError: cannot import name UTC` | Python 3.10 missing `datetime.UTC` | Upgrade to Python 3.11 or set `SPARK_PYTHON` to 3.11 |
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
