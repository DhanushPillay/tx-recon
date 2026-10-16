# Runbook

Operations, error handling, and troubleshooting for tx-recon.

## Error handling

The pipeline separates bad data from good data rather than failing the entire process.

### Webhook dead-letter queue (DLQ)

The ingestion path (`ingest_webhooks.py`) splits each micro-batch into valid and invalid rows. Invalid rows (corrupt Avro decode, `amount_paise <= 0`, `transaction_id IS NULL`) are appended to the DLQ table (`nessie.db.webhooks_dlq`).

**Querying the DLQ:**

```sql
SELECT * FROM nessie.db.webhooks_dlq ORDER BY timestamp_utc DESC LIMIT 100;
```

Common causes: schema registry mismatch, producer emitting garbage, Kafka tombstone messages.

### Settlement quarantine

Pandera validates settlement CSVs before they reach Spark. Rows that fail (negative amounts, bad dates, unknown instrument `UNKNOWN` → `isin` quarantine, non-INR currency, Luhn PAN hit) are written to `quarantine_YYYYMMDD.csv` and the pipeline raises `SettlementValidationError`. Duplicate `transaction_id` rows pass validation by design (`unique=False`, otherwise whole files would quarantine) and are collapsed pre-MERGE by `WINDOW row_number() ORDER BY settlement_date DESC, bank_ref_id/settlement_id ASC` (deterministic tiebreak), counted as `duplicate_settlement_rows`. The content-hash registry records a file's `sha256` in `.processed_files.json` only after a batch validates and curates cleanly — a quarantined batch records nothing, so redelivery warns without double-counting.

Validated rows are persisted as `curated_<basename>.csv` (PG-normalized: INR→paise, ISO dates, canonical instruments + `provider` stamp). The curated file is written only after validation and quarantine both pass, so a failed batch leaves nothing for the MERGE to read. Reconcile reads the curated file for the requested date and raises `FileNotFoundError` naming it if it is absent — set `ALLOW_UNVALIDATED_SETTLEMENT=1` to read the raw drop instead. `_settlement_source` globs only `settlement_*.csv`/`curated_*.csv`, never `quarantine_*.csv`. When debugging a MERGE, inspect the curated file — it is what Spark actually read.

**To reprocess quarantined rows:** fix the source CSV, delete the quarantine file, and re-run the pipeline for that date.

### MERGE errors

The MERGE wraps all SQL in a `RuntimeError`. Common causes:

- **Duplicate `transaction_id` in the Iceberg target:** the MERGE `ON` clause sees ambiguity. Fix: ensure the ingestion path deduplicates (it does via `dropDuplicates`).
- **Schema mismatch between settlement CSV and Spark schema:** the `bank_schema` in `reconcile.py` defines 13 columns. Extra or missing columns cause parse errors.
- **Nessie/MinIO unreachable:** Spark throws `AnalysisException`. Check `docker ps` and service health.

## Batch drift

`check_batch_drift` raises `RuntimeError` when `settlement_rows_evaluated != sum(batch_*)` (excluding the `batch_EXCEPTION_DUPLICATE_SETTLEMENT` diagnostic and float gauges like `batch_match_rate`). This means the MERGE missed rows (NULL guards) or double-counted. `missing_within_lag` is intentionally not `batch_`-prefixed so it never enters the tile. Treat as P1: check the dedup window and `WHERE s.transaction_id IS NOT NULL` clause in `reconcile.py`. `run_daily` (`src/pipeline.py`) enforces it for both Airflow and CLI runs, so drift fails the batch instead of logging green. An empty batch logs a warning and returns without the drift or SLO checks.

## Strict mode (fail-closed switches)

Defaults are strict (`strict_slo=true`): a sub-SLO match rate, a stale mart, or a
>50% volume drop fails the batch instead of warning. A stale settlement file warns even
under strict, because an old date is a legitimate reason to backfill that day. Real-data
conveniences are opt-in and never inherited:

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

Replay the same `transaction_id` twice in a Kafka micro-batch. Within-batch `dropDuplicates` drops the second copy; across micro-batches the idempotent `MERGE WHEN NOT MATCHED` keeps one row, so the duplicate is a no-op. Verify `EXCEPTION_DUPLICATE_WEBHOOK` is never spuriously emitted.

### Duplicate settlement

Add two rows with the same `transaction_id` and different `settlement_date` in one CSV. Pandera quarantines schema-invalid rows; intra-file duplicates pass validation (`unique=False` by design — uniqueness would quarantine whole files) and pre-MERGE `WINDOW row_number() ORDER BY settlement_date DESC` keeps the latest. Check `count(batch_*) == settlement_rows_deduped`.

### Late correction

Generate a day-T file, run MERGE, then re-run with an amendment file carrying the same `transaction_id` with a newer `settlement_date`. The second MERGE updates `bank_ref_id` and status in place. Re-running again is idempotent.

### Fee mismatch

Mutate one settlement `settled_amount_paise` by +500 paise. Expect `batch_EXCEPTION_FEE_MISMATCH` increments by one, `batch_MATCHED` drops by one, `FP==0` preserved.

### Missing webhook (orphan)

Add a settlement `transaction_id` with no matching webhook row. Expect `EXCEPTION_MISSING_WEBHOOK` placeholder inserted. Second run preserves its status (first `WHEN MATCHED` clause) and only refreshes `bank_ref_id`. A late-arriving webhook heals the placeholder via the ingestion MERGE, which writes the real amount and sets `PENDING_RECONCILIATION`; the next batch containing that transaction settles the verdict.

These are manual steps against a local stack. To exercise the same paths in the suite, the
relevant modules are `tests/processing/test_reconcile.py` (dedup, drift, MERGE clauses),
`tests/processing/test_idempotency.py` (re-run convergence) and
`tests/ingestion/test_ingest_decode.py` (corrupt-record handling).

## Replaying data

The pipeline is idempotent. Re-running over the same settlement files converges to identical state.

- **DLQ replay:** `python scripts/replay_dlq.py --reason "TICKET-123 fix" --approved-by alice [--delete]` replays `webhooks_dlq`
  rows back through validation into the target via MERGE. `--reason` and `--approved-by` are required (maker-checker) and journaled to `data/corrections.jsonl`; `--delete` purges only after a successful replay and journals the purge. Re-run without `--delete` first to verify counts.
- **Quarantine replay:** fix the offending rows in `data/`, delete the
  `quarantine_*.csv` marker, and re-run the pipeline; only validated rows reach
  the MERGE (Write-Audit-Publish). `curated_*.csv` is what Spark actually read — inspect it when debugging a MERGE.
- **Bank leg re-run:** the leg runs inside `run_reconciliation` after the main MERGE, reading `data/statement_<YYYYMMDD>.sta`. It is idempotent (`MERGE ... WHEN MATCHED AND status=MATCHED`) and skips with a warning when the statement file is absent, so re-running the batch with a corrected MT940 is enough. Orphans are counted, not statused.
- **SLO/SLA knobs:** `MATCH_RATE_SLO` (default `0.95`) fails the batch in `strict_slo` mode; `LATE_SLA_DAYS` (env, default from `config/providers.yaml`) is the single late threshold and flips aged `MISSING_WEBHOOK` / `PENDING_RECONCILIATION` rows to `LATE_UNRESOLVED`. That sweep reads the target table, so it ages rows from earlier batches too. `missing_within_lag` still uses the per-provider `lag_days` from `config/providers.yaml`, because it is a judgement about a specific row's bank. Late marking emits `late_unresolved_marked` + `missing_within_lag` gauges after the sweep commits.

## Oncall

Signals come from one JSON line per batch: `metrics run_date=<ds> {"batch_match_rate": ...,
"dlq_depth": ..., "late_unresolved_marked": ..., "late_unresolved_total": ..., "missing_within_lag": ..., "bank_missing": ...}`.
Batch gauges are `batch_*` (scoped to this settlement's ids); `missing_within_lag` is *not* `batch_`-prefixed because it overlaps MISSING and must not enter the drift tile.

- **`batch_match_rate < MATCH_RATE_SLO`** (tolerance breach): in `strict_slo` this raises `RuntimeError` and fails the batch; otherwise it warns. Settlements whose webhook is still inside the provider's `lag_days` are excluded from the denominator, since a webhook that has not arrived yet is not a mismatch. Owner is whoever changed `config/fee_rates.yaml` last (`git log -- config/fee_rates.yaml`). Do NOT widen `tolerance_paise` to silence it — first check whether a PG changed MDR without notice (compare `EXCEPTION_FEE_MISMATCH` rows' instruments against the active card). Escalate to the payments owner if the mismatch persists across two batches.
- **`dlq_depth` growing across batches**: upstream is publishing corrupt rows.
  Inspect `SELECT * FROM nessie.db.webhooks_dlq ORDER BY timestamp_utc DESC LIMIT 20`,
  fix the producer, then replay with `python scripts/replay_dlq.py --reason ... --approved-by ... --delete`.
  Replay when the cause is fixed; never `--delete` before verifying the replayed rows merge (re-run without the flag first and compare counts).
- **`late_unresolved_marked > 0`** (or `late_sla_failed: 1`, which means the sweep itself raised and the count above is not trustworthy): counterparty webhooks never arrived past the `LATE_SLA_DAYS` window (default 7, env override). Page the gateway integrator; these rows are terminal for the batch MERGE and heal only via a late webhook through ingestion (which sets `PENDING_RECONCILIATION` for a subsequent batch to settle). `missing_within_lag` tells whether the MISSING was still within the per-provider `lag_days` (expected delay) vs truly missing.
- **`bank_missing > 0` / `EXCEPTION_MISSING_BANK_STATEMENT` growing**: gateway pair matched but no bank credit found. Confirm the MT940 was fetched, `lag_days` matches the rail, and `link_tx` extraction ( `TXN\d{6,}` ) isn't missing due to narration truncation.
- **`schema-id drift` RuntimeError from ingestion**: writer ids vs registry latest (`gateway_webhooks-value`) differ — the producer evolved the Avro schema without updating the pinned `WEBHOOK_AVRO_SCHEMA`, and the MERGE would silently drop any new field. Diff the registry subject against `src/common/schemas.py`, add any new field to `_WEBHOOK_COLS` and the table, then restart. The supervisor restarts the query up to 5 times, so this error repeats rather than halting silently. With `REQUIRE_SCHEMA_REGISTRY=1`, registry unreachable also fails ingestion.
- **`volume shift` warning in validation**: the PG drop shrank >50% vs the previous curated batch, including a re-drop of the same date. Strict mode fails the batch on truncation; confirm the drop is complete before trusting the batch counts.
- **`stale settlement file` warning in validation**: the date in the filename is more than 48h old (mtime is only a fallback for unparseable names). This warns even under strict, because backfilling an older date legitimately looks stale.
- **PAN hit**: validation raises with the offending column and row counts. The gate requires a real issuer prefix and a Luhn-valid 13–19 digit run, so a hit is a genuine card number far more often than not. Confirm against the source export before acting, then store last-4 + issuer only. Do not purge the file blind, and do not hand-scrub it either: quarantine the offending rows and re-run.
- **`redelivery` warning**: `file_registry` saw the same `sha256` under a different name — idempotent re-merge, not double count.
- **Backfill (whole day re-run)**: only when the settlement file itself was wrong.
  Replace `data/settlement_<date>.csv`, delete its `quarantine_*.csv` marker if any,
  re-run the pipeline. MERGE converges; do not hand-edit the Iceberg table.
- **Maintenance `status: failed`** in the `Maintenance complete` log: batch is unaffected (maintenance never raises) but `entry["error"]` records why (expired snapshots / orphan removal / binpack / manifests) and file debt grows. Re-run `maintain_tables()` standalone; if binpack keeps failing, check MinIO disk before the next batch. Order is load-bearing: `expire_snapshots -> remove_orphan_files (minAge 3d) -> rewrite_data_files(binpack) -> rewrite_manifests`.

## Maintenance

`maintain_tables()` runs `expire_snapshots` → `remove_orphan_files` (minAge 3d, never sweeps in-flight WAP) → `rewrite_data_files(binpack)` → `rewrite_manifests` after each MERGE (see `src/processing/reconcile.py: maintain_tables` for the exact order). Tables carry `gc.enabled=true` (Nessie defaults it off, which rejects expiry); expiry runs only after the WAP branch is dropped or published. It covers the webhook table, the DLQ, and the mart; the mart keeps `retain_last=1` because it is replaced every batch and older generations are dead weight. Everything else follows `MAINTAIN_RETAIN_LAST` (default 7; malformed values fall back to 7 with a warning). If MERGE latency creeps up week over week, check `files_before/after` in the counts log — a growing delta means maintenance is disabled (`MAINTAIN_AFTER_MERGE=0`) or failing. Never-raises does not mean never-seen: each table's `entry["status"]="failed"` + `entry["error"]` is the oncall signal.

- **Midway failure:** re-run. `run_daily` writes to a WAP branch (`ingest/<date>`) and only fast-forwards to `main` after the drift check passes, so a failed batch leaves `main` untouched and the branch is dropped.
- **Late corrections:** place the updated CSV in `data/` and re-run. `EXCEPTION_FEE_MISMATCH` is deliberately not terminal, so a corrected settlement promotes the row back to `MATCHED` with no operator action; the exception queue self-drains.
- **Orphan surprise:** `remove_orphan_files` without `minAge 3d` would delete in-flight WAP files. The default retains active writes; warn-only with `error` when it fails.

## Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| `docker compose up` fails with `MINIO_ROOT_PASSWORD` | Missing from `.env` | Set `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env` |
| `RuntimeError` on `s3a://` warehouse at startup | `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` empty | Both must be set and must match the MinIO root pair; `.env.example` ships matching placeholders |
| `FileNotFoundError` in pipeline | No settlement CSV in `data/` | Place the settlement file at `data/settlement_YYYYMMDD.csv` (the pipeline never synthesizes input) |
| `AnalysisException: Table not found` | Nessie namespace/table missing, including after a Nessie container recreate: it has no volume, so the catalog goes with the container while MinIO keeps the data files | Run `python -m src.ingestion.ingest_webhooks` to create DDL, then reconcile for the date you want |
| `ValueError: run_reconciliation requires date_str` | `run_reconciliation` called directly without a date (a date-less run would merge all history) | Pass `date_str`, or use `run_daily`, which resolves it from the curated filename or `--date` |
| `FileNotFoundError` naming `curated_settlement_<date>.csv` | Validation never ran for that date, or it quarantined rows | Run validation for the date first; `ALLOW_UNVALIDATED_SETTLEMENT=1` reads the raw drop, but then INR→paise and instrument normalization are skipped |
| `EXCEPTION_MISSING_BANK_STATEMENT` spiking | MT940 not fetched or `lag_days` wrong | Check `config/providers.yaml` lag, bank file dates, `link_tx` regex |
| `redelivery: ... sha256 matches known` | Same file re-dropped under new name | Expected: idempotent re-merge via file registry |
| `AnalysisException: UNRESOLVED_COLUMN s.settlement_date` | Old `build_fee_case_sql` without `settlement_date_col` | Pull latest `main` |
| `ImportError: cannot import name UTC` | Python 3.10 missing `datetime.UTC` | Upgrade to Python 3.11 or set `SPARK_PYTHON` to 3.11 |
| `pyarrow` not found | PySpark CSV read needs Arrow | `pip install pyarrow` |
| Integration tests hang on Ivy | First run downloading jars | Wait; subsequent runs use cached jars |
| `Python worker exited unexpectedly` / `Py4JJavaError` in a Spark test | pytest is not the project venv, so the Python worker it starts is a different interpreter and dies on the socket | Run tests as `.venv/Scripts/python.exe -m pytest ...` (LOCAL_SETUP step 3); a global interpreter reports this, not the real cause |
| Integration tests cannot reach MinIO or Nessie | The Docker daemon is not running, so `docker compose up` did nothing | Start Docker, then `make up`, then wait for the readiness checks before rerunning |

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
