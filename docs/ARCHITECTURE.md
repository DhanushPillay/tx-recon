# Architecture

System design for the tx-recon reconciliation pipeline.

## Three legs, one table

tx-recon has three data legs that converge on a single Iceberg table:

1. **Streaming ingestion** (real-time): Webhooks arrive via Kafka, are decoded from Avro, deduplicated, and upserted into the `webhooks` Iceberg table via a `MERGE ... WHEN NOT MATCHED` inside `foreachBatch`. This path runs continuously. Table layout is `PARTITIONED BY bucket(16, transaction_id)`, which colocates MERGE join keys on both sides.

2. **Batch reconciliation** (daily, by provider batch): A settlement CSV arrives, is scanned for PAN (Luhn), de-duplicated by content hash, validated by Pandera, normalized by an adapter that stamps `provider`, and merged by provider batch `{provider}:{batch_date}` against the webhook table via `MERGE INTO ... ON transaction_id` on an Iceberg WAP branch. The branch merges to `main` only on gate pass. This path runs once per day per provider via `pipeline.py` or Airflow.

3. **Bank evidence** (independent truth): Bank statement MT940 files are parsed via the `mt-940` package (never hand-rolled) into `bank_statements` and joined to settlements. MATCHED rows without bank evidence demote to `EXCEPTION_MISSING_BANK_STATEMENT` — without this third leg, a gateway-consistent error is invisible.

```mermaid
%%{init: {"look": "classic", "layout": "dagre", "flowchart": {"curve": "basis", "nodeSpacing": 40, "rankSpacing": 55, "padding": 12, "useMaxWidth": true}}}%%
flowchart TB
  accTitle: Components and publish gate
  accDescr: Streaming ingest and the batch reconciler write to webhooks, bank evidence only demotes, and the publish gate decides whether main advances.

  STREAM["Streaming ingest<br/>Kafka · Avro"]:::src
  BATCH["Batch reconcile<br/>CSV · MERGE"]:::src
  BANK["Bank evidence<br/>MT940"]:::src
  WEB[("webhooks")]:::store
  GATE{{"Publish gate"}}:::dec
  BI["Trino + Metabase<br/>reads main"]:::serve
  DROP["Drop branch<br/>main untouched"]:::fail

  STREAM --> WEB
  BATCH --> WEB
  BANK -.-> WEB
  WEB --> GATE
  GATE --> BI
  GATE --> DROP

  classDef src fill:#F8FAFC,stroke:#94A3B8,color:#1E293B,stroke-width:1px;
  classDef serve fill:#F1F5F9,stroke:#94A3B8,color:#334155,stroke-width:1px;
  classDef store fill:#0F766E,stroke:#0B5F58,color:#FFFFFF,stroke-width:1px;
  classDef dec fill:#FFFFFF,stroke:#475569,color:#0F172A,stroke-width:1px;
  classDef fail fill:#FFFFFF,stroke:#B91C1C,color:#991B1B,stroke-width:1px;
  linkStyle default stroke:#94A3B8,stroke-width:1.2px;
```

Every write is a MERGE. Only two legs may insert; the bank leg has no insert arm, so it can never create a row. Both the bank leg and the residual scorer only ever demote; nothing promotes a row back to MATCHED.

## MERGE outcomes

The batch MERGE in `reconcile.py` evaluates rows in this order: placeholder-preserve, null-amount, currency-mismatch, tolerance (gated on non-terminal status), then `WHEN NOT MATCHED`. The diagram groups the outcomes rather than the order; the order is what decides which arm wins.

```mermaid
%%{init: {"look": "classic", "layout": "dagre", "flowchart": {"curve": "basis", "nodeSpacing": 40, "rankSpacing": 55, "padding": 12, "useMaxWidth": true}}}%%
flowchart TB
  accTitle: MERGE outcomes
  accDescr: Each curated row takes exactly one arm: insert, keep status, exception, no change, or a tolerance update.

  IN["Curated rows"]:::src --> DD["Dedup"]:::proc --> M{"MERGE"}:::dec
  M --> I["No row<br/>missing webhook"]:::out
  M --> K["Placeholder<br/>keep status"]:::out
  M --> E["Amount/currency<br/>exception"]:::out
  M --> N["Terminal<br/>no change"]:::out
  M --> T["Tolerance<br/>MATCHED or fee"]:::out

  classDef src fill:#F8FAFC,stroke:#94A3B8,color:#1E293B,stroke-width:1px;
  classDef proc fill:#F8FAFC,stroke:#94A3B8,color:#1E293B,stroke-width:1px;
  classDef dec fill:#FFFFFF,stroke:#475569,color:#0F172A,stroke-width:1px;
  classDef out fill:#F8FAFC,stroke:#CBD5E1,color:#334155,stroke-width:1px;
  linkStyle default stroke:#94A3B8,stroke-width:1.2px;
```

The arms are: `INSERT` with `gateway_status = UNKNOWN` when the row is absent; keep the placeholder's status and enrich `bank_ref_id` and `instrument_type`; `EXCEPTION_FEE_MISMATCH` for a missing settled amount; `EXCEPTION_CURRENCY_MISMATCH` for differing currencies; no change for terminal rows; and the tolerance case sets `MATCHED` or `EXCEPTION_FEE_MISMATCH`.

First matching clause wins. There is no DELETE arm: a row is never removed. Duplicate settlement rows are counted in `batch_EXCEPTION_DUPLICATE_SETTLEMENT`, not silently merged away.

The fee and tolerance in the tolerance-case arm are computed per-row via a `CASE` expression that selects the correct rate card by instrument type, merchant ID, and settlement date. See "Versioned rate cards" below.

That is the whole batch MERGE: **4 `UPDATE SET` clauses and 1 `WHEN NOT MATCHED` INSERT, no DELETE**. `tests/processing/test_reconcile.py::test_run_reconciliation_wiring` pins exactly that shape.

Rows already marked `LATE_UNRESOLVED` by the late-SLA pass (`LATE_SLA_DAYS`, default 7) hit the shared placeholder-preserve clause first (they are placeholders, not terminal rows), so the batch never demotes them on re-run and stays idempotent; only a late-arriving webhook heals them via the ingestion MERGE. Genuinely terminal rows (`MATCHED`, `MISSING_BANK_STATEMENT`) are frozen by the `NOT IN TERMINAL` guard on the tolerance arm.

## Table layout

`ensure_webhook_table()` (`src/ingestion/ingest_webhooks.py`) creates `PARTITIONED BY bucket(16, transaction_id)` with `merge-on-read` for data/deletes/updates. Bucketing colocates the MERGE join keys so the settlement side and the target side agree on partition count, which cuts the shuffle. It does not prune the target scan: a `MERGE ... ON a = b` equi-condition is a join predicate, and Iceberg only prunes a scan filter translatable via `Expressions.bucket`. An earlier `WRITE ORDERED BY` clause was removed because `requiredOrdering()` sorts only the rows of the write in progress, so it bought a `SortExec` per micro-batch and left the table as a union of independently-sorted runs. The spec applies to new tables only (`IF NOT EXISTS`); migrating an unpartitioned table requires create-new + insert + swap, never in-place alteration.

## Streaming dedup

The ingestion path (`ingest_webhooks.py`) uses `foreachBatch` with a single sink (not dual `toTable`). Each micro-batch:

1. Strips the Confluent wire header (magic byte + schema ID) and carries `schema_id` alongside `fixed_value`.
2. Decodes Avro via `from_avro` with the pinned `WEBHOOK_AVRO_SCHEMA` and compares schema-id against the registry, raising on drift because the MERGE would otherwise silently drop any new producer field.
3. Splits valid (`amount_paise > 0 AND transaction_id IS NOT NULL`) from invalid using `coalesce` to catch `NULL` from corrupt decode (3VL problem).
4. Deduplicates valid rows by `transaction_id` via `dropDuplicates` (watermark `STREAM_WATERMARK_DELAY`, default `1 day`, bounds state).
5. Upserts valid rows via `MERGE INTO ... WHEN NOT MATCHED THEN INSERT (<named columns>)` (with healing of `EXCEPTION_MISSING_WEBHOOK` placeholders when a late webhook arrives). Columns are named explicitly, never `INSERT *`, so a schema addition cannot silently misalign the write.
6. Appends invalid rows to the DLQ table (single guarded write; skipped when the batch has none).
7. Counts late rows (`event_time < now - INTERVAL`) as a side-output warning.

Checkpoint at `warehouse/checkpoints/webhooks_all` gives Spark streaming exactly-once semantics. Registry unreachable disables the drift comparison without blocking ingestion unless `REQUIRE_SCHEMA_REGISTRY=1` (fail-closed). A stream failure is retried by a supervisor loop with exponential backoff (5 attempts, 5s to 300s), so one corrupt message no longer halts ingestion permanently; the checkpoint is derived from the warehouse unless `STREAM_CHECKPOINT_LOCATION` overrides it.

## Versioned rate cards

`config/fee_rates.yaml` contains a `history` list of rate cards, each with `effective_from` and `effective_to` dates. The `FeeEngine` selects the card where the settlement date falls within the range. The SQL builder emits a versioned `CASE`:

```sql
CASE
  WHEN s.settlement_date >= '2025-04-01' THEN <v2 fee CASE>
  ELSE <v1 fee CASE>
END
```

Each version's inner CASE covers merchant overrides first (most specific), then instrument overrides, then the default rate. The same integer-only formula (`(amount * bps + 5000) DIV 10000`) appears in both Python and Spark SQL.

## Merchant-aware MERGE

Per-merchant negotiated rates override instrument rates. The SQL builder generates `WHEN merchant_id = 'X' AND instrument_type = 'Y' THEN ...` clauses before the instrument-only clauses. Settlements without a `merchant_id` get `COALESCE(s.merchant_id, 'UNKNOWN')`.

This means a credit card payment from `merch_001` (180 bps) is reconciled differently from the same instrument at standard rate (200 bps).

## Provider batches

Calendar-date joins create false MISSINGs (cross-midnight cutoffs, holidays, multi-day cycles). Every settlement belongs to a provider batch `{provider}:{batch_date}` defined in `config/providers.yaml` (see `src/processing/batches.py`):

- `lag_days` — webhook..settlement window still considered on-time (used for `within_lag` and the `missing_within_lag` gauge).
- `late_sla_days` — MISSING or `PENDING_RECONCILIATION` rows older than this flip to `LATE_UNRESOLVED`. One global threshold (default 7, overridden by env `LATE_SLA_DAYS`), because the sweep reads the target table and has no provider to read per row. `lag_days` stays per-provider: it is a judgement about a specific row's bank.
- `cadence`/`cutoff` — documents expected drop frequency and rail cut-off (e.g. Razorpay `22:00 Asia/Kolkata`).

The settlement adapter stamps `provider` into the canonical row; `_settlement_source` and the late-SLA MERGE consult `load_providers`/`provider_spec` so each provider's rows are evaluated against its own window. The `missing_within_lag` gauge is computed before the late-SLA MERGE (which consumes MISSING rows) and is excluded from `check_batch_drift`.

## Bank third leg

`src/adapters/bank_statement.py` delegates to the `mt-940` package (typed, fixture-tested) — never hand-rolled SWIFT. `parse_mt940` extracts `bank_ref`, signed `amount_paise` (HALF_UP), `value_date`, `direction`, `link_tx` (from `TXN\d{6,}` in narration), and `narration`. `src/processing/reconcile.py: build_bank_leg_sql/reconcile_bank_leg` joins it after the main MERGE: evidence is an exact `link_tx` match OR (no `link_tx` on the statement AND net within 1 paise AND value date within `lag_days`). The fuzzy arm is open only to an unlinked line, read in both directions: a statement that already names a transaction is not corroborated by some other settlement that happens to share its amount and date, and `_bank_orphan_where` counts orphans through the same relation backwards. MATCHED rows without evidence demote via `MERGE ... WHEN MATCHED AND status=MATCHED THEN UPDATE -> EXCEPTION_MISSING_BANK_STATEMENT`; bank rows matching nothing are counted as orphans (logged, not statused). The DLQ/bank tables share the bucket layout.

## Residual scorer

After the rule-based MERGE, a downgrade-only residual scorer (`src/processing/residual.py`) can demote `MATCHED` rows to `EXCEPTION_FEE_MISMATCH` when the match is suspicious (e.g. merchant-rate disagreement). It never promotes `MISMATCH -> MATCHED`, so false positives are monotone non-increasing. Proven in `docs/PROOF.md`.

The scorer uses a sigmoid on the difference between default-rate expected and actual settled amount. Threshold `tau` (default 0.9) controls sensitivity. On clean real rows the residual is identity: no rows are demoted.

Production scoring runs in SQL: `score_match_sql()` ports the exact curve (merchant-disagreement branch plus sigmoid on the worst excess) reusing the fee/tolerance builders, and `apply_residual` demotes via a single set-based `MERGE` — no driver `collect()`. The pure-Python `score_match` remains the auditable oracle; a golden test pins SQL ≈ Python.

## Iceberg maintenance

`maintain_tables()` runs in dependency order after each successful MERGE (verified order: `expire_snapshots -> remove_orphan_files (minAge 3d, never sweeps in-flight WAP) -> rewrite_data_files(binpack) -> rewrite_manifests`, see `src/processing/reconcile.py: maintain_tables`). Default `retain_last=7` (via `MAINTAIN_RETAIN_LAST`) preserves roughly a week of time-travel while bounding delete-file debt. Each step is logged and recorded per-table (`files_before/after`, `status: failed` with `error` when a statement throws); maintenance never raises — it never fails a batch — but failures are visible in the `Maintenance complete` log and the returned dict. Callers gate on `status` (see RUNBOOK), not on silence.

The 64–128 MB `write.target-file-size-bytes` plus hash distribution keeps file counts bounded; `MAINTAIN_AFTER_MERGE=0` disables the post-MERGE run (bench hygiene runs its own). Tables are created with `gc.enabled=true` — Nessie defaults it off (branches share files), which makes `expire_snapshots` fail; expiry only ever runs after the WAP branch is dropped or published, so no open branch is collected.

## Write-Audit-Publish

The batch boundary enforces a Write-Audit-Publish pattern:

1. **Write:** Pandera validates the settlement CSV against `settlement_schema` (strict types, real calendar dates, `UNKNOWN` instrument fallback → quarantine, not silent pricing).
2. **Audit:** Three guards run before the MERGE:
   - **PAN gate** (`src/validation/pan_guard.py`): Luhn-checked scan of free-text/ID columns fails the batch if a card number is present — one PAN puts the whole lake in PCI scope.
   - **File registry** (`src/validation/file_registry.py`): `sha256` of raw bytes recorded in `.processed_files.json`; redelivery under a different name is flagged (idempotent re-merge, not double count).
   - **Quarantine:** Invalid rows isolated to `quarantine_YYYYMMDD.csv`; the pipeline raises `SettlementValidationError` and stops. Validated rows persist as `curated_<basename>.csv` (PG-normalized: INR→paise, ISO dates, canonical instruments + `provider`). `curated_*` survives into the MERGE; `quarantine_*` never merges (`_settlement_source` globs only `settlement_*`/`curated_*`).
3. **Publish:** Only validated, PAN-clean rows on a WAP branch reach Spark for the MERGE. See WAP below.

This prevents corrupt data (negative amounts, duplicate IDs, bad dates, PAN) from entering the Iceberg table.

## WAP branches

`src/processing/wap.py` implements Iceberg-native branching (not Nessie-level branching — Nessie Spark-SQL extensions speak API v1 while the server is v2, so Nessie `CREATE BRANCH` statements fail here, verified live):

- `enable_wap(table)`: `ALTER TABLE ... SET TBLPROPERTIES ('write.wap.enabled'='true')` — one-time per table, the property persists.
- `use_branch(spark, branch)`: `spark.conf.set("spark.wap.branch", "ingest/YYYY-MM-DD")` — routes subsequent writes to the branch. Conf alone leaks to `main` (verified); both the property and the conf are required for isolation.
- `create_branch` / `drop_branch`: `ALTER TABLE ... CREATE/DROP BRANCH IF EXISTS <branch>` (idempotent).
- `merge_branch`: `CALL catalog.system.fast_forward(table=>'db.webhooks', branch=>'main', to=>'ingest/...')` — publish only after validation + match-rate gates pass on the branch. Failed validation leaves the branch dropped, `main` untouched. Branches are short-lived (`ingest/YYYY-MM-DD`, TTL 3 days).

## Fail-closed switches

Defaults are strict (`src/common/settings.py`): `strict_slo=true` (sub-SLO match rate, stale mart, volume truncation fail the batch; a stale settlement file only warns, since backfilling an older date legitimately looks stale), `REQUIRE_KAFKA_SASL`/`REQUIRE_SCHEMA_REGISTRY` refuse plaintext/trust-on-bad-registry, `ALLOW_DESTRUCTIVE_SEED` must be set for real-data webhook seeding (MERGE-DELETE). Secrets follow `*_FILE` (mounted file wins over empty env, explicit env wins over file). Unknown PG instruments normalize to `UNKNOWN` and quarantine via the schema `isin` check — never silently priced as `CREDIT_CARD`.

## Integer paise

All financial amounts are integers (paise). No floating-point money. The formula `(amount * bps + 5000) // 10000` uses floor division with half-up rounding, and the same arithmetic appears in both Python (`fee_engine.py`) and Spark SQL (`reconcile.py: DIV 10000`). This eliminates the rounding drift that affects float-based fee calculations.

## Corrections

The MERGE mutates statuses in place by design; the audit counterweight is append-only. `src/processing/corrections.py: journal_correction` requires `reason` + `approved_by` (maker-checker) and appends to `data/corrections.jsonl`. `scripts/replay_dlq.py` now requires `--reason`/`--approved-by` and journals the purge after a successful replay. Every manual money mutation recorded, no entry without two eyes.

## Component mapping

| Local (Docker) | Production equivalent |
| :--- | :--- |
| Redpanda | Amazon MSK / Confluent Cloud |
| MinIO | Amazon S3 / GCS |
| Nessie (API v2, Spark extensions v1) | AWS Glue Data Catalog / Nessie REST (Iceberg-native branches preferred) |
| PySpark single node (`local[*]`) | Amazon EMR (single-node dev) / Dataproc |
| `pipeline.py` cron | MWAA / Cloud Composer |
| Trino + Metabase | Athena / Looker |
| `mt-940` (bank leg) | SWIFT MT940 / camt.053 parsers (never hand-rolled) |

## Project structure

```
src/
  adapters/        PG settlement normalizers (Razorpay/Cashfree/PayU/Generic) + bank_statement (MT940) + real_data (thiru 13.3M)
  common/          Settings (strict flags, *_FILE secrets), Spark session, domain contracts
  ingestion/       Spark streaming Kafka -> Iceberg (+DLQ, schema-id drift, late-data, bucket 16)
  processing/      FeeEngine + MERGE reconciliation (4×UPDATE/1×INSERT) + residual scorer + batches (provider windows) + wap (branches) + corrections (journal)
  validation/      Pandera schemas + quarantine + pan_guard (Luhn) + file_registry (sha256)
  pipeline.py      validate (PAN/registry/curated) -> reconcile (bank leg, WAP branch, metrics) -> metrics_<YYYYMMDD>.json + maintenance (real data only, fail-closed)
config/
  fee_rates.yaml   versioned rate cards (effective_from/to, merchant overrides, instruments)
  providers.yaml   per-provider lag_days / cutoff / cadence, plus the global late_sla_days
sql/marts/         fact_reconciliation mart (migrations/V* is source of truth)
```
