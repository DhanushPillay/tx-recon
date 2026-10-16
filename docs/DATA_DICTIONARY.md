# Data Dictionary

Schema reference for the core data structures in tx-recon. Every column, type, and constraint that flows through the pipeline.

## Webhook table (`nessie.db.webhooks`)

Ingested from Kafka by `src/ingestion/ingest_webhooks.py`. The Avro wire format carries 6 fields; the Iceberg table adds 4 enrichment columns at ingest time and is `PARTITIONED BY bucket(16, transaction_id)` so MERGE join keys are colocated.

| Column | Type | Source | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `transaction_id` | `string` | Avro | no | Primary key. Format `tx_[a-f0-9]{12}`. Bucket key. |
| `amount_paise` | `bigint` | Avro | yes | Transaction amount in paise (1 INR = 100 paise). |
| `gateway_status` | `string` | Avro | yes | Gateway-reported status: `SUCCESS`, `FAILED`, etc. |
| `timestamp_utc` | `string` | Avro | yes | ISO 8601 timestamp of the webhook event (watermarked for dedup state). |
| `merchant_id` | `string` | Avro | yes | Merchant identifier (e.g. `merch_12345`). |
| `processing_run_id` | `string` | Avro | yes | Batch run identifier, if applicable. |
| `reconciliation_status` | `string` | Enriched | yes | Set to `gateway_status` at ingest. Later overwritten by MERGE to one of the terminal statuses (see below). |
| `bank_ref_id` | `string` | Enriched | yes | Bank reference ID from the settlement file. `NULL` until reconciliation populates it. |
| `ingested_at` | `timestamp` | Enriched | yes | Spark `current_timestamp()` at ingest time. |
| `instrument_type` | `string` | Enriched | yes | PG instrument, populated by batch MERGE (canonical `INSTRUMENT_TYPES` + `UNKNOWN` quarantine path). |

### DLQ table (`nessie.db.webhooks_dlq`)

Malformed or invalid webhooks (corrupt Avro decode, `amount_paise <= 0`, or `transaction_id IS NULL`). 6 columns, no enrichment fields.

| Column | Type | Nullable |
| :--- | :--- | :--- |
| `transaction_id` | `string` | yes |
| `amount_paise` | `bigint` | yes |
| `gateway_status` | `string` | yes |
| `timestamp_utc` | `string` | yes |
| `merchant_id` | `string` | yes |
| `processing_run_id` | `string` | yes |

## Settlement CSV (`data/settlement_YYYYMMDD.csv`)

**Derived here, not downloaded.** The gateway leg is the public `thiru1711` dataset, but no public source pairs gateway rows with settlement rows, so the nets come from an independent loader-side schedule (`src/adapters/real_data.py`, `build_real_files`, calibrated to `fee_rates.yaml` v1 — see `docs/REAL_DATA.md`). Full files land under `data/`, the 10k sample is checked in at `data/samples/`. Validated by Pandera (`src/validation/settlement_schema.py`) before reaching Spark. Every row is scanned for PAN (`src/validation/pan_guard.py`, issuer-prefixed Luhn) and the file's `sha256` is recorded in `data/.processed_files.json` (`src/validation/file_registry.py`) only once the batch validates and curates cleanly — same bytes under a different name warn as redelivery, and a quarantined batch records nothing.

### Minimal 5-column format (Generic adapter)

| Column | Type | Nullable | Unique | Constraint | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `bank_ref_id` | `string` | no | yes | | Bank reference / UTR. |
| `transaction_id` | `string` | no | yes | `^tx_[a-f0-9]{12}$` | Matches a webhook `transaction_id`. |
| `settled_amount_paise` | `int` | no | | `> 0` | Net amount deposited. |
| `settlement_date` | `string` | no | | `^\d{4}-\d{2}-\d{2}$` + valid calendar | ISO date of settlement. |
| `instrument_type` | `string` | no | | `IN` (see below) | Payment method used. |

### Canonical 13-column format (adapters)

`src/adapters/settlement.py` normalizes Razorpay, Cashfree, and PayU CSVs into this shape (12 + `provider`). The extra 8 columns are nullable `Int64` or `string` and carry PG-specific detail. `provider` is stamped by the adapter from `detect_adapter` / header heuristics and survives `curated_*` → MERGE.

| Column | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | `string` | PG payment ID mapped to canonical format. |
| `bank_ref_id` | `string` | UTR or settlement reference. |
| `settled_amount_paise` | `Int64` | Net settlement in paise (INR decimal converted via `Decimal`). |
| `settlement_date` | `string` | ISO date. |
| `instrument_type` | `string` | Canonical instrument (see below). Unknown strings normalize to `UNKNOWN` and quarantine, never silently priced as `CREDIT_CARD`. |
| `merchant_id` | `string` | `UNKNOWN` if PG file is single-account. |
| `fee_paise` | `Int64` | PG-reported fee in paise. |
| `gst_paise` | `Int64` | PG-reported tax in paise. |
| `settlement_id` | `string` | PG settlement batch ID (e.g. `setl_...`). |
| `utr` | `string` | Bank UTR, often same as `bank_ref_id`. |
| `currency` | `string` | ISO 4217 code, validated `isin(["INR"])` (NULL → INR at normalization, non-INR → quarantine). |
| `gross_amount_paise` | `Int64` | Pre-fee gross amount in paise. |
| `provider` | `string` | PG identity (`razorpay`/`cashfree`/`payu`/`gateway` fallback) — drives `config/providers.yaml` lag/SLA windows. |

### Bank statement (third leg)

`src/adapters/bank_statement.py` parses MT940 via `mt-940` into a Spark-visible frame (also validated for WAP publish):

| Column | Type | Description |
| :--- | :--- | :--- |
| `bank_ref` | `string` | `customer_reference` / `bank_reference`. |
| `amount_paise` | `int` | Signed paise, `Decimal` HALF_UP → int. The sign comes from `mt940`, which negates on a `D` mark; the parser is called with `reversal_sign` and `case_insensitive_marks` on so `RC` and lowercase `d` are signed too. |
| `value_date` | `string` | `YYYY-MM-DD` from MT940 `:61:` value date. |
| `direction` | `string` | `CREDIT` / `DEBIT`, read off the sign of `amount_paise` so the label cannot contradict the amount. |
| `link_tx` | `string` | `TXN\d{6,}` from narration, or `NULL`. |
| `narration` | `string` | `transaction_details` + `extra_details` (≤256c). |

 Evidence for a settlement is an exact `link_tx = transaction_id` OR (`link_tx IS NULL` AND `ABS(bank.amount - settled) ≤ 1` AND `ABS(DATEDIFF(bank.value_date, settlement_date)) ≤ lag_days`). The amount-and-date arm is open only to a statement that names no transaction, because a linked line is evidence about the transaction it names. Both directions read the same relation: a statement already linked elsewhere, and matched by nothing, is counted as an orphan. See `src/processing/reconcile.py: build_bank_leg_sql` and `_bank_orphan_where`.

## Reconciliation statuses

Written to `reconciliation_status` by the MERGE in `src/processing/reconcile.py`, by the bank leg, and by ingestion. Only `MATCHED` and `EXCEPTION_MISSING_BANK_STATEMENT` are terminal (`src/common/schemas.py: TERMINAL_STATUSES`); the rest can be refreshed by a later run, which is how a corrected settlement promotes a mismatch back to `MATCHED` without an operator.

| Status | Meaning | Set by |
| :--- | :--- | :--- |
| `MATCHED` | Webhook amount minus fee (MDR + GST) equals settled amount within tolerance, *and* bank evidence exists when the third leg is run. | MERGE `WHEN MATCHED` with fee check. |
| `EXCEPTION_FEE_MISMATCH` | Fee check failed: difference exceeds tolerance. Also set when the webhook amount is NULL and the fee cannot be computed at all. | MERGE `WHEN MATCHED` with `> tolerance` (3rd `WHEN MATCHED`), and the 2nd clause for NULL `amount_paise`. |
| `EXCEPTION_MISSING_WEBHOOK` | Settlement received but no matching webhook exists. Placeholder row inserted with `amount_paise = settled_amount` and `gateway_status='UNKNOWN'`. | MERGE `WHEN NOT MATCHED`. |
| `EXCEPTION_MISSING_BANK_STATEMENT` | Gateway pair matched but no independent bank credit found (third leg). Gateway-consistent errors are invisible without this. | Bank leg `MERGE WHEN MATCHED AND status=MATCHED -> UPDATE` (`src/processing/reconcile.py: build_bank_leg_sql`). |
| `EXCEPTION_DUPLICATE_SETTLEMENT` | Multiple settlement rows for the same `transaction_id` in one file. | Defined in `schemas.py`; *not* quarantined (`unique=False`) — collapsed pre-MERGE by `WINDOW row_number() ORDER BY settlement_date DESC, bank_ref_id/settlement_id ASC` (deterministic latest-wins), counted as `duplicate_settlement_rows`. |
| `EXCEPTION_DUPLICATE_WEBHOOK` | Multiple webhooks for the same `transaction_id`. | Defined in `schemas.py`; streaming `dropDuplicates` + watermark prevents this at ingest. |
| `EXCEPTION_INVALID` | Failed schema validation or quarantine (`quarantine_*.csv` / DLQ). | Defined in `schemas.py`; Pandera quarantine + DLQ path. |
| `EXCEPTION_LATE_UNRESOLVED` | `MISSING_WEBHOOK` or `PENDING_RECONCILIATION` older than the `LATE_SLA_DAYS` window (default 7). Terminal for the batch MERGE, which preserves it (status untouched; only `bank_ref_id` and `instrument_type` refresh) so re-runs stay idempotent; only a late-arriving webhook heals it via ingestion MERGE. | Late-SLA sweep in `reconcile.py` (single-table `UPDATE` over the target, so it ages rows from earlier batches too); preserved by the batch MERGE's 1st `WHEN MATCHED` clause, which is shared with `EXCEPTION_MISSING_WEBHOOK`. |
| `EXCEPTION_CURRENCY_MISMATCH` | Both sides exist but name different currencies, so the two paise amounts are not the same unit and no fee comparison is meaningful. | Batch MERGE decision tree, before the tolerance arm (`currency_mismatch_guard` in `reconcile.py`); counted as `batch_EXCEPTION_CURRENCY_MISMATCH`. |
| `EXCEPTION_REVERSAL` | A webhook whose `amount_paise <= 0`: a refund or a reversal. The settlement contract carries no direction field yet, so the row is parked here rather than matched or thrown away. | Ingestion status assignment (`src/ingestion/ingest_webhooks.py`); counted as `table_EXCEPTION_REVERSAL` in the next batch's counts. |
| `PENDING_RECONCILIATION` | A webhook ingested and waiting for its settlement. Not a placeholder: a fresh row still reaches the tolerance arm. | Ingestion, on insert and on a late-arriving webhook's heal; the batch MERGE refreshes these rows rather than overwriting their status. |

## Instrument types

Single source of truth: `src/common/schemas.py:INSTRUMENT_TYPES`. Keys in `config/fee_rates.yaml`. The MERGE generates a `CASE WHEN` per instrument.

| Value | Description |
| :--- | :--- |
| `UPI` | Unified Payments Interface (zero MDR by NPC mandate). |
| `CREDIT_CARD` | Credit card payments. |
| `DEBIT_CARD` | Debit card payments. |
| `NETBANKING` | Internet banking transfers. |
| `WALLET` | Prepaid wallets (PhonePe, Paytm, etc.). |
| `INTERNATIONAL` | Cross-border payments. |

## Fee calculation

All math is integer-only in paise. No floating-point money.

```
fee_before_gst = (amount_paise * mdr_rate_bps + 5000) // 10000
gst_paise      = (fee_before_gst * (gst_on_mdr * 100) + 5000) // 10000
total_fee      = fee_before_gst + gst_paise
net_paise      = amount_paise - total_fee
```

The `//` operator is floor division. The `+ 5000` before `// 10000` implements half-up rounding to the nearest paisa. The same formula appears in Spark SQL via `DIV 10000` in `reconcile.py`.

Worked example: a Rs.1,000 (100,000 paise) credit card payment at 200 bps MDR + 18% GST:

```
fee_before_gst = (100000 * 200 + 5000) // 10000 = 2000   (Rs.20)
gst_paise      = (2000 * 1800 + 5000) // 10000   = 360    (Rs.3.60)
net_paise      = 100000 - 2360                     = 97640  (Rs.976.40)
```

## Versioned rate cards

`config/fee_rates.yaml` supports a `history` list of rate cards with `effective_from` / `effective_to` date ranges. The MERGE uses a versioned `CASE` on `settlement_date` so historical settlements use the rates that were active when they settled.

```
CASE
  WHEN s.settlement_date >= '2025-04-01' THEN <v2 fee CASE>
  ELSE <v1 fee CASE>
END
```

Per-merchant negotiated rates override instrument rates within each card. The MERGE checks `merchant_id` before `instrument_type`.
