# Real-data scale runs (thiru 13.3M)

Public row-level gateway+settlement pairs do not exist (that pairing is
proprietary PG data), so the pattern is: **one leg real, one leg derived**.
Real amounts, dates, merchants flow through validation → dedup → MERGE;
settlement nets come from an independent loader-side schedule, and
`fee_rates.yaml` v1 is calibrated to the same numbers.

## Sources

- `thiru1711/Financial_Transactions` ([Hugging Face](https://huggingface.co/datasets/thiru1711/Financial_Transactions), 13,305,915 rows, 1.02 GB
  parquet, no login). No license card on the repo — provenance risk accepted
  by maintainer; recorded in SECURITY.md. PII (`card_number`, names,
  addresses, balances) stripped at load, never leaves parquet.
- `belbino/indian-banking-transactions-20192024` (Kaggle, 550K, INR-native):
  **blocked on Kaggle credentials**, not yet ingested.

## Loader (`src/adapters/real_data.py`)

- Raw `date` is TIMESTAMP(NANOS): Spark 3.5 cannot read it
  (`Illegal Parquet type`). One-time clean step → `data/thiru_clean.parquet`
  (126 MB): nanos → `YYYY-MM-DD` string + PII strip.
- Mapping: `transaction_id` (globally unique, verified via `np.unique`;
  regex-clean) → id; `amount*100` → paise brut (USD magnitudes treated as
  notional paise — ledger stays INR-only by treating currency as NULL);
  `card_type` → CREDIT/DEBIT_CARD; `merchant_id` → merchant (74,831 distinct).
- Fee schedule inline (200/100 bps, 18% GST, tol 1) = v1 card exactly.
  Pre-2024 dates resolve to v1 via earliest-card fallback (verified).
- Exception buckets by `sha256(seed|tx) % 100` (deterministic, no RNG state):
  5% mismatch (+50..5000), 2% noise (±1, still MATCHED), 5% orphan (webhook
  withheld), 2% dup row. Negatives/zeros (660k refunds + 10k zeros) are
  counted (`dropped_invalid`) and kept out of the clean file.
- Webhook leg: staging CSV + `seed_real_webhooks()` (Spark-native
  MERGE-DELETE + INSERT, no driver-side 13M list). Pandas-CSV path used
  throughout: Spark Python workers hit `SocketTimeout: Accept timed out` on
  this Windows host, so `createDataFrame` is avoided in the loader.

## Results

| Run | Settlement rows | Deduped | MATCHED | MISSING | LATE | MISMATCH | Match rate |
| --- | --------------- | ------- | ------- | ------- | ---- | -------- | ---------- |
| 1% sample (seed 7) | 128,088 | 125,575 | 112,911 | 6,315 | 6,315* | 6,349 | 89.92% |
| Full (seed 7) | 12,888,397 | 12,635,227 | 11,368,871 | 625,665 | 6,315* | 634,376 | 89.98% |

\* sample: orphans land LATE on first pass (2010–2019 dates ≫ 7d SLA).
Full run: 625,665 MISSING flipped by the late-SLA pass after counts.
Every column tiles exactly (e.g. 11,368,871+625,665+6,315+634,376=12,635,227);
`check_batch_drift` passes.

- Full MERGE end-to-end (CSV read + dedup + MERGE + mart): **105 s for
  12.6M rows** on 15.8 GB RAM box (8g driver / 128 shuffle partitions).
  Default 2g driver OOMs at this scale — scale runs need the override.
- Validation: 12.9M rows clean in 173 s (chunked path). One real catch during
  the work: a $0.005 dust amount + noise −1 produced `settled=0` and was
  quarantined (1/12.9M). Loader now clamps noise to ≥1 (`_noise_delta` +
  regression test).
- Mart `fact_reconciliation` matches webhook counts exactly (112,911 /
  6,315 / 6,349 on the sample).

## Bugs the real run exposed (all fixed)

1. `check_batch_drift` summed `batch_match_rate` (float) with status counts →
   false drift alarm (`125575.8992`). Now sums ints only (`src/pipeline.py`).
2. `_frames[0].unionByName(*_frames[1:])` passes frames into a bool slot
   (pre-existing dirty-tree bug, caught by mypy).
3. Dust-amount clamp above.
4. Bench settlement slice carried dup tx rows → MERGE_CARDINALITY_VIOLATION;
   bench now dedups in setup like prod.
5. Kill-safety: bench results flush per scale (`results_iceberg_real.json`
   checkpoint after each scale) after an aborted 5M run lost its numbers.

## Benchmarks on real data (15.8 GB RAM, 8g driver, 128 shuffles ≥5M)

Full method in `tests/performance/` (`--source real` / `--input` /
`--replay-csv`); combined record in `results_real.json`. Synthetic baselines
untouched (separate files; regression gates stay on synthetic only).

| Suite | Scale | Result |
| --- | --- | --- |
| Iceberg MERGE | 1M, 10% update | 2.99 s write, 94.67% match |
| Iceberg MERGE | 1M, 50% update | 6.21 s write, 94.66% match |
| Iceberg MERGE | 5M, 10% update | 7.20 s write, 94.7% match |
| Iceberg MERGE | 5M, 50% update | 11.38 s write, 94.71% match |
| Iceberg MERGE | 12M, 10% update | 13.12 s write, ~94.7% match |
| Iceberg MERGE | 12M, 50% update | 23.69 s write, 94.71% match |
| Validation (12.6M rows) | pandera / manual / pydantic / polars | 1.44M / 3.63M / 208k / 8.44M rows/s |
| Kafka producer (real ~150 B records) | 1M msgs, acks=all, lz4 | 239,061 msgs/s; serial p50 0.98 ms, p99 20.59 ms |

Notes: match rate ~94.7% (not 90%) because tx-ordered bench slices sample
the mix unevenly; health gate is ≥85%. Kafka latency measured isolated —
the combined run's p50 (50 ms) was post-burst backlog, documented in
`results_kafka_real.json`. Per-scale wall time ≈ 2× the timed MERGE (setup
scans + binpack hygiene); full 3-scale suite ≈ 13 min in one JVM.
