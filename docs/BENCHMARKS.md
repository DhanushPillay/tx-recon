# Benchmarks

Method and measured results for the reconciliation pipeline. The summary table also appears in `README.md`; this file is the source of truth and includes repro commands.

## Environment

Two single-node runs on the same machine, same Iceberg 1.11.0 / Nessie 0.107.9 / Spark 3.5.1 JDK 17. The 15-Sep run is the pre-tuning baseline; the 16-Sep run measures the tuning in `761c37c` plus a partition-count fix found during that re-run (see below).

```
Single-node: Windows-10-10.0.26200-SP0, 28 cores, 15.8GB RAM, Python 3.11.14, Fingerprint d2e5f67d5861
YARN/HDFS:   Linux-5.15.153.1-microsoft-standard-WSL2-x86_64-with-glibc2.31, 14 cores, 11.7GB RAM, Python 3.11.16, Fingerprint 7af266c58991
Services: Redpanda, MinIO, Nessie via Docker Compose; Hadoop 3.3.6 (namenode+2NMs 4GB/4vcores) for YARN
Spark: 3.5.1, JDK 17 (host 17.0.13 Temurin, NMs 17.0.15 openjdk)
```

Each suite writes a per-suite file, all gitignored: `results_accuracy.json` (sample mix), `results_pandera_real.json` (validation), `results_iceberg_real.json` (per-scale MERGE, from `reconciliation_benchmark.py` standalone), and the combined `results_real.json` (orchestrator). All suites are real-data only: `tests/performance/run_benchmarks.py` runs kafka replay, pandera validation and Iceberg MERGE against the thiru CSVs, with a 10k sample fallback when the full files are absent. The combined `results_real.json` is the cited source for the figures below; the per-scale `results_iceberg_real.json` holds only the scales that suite was last run at, which is why the six-row table does not come from it. Only `baseline_real.json` is checked in.

Last-verified status (9 Oct 2026): the figures below were re-measured 9 Oct 2026 on the same host (full run: sample mix check on the 10k sample + full 12.6M joined match rate, kafka replay 1M, pandera validation, MERGE 1M/5M/12M, pyspark streaming ingestion, regression gate). Numbers in each section are the 9 Oct run; the 25 Sep 2026 run is kept in git history. The per-suite JSON files are gitignored, so this prose is the record.


## What each number measures

Six kinds of measurement live in this file. They are not interchangeable, and the earlier revision of this document mixed them.

| Kind | What it can tell you | What it cannot |
| :--- | :--- | :--- |
| Public source records | The gateway leg's ids, amounts, dates, merchants and card types are real thiru rows. | Anything about reconciliation accuracy. No public file pairs gateway rows to bank settlement rows, and that pairing is proprietary payment-gateway data. |
| Derived records | Scale, join behaviour, throughput at 1M to 12M rows. | Correctness. The settlement leg is computed by `src/adapters/real_data.py`, not downloaded. |
| Fault injection | That the pipeline routes a known-bad row to a known state. | Hit rates. The injection rate is a plan, not a finding. |
| Functional correctness | The pipeline lands on the hand-written status for every case in `tests/fixtures/recon_cases.yaml`. | Scale. 41 cases prove the decision rules, not throughput. |
| Throughput and latency | Wall time, rows per second, p50/p95/p99 on the stated hardware. | Accuracy. A fast wrong answer is still wrong. |
| End-to-end integration | The services come up and a batch publishes. | Any of the above under load. |

**There is no independently measured reconciliation accuracy in this repository, and there cannot be one.** Paired payment-gateway and bank data does not exist in public. Any figure presented as one would be a measurement of this repository's own generator.

## Correctness: hand-labelled cases (functional)

This is the only correctness number here, and it is pass or fail rather than a percentage.

- **Fixture:** `tests/fixtures/recon_cases.yaml`, 41 cases covering all six instruments, both rate cards, both sides of the tolerance boundary, partial and duplicate settlements, refunds, late arrival, currency conflicts, unknown merchants, and out-of-range or unparseable settlement dates.
- **Every expected value is a literal** typed by hand, with a prose reason for each. None was produced by calling the code under test.
- **Driver:** `tests/test_recon_cases.py` runs the batch MERGE's own arms and expressions as a SELECT. Spark's session catalog refuses MERGE INTO and the Iceberg catalog needs the docker services, so the MERGE itself is exercised in `tests/processing/test_recon_cases_integration.py`, marked `integration`.
- **Reported as a confusion table**, not a rate. `false_positive` and `false_negative` are asserted empty, with the offending transaction ids in the failure message. A single percentage could hide a swap: one row missed, one row wrongly flagged, ratio unchanged.
- **Run:** `pytest tests/test_recon_cases.py -v`

Why the percentage form was dropped: the previous gate reported `match_rate = 94.63%`, and that number is reproducible arithmetically from the generator's own `sha256(seed|tx) % 100` bucket plan, `(196 + 8617) / (9805 - 492) = 0.94631`, matching the recorded result bit for bit. A fee engine that miscalculated every clean row and one that miscalculated none produced the same figure.

## Sample mix check (not accuracy)

`tests/performance/recon_accuracy.py` still runs, and it is worth running, but it measures the fixture rather than the pipeline.

- **Gate:** `tests/performance/test_recon_accuracy.py` requires the checked-in 9,805-row sample to still produce the counts it was checked in with, 8813 matched, 500 mismatched, 492 orphans. It also fails if the mix drifts, so a regenerated fixture fails loudly instead of quietly redefining the gate.
- **Method:** gateway rows joined through `FeeEngine.check_match`; dedup latest-wins mirrors the `row_number() ORDER BY settlement_date DESC` in `reconcile.py`.
- **What it establishes:** the sample still loads, still folds, and the fee engine still agrees with the rate the generator used. That is a smoke test with teeth, not an accuracy measurement.
- **Run:** `python tests/performance/quick_perf.py`, `make mix`, or `pytest tests/performance/test_recon_accuracy.py -v`.
- **Result:** `match_rate=94.63%` (8813 matched / 500 mismatched / 492 orphans on 9805 settlement ids).
- **Correction to the previous revision of this file,** which claimed "No synthetic breaks, no answer key". That was wrong. `src/adapters/real_data.py:44-46` injects breaks by `sha256(seed|tx) % 100`: 5% deliberate mismatch, 2% plus or minus 1 paise noise, 5% orphan, 2% duplicate settlement row. There is an answer key, and the generator is it.
- **Full-file result (9 Oct 2026):** 94.71% on 12,635,227 rows in 244s. Same caveat at larger scale. Source: `tests/performance/results_accuracy_full.json`.

## Which denominator

Two match rates are in circulation for the same batch, and they are not interchangeable:

| Figure | Formula | Value | Used by |
| :--- | :--- | :--- | :--- |
| End-to-end match rate | `MATCHED / settlement_rows_deduped`, orphans in the denominator | **89.98%** (11,368,871 / 12,635,227) | `README.md`, `docs/REAL_DATA.md`, and the shipped `data/metrics_*.json` as `batch_match_rate`, which reads 0.8988 on the 10k sample |
| Joined match rate | `matched / (matched + mismatched)`, orphans excluded | **94.71%** | `recon_accuracy.py` and the benchmark tables below |

A transaction with no gateway row cannot be matched, so including it measures settlement-to-webhook coverage rather than fee correctness. The SLO gate uses the end-to-end figure; the benchmark tables use the joined one. A comparison between the two is meaningless.

## Kafka producer

Measures sustained throughput and serial flush latency against a local Redpanda.

- **Scripts:** `tests/performance/kafka_producer_benchmark.py` (module), `tests/performance/run_benchmarks.py --suite kafka`
- **Config:** `bootstrap.servers=127.0.0.1:19092`, `acks=all`, `compression=lz4`, `linger.ms=20`, `batch 131072`, `queue 2M`, `record_size=real` (real Avro rows ~150 bytes). `WARMUP_MESSAGES=5000`, `LATENCY_SAMPLE=1000`. IPv4 literal, not `localhost`: on hosts where `localhost` resolves to `::1` first the client fails to connect (Redpanda listens IPv4-only); fixed 9 Oct 2026.
- **Throughput:** async produce of `count` messages then `flush`; elapsed via `perf_counter`.
- **Latency:** serial `produce + flush` per message; p50/p95/p99 from sorted latencies.
- **Repro:**
  ```bash
  docker compose up -d redpanda
  python tests/performance/run_benchmarks.py --suite kafka
  ```
- **Measured result (Redpanda `127.0.0.1:19092`, 9 Oct 2026, 1M real rows):**
  - Throughput: **224,237 msgs/sec** (async, 1M msgs after 5000 warmup)
  - Ack latency (serial flush, single sample of 1000): **p50 0.75ms / p95 1.17ms / p99 2.82ms**, mean 0.86ms, max 8.46ms
  - Config: `acks=all`, `lz4`, real records. Broker, tuning, and `WARMUP` are part of the claim. (Prior: 225,123 msgs/sec, p50 0.65ms / p99 3.97ms on 25 Sep 2026 — unchanged within noise.)

## Validation

Compares validation paths at the batch boundary on the same in-memory DataFrame.

- **Script:** `tests/performance/pandas_validation_benchmark.py`
- **Paths:** Pandera (via `settlement_schema`), manual pandas checks, Polars (via `settlement_schema_pl`), and a row-loop Pydantic baseline.
- **Method:** real settlement CSV -> `pd.read_csv` once -> each path validates the same `DataFrame` in memory (Polars also validates in memory; the reported time excludes the single shared CSV load). Full file when present, 10k sample fallback; `iterations=7`.
- **Repro:**
  ```bash
  python tests/performance/pandas_validation_benchmark.py
  # writes tests/performance/results_pandera_real.json
  ```
- **Measured result (`results_pandera_real.json`, 7 iterations, 9 Oct 2026, full 12.9M-row file):**

  | Path | rows/sec | mean |
  | :--- | :--- | :--- |
  | Manual pandas | 2.93M rows/sec | 4311.09ms |
  | Polars | 7.07M rows/sec | 1787.56ms |
  | Pandera | 1.24M rows/sec | 10178.84ms |
  | Pydantic (row loop) | 132K rows/sec | 95445.66ms |

  Polars leads; manual pandas is second on lower overhead. Pandera sustains 1.24M rows/sec on the full file (up from 763k on 25 Sep — same code, host variance); the declarative cost is covered at the batch boundary. Pydantic is the row-at-a-time reference baseline, not the validation path.

## Iceberg MERGE

Measures the `MERGE INTO nessie.db.webhooks` across 1M/5M/12M real rows (10k sample smoke). Iceberg 1.11.0 / Nessie 0.107.9 / Spark 3.5.1 JDK 17 / py 3.11, median of 4 repeats.

- **Script:** `tests/performance/reconciliation_benchmark.py --catalog nessie`, orchestrated by `run_benchmarks.py --suite iceberg`
- **Method:** real thiru hooks + settlements (dedup via `WINDOW row_number()` semi-joined to target ids), fee CASE from live `FeeEngine.get_rate`, `MERGE ... ABS((amount - fee - gst) - settled) <= tolerance`. `rows_per_sec = (matched + mismatched) / median_write_sec`, `healthy = rows_per_sec > 0 and matched > 0`, plus a fail-closed gate: `match_rate < 85%` raises. Timers use `perf_counter` (monotonic). Files via `files_before/files_after`. `s3://lakehouse/warehouse` (S3FileIO). `--hooks-csv` overrides the input (default: full real file, 10k sample fallback).
- **Repro (single-node):**
  ```bash
  docker compose up -d minio nessie
  SPARK_MODE=local .venv/Scripts/python tests/performance/reconciliation_benchmark.py --scale 10000 --catalog nessie
  ```
  Full scales (`--scale 1000000|5000000|12000000`) need `data/real_thiru_full_*.csv` plus an 8g driver at 12M.
- **Measured result single-node, real data (the `iceberg` block of `results_real.json`, 8g bench driver via `BENCH_SHUFFLE=1`, 32 partitions, 128 shuffles ≥5M, 28 cores, d2e5f67d5861, 9 Oct 2026):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | match rate | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 1M | 10% | 3.09s | 32,362 | 94,671 | 5,329 | 94.67% | true |
  | 1M | 50% | 11.21s | 44,623 | 473,293 | 26,707 | 94.66% | true |
  | 5M | 10% | 12.0s | 41,649 | 473,293 | 26,707 | 94.66% | true |
  | 5M | 50% | 19.81s | 126,199 | 2,367,662 | 132,338 | 94.71% | true |
  | 12M | 10% | 18.69s | 64,206 | 1,136,314 | 63,686 | 94.69% | true |
  | 12M | 50% | 34.32s | 174,825 | 5,682,415 | 317,585 | 94.71% | true |

  Match rate is stable at ~94.7% across all scales (tx-ordered bench slices sample the loader mix unevenly; the gate is ≥85%). Throughput note: 1M is faster and 12M/50% much faster than 25 Sep, but both 5M rows are ~43% below it (confirmed in a separate fresh-JVM 5M re-run) — match rates bit-identical, MERGE writes healthy (12–34s), no bench-path code changed since, so this reads as host state, not a code regression; re-check on a quiet box before treating it as real. Earlier synthetic baselines (100k–1M, 13–16 Sep) are archived in git history (`results_iceberg.baseline.json`); real-data runs never overwrite them. Operational notes from this run: Spark needs an explicit `PYSPARK_PYTHON` (venv interpreter) on this host, a 5M+ MERGE needs the 8g driver (now the shipped default; the old 2g/32 pair OOMed), and the 12M warmup needs host RAM headroom (stop idle services first).

### What changed between the two single-node runs, and why

1. **Single-pass fee `CASE` (`src/processing/reconcile.py`, mirrored in the harness).** Two `WHEN MATCHED` arms each evaluating `ABS(...)` became one arm with a `CASE` — one `ABS` eval per row instead of two. Pure CPU saving on the match path.
2. **Write tuning:** `merge-on-read` for data and deletes (rewrites become delete files instead of full rewrites), `snappy` instead of `zstd` (cheaper encode on this host), 64MB target files, `hash` distribution, shuffle partitions 400 -> 32, AQE partition coalescing, settlement side persisted + warmed so the 4 repeats don't recompute the LIMIT + fee math.
3. **Partition-count fix (found by the re-run, not assumed).** `761c37c` raised table-build partitions 4 -> 8 for scales >= 500k, intending bigger files — but tables are built in 50k-row batches, so it doubled file count instead (500k: 40 -> 80 files). The fresh 500k run regressed 20-46% vs baseline, which caught it. Reverted to 4 partitions; with file layout held equal the real optimizations show. Lesson recorded: partition count and batch size multiply — tune total files, not partitions alone.
4. **Read the pattern honestly:** 10%-update MERGEs gained 17-71% (scan-dominated: they read the whole table but rewrite little, so MoR + cheaper CPU wins big). 50%-update MERGEs are flat (write-dominated: rewriting half the table costs what it costs). The tuning moved the bottleneck, it didn't remove writes.

## Real-data benchmarks ([thiru1711/Financial_Transactions](https://huggingface.co/datasets/thiru1711/Financial_Transactions))

13,305,915 public card transactions through the same harnesses with real
amount/date/merchant distributions. Loader (`src/adapters/real_data.py`)
derives settlement nets from an independent schedule calibrated to the v1
rate card; ~10% of rows are exceptions by design (5% mismatch, 5% orphan),
so the health gate here is match rate ≥ 85%. Full method and
per-run tables: `docs/REAL_DATA.md`.

- **MERGE** (`reconciliation_benchmark.py`, 8g bench driver via `BENCH_SHUFFLE=1`,
  128 shuffles ≥5M, 9 Oct 2026): 1M 50% 11.21s, 5M 50% 19.81s, **12M 50% 34.32s**,
  match ~94.7% throughout. Source: the `iceberg` block of `results_real.json`. **Correction:** the previous revision cited `results_iceberg_real.json`, which holds only two of the six labels (5M at 10% and 50%, with write times 12.39 and 22.14) and no 1M or 12M row at all.
- **Validation** (`pandas_validation_benchmark.py`, 12.9M rows): polars 7.07M,
  manual 2.93M, pandera 1.24M, pydantic 132k rows/s. Source:
  `results_pandera_real.json`.
- **Kafka** (`kafka_producer_benchmark.py --replay-csv
  data/real_thiru_full_hooks.csv`, real ~150B records, acks=all, lz4):
  **224,237 msgs/sec**, serial p50 0.75ms / p95 1.17ms / p99 2.82ms. Source:
  the `kafka` block of `results_real.json`.
  **Correction:** the previous revision cited `results_kafka_real.json` for these
  figures. That file holds a different run: 239,061 msgs/sec, p50 0.98, p95
  12.98, p99 20.59, with a note that it was measured isolated while the combined
  run's p50 was 50ms under post-burst backlog. Both runs are real; only one of
  them is the claim.
- **Streaming ingestion** (`pyspark_ingestion_benchmark.py`, Kafka -> Iceberg,
  16 partitions): **1,540,000 rows @ 12,832 rows/sec** sustained over 120s
  (standalone run, 9 Oct 2026). Previously unmeasurable: the bench needed
  Spark 3.5.1 connector pins (settings asked for 3.5.5 against `pyspark==3.5.1`)
  and a `bigint` bench table to match prod.
- **Repro:** `python tests/performance/run_benchmarks.py`
  (kafka replay 1M + pandera real + MERGE 1M/5M/12M, ~30–45 min) or each
  module directly (`--suite kafka|pandera|iceberg` selects one).

## PySpark streaming ingestion

- **Script:** `tests/performance/pyspark_ingestion_benchmark.py`
- **Method:** Structured Streaming Kafka -> Iceberg, `count_after - count_before` over 120s after 30s warmup, `maxOffsetsPerTrigger=500000`, `trigger 5s`.
- **Repro:** `python tests/performance/run_benchmarks.py --suite pyspark` (needs live Redpanda/MinIO/Nessie).
- **Measured result (9 Oct 2026, standalone, 16 partitions):** 1,540,000 rows written in 120.0s = **12,832 rows/sec** sustained, avg batch 10,254ms. Caveat: the `--suite all` record for this suite reads 0.0 rows/sec when the topic backlog drains during the 30s warmup — the stream works (79k rows/s processed), the fixed 120s window just catches an idle period. Treat the standalone number as the claim.

## Regression gate

- **Script:** `tests/performance/check_regression.py`
- **Baseline:** `tests/performance/baseline_real.json`, which is checked in. **Correction:** the previous revision cited `results_real.json` as the baseline; that file is gitignored, is the current run's *output*, and does not exist in a clean checkout, so the repro command as written could not run.
- **Thresholds:** fails when throughput drops more than 15 percent, p95 or p99 rises more than 20 percent, iceberg rows/sec drops more than 15 percent, or the sample mix falls below 85 percent.
- **Required families:** `--require` names the metric families that must be present. A required family the run did not measure is a **failure**, not a warning, so widening the measurement widens the gate. CI passes `--require mix`; `make regression-full` requires all four and fails on a hardware fingerprint change.
- **Hardware fingerprint change** is a warning by default, so a new machine is not mistaken for a slowdown, and a failure under `--hardware-policy fail`.
- **Repro:** `python tests/performance/check_regression.py --results tests/performance/results_accuracy.json --baseline tests/performance/baseline_real.json --require mix`

## Retired claims

Earlier revisions cited numbers that are no longer reproducible:

- Kafka **142,188 msgs/sec, p99 7.53ms on 2000 messages** from a mixed `acks=1` run with a different warmup method. Current number is the `acks=all` serial-flush measurement above.
- Iceberg **sub-linear scaling** from single-sample writes with non-monotonic timing. Replaced by seeded median at 100k only.
- Validation footnote about Polars "cold-start overhead" from a run that re-read CSV from disk. Current run validates in-memory.

Per-suite files (`results_accuracy.json`, `results_pandera_real.json`, `results_iceberg_real.json`, `results_kafka_real.json`) are the cited sources. `results_real.json` is an aggregate convenience copy.

## Hardware

`tests/performance/hardware.py` reports `platform, cpu_count, ram_gb, python_version, fingerprint`. Include it when citing numbers.
