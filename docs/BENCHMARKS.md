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

Each suite writes a per-suite file (`results_accuracy.json`, `results_pandera_real.json`, `results_iceberg_real.json`, combined `results_real.json`). All suites are real-data only: the orchestrator `tests/performance/run_benchmarks.py` runs kafka replay, pandera validation, and Iceberg MERGE against the thiru CSVs (10k sample fallback when the full files are absent). Treat the per-suite files as the cited source; `results_real.json` is a convenience copy.

Last-verified status (25 Sep 2026): the figures below were measured 25 Sep 2026 on Python 3.11 (full run: unit tests, accuracy gate, kafka replay, pandera validation, MERGE 1M/5M/12M, regression gate, integration). The per-suite JSON files are gitignored, so this prose is the record.

## Accuracy gate (real data)

Correctness first. A fast matcher that categorizes records incorrectly corrupts the ledger.

- **Script:** `tests/performance/recon_accuracy.py`
- **Gate:** `tests/performance/test_recon_accuracy.py` requires `match_rate >= 85%` on the checked-in 10k real sample (`data/samples/real_10k_*.csv`).
- **Method:** Real hooks + settlements joined through `FeeEngine.check_match` (dedup latest-wins mirrors the `WINDOW row_number() ORDER BY settlement_date DESC` in `reconcile.py`). No synthetic breaks, no answer key: the loader mix targets ~90% matched, so a drop below 85% means fee miscalibration or drift.
- **Run:** `python tests/performance/quick_perf.py` or `make accuracy` — or `pytest tests/performance/test_recon_accuracy.py -v`.
- **Result:** `match_rate=94.6%` (8813 matched / 500 mismatched / 492 orphans on 9805 settlement ids). Source: `tests/performance/results_accuracy.json`.

## Kafka producer

Measures sustained throughput and serial flush latency against a local Redpanda.

- **Scripts:** `tests/performance/kafka_producer_benchmark.py` (module), `tests/performance/run_benchmarks.py --suite kafka`
- **Config:** `bootstrap.servers=localhost:19092`, `acks=all`, `compression=lz4`, `linger.ms=20`, `batch 131072`, `queue 2M`, `record_size=real` (real Avro rows ~150 bytes). `WARMUP_MESSAGES=5000`, `LATENCY_SAMPLE=1000`.
- **Throughput:** async produce of `count` messages then `flush`; elapsed via `perf_counter`.
- **Latency:** serial `produce + flush` per message; p50/p95/p99 from sorted latencies.
- **Repro:**
  ```bash
  docker compose up -d redpanda
  python tests/performance/run_benchmarks.py --suite kafka
  ```
- **Measured result (Redpanda `localhost:19092`, 25 Sep 2026, 1M real rows):**
  - Throughput: **225,123 msgs/sec** (async, 1M msgs after 5000 warmup)
  - Ack latency (serial flush, single sample of 1000): **p50 0.65ms / p95 1.11ms / p99 3.97ms**, mean 0.78ms, max 9.15ms
  - Config: `acks=all`, `lz4`, real records. Broker, tuning, and `WARMUP` are part of the claim.

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
- **Measured result (`results_pandera_real.json`, 7 iterations, 25 Sep 2026, full 12.9M-row file):**

  | Path | rows/sec | mean |
  | :--- | :--- | :--- |
  | Manual pandas | 3.34M rows/sec | 3779.09ms |
  | Polars | 9.43M rows/sec | 1339.65ms |
  | Pandera | 763K rows/sec | 16567.25ms |
  | Pydantic (row loop) | 178K rows/sec | 71120.27ms |

  Polars leads; manual pandas is second on lower overhead. Pandera sustains 763K rows/sec on the full file; the declarative cost is covered at the batch boundary. Pydantic is the row-at-a-time reference baseline, not the validation path.

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
- **Measured result single-node, real data (`results_iceberg_real.json`, 8g bench driver via `BENCH_SHUFFLE=1`, 32 partitions, 128 shuffles ≥5M, 28 cores, d2e5f67d5861, 25 Sep 2026):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | match rate | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 1M | 10% | 6.0s | 16,667 | 94,671 | 5,329 | 94.67% | true |
  | 1M | 50% | 13.25s | 37,736 | 473,293 | 26,707 | 94.66% | true |
  | 5M | 10% | 6.83s | 73,206 | 473,293 | 26,707 | 94.66% | true |
  | 5M | 50% | 11.61s | 215,332 | 2,367,662 | 132,338 | 94.71% | true |
  | 12M | 10% | 10.32s | 116,335 | 1,136,314 | 63,686 | 94.69% | true |
  | 12M | 50% | 56.73s | 105,774 | 5,682,415 | 317,585 | 94.71% | true |

  Match rate is stable at ~94.7% across all scales (tx-ordered bench slices sample the loader mix unevenly; the gate is ≥85%). Earlier synthetic baselines (100k–1M, 13–16 Sep) are archived in git history (`results_iceberg.baseline.json`); real-data runs never overwrite them. Operational notes from this run: Spark needs an explicit `PYSPARK_PYTHON` (venv interpreter) on this host, the default 2g driver OOMs at 5M+ (use `BENCH_SHUFFLE=1` for the 8g bench driver), and the 12M warmup needs host RAM headroom (stop idle services first).

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
  128 shuffles ≥5M, 25 Sep 2026): 1M 50% 13.25s, 5M 50% 11.61s, **12M 50% 56.73s**,
  match ~94.7% throughout. Source: `results_iceberg_real.json`.
- **Validation** (`pandas_validation_benchmark.py`, 12.9M rows): polars 9.43M,
  manual 3.34M, pandera 763k, pydantic 178k rows/s. Source:
  `results_pandera_real.json`.
- **Kafka** (`kafka_producer_benchmark.py --replay-csv
  data/real_thiru_full_hooks.csv`, real ~150B records, acks=all, lz4):
  **225,123 msgs/sec**, serial p50 0.65ms / p95 1.11ms / p99 3.97ms. Source:
  `results_kafka_real.json`.
- **Repro:** `python tests/performance/run_benchmarks.py`
  (kafka replay 1M + pandera real + MERGE 1M/5M/12M, ~30–45 min) or each
  module directly (`--suite kafka|pandera|iceberg` selects one).

## PySpark streaming ingestion

- **Script:** `tests/performance/pyspark_ingestion_benchmark.py`
- **Method:** Structured Streaming Kafka -> Iceberg, `count_after - count_before` over 120s after 30s warmup, `maxOffsetsPerTrigger=500000`, `trigger 5s`.
- **Repro:** `python tests/performance/run_benchmarks.py --suite pyspark` (needs live Redpanda/MinIO/Nessie). No fresh local measurement is published in this revision.

## Regression gate

- **Script:** `tests/performance/check_regression.py`
- **Thresholds:** fails the PR when throughput drops more than 15 percent, p99 rises more than 20 percent, iceberg rows/sec drops more than 15 percent, or any real-data `match_rate` falls below 85 percent, versus the baseline `results_real.json`; hardware fingerprint change is a warning, not a failure, so a new machine is not mistaken for a slowdown.
- **Repro:** `python tests/performance/check_regression.py --baseline tests/performance/results_real.json`

## Retired claims

Earlier revisions cited numbers that are no longer reproducible:

- Kafka **142,188 msgs/sec, p99 7.53ms on 2000 messages** from a mixed `acks=1` run with a different warmup method. Current number is the `acks=all` serial-flush measurement above.
- Iceberg **sub-linear scaling to 227K rows/sec at 5M** from single-sample writes with non-monotonic timing. Replaced by seeded median at 100k only.
- Validation footnote about Polars "cold-start overhead" from a run that re-read CSV from disk. Current run validates in-memory.

Per-suite files (`results_accuracy.json`, `results_pandera_real.json`, `results_iceberg_real.json`, `results_kafka_real.json`) are the cited sources. `results_real.json` is an aggregate convenience copy.

## Hardware

`tests/performance/hardware.py` reports `platform, cpu_count, ram_gb, python_version, fingerprint`. Include it when citing numbers.
