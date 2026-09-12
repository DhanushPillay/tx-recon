# Benchmarks

Method and measured results for the reconciliation pipeline. The summary table also appears in `README.md`; this file is the source of truth and includes repro commands.

## Environment

Measured September 2026.

```
Hardware: Windows-10-10.0.26200-SP0, 28 cores, 15.8GB RAM
Python: 3.11.14
Fingerprint: d2e5f67d5861 (sha1 of platform + cores + RAM, see tests/performance/hardware.py)
Services: Redpanda, MinIO, Nessie via Docker Compose
Spark: 3.5.1, JDK 17
```

Each suite writes a per-suite file (`results_accuracy.json`, `results_pandera.json`, `results_iceberg.json`). The orchestrator `tests/performance/run_benchmarks.py` aggregates them into `tests/performance/results.json`. Treat the per-suite files as the cited source; `results.json` is a convenience copy.

## Accuracy harness (sealed key)

Correctness first. A fast matcher that categorizes records incorrectly corrupts the ledger.

- **Script:** `tests/performance/recon_accuracy.py`
- **Gate:** `tests/performance/test_recon_accuracy.py` requires `precision == recall == F1 == 1.0`, `FP == 0`, `fanout == 1`.
- **Method:** Synthetic webhooks and settlements with a fixed break mix: 70% EXACT, 10% ROUNDING, 5% FEE_MISMATCH, 5% ORPHAN, 5% DUPLICATE, 2.5% OUT_OF_ORDER, 2.5% LATE_CORRECTION. Dedup by `seq` mirrors the `WINDOW row_number() ORDER BY settlement_date DESC` in `reconcile.py`. The answer key (`_expected_net_raw`) reads `config/fee_rates.yaml` directly and never imports `FeeEngine`.
- **Run:** `python tests/performance/quick_perf.py` or `make demo` — 2000 rows x 3 seeds (7, 42, 123) — or `pytest tests/performance/test_recon_accuracy.py -v`.
- **Result:** `min_f1=1.0`, `max_false_positives=0`, per-class recall `1.0` across `MATCHED / FEE_MISMATCH / MISSING_WEBHOOK`. Source: `tests/performance/results_accuracy.json`.

## Kafka producer

Measures sustained throughput and serial flush latency against a local Redpanda.

- **Scripts:** `tests/performance/kafka_producer_benchmark.py` (module), `tests/performance/run_benchmarks.py --suite kafka`
- **Config:** `bootstrap.servers=localhost:19092`, `acks=all`, `compression=lz4`, `linger.ms=20`, `batch 131072`, `queue 2M`, `record_size=1024` (real Avro is about 150 bytes; 1KB pads with filler `x * 904`). `WARMUP_MESSAGES=5000`, `LATENCY_SAMPLE=1000`.
- **Throughput:** async produce of `count` messages then `flush`; elapsed via `perf_counter`.
- **Latency:** serial `produce + flush` per message; p50/p95/p99 from sorted latencies.
- **Repro:**
  ```bash
  docker compose up -d redpanda
  python tests/performance/kafka_producer_benchmark.py --count 5000 --acks all --compression lz4
  python tests/performance/run_benchmarks.py --suite kafka --kafka-count 5000 --kafka-acks all
  ```
- **Measured result (Redpanda `localhost:19092`, 12 Sep 2026):**
  - Throughput: **135,091 msgs/sec** (async, 5000 msgs after 5000 warmup)
  - Ack latency (serial flush, single sample of 1000): **p50 0.76ms / p95 1.07ms / p99 1.72ms**, mean 0.82ms, max 8.4ms
  - Config: `acks=all`, `lz4`, 1KB records. Broker, tuning, and `WARMUP` are part of the claim.

## Validation

Compares validation paths at the batch boundary on the same in-memory DataFrame.

- **Script:** `tests/performance/pandas_validation_benchmark.py`
- **Paths:** Pandera (via `settlement_schema`), manual pandas checks, Polars (via `settlement_schema_pl`), and a row-loop Pydantic baseline.
- **Method:** `generate_settlement_file` -> `pd.read_csv` once -> each path validates the same `DataFrame` in memory (Polars also validates in memory; the reported time excludes the single shared CSV load). `rows` in `[10_000, 100_000, 1_000_000, 10_000_000]`, `iterations=7`, seeded.
- **Repro:**
  ```bash
  python tests/performance/pandas_validation_benchmark.py
  # writes tests/performance/results_pandera.json
  ```
- **Measured result (`results_pandera.json`, 7 iterations, seed 7, 12 Sep 2026):**

  | Rows | Manual pandas | Polars | Pandera | Pydantic |
  | :--- | :--- | :--- | :--- | :--- |
  | 10,000 | 7.7M rows/sec (1.29ms, std 0.86) | 2.2M rows/sec (4.38ms, std 7.94) | 1.0M rows/sec (9.86ms, std 8.41) | 470K rows/sec (21.26ms) |
  | 100,000 | 11.7M rows/sec (8.54ms) | 19.1M rows/sec (5.22ms) | 2.7M rows/sec (36.92ms) | 511K rows/sec (195.59ms) |
  | 1,000,000 | 8.5M rows/sec (116.93ms) | 19.0M rows/sec (52.59ms) | 2.8M rows/sec (355.47ms) | 529K rows/sec (1891.23ms) |
  | 10,000,000 | 2.4M rows/sec (4016.12ms) | 8.7M rows/sec (1145.58ms) | 1.5M rows/sec (6603.35ms) | 478K rows/sec (20920.47ms) |

  Manual pandas leads below 100k rows due to lower overhead. Polars leads at 100k and above. Pandera sustains 1.0-2.8M rows/sec across scales; the declarative cost is covered at the batch boundary. The old footnote about Polars "cold-start overhead on the first run" no longer applies — the current run validates from a shared in-memory frame.

## Iceberg MERGE

Measures the `MERGE INTO nessie.db.webhooks` at 100k rows. Larger scales are not published until remeasured with repeats.

- **Script:** `tests/performance/reconciliation_benchmark.py`, orchestrated by `run_benchmarks.py --suite iceberg`
- **Method:** seeded synthetic webhooks and settlements, dedup via `WINDOW row_number()`, fee CASE generated from live `FeeEngine.get_rate`, `MERGE` with tolerance `ABS((amount - fee - gst) - settled) <= 1`. Each scale runs 4 repeats; reported write time is the median. `rows_per_sec = (matched + mismatched) / median_write_sec`, `healthy = rows_per_sec > 0 and matched > 0`.
- **Scales:** `--scale` selects rows; default `100k` with 10% and 50% update. No warmup; file counts via `files_before/files_after`.
- **Repro:**
  ```bash
  docker compose up -d minio nessie
  python tests/performance/run_benchmarks.py --suite iceberg --scale 100000
  # or directly
  SPARK_MODE=local python -m tests.performance.reconciliation_benchmark
  ```
- **Measured result (`results_iceberg.json`, SPARK_MODE=local, 4 repeats, median):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | files | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 100k | 10% | 1.28s | 7,822 | 10,000 | 0 | 8 -> 1 | true |
  | 100k | 50% | 1.36s | 36,887 | 50,000 | 0 | 1 -> 1 | true |

  `SPARK_MODE=local` was required on this host; a multi-node Spark run is not yet measured.

## PySpark streaming ingestion

- **Script:** `tests/performance/pyspark_ingestion_benchmark.py`
- **Method:** Structured Streaming Kafka -> Iceberg, `count_after - count_before` over 120s after 30s warmup, `maxOffsetsPerTrigger=500000`, `trigger 5s`.
- **Repro:** `python tests/performance/run_benchmarks.py --suite pyspark` (needs live Redpanda/MinIO/Nessie). No fresh local measurement is published in this revision.

## Regression gate

- **Script:** `tests/performance/check_regression.py`
- **Thresholds:** fails the PR when throughput drops more than 15 percent or p99 rises more than 20 percent versus the baseline `results.json`; hardware fingerprint change is a warning, not a failure, so a new machine is not mistaken for a slowdown.
- **Repro:** `python tests/performance/check_regression.py --baseline tests/performance/results.json`

## Retired claims

Earlier revisions of `README.md` cited:

- Kafka **142,188 msgs/sec, p50 0.78ms/p95 4.03ms/p99 7.53ms on 2000 messages** — from a run that mixed `acks=1` and a different warmup/latency method. The current published number is the `acks=all` serial-flush measurement above.
- Iceberg **100k-5M table with sub-linear scaling to 227K rows/sec at 5M** — single-sample writes with non-monotonic timing (1M/50% 10.66s slower than 2M/50% 5.49s) and zero-matched runs reported as `0.0 rows/sec`. Replaced by the seeded median healthy run at 100k only.
- Validation footnote claiming Polars "cold-start overhead" — stale; the fair run seeds and validates in memory.

The old `tests/performance/results.json` blob mixing those numbers is no longer cited. Per-suite files are the source; regenerate via `run_benchmarks.py` with small counts to refresh `results.json`.

## Hardware

`tests/performance/hardware.py` reports `platform, cpu_count, ram_gb, python_version, fingerprint`. Include it when citing numbers.
