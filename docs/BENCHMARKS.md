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
- **Measured result (Redpanda `localhost:19092`, 13 Sep 2026):**
  - Throughput: **131,887 msgs/sec** (async, 5000 msgs after 5000 warmup)
  - Ack latency (serial flush, single sample of 1000): **p50 0.73ms / p95 0.94ms / p99 1.31ms**, mean 0.77ms, max 10.06ms
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
- **Measured result (`results_pandera.json`, 7 iterations, seed 7, 13 Sep 2026):**

  | Rows | Manual pandas | Polars | Pandera | Pydantic |
  | :--- | :--- | :--- | :--- | :--- |
  | 10,000 | 8.4M rows/sec (1.19ms, std 0.14) | 4.4M rows/sec (2.25ms, std 0.85) | 439K rows/sec (22.77ms, std 43.51) | 371K rows/sec (26.95ms) |
  | 100,000 | 10.4M rows/sec (9.55ms, std 0.55) | 15.8M rows/sec (6.31ms, std 1.28) | 2.57M rows/sec (38.82ms, std 2.25) | 415K rows/sec (240.87ms) |
  | 1,000,000 | 7.9M rows/sec (125.32ms, std 3.75) | 14.7M rows/sec (67.58ms, std 9.28) | 2.66M rows/sec (375.64ms, std 3.54) | 412K rows/sec (2425.59ms) |
  | 10,000,000 | 2.6M rows/sec (3832.9ms, std 52.09) | 9.0M rows/sec (1108.64ms, std 103.43) | 1.53M rows/sec (6522.49ms, std 132.34) | 369K rows/sec (27105.93ms) |

  Manual pandas leads below 100k rows due to lower overhead. Polars leads at 100k and above. Pandera sustains 1.5-2.66M rows/sec at 100k+ scales; the declarative cost is covered at the batch boundary. The old footnote about Polars "cold-start overhead on the first run" no longer applies — the current run validates from a shared in-memory frame.

## Iceberg MERGE

Measures the `MERGE INTO nessie.db.webhooks` (and `nessie_hdfs.db.webhooks` for YARN) across 100k/500k/1M. Iceberg 1.11.0 / Nessie 0.107.9 / Spark 3.5.1 JDK 17 / py 3.11, median of 4 repeats. See `HADOOP.md` for YARN wiring.

- **Script:** `tests/performance/reconciliation_benchmark.py --catalog nessie|nessie_hdfs`, orchestrated by `run_benchmarks.py --suite iceberg`
- **Method:** seeded synthetic webhooks and settlements, dedup via `WINDOW row_number()`, fee CASE from live `FeeEngine.get_rate`, `MERGE ... ABS((amount - fee - gst) - settled) <= 1`. `rows_per_sec = (matched + mismatched) / median_write_sec`, `healthy = rows_per_sec > 0 and matched > 0`. Files via `files_before/files_after`. `--catalog` selects `s3://lakehouse/warehouse` (S3FileIO) vs `hdfs://namenode:8020/warehouse` (HadoopFileIO).
- **Repro (single-node):**
  ```bash
  docker compose up -d minio nessie
  SPARK_MODE=local .venv/Scripts/python tests/performance/reconciliation_benchmark.py --scale 100000 --catalog nessie
  SPARK_MODE=local .venv/Scripts/python tests/performance/reconciliation_benchmark.py --scale 500000 --catalog nessie
  SPARK_MODE=local .venv/Scripts/python tests/performance/reconciliation_benchmark.py --scale 1000000 --catalog nessie
  ```
- **Measured result single-node, baseline (`results_iceberg.json`, SPARK_MODE=local, 28 cores, d2e5f67d5861, 15 Sep 2026, pre-tuning):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | files | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 100k | 10% | 0.95s | 10,498 | 10,000 | 0 | 8 -> 1 | true |
  | 100k | 50% | 0.84s | 59,381 | 50,000 | 0 | 1 -> 1 | true |
  | 500k | 10% | 2.04s | 24,510 | 50,000 | 0 | 40 -> 1 | true |
  | 500k | 50% | 1.96s | 127,551 | 250,000 | 0 | 1 -> 20 | true |
  | 1M | 10% | 2.66s | 37,647 | 100,000 | 0 | 80 -> 28 | true |
  | 1M | 50% | 3.07s | 162,856 | 500,000 | 0 | 13 -> 28 | true |

- **Measured result single-node, tuned (same machine/commit `9b61903` tree, 16 Sep 2026, includes `761c37c` + partition fix):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | files | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 100k | 10% | 0.81s | 12,337 | 10,000 | 0 | 8 -> 4 | true |
  | 100k | 50% | 0.87s | 57,582 | 50,000 | 0 | 2 -> 5 | true |
  | 500k | 10% | 1.19s | 42,074 | 50,000 | 0 | 40 -> 9 | true |
  | 500k | 50% | 1.93s | 129,766 | 250,000 | 0 | 6 -> 4 | true |
  | 1M | 10% | 1.66s | 60,268 | 100,000 | 0 | 80 -> 12 | true |
  | 1M | 50% | 3.05s | 163,747 | 500,000 | 0 | 9 -> 5 | true |

### What changed between the two single-node runs, and why

1. **Single-pass fee `CASE` (`src/processing/reconcile.py`, mirrored in the harness).** Two `WHEN MATCHED` arms each evaluating `ABS(...)` became one arm with a `CASE` — one `ABS` eval per row instead of two. Pure CPU saving on the match path.
2. **Write tuning:** `merge-on-read` for data and deletes (rewrites become delete files instead of full rewrites), `snappy` instead of `zstd` (cheaper encode on this host), 64MB target files, `hash` distribution, shuffle partitions 400 -> 32, AQE partition coalescing, settlement side persisted + warmed so the 4 repeats don't recompute the LIMIT + fee math.
3. **Partition-count fix (found by the re-run, not assumed).** `761c37c` raised table-build partitions 4 -> 8 for scales >= 500k, intending bigger files — but tables are built in 50k-row batches, so it doubled file count instead (500k: 40 -> 80 files). The fresh 500k run regressed 20-46% vs baseline, which caught it. Reverted to 4 partitions; with file layout held equal the real optimizations show. Lesson recorded: partition count and batch size multiply — tune total files, not partitions alone.
4. **Read the pattern honestly:** 10%-update MERGEs gained 17-71% (scan-dominated: they read the whole table but rewrite little, so MoR + cheaper CPU wins big). 50%-update MERGEs are flat (write-dominated: rewriting half the table costs what it costs). The tuning moved the bottleneck, it didn't remove writes.

## YARN (Hadoop) bench — measured

Same suite on `hdfs://namenode:8020/warehouse` via `nessie_hdfs` (HDFS proof). Driver runs inside `tx-recon_default` (`tx-recon-driver:bench`, Python 3.11.16 Linux, JDK 17) because Windows host driver cannot fetch executor blocks over Docker Desktop bridge (host.docker.internal callback).

```bash
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml up -d --wait
bash scripts/hdfs_init.sh
# single command per scale (driver inside network):
docker run --rm --platform linux/amd64 --network tx-recon_default -v "E:\Personal Projects\tx-recon:/opt/tx-recon" tx-recon-driver:bench bash -c 'export PYTHONPATH=/opt/tx-recon; export SPARK_MODE=yarn; python /opt/tx-recon/tests/performance/reconciliation_benchmark.py --scale 100000 --catalog nessie_hdfs'
# or helpers: bash scripts/driver_bench.sh       # 100k
#            bash scripts/driver_bench_scale.sh  # 500k 1M (same, pip baked)
```

- **Measured result YARN (`results_iceberg_yarn_hdfs.json`, SPARK_MODE=yarn, 14 cores, 7af266c58991, 7-service Hadoop 4096MB/4vcores per NM):**

  | Scale | Update | Median write | rows/sec | matched | mismatched | files | healthy |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 100k | 10% | 2.09s | 4,776 | 10,000 | 0 | 8 -> 1 | true |
  | 100k | 50% | 2.11s | 23,710 | 50,000 | 0 | 1 -> 1 | true |
  | 500k | 10% | 3.59s | 13,911 | 50,000 | 0 | 40 -> 1 | true |
  | 500k | 50% | 5.71s | 43,810 | 250,000 | 0 | 1 -> 4 | true |
  | 1M | 10% | 4.74s | 21,114 | 100,000 | 0 | 80 -> 1 | true |
  | 1M | 50% | 7.02s | 71,207 | 500,000 | 0 | 1 -> 4 | true |

- **Comparison single vs YARN (median write, same 4 repeats, same code):**

  | Scale | Update | Single (s) | YARN (s) | Δ | Single rows/s | YARN rows/s |
  | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
  | 100k | 10% | 0.95 | 2.09 | +120% YARN slower | 10,498 | 4,776 |
  | 100k | 50% | 0.84 | 2.11 | +151% | 59,381 | 23,710 |
  | 500k | 10% | 2.04 | 3.59 | +76% | 24,510 | 13,911 |
  | 500k | 50% | 1.96 | 5.71 | +191% | 127,551 | 43,810 |
  | 1M | 10% | 2.66 | 4.74 | +78% | 37,647 | 21,114 |
  | 1M | 50% | 3.07 | 7.02 | +129% | 162,856 | 71,207 |

  YARN slower on small scales due to staging/YARN AM startup (~1s) and constrained NM (4096MB/4vcores, driver 5.61GB image vs 15.8GB host, files `1->4` vs `1->20/13->28` due to fewer tasks). Proves true distributed scheduling on `hdfs://` (2 NMs, HDFS Live 1/2 DNs, YARN UI :8088, History :19888) — not raw speed. Source JSONs are `results_iceberg.json` and `results_iceberg_yarn_hdfs.json`.

## PySpark streaming ingestion

- **Script:** `tests/performance/pyspark_ingestion_benchmark.py`
- **Method:** Structured Streaming Kafka -> Iceberg, `count_after - count_before` over 120s after 30s warmup, `maxOffsetsPerTrigger=500000`, `trigger 5s`.
- **Repro:** `python tests/performance/run_benchmarks.py --suite pyspark` (needs live Redpanda/MinIO/Nessie). No fresh local measurement is published in this revision.

## Regression gate

- **Script:** `tests/performance/check_regression.py`
- **Thresholds:** fails the PR when throughput drops more than 15 percent or p99 rises more than 20 percent versus the baseline `results.json`; hardware fingerprint change is a warning, not a failure, so a new machine is not mistaken for a slowdown.
- **Repro:** `python tests/performance/check_regression.py --baseline tests/performance/results.json`

## Retired claims

Earlier revisions cited numbers that are no longer reproducible:

- Kafka **142,188 msgs/sec, p99 7.53ms on 2000 messages** from a mixed `acks=1` run with a different warmup method. Current number is the `acks=all` serial-flush measurement above.
- Iceberg **sub-linear scaling to 227K rows/sec at 5M** from single-sample writes with non-monotonic timing. Replaced by seeded median at 100k only.
- Validation footnote about Polars "cold-start overhead" from a run that re-read CSV from disk. Current run validates in-memory.

Per-suite files (`results_accuracy.json`, `results_pandera.json`, `results_iceberg.json`) are the cited sources. `results.json` is an aggreg convenience copy.

## Hardware

`tests/performance/hardware.py` reports `platform, cpu_count, ram_gb, python_version, fingerprint`. Include it when citing numbers.
