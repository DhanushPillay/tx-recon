# PySpark Ingestion Benchmark Explained

How we measure sustained Kafka-to-Iceberg streaming throughput and what the numbers mean.

## What This Benchmark Does

It runs a Spark Structured Streaming job that reads from a Kafka topic (`gateway_webhooks`), parses JSON payloads, filters invalid rows, and writes to an Iceberg table. We measure rows/sec over a fixed window.

## The Pipeline

```
Redpanda (Kafka) → readStream → parse JSON → filter → enrich → writeStream → Iceberg table
```

Each step adds overhead:

1. **Network read**: Pull batches from Redpanda (up to 500K offsets per trigger)
2. **JSON parsing**: Deserialize webhook payload into columns
3. **Filtering**: Drop rows with null `transaction_id` or zero/negative amounts
4. **Enrichment**: Add `reconciliation_status`, `bank_ref_id`, `ingested_at` columns
5. **Iceberg write**: Append to Parquet files via Iceberg's write path

## Key Configuration

| Parameter | Value | Why |
|---|---|---|
| `maxOffsetsPerTrigger` | 500,000 | Caps per-batch size to prevent OOM |
| `trigger.processingTime` | 5 seconds | Micro-batch interval |
| `spark.sql.shuffle.partitions` | 16 | Matches local core count |
| `spark.sql.streaming.pollingDelay` | 1000 ms | How often Spark checks for new data |
| `spark.python.worker.reuse` | true | Avoids Python worker restart overhead |

## How We Measure

- **30-second warmup**: Let streaming stabilize, JIT compile, caches fill
- **120-second measure window**: Count rows written in this window
- **StreamingQueryListener**: Captures per-batch `inputRowsPerSecond` and `processedRowsPerSecond` from Spark's internal metrics

The listener tracks every batch — we report both the sustained rate (total rows / total time) and the average from Spark's internal counters.

## Results

| Metric | Value |
|---|---|
| Sustained throughput | ~27,000 rows/sec |
| Avg batch duration | ~5 seconds (matches trigger interval) |

## Why ~27K and Not Higher

The bottleneck is **not** Spark compute — it's the micro-batch trigger interval. With a 5-second trigger, Spark reads, processes, and writes in bursts. The actual processing per batch is fast (sub-second), but Spark waits for the next trigger.

To go faster:
- Reduce `trigger.processingTime` to `"1 second"` — trades more checkpoint overhead for lower latency
- Increase `maxOffsetsPerTrigger` — but risks OOM on large batches
- Add more partitions — but only helps with more cores

27K rows/sec is plenty for daily reconciliation (millions of transactions, not billions per second).

## Run It

```bash
# Default (16 partitions, 150s total)
python tests/performance/pyspark_ingestion_benchmark.py

# Custom partitions
python tests/performance/pyspark_ingestion_benchmark.py --partitions 8
```

Requires Docker running (`docker-compose up -d`) — PySpark can't run natively on Windows.
