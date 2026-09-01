import argparse
import logging
import os
import platform
import statistics
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import get_spark_session
from src.common.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WARMUP_SECONDS = 30
MEASURE_SECONDS = 120


def get_hardware_info():
    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": platform.python_version(),
    }


class BenchListener(StreamingQueryListener):
    def __init__(self):
        self.batches = []

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        self.batches.append(
            {
                "batchId": p.batchId,
                "inputRowsPerSecond": p.inputRowsPerSecond,
                "processedRowsPerSecond": p.processedRowsPerSecond,
                "durationMs": dict(p.durationMs) if p.durationMs else {},
            }
        )

    def onQueryTerminated(self, event):
        pass


def run_benchmark(partitions=16):
    hw = get_hardware_info()
    logger.info(
        f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}"
    )
    logger.info(
        f"Config: warmup={WARMUP_SECONDS}s, measure={MEASURE_SECONDS}s, partitions={partitions}"
    )

    spark = get_spark_session("PySparkIngestionBenchmark")
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    spark.conf.set("spark.sql.streaming.pollingDelay", "1000")

    listener = BenchListener()
    spark.streams.addListener(listener)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.db.webhooks_bench (
            transaction_id string,
            amount_paise int,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp
        ) USING iceberg
    """
    )

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("amount_paise", IntegerType(), True),
            StructField("gateway_status", StringType(), True),
            StructField("timestamp_utc", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("source_ts", DoubleType(), True),
        ]
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{get_settings().redpanda_host}:19092")
        .option("subscribe", "gateway_webhooks")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500000)
        .load()
    )

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select(
        "data.*"
    )

    valid_df = parsed_df.filter((col("amount_paise") > 0) & (col("transaction_id").isNotNull()))

    enriched_df = (
        valid_df.withColumn("reconciliation_status", col("gateway_status"))
        .withColumn("bank_ref_id", col("transaction_id"))
        .withColumn("ingested_at", current_timestamp())
        .select(
            "transaction_id",
            "amount_paise",
            "gateway_status",
            "timestamp_utc",
            "merchant_id",
            "reconciliation_status",
            "bank_ref_id",
            "ingested_at",
        )
    )

    checkpoint_path = f"s3a://lakehouse/checkpoints/benchmark_ingestion_p{partitions}"

    logger.info(f"Starting streaming ingestion for {WARMUP_SECONDS + MEASURE_SECONDS}s")

    query = (
        enriched_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .toTable("nessie.db.webhooks_bench")
    )

    # Warmup
    logger.info(f"Warmup: {WARMUP_SECONDS}s...")
    time.sleep(WARMUP_SECONDS)

    # Measure
    logger.info(f"Measuring: {MEASURE_SECONDS}s...")
    measure_start = time.time()
    time.sleep(MEASURE_SECONDS)
    query.stop()
    total_duration = time.time() - measure_start

    spark.streams.removeListener(listener)

    count_result = spark.sql("SELECT COUNT(*) as cnt FROM nessie.db.webhooks_bench").collect()[0][
        "cnt"
    ]

    sustained_rate = count_result / total_duration if total_duration > 0 else 0

    # Aggregate listener data
    listener_batches = listener.batches
    avg_processed_rps = 0
    avg_input_rps = 0
    avg_batch_duration_ms = 0
    if listener_batches:
        avg_processed_rps = statistics.mean(b["processedRowsPerSecond"] for b in listener_batches)
        avg_input_rps = statistics.mean(b["inputRowsPerSecond"] for b in listener_batches)
        avg_batch_duration_ms = statistics.mean(
            sum(b["durationMs"].values()) for b in listener_batches
        )

    result = {
        "hardware": hw,
        "config": {
            "partitions": partitions,
            "warmup_seconds": WARMUP_SECONDS,
            "measure_seconds": MEASURE_SECONDS,
        },
        "total_rows_written": count_result,
        "total_duration_sec": round(total_duration, 1),
        "sustained_throughput_rows_sec": round(sustained_rate, 0),
        "avg_processed_rows_per_sec": round(avg_processed_rps, 0),
        "avg_input_rows_per_sec": round(avg_input_rps, 0),
        "avg_batch_duration_ms": round(avg_batch_duration_ms, 0),
        "num_batches": len(listener_batches),
    }

    logger.info("=== BENCHMARK RESULTS ===")
    logger.info(f"Total rows written:     {count_result:,}")
    logger.info(f"Total time:             {total_duration:.1f}s")
    logger.info(f"Sustained throughput:   {sustained_rate:,.0f} rows/sec")
    logger.info(f"Avg batch duration:     {avg_batch_duration_ms:.0f}ms")

    spark.sql("DROP TABLE IF EXISTS nessie.db.webhooks_bench")
    spark.stop()

    return result


def main():
    parser = argparse.ArgumentParser(description="PySpark Ingestion Benchmark")
    parser.add_argument("--partitions", type=int, default=16)
    args = parser.parse_args()
    run_benchmark(args.partitions)


if __name__ == "__main__":
    main()
