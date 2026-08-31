import logging
import os
import platform
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import get_spark_session, redpanda_host

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

WARMUP_SECONDS = 30
MEASURE_SECONDS = 120


def get_hardware_info():
    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or "unknown",
        "python": platform.python_version(),
        "cores": os.cpu_count(),
    }


def run_benchmark():
    hw = get_hardware_info()
    logger.info(f"Hardware: {hw['os']}, {hw['cores']} cores, Python {hw['python']}")
    logger.info(
        f"Config: warmup={WARMUP_SECONDS}s, measure={MEASURE_SECONDS}s, "
        f"redpanda={redpanda_host}:19092"
    )

    spark = get_spark_session("PySparkIngestionBenchmark")

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
        .option("kafka.bootstrap.servers", f"{redpanda_host}:19092")
        .option("subscribe", "gateway_webhooks")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500000)
        .load()
    )

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    valid_df = parsed_df.filter(
        (col("amount_paise") > 0) & (col("transaction_id").isNotNull())
    )

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

    checkpoint_path = "s3a://lakehouse/checkpoints/benchmark_ingestion_v4"

    logger.info(f"Starting streaming ingestion for {WARMUP_SECONDS + MEASURE_SECONDS}s")

    start_time = time.time()

    query = (
        enriched_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .toTable("nessie.db.webhooks_bench")
    )

    time.sleep(WARMUP_SECONDS + MEASURE_SECONDS)
    query.stop()

    end_time = time.time()
    total_duration = end_time - start_time

    count_result = spark.sql(
        "SELECT COUNT(*) as cnt FROM nessie.db.webhooks_bench"
    ).collect()[0]["cnt"]

    sustained_rate = count_result / total_duration if total_duration > 0 else 0

    logger.info("=== BENCHMARK RESULTS ===")
    logger.info(f"Total rows written:     {count_result:,}")
    logger.info(f"Total time:             {total_duration:.1f}s")
    logger.info(f"Sustained throughput:   {sustained_rate:,.0f} rows/sec")
    logger.info(f"Hardware: {hw}")

    spark.sql("DROP TABLE IF EXISTS nessie.db.webhooks_bench")
    spark.stop()


if __name__ == "__main__":
    run_benchmark()
