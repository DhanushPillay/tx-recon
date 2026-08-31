import time
import logging
import sys

# Add parent directory to path so we can import src modules
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.avro.functions import from_avro
from src.common.config import get_spark_session, redpanda_host
from src.ingestion.ingest_webhooks import avro_schema_str

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_benchmark():
    logger.info("Initializing Spark Session for PySpark Ingestion Benchmark")
    spark = get_spark_session("PySparkIngestionBenchmark")

    # Initialize namespaces and tables
    logger.info("Initializing Iceberg tables via Nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.db.webhooks (
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

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.db.webhooks_dlq (
            transaction_id string,
            amount_paise int,
            gateway_status string,
            timestamp_utc string,
            merchant_id string
        ) USING iceberg
    """
    )

    logger.info(f"Connecting to Redpanda at {redpanda_host}:19092")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{redpanda_host}:19092")
        .option("subscribe", "gateway_webhooks")
        .option("startingOffsets", "earliest")
        .load()
    )

    df = df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
    parsed_df = df.select(
        from_avro(col("fixed_value"), avro_schema_str, {"mode": "PERMISSIVE"}).alias(
            "data"
        )
    ).select("data.*")

    valid_df = parsed_df.filter(
        (col("amount_paise") > 0) & (col("transaction_id").isNotNull())
    )

    invalid_df = parsed_df.filter(
        (col("amount_paise") <= 0) | (col("transaction_id").isNull())
    )

    enriched_df = (
        valid_df.withColumn("reconciliation_status", col("gateway_status"))
        .withColumn("bank_ref_id", col("transaction_id"))
        .withColumn("ingested_at", current_timestamp())
    )

    # Use unique checkpoint locations so it doesn't conflict with normal streaming
    checkpoint_valid = "s3a://lakehouse/checkpoints/benchmark_webhooks_valid"
    checkpoint_invalid = "s3a://lakehouse/checkpoints/benchmark_webhooks_dlq"

    logger.info("Starting batch consumption (Trigger.AvailableNow)")

    start_time = time.time()

    query_valid = (
        enriched_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_valid)
        .trigger(availableNow=True)
        .toTable("nessie.db.webhooks")
    )

    query_invalid = (
        invalid_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_invalid)
        .trigger(availableNow=True)
        .toTable("nessie.db.webhooks_dlq")
    )

    query_valid.awaitTermination()
    query_invalid.awaitTermination()

    end_time = time.time()
    duration = end_time - start_time

    # Get row count from iceberg
    count_df = spark.sql("SELECT COUNT(*) as cnt FROM nessie.db.webhooks")
    total_rows = count_df.collect()[0]["cnt"]

    logger.info(f"BENCHMARK COMPLETE")
    logger.info(f"Time taken to consume and write to Iceberg: {duration:.2f} seconds")
    logger.info(f"Total rows currently in Iceberg valid table: {total_rows}")


if __name__ == "__main__":
    run_benchmark()
