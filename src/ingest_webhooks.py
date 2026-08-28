import logging
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from config import get_spark_session, redpanda_host

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_ingestion():
    logger.info("Initializing Spark Session for Webhook Ingestion")
    spark = get_spark_session("WebhookIngestion")

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("amount_paise", IntegerType(), True),
            StructField("gateway_status", StringType(), True),
            StructField("timestamp_utc", StringType(), True),
            StructField("merchant_id", StringType(), True),
        ]
    )

    logger.info(f"Connecting to Redpanda at {redpanda_host}:9092")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", f"{redpanda_host}:9092")
        .option("subscribe", "gateway_webhooks")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # DATA QUALITY CHECKS (Filter good vs bad records)
    # Valid: Amount > 0 and transaction_id is not null
    valid_df = parsed_df.filter(
        (col("amount_paise") > 0) & (col("transaction_id").isNotNull())
    )

    # Invalid: Dead Letter Queue (DLQ)
    invalid_df = parsed_df.filter(
        (col("amount_paise") <= 0) | (col("transaction_id").isNull())
    )

    enriched_df = (
        valid_df.withColumn("reconciliation_status", col("gateway_status"))
        .withColumn("bank_ref_id", col("transaction_id"))
        .withColumn("ingested_at", current_timestamp())
    )

    # Write valid records to Iceberg
    logger.info("Starting stream to Iceberg nessie.db.webhooks")
    query_valid = (
        enriched_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/webhooks_valid")
        .toTable("nessie.db.webhooks")
    )

    # Write invalid records to DLQ (Iceberg table)
    query_invalid = (
        invalid_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/webhooks_dlq")
        .toTable("nessie.db.webhooks_dlq")
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logger.info("Initializing Iceberg tables via Nessie")
    spark = get_spark_session("Init")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    # Initialize valid table
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

    # Initialize DLQ table
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

    run_ingestion()
