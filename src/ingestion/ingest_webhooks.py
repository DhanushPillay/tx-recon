import logging

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, lit

from src.common.config import get_spark_session
from src.common.schemas import WEBHOOK_AVRO_SCHEMA
from src.common.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_ingestion():
    settings = get_settings()

    logger.info("Initializing Spark Session for Webhook Ingestion")
    spark = get_spark_session("WebhookIngestion")

    logger.info(f"Connecting to Redpanda at {settings.kafka_broker}")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_broker)
        .option("subscribe", settings.topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Confluent Avro wire format: Magic Byte (1 byte) + Schema ID (4 bytes)
    df = df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))

    parsed_df = df.select(from_avro(col("fixed_value"), WEBHOOK_AVRO_SCHEMA).alias("data")).select(
        "data.*"
    )

    valid_df = parsed_df.filter((col("amount_paise") > 0) & (col("transaction_id").isNotNull()))

    invalid_df = parsed_df.filter((col("amount_paise") <= 0) | (col("transaction_id").isNull()))

    enriched_df = (
        valid_df.withColumn("reconciliation_status", col("gateway_status"))
        .withColumn("bank_ref_id", lit(None).cast("string"))
        .withColumn("ingested_at", current_timestamp())
    )

    warehouse = settings.iceberg_warehouse

    logger.info(f"Starting stream to Iceberg {settings.webhook_table}")
    (
        enriched_df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .option("maxOffsetsPerTrigger", 50000)
        .option("checkpointLocation", f"{warehouse}/checkpoints/webhooks_valid")
        .toTable(settings.webhook_table)
    )

    (
        invalid_df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .option("maxOffsetsPerTrigger", 50000)
        .option("checkpointLocation", f"{warehouse}/checkpoints/webhooks_dlq")
        .toTable(settings.dlq_table)
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    settings = get_settings()

    logger.info("Initializing Iceberg tables via Nessie")
    spark = get_spark_session("Init")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.webhook_table} (
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
        f"""
        CREATE TABLE IF NOT EXISTS {settings.dlq_table} (
            transaction_id string,
            amount_paise int,
            gateway_status string,
            timestamp_utc string,
            merchant_id string
        ) USING iceberg
    """
    )

    run_ingestion()
