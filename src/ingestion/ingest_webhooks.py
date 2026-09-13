import logging
import re

import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.streaming.listener import StreamingQueryListener

from src.common.config import get_spark_session
from src.common.schemas import WEBHOOK_AVRO_SCHEMA
from src.common.settings import get_settings

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z0-9_.]+$")


class _BatchProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        logger.info(
            f"ingest_batch query={p.name} batch={p.batchId} "
            f"rows={p.numInputRows} duration_ms={p.durationMs.get('triggerExecution', 0)}"
        )

    def onQueryTerminated(self, event):
        pass


def _qualified_table(name: str) -> str:
    if not _TABLE_RE.match(name):
        raise ValueError(f"Unsafe table identifier: {name!r}")
    return name


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

    # NULL-safe split: corrupt from_avro rows yield NULLs, and NULL > 0 / NULL <= 0
    # are both NULL (3VL), so a naive <= 0 complement silently drops them.
    # valid keeps only TRUE; invalid keeps FALSE *or* NULL via coalesce.
    valid_cond = (col("amount_paise") > 0) & (col("transaction_id").isNotNull())

    warehouse = settings.iceberg_warehouse
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)

    logger.info(f"Starting stream to Iceberg {webhook_table} (+ DLQ {dlq_table})")

    def _write_batch(batch_df, _epoch: int) -> None:
        # ponytail: no persist/head — MERGE on empty is no-op cheaper than extra jobs
        v = batch_df.filter(valid_cond).dropDuplicates(["transaction_id"])
        v = (
            v.withColumn("reconciliation_status", col("gateway_status"))
            .withColumn("bank_ref_id", lit(None).cast("string"))
            .withColumn("ingested_at", current_timestamp())
        )
        v.createOrReplaceTempView("batch_valid")
        spark.sql(
            f"MERGE INTO {webhook_table} t USING batch_valid s "
            "ON t.transaction_id = s.transaction_id "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        inv = batch_df.filter(~F.coalesce(valid_cond, F.lit(False)))
        inv.writeTo(dlq_table).append()

    spark.streams.addListener(_BatchProgressLogger())
    query = (
        parsed_df.writeStream.foreachBatch(_write_batch)
        .queryName("webhooks_all")
        .trigger(processingTime="10 seconds")
        .option("maxOffsetsPerTrigger", 200000)
        .option("checkpointLocation", f"{warehouse}/checkpoints/webhooks_all")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    settings = get_settings()
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)

    logger.info("Initializing Iceberg tables via Nessie")
    spark = get_spark_session("Init")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {webhook_table} (
            transaction_id string,
            amount_paise bigint,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            processing_run_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd'
        )
    """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dlq_table} (
            transaction_id string,
            amount_paise bigint,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            processing_run_id string
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd'
        )
    """
    )

    run_ingestion()
