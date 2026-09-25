import logging
import re

import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, lit, to_timestamp
from pyspark.sql.streaming.listener import StreamingQueryListener

from src.common.config import get_spark_session
from src.common.schemas import EXCEPTION_MISSING_WEBHOOK, WEBHOOK_AVRO_SCHEMA
from src.common.settings import get_settings
from src.processing.reconcile import _qualified_table

logger = logging.getLogger(__name__)

_WATERMARK_RE = re.compile(r"^(\d+)\s+(seconds?|minutes?|hours?|days?)$", re.IGNORECASE)


def parse_watermark_delay(delay: str) -> tuple[int, str]:
    """Validate STREAM_WATERMARK_DELAY into (amount, unit) for INTERVAL use.

    Allowlist only (digits + time unit) so the parts are safe to interpolate
    into Spark SQL — the table names already go through _qualified_table.
    """
    m = _WATERMARK_RE.match(delay.strip())
    if not m:
        raise ValueError(f"Bad STREAM_WATERMARK_DELAY {delay!r}: want e.g. '1 day', '12 hours'")
    return int(m.group(1)), m.group(2).lower()


def _registry_schema_id(settings, required: bool = False) -> str | None:
    """Latest Avro schema id for the topic value-subject (warn-only if unreachable).

    The streaming parse uses a pinned local copy (WEBHOOK_AVRO_SCHEMA), so a
    producer-side evolution shows up as per-batch id drift in _write_batch
    instead of a silent DLQ flood. Never raises unless required: registry down
    must not block ingestion, it just disables the drift comparison.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        client = SchemaRegistryClient({"url": settings.schema_registry_url})
        latest = client.get_latest_version(f"{settings.topic_name}-value")
        return str(latest.schema_id)
    except Exception as exc:
        msg = f"Schema-registry lookup skipped: {exc}"
        if required:
            raise RuntimeError(msg) from exc
        logger.warning(msg)
        return None


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


def run_ingestion():
    settings = get_settings()

    logger.info("Initializing Spark Session for Webhook Ingestion")
    spark = get_spark_session("WebhookIngestion")

    logger.info(f"Connecting to Redpanda at {settings.kafka_broker}")
    kafka_opts = {
        "kafka.bootstrap.servers": settings.kafka_broker,
        "subscribe": settings.topic_name,
        "startingOffsets": "earliest",
    }
    if settings.kafka_security_protocol != "PLAINTEXT":
        kafka_opts.update(
            {
                "kafka.security.protocol": settings.kafka_security_protocol,
                "kafka.sasl.mechanism": settings.kafka_sasl_mechanism or "SCRAM-SHA-256",
                "kafka.sasl.jaas.config": (
                    f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                    f'username="{settings.kafka_sasl_username}" '
                    f'password="{settings.kafka_sasl_password}";'
                ),
            }
        )
    elif settings.require_kafka_sasl:
        raise RuntimeError(
            "KAFKA_SECURITY_PROTOCOL=PLAINTEXT with REQUIRE_KAFKA_SASL=1: "
            "forged Kafka records would be trusted — enable SASL"
        )
    if settings.webhook_secret:
        logger.warning(
            "WEBHOOK_SECRET set but Kafka source exposes no headers: HMAC verify "
            "runs at producer/gateway edge, Spark ingestion trusts SASL + schema. "
            "Enable SASL (KAFKA_SECURITY_PROTOCOL) so forging requires broker creds."
        )
    reader = spark.readStream.format("kafka")
    for k, v in kafka_opts.items():
        reader = reader.option(k, v)
    df = reader.load()

    # Confluent Avro wire format: Magic Byte (1 byte) + Schema ID (4 bytes)
    df = df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
    # Carry the writer schema id alongside the payload: from_avro parses with
    # the pinned local copy, so per-batch ids are the drift signal that catches
    # a producer-side evolution before it becomes a DLQ flood.
    df = df.withColumn("schema_id", expr("conv(hex(substring(value, 2, 4)), 16, 10)"))
    _expected_schema_id = _registry_schema_id(settings, required=settings.require_schema_registry)

    parsed_df = df.select(
        from_avro(col("fixed_value"), WEBHOOK_AVRO_SCHEMA).alias("data"), col("schema_id")
    ).select("data.*", "schema_id")
    # Event time for watermarking (Avro carries ISO string; watermark needs TimestampType).
    # withWatermark bounds dropDuplicates state: keys older than the delay are evicted,
    # so the state store cannot grow without bound on a long-lived stream.
    _wm_n, _wm_unit = parse_watermark_delay(settings.stream_watermark_delay)
    parsed_df = parsed_df.withColumn(
        "event_time", to_timestamp(col("timestamp_utc"))
    ).withWatermark("event_time", f"{_wm_n} {_wm_unit}")

    # NULL-safe split: corrupt from_avro rows yield NULLs, and NULL > 0 / NULL <= 0
    # are both NULL (3VL), so a naive <= 0 complement silently drops them.
    # valid keeps only TRUE; invalid keeps FALSE *or* NULL via coalesce.
    valid_cond = (col("amount_paise") > 0) & (col("transaction_id").isNotNull())

    warehouse = settings.iceberg_warehouse
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)

    logger.info(f"Starting stream to Iceberg {webhook_table} (+ DLQ {dlq_table})")

    def _write_batch(batch_df, _epoch: int) -> None:
        # Empty triggers are common: limit-1 check skips count + MERGE planning.
        if batch_df.isEmpty():
            return
        # Cache once: this batch otherwise gets re-scanned from the source on
        # every action below (schema-id distinct, late count, MERGE, DLQ count).
        batch_df = batch_df.cache()
        try:
            _write_batch_cached(batch_df)
        finally:
            batch_df.unpersist()

    def _write_batch_cached(batch_df) -> None:
        # Schema-id drift: writer evolved without updating the pinned copy.
        # Warn-only (parse still runs); the oncall checks the registry diff.
        try:
            _ids = sorted(
                {str(r["schema_id"]) for r in batch_df.select("schema_id").distinct().collect()}
            )
        except Exception as exc:
            _ids = []
            logger.warning(f"schema-id check skipped: {exc}")
        if _ids and _expected_schema_id and any(i != _expected_schema_id for i in _ids):
            logger.warning(
                f"schema-id drift: batch ids {_ids} vs registry {_expected_schema_id} "
                f"({settings.topic_name}-value) — verify producer evolution"
            )
        # Late-data policy: count rows arriving older than the watermark delay.
        # They still merge (effect path is idempotent) but the count is the
        # side-output the oncall watches; alert threshold lives in the runbook.
        _late_n = batch_df.filter(
            col("event_time").isNotNull()
            & (col("event_time") < F.expr(f"current_timestamp() - INTERVAL {_wm_n} {_wm_unit}"))
        ).count()
        if _late_n:
            logger.warning(f"late-data: {_late_n} rows older than {_wm_n} {_wm_unit} in batch")
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
            f"WHEN MATCHED AND t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' THEN "
            "UPDATE SET t.amount_paise = s.amount_paise, t.gateway_status = s.gateway_status, "
            "t.timestamp_utc = s.timestamp_utc, t.merchant_id = s.merchant_id, "
            "t.processing_run_id = s.processing_run_id, "
            "t.reconciliation_status = s.gateway_status, t.ingested_at = s.ingested_at "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        inv = batch_df.filter(~F.coalesce(valid_cond, F.lit(False))).dropDuplicates(
            ["transaction_id"]
        )
        # Single count: isEmpty()+count() was two jobs per microbatch.
        n_inv = inv.count()
        if n_inv:
            logger.warning(f"DLQ batch: {n_inv} invalid rows -> {dlq_table}")
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


def ensure_webhook_table(spark, table: str) -> None:
    """Create namespace + webhooks Iceberg table if missing (shared by the
    streaming __main__ init and the pipeline real-data seeder).

    Layout: bucket(16, transaction_id) co-locates MERGE join keys so batches
    prune to file groups instead of full-scanning history (verified live).
    Applies to NEW tables only (IF NOT EXISTS): existing unpartitioned tables
    keep their spec — migrate via CREATE new + INSERT + swap, never in place.
    """
    *parts, _ = table.split(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(parts)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            transaction_id string,
            amount_paise bigint,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            processing_run_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp,
            instrument_type string
        ) USING iceberg
        PARTITIONED BY (bucket(16, transaction_id))
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """
    )
    # Cluster MERGE join key so equality lookups avoid full sort on every write.
    try:
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY transaction_id")
    except Exception as exc:
        logger.warning(f"Write ordering skipped for {table}: {exc}")


if __name__ == "__main__":
    settings = get_settings()
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)

    logger.info("Initializing Iceberg tables via Nessie")
    spark = get_spark_session("Init")
    ensure_webhook_table(spark, webhook_table)

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
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """
    )

    run_ingestion()
