import logging
import re
import time

import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.column import Column
from pyspark.sql.functions import col, current_timestamp, expr, lit, to_timestamp
from pyspark.sql.streaming.listener import StreamingQueryListener

from src.common.config import get_spark_session
from src.common.schemas import (
    EXCEPTION_LATE_UNRESOLVED,
    EXCEPTION_MISSING_WEBHOOK,
    EXCEPTION_REVERSAL,
    PENDING_RECONCILIATION,
    REVERSAL_GATEWAY_STATUSES,
    WEBHOOK_AVRO_SCHEMA,
)
from src.common.settings import get_settings
from src.processing.reconcile import _qualified_table

logger = logging.getLogger(__name__)

_WEBHOOK_COLS = [
    "transaction_id",
    "amount_paise",
    "gateway_status",
    "timestamp_utc",
    "merchant_id",
    "processing_run_id",
    "reconciliation_status",
    "bank_ref_id",
    "ingested_at",
    "instrument_type",
    "currency",
]
_DLQ_COLS = [
    "transaction_id",
    "amount_paise",
    "gateway_status",
    "timestamp_utc",
    "merchant_id",
    "processing_run_id",
]

_WATERMARK_RE = re.compile(r"^(\d+)\s+(seconds?|minutes?|hours?|days?)$", re.IGNORECASE)


class PermanentIngestError(RuntimeError):
    """A batch that will fail identically however many times it is retried.

    Schema drift and a PAN in the payload both need a config or a producer
    change before the batch could ever succeed. The supervisor used to retry
    them like a broker blip, spending about nine minutes of backoff and five
    replays of the same offsets to arrive at the same refusal.
    """


def _dlq_dedupe_hash(cols: list[str]) -> Column:
    """A content key for a DLQ row, so a replayed poison message lands once.

    The DLQ MERGE keys on `transaction_id`, and `NULL != NULL`, so a record
    with no transaction id matched nothing and appended a fresh row on every
    replay -- unbounded, and `replay_dlq.py --delete` filters on a non-null id
    so it could never remove them. Hashing the row's own content gives every
    DLQ row a key that is both present and stable.

    A NULL field reads as the empty string rather than propagating, so one
    missing column cannot null the key.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("\x1f", *parts), 256)


def _reversal_condition():
    """Rows the gateway reported as money moving backwards.

    A refund, reversal or chargeback carries a non-positive amount. Gating on
    `amount_paise > 0` alone sent those to the DLQ with the same shape as a
    corrupt record, and `replay_dlq.py` filtered on the same predicate, so they
    could be neither replayed nor purged.
    """
    return (
        F.coalesce(F.col("decodable"), F.lit(False))
        & F.col("transaction_id").isNotNull()
        & (F.col("amount_paise") <= 0)
        & F.upper(F.trim(F.coalesce(F.col("gateway_status"), F.lit("")))).isin(
            list(REVERSAL_GATEWAY_STATUSES)
        )
    )


def _forward_condition():
    """The original valid-row predicate: a positive payment with an id."""
    return (
        F.coalesce(F.col("decodable"), F.lit(False))
        & (F.col("amount_paise") > 0)
        & F.col("transaction_id").isNotNull()
    )


def _accepted_condition():
    """What lands in the webhook table rather than the DLQ."""
    return _forward_condition() | _reversal_condition()


def _ingest_status_column():
    """A reversal must not enter the settlement queue.

    There is no settlement row for it yet - `settlement_schema.Check.gt(0)`
    still quarantines a negative settled amount - so PENDING would have the
    late-SLA sweep age it as an ordinary missing settlement. The amount is the
    discriminator, not the status word, so a positive amount that happens to
    carry a refund word still reconciles normally.
    """
    return F.when(F.col("amount_paise") <= 0, F.lit(EXCEPTION_REVERSAL)).otherwise(
        F.lit(PENDING_RECONCILIATION)
    )


_SUPERVISOR_MAX_ATTEMPTS = 5
_SUPERVISOR_BASE_DELAY_S = 5.0
_SUPERVISOR_MAX_DELAY_S = 300.0


def supervisor_delay(attempt: int) -> float:
    return min(_SUPERVISOR_BASE_DELAY_S * (2**attempt), _SUPERVISOR_MAX_DELAY_S)


def parse_watermark_delay(delay: str) -> tuple[int, str]:
    m = _WATERMARK_RE.match(delay.strip())
    if not m:
        raise ValueError(f"Bad STREAM_WATERMARK_DELAY {delay!r}: want e.g. '1 day', '12 hours'")
    return int(m.group(1)), m.group(2).lower()


# nulling the payload turns a bad message into a dlq row instead of killing the stream
def _decode_guard(df):
    return (
        df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
        .withColumn("schema_id", expr("conv(hex(substring(value, 2, 4)), 16, 10)"))
        .withColumn(
            "decodable",
            expr("value IS NOT NULL AND length(value) > 5 AND substr(value, 1, 1) = X'00'"),
        )
        .withColumn("fixed_value", expr("CASE WHEN decodable THEN fixed_value END"))
    )


def _registry_schema_id(settings, required: bool = False) -> str | None:
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
        "startingOffsets": settings.stream_starting_offsets,
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

    df = _decode_guard(df)
    _expected_schema_id = _registry_schema_id(settings, required=settings.require_schema_registry)

    parsed_df = df.select(
        from_avro(col("fixed_value"), WEBHOOK_AVRO_SCHEMA).alias("data"),
        col("schema_id"),
        col("decodable"),
    ).select("data.*", "schema_id", "decodable")
    _wm_n, _wm_unit = parse_watermark_delay(settings.stream_watermark_delay)
    parsed_df = parsed_df.withColumn(
        # Same pattern the batch side uses, so a timestamp that reads here reads
        # there. A shape this rejects is NULL, and a NULL never ages to
        # LATE_UNRESOLVED and never reaches missing_within_lag, which would
        # read as a better batch than it is.
        "event_time",
        to_timestamp(col("timestamp_utc")),
    ).withWatermark("event_time", f"{_wm_n} {_wm_unit}")

    reversal_cond = _reversal_condition()
    accepted_cond = _accepted_condition()

    warehouse = settings.iceberg_warehouse
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)
    checkpoint = settings.stream_checkpoint_location or f"{warehouse}/checkpoints/webhooks_all"

    ensure_webhook_table(spark, webhook_table)
    ensure_dlq_table(spark, dlq_table)

    logger.info(f"Starting stream to Iceberg {webhook_table} (+ DLQ {dlq_table})")

    def _write_batch(batch_df, _epoch: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df = batch_df.cache()
        try:
            _write_batch_cached(batch_df)
        finally:
            batch_df.unpersist()

    def _write_batch_cached(batch_df) -> None:
        try:
            _ids = sorted(
                {str(r["schema_id"]) for r in batch_df.select("schema_id").distinct().collect()}
            )
        except Exception as exc:
            # Not drift, so still retryable -- but not silent either. An empty
            # _ids made the guard below fall through, so a batch nobody could
            # verify was merged with any new producer field quietly dropped.
            logger.error(f"schema-id check failed, batch not verified: {exc}")
            raise
        if _ids and _expected_schema_id and any(i != _expected_schema_id for i in _ids):
            raise PermanentIngestError(
                f"schema-id drift: batch ids {_ids} vs registry {_expected_schema_id} "
                f"({settings.topic_name}-value). Fields outside {_WEBHOOK_COLS} are "
                "dropped by the MERGE, so the batch is refused. Update "
                "src/common/schemas.py (and ALTER the table) before ingesting."
            )
        _late_n = batch_df.filter(
            col("event_time").isNotNull()
            & (col("event_time") < F.expr(f"current_timestamp() - INTERVAL {_wm_n} {_wm_unit}"))
        ).count()
        if _late_n:
            logger.warning(f"late-data: {_late_n} rows older than {_wm_n} {_wm_unit} in batch")
        _bad_ts_n = batch_df.filter(
            col("event_time").isNull() & col("timestamp_utc").isNotNull()
        ).count()
        if _bad_ts_n:
            logger.warning(
                f"unparseable timestamp_utc: {_bad_ts_n} rows in batch, excluded from "
                "the late-data count (timestamp_utc is an unconstrained Avro string)"
            )
        from src.validation.pan_guard import count_spark_pan_hits

        _pan_n = count_spark_pan_hits(batch_df)
        if _pan_n:
            raise PermanentIngestError(
                f"PAN detected in streaming batch ({_pan_n} rows): refusing batch "
                "(store last-4 + issuer only)"
            )
        _n_reversal = batch_df.filter(reversal_cond).count()
        if _n_reversal:
            logger.warning(
                f"reversals: {_n_reversal} refund/reversal/chargeback rows accepted and parked "
                f"at {EXCEPTION_REVERSAL}. The settlement contract has no direction field yet "
                "(`settlement_schema.Check.gt(0)` still quarantines a negative settled amount), "
                "so these wait for the matching debit rather than being reconciled. They surface "
                "as table_EXCEPTION_REVERSAL in the next batch's reconciliation counts."
            )
        v = batch_df.filter(accepted_cond).dropDuplicates(["transaction_id"])
        v = (
            v.withColumn("reconciliation_status", _ingest_status_column())
            .withColumn("bank_ref_id", lit(None).cast("string"))
            .withColumn("ingested_at", current_timestamp())
            .withColumn("instrument_type", lit(None).cast("string"))
        )
        v.createOrReplaceTempView("batch_valid")
        _cols = ", ".join(_WEBHOOK_COLS)
        _vals = ", ".join(f"s.{c}" for c in _WEBHOOK_COLS)
        spark.sql(
            f"MERGE INTO {webhook_table} t USING batch_valid s "
            "ON t.transaction_id = s.transaction_id "
            f"WHEN MATCHED AND (t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' "
            f"OR t.reconciliation_status = '{EXCEPTION_LATE_UNRESOLVED}' "
            f"OR t.reconciliation_status = '{PENDING_RECONCILIATION}') THEN "
            "UPDATE SET t.amount_paise = s.amount_paise, t.gateway_status = s.gateway_status, "
            "t.timestamp_utc = s.timestamp_utc, t.merchant_id = s.merchant_id, "
            "t.processing_run_id = s.processing_run_id, "
            f"t.reconciliation_status = '{PENDING_RECONCILIATION}', t.ingested_at = s.ingested_at "
            # Second WHEN MATCHED, so it only sees rows the first one declined:
            # the terminal ones. A redelivery carrying a different amount for an
            # already-reconciled transaction used to vanish here -- no clause
            # fired, no DLQ row, no counter, nothing. It never overwrites the
            # settled amount; it records that the gateway sent us a second,
            # disagreeing number for a transaction we already closed.
            "WHEN MATCHED THEN UPDATE SET "
            "t.duplicate_mutation_count = COALESCE(t.duplicate_mutation_count, 0) + 1, "
            "t.last_duplicate_at = current_timestamp() "
            f"WHEN NOT MATCHED THEN INSERT ({_cols}) VALUES ({_vals})"
        )
        inv = batch_df.filter(~F.coalesce(accepted_cond, F.lit(False))).select(*_DLQ_COLS)
        n_inv = inv.count()
        if n_inv:
            logger.warning(f"DLQ batch: {n_inv} invalid rows -> {dlq_table}")
            # Keyed on a hash of the row's own content, not transaction_id:
            # NULL != NULL meant every replay of a poison record with no id
            # appended another one, and replay_dlq.py --delete could never
            # reach them because it filters on a non-null id.
            inv.withColumn("payload_hash", _dlq_dedupe_hash(_DLQ_COLS)).dropDuplicates(
                ["payload_hash"]
            ).createOrReplaceTempView("batch_invalid")
            _dlq_cols = ", ".join([*_DLQ_COLS, "payload_hash"])
            _dlq_vals = ", ".join(f"s.{c}" for c in [*_DLQ_COLS, "payload_hash"])
            spark.sql(
                f"MERGE INTO {dlq_table} t USING batch_invalid s "
                "ON t.payload_hash = s.payload_hash "
                f"WHEN NOT MATCHED THEN INSERT ({_dlq_cols}) VALUES ({_dlq_vals})"
            )

    spark.streams.addListener(_BatchProgressLogger())
    logger.info(
        f"startingOffsets={settings.stream_starting_offsets} (no-checkpoint only) checkpoint={checkpoint}"
    )
    _attempt = 0
    while True:
        try:
            query = (
                parsed_df.writeStream.foreachBatch(_write_batch)
                .queryName("webhooks_all")
                .trigger(processingTime="60 seconds")
                .option("maxOffsetsPerTrigger", 200000)
                .option("checkpointLocation", checkpoint)
                .start()
            )
            query.awaitTermination()
            break
        except PermanentIngestError:
            # Nothing about a retry changes the answer: the config or the
            # producer has to change first. Retrying here burned five replays
            # and about nine minutes of backoff on the same refusal.
            logger.error(
                "ingestion refused a batch for a reason a retry cannot fix; "
                "not restarting the stream"
            )
            raise
        except Exception:
            _attempt += 1
            if _attempt >= _SUPERVISOR_MAX_ATTEMPTS:
                logger.error(
                    f"ingestion stream failed {_attempt} times, giving up. Inspect the "
                    "batch error and the DLQ before restarting; a message that always "
                    "fails will fail again on the replay."
                )
                raise
            _delay = supervisor_delay(_attempt - 1)
            logger.warning(
                f"ingestion stream died, restart {_attempt}/{_SUPERVISOR_MAX_ATTEMPTS} "
                f"in {_delay:.0f}s (offsets commit only on success, so the failed batch "
                "replays and the MERGE converges)"
            )
            time.sleep(_delay)


def ensure_webhook_table(spark, table: str) -> None:
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
            instrument_type string,
            duplicate_mutation_count bigint,
            last_duplicate_at timestamp,
            currency string
        ) USING iceberg
        PARTITIONED BY (bucket(16, transaction_id))
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'gc.enabled' = 'true'
        )
    """
    )
    # A table created before the duplicate-mutation columns existed still has the
    # old shape, and CREATE TABLE IF NOT EXISTS above is a no-op for it.
    spark.sql(
        f"ALTER TABLE {table} ADD COLUMNS IF NOT EXISTS ("
        "duplicate_mutation_count bigint, last_duplicate_at timestamp, currency string)"
    )


def ensure_dlq_table(spark, table: str) -> None:
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
            payload_hash string
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'gc.enabled' = 'true'
        )
    """
    )
    spark.sql(f"ALTER TABLE {table} ADD COLUMNS IF NOT EXISTS (payload_hash string)")


if __name__ == "__main__":
    settings = get_settings()
    webhook_table = _qualified_table(settings.webhook_table)
    dlq_table = _qualified_table(settings.dlq_table)

    logger.info("Initializing Iceberg tables via Nessie")
    spark = get_spark_session("Init")
    ensure_webhook_table(spark, webhook_table)
    ensure_dlq_table(spark, dlq_table)

    run_ingestion()
