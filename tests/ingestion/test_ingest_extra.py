"""Ingestion listener + write-batch + run wiring."""

from unittest.mock import MagicMock

import pytest

from src.ingestion.ingest_webhooks import _BatchProgressLogger, _qualified_table

pytestmark = pytest.mark.unit


class _FakeCol:
    """JVM-free stand-in for pyspark Column (supports operators used in ingest)."""

    def __gt__(self, other):
        return _FakeCol()

    def __lt__(self, other):
        return _FakeCol()

    def __le__(self, other):
        return _FakeCol()

    def __and__(self, other):
        return _FakeCol()

    def __invert__(self):
        return _FakeCol()

    def isNotNull(self):
        return _FakeCol()

    def alias(self, *a):
        return _FakeCol()


def test_listener_noops():
    _BatchProgressLogger().onQueryStarted(MagicMock())
    _BatchProgressLogger().onQueryTerminated(MagicMock())
    p = MagicMock()
    p.name = "q"
    p.batchId = 1
    p.numInputRows = 5
    p.durationMs = {"triggerExecution": 10}
    _BatchProgressLogger().onQueryProgress(MagicMock(progress=p))


def test_qualified_table_ok_and_bad():
    assert _qualified_table("nessie.db.webhooks") == "nessie.db.webhooks"
    with pytest.raises(ValueError):
        _qualified_table("x; DROP")


def test_registry_schema_id_success_and_failure(mocker, caplog):
    """Registry id lookup: returns the id string; None (warn-only) when down."""
    import logging
    import sys
    import types

    import src.ingestion.ingest_webhooks as ing

    fake_client_cls = MagicMock()
    fake_client_cls.return_value.get_latest_version.return_value.schema_id = 42
    fake_sr = types.ModuleType("confluent_kafka.schema_registry")
    fake_sr.SchemaRegistryClient = fake_client_cls
    mocker.patch.dict(
        sys.modules,
        {
            "confluent_kafka": types.ModuleType("confluent_kafka"),
            "confluent_kafka.schema_registry": fake_sr,
        },
    )
    settings = MagicMock(schema_registry_url="http://x", topic_name="gateway_webhooks")
    assert ing._registry_schema_id(settings) == "42"
    fake_client_cls.side_effect = RuntimeError("registry down")
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        assert ing._registry_schema_id(settings) is None
    assert any("lookup skipped" in r.message for r in caplog.records)


def _mock_stream(spark, parsed, ws, captured):
    """Wire the readStream->foreachBatch chain; return hook capturing the batch fn."""
    rs = spark.readStream.format.return_value
    rs.option.return_value = rs
    # run_ingestion calls withColumn twice (payload + schema_id), then
    # select(...).select(...) to unpack the Avro struct.
    staged = MagicMock()
    staged.withColumn.return_value = staged
    staged.select.return_value.select.return_value = parsed
    rs.load.return_value.withColumn.return_value = staged
    parsed.withColumn.return_value = parsed
    parsed.withWatermark.return_value = parsed
    parsed.writeStream.foreachBatch.return_value = ws
    ws.queryName.return_value = ws
    ws.trigger.return_value = ws
    ws.option.return_value = ws

    def fake_foreach(fn):
        captured["fn"] = fn
        return ws

    parsed.writeStream.foreachBatch.side_effect = fake_foreach


def _patch_ingest(ing, mocker, spark):
    mocker.patch.object(ing, "get_spark_session", return_value=spark)
    mocker.patch.object(ing, "from_avro", return_value=MagicMock())
    mocker.patch.object(ing, "expr", return_value=MagicMock())
    mocker.patch.object(ing, "to_timestamp", return_value=_FakeCol())
    mocker.patch.object(ing, "col", side_effect=lambda *a, **k: _FakeCol())
    # Registry lookup is a live HTTP call: pin it, the drift logic is
    # exercised through _ids, not through the network.
    mocker.patch.object(ing, "_registry_schema_id", return_value="123")


def test_run_ingestion_wiring(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    assert "fn" in captured
    spark.streams.addListener.assert_called_once()
    ws.start.assert_called_once()
    ws.start.return_value.awaitTermination.assert_called_once()


def test_write_batch_merges_and_dlqs(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    enriched = MagicMock(name="enriched")
    valid = MagicMock(name="valid")
    # filter(valid_cond) -> valid; valid.dropDuplicates -> valid
    valid.dropDuplicates.return_value = valid
    # valid.withColumn x3 -> enriched (has createOrReplaceTempView)
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 1
    late = MagicMock(name="late")
    late.count.return_value = 0
    batch.isEmpty.return_value = False
    # _write_batch filters 3x: late-data count, valid split, invalid split
    batch.filter.side_effect = [late, valid, invalid]
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    captured["fn"](batch, 0)
    spark.sql.assert_called_once()
    assert "MERGE INTO" in spark.sql.call_args_list[0][0][0]
    enriched.createOrReplaceTempView.assert_called_once_with("batch_valid")
    invalid.writeTo.assert_called_once()


def test_write_batch_warns_on_schema_id_drift(mocker, caplog):
    """Batch ids != registry id -> schema-id drift warning (warn-only, still merges)."""
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.select.return_value.distinct.return_value.collect.return_value = [{"schema_id": "999"}]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    late = MagicMock(name="late")
    late.count.return_value = 0
    batch.filter.side_effect = [late, valid, invalid]
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        captured["fn"](batch, 0)
    assert any("schema-id drift" in r.message for r in caplog.records)
    spark.sql.assert_called_once()  # warn-only: the MERGE still ran


def _run_batch(ing, mocker, captured, *, empty=False, late_n=0, select_side_effect=None):
    """Drive the captured foreachBatch fn with a canned batch."""
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = empty
    if select_side_effect is not None:
        batch.select.side_effect = select_side_effect
    else:
        batch.select.return_value.distinct.return_value.collect.return_value = [
            {"schema_id": "123"}
        ]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    late = MagicMock(name="late")
    late.count.return_value = late_n
    batch.filter.side_effect = [late, valid, invalid]
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    captured["fn"](batch, 0)
    return batch


def test_write_batch_empty_is_noop(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = True
    captured["fn"](batch, 0)
    spark.sql.assert_not_called()


def test_write_batch_schema_id_check_skipped_warns(mocker, caplog):
    """Uncollectable schema ids -> warn, batch still merges (never fail on telemetry)."""
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        _run_batch(ing, mocker, captured, select_side_effect=RuntimeError("boom"))
    assert any("schema-id check skipped" in r.message for r in caplog.records)
    spark.sql.assert_called_once()


def test_write_batch_late_data_warns(mocker, caplog):
    """Rows older than the watermark delay are counted and warned, then merged."""
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        _run_batch(ing, mocker, captured, late_n=3)
    assert any("late-data" in r.message for r in caplog.records)
    spark.sql.assert_called_once()


def test_ensure_webhook_table_ordering_failure_warns(mocker, caplog):
    """ALTER WRITE ORDERED BY failing (e.g. existing table) warns, never raises."""
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark = MagicMock()
    spark.sql.side_effect = [MagicMock(), MagicMock(), RuntimeError("already ordered")]
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        ing.ensure_webhook_table(spark, "nessie.db.webhooks")
    assert any("Write ordering skipped" in r.message for r in caplog.records)


def test_ensure_webhook_table_bucket_partitions_join_key():
    """New tables partition by bucket(transaction_id): MERGE prunes to file
    groups instead of full-scanning history (verified live on Nessie)."""
    spark = MagicMock()
    import src.ingestion.ingest_webhooks as ing

    ing.ensure_webhook_table(spark, "nessie.db.webhooks")
    ddl = spark.sql.call_args_list[1][0][0]
    assert "PARTITIONED BY (bucket(16, transaction_id))" in ddl


def test_write_batch_merge_failure_propagates(mocker):
    """MERGE failure must kill the microbatch (Spark restarts from checkpoint).

    Swallowing here would ACK offsets past unmerged rows — silent money loss.
    """
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.select.return_value.distinct.return_value.collect.return_value = [{"schema_id": "123"}]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    late = MagicMock(name="late")
    late.count.return_value = 0
    batch.filter.side_effect = [late, valid, invalid]
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    spark.sql.side_effect = RuntimeError("merge boom")
    with pytest.raises(RuntimeError, match="merge boom"):
        captured["fn"](batch, 0)


def test_run_ingestion_requires_sasl_when_flagged(mocker):
    """REQUIRE_KAFKA_SASL=1 + PLAINTEXT refuses to trust forgeable records."""
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    mocker.patch.object(
        ing,
        "get_settings",
        return_value=MagicMock(
            kafka_broker="x:9092",
            topic_name="gateway_webhooks",
            kafka_security_protocol="PLAINTEXT",
            require_kafka_sasl=True,
            webhook_secret="",
            stream_watermark_delay="1 day",
            iceberg_warehouse="s3a://w",
            webhook_table="nessie.db.webhooks",
            dlq_table="nessie.db.dlq",
        ),
    )
    with pytest.raises(RuntimeError, match="REQUIRE_KAFKA_SASL"):
        ing.run_ingestion()


def test_registry_required_raises_when_down(mocker):
    """REQUIRE_SCHEMA_REGISTRY=1 turns an unreachable registry into failure."""
    import sys
    import types

    import src.ingestion.ingest_webhooks as ing

    fake_client_cls = MagicMock(side_effect=RuntimeError("down"))
    fake_sr = types.ModuleType("confluent_kafka.schema_registry")
    fake_sr.SchemaRegistryClient = fake_client_cls
    mocker.patch.dict(
        sys.modules,
        {
            "confluent_kafka": types.ModuleType("confluent_kafka"),
            "confluent_kafka.schema_registry": fake_sr,
        },
    )
    settings = MagicMock(schema_registry_url="http://x", topic_name="t")
    with pytest.raises(RuntimeError, match="lookup skipped"):
        ing._registry_schema_id(settings, required=True)
