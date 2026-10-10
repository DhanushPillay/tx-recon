from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.ingestion.ingest_webhooks import _BatchProgressLogger, _qualified_table

pytestmark = pytest.mark.unit


class _FakeCol:
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

    def isNull(self):
        return _FakeCol()

    def cast(self, *a):
        return _FakeCol()

    def rlike(self, *a):
        return _FakeCol()

    def __or__(self, other):
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
    rs = spark.readStream.format.return_value
    rs.option.return_value = rs
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
    mocker.patch.object(ing, "_registry_schema_id", return_value="123")
    mocker.patch("src.validation.pan_guard.count_spark_pan_hits", return_value=0)


def _merge_sql(spark):
    for call in spark.sql.call_args_list:
        if "MERGE INTO" in call[0][0]:
            return call[0][0]
    return None


def _gauge(n):
    g = MagicMock(name="gauge")
    g.count.return_value = n
    return g


def _filter_stubs(valid, invalid, late_count=0):
    return [_gauge(late_count), _gauge(0), valid, invalid]


def test_supervisor_delay_doubles_then_caps():
    from src.ingestion.ingest_webhooks import (
        _SUPERVISOR_MAX_DELAY_S,
        supervisor_delay,
    )

    delays = [supervisor_delay(i) for i in range(12)]
    assert delays[:7] == [5.0, 10.0, 20.0, 40.0, 80.0, 160.0, _SUPERVISOR_MAX_DELAY_S]
    assert all(d == _SUPERVISOR_MAX_DELAY_S for d in delays[7:])


def test_run_ingestion_restarts_a_dead_stream_then_gives_up(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    slept = []
    mocker.patch.object(ing, "time", SimpleNamespace(sleep=slept.append))
    ws.start.side_effect = RuntimeError("stream died")

    with pytest.raises(RuntimeError, match="stream died"):
        ing.run_ingestion()

    from src.ingestion.ingest_webhooks import _SUPERVISOR_MAX_ATTEMPTS

    assert ws.start.call_count == _SUPERVISOR_MAX_ATTEMPTS
    assert len(slept) == _SUPERVISOR_MAX_ATTEMPTS - 1


def test_run_ingestion_does_not_restart_a_clean_stop(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)

    def _no_sleep(_seconds):
        raise AssertionError("slept")

    mocker.patch.object(ing, "time", SimpleNamespace(sleep=_no_sleep))
    ws.start.return_value.awaitTermination.return_value = None

    ing.run_ingestion()
    assert ws.start.call_count == 1


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
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.select.return_value = invalid
    invalid.count.return_value = 1
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    batch.filter.side_effect = _filter_stubs(valid, invalid, 0)
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    captured["fn"](batch, 0)
    assert "MERGE INTO" in _merge_sql(spark)
    enriched.createOrReplaceTempView.assert_called_once_with("batch_valid")
    merges = [c[0][0] for c in spark.sql.call_args_list if "MERGE INTO" in c[0][0]]
    assert len(merges) == 2  # target MERGE + idempotent DLQ MERGE (no append)
    assert any("webhooks_dlq" in m for m in merges)


def test_write_batch_refuses_on_schema_id_drift(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    batch.select.return_value.distinct.return_value.collect.return_value = [{"schema_id": "999"}]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    batch.filter.side_effect = _filter_stubs(valid, invalid, 0)
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    with pytest.raises(RuntimeError, match="schema-id drift"):
        captured["fn"](batch, 0)
    assert _merge_sql(spark) is None


def test_write_batch_uses_explicit_columns(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.select.return_value = invalid
    invalid.count.return_value = 1
    batch.filter.side_effect = _filter_stubs(valid, invalid, 0)
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    captured["fn"](batch, 0)
    merge_sql = _merge_sql(spark)
    assert "INSERT *" not in merge_sql
    assert "INSERT (transaction_id, amount_paise" in merge_sql
    invalid.select.assert_called_once_with(*ing._DLQ_COLS)
    assert ing._WEBHOOK_COLS == [
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
    ]
    assert ing._DLQ_COLS == [
        "transaction_id",
        "amount_paise",
        "gateway_status",
        "timestamp_utc",
        "merchant_id",
        "processing_run_id",
    ]


def test_write_batch_pan_refuses_batch(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    mocker.patch("src.validation.pan_guard.count_spark_pan_hits", return_value=2)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    mocker.patch.object(ing, "F", new=MagicMock())
    with pytest.raises(RuntimeError, match="PAN detected"):
        captured["fn"](batch, 0)
    assert _merge_sql(spark) is None
    batch.unpersist.assert_called_once()


def test_count_spark_pan_hits_luhn_verifies(mocker):
    from unittest.mock import MagicMock

    from src.validation.pan_guard import count_spark_pan_hits

    mocker.patch("pyspark.sql.functions.col", side_effect=lambda *a, **k: _FakeCol())

    df = MagicMock()
    df.columns = ["transaction_id", "merchant_id"]
    cands = MagicMock()
    df.filter.return_value = cands
    cands.select.return_value.collect.return_value = [
        ("4111111111111111", "m1"),
        ("TXN000000001", "m2"),
    ]
    assert count_spark_pan_hits(df, ["transaction_id", "merchant_id"]) == 1
    cands.select.return_value.collect.return_value = [("TXN000000001", "m2")]
    assert count_spark_pan_hits(df, ["transaction_id", "merchant_id"]) == 0


def test_write_batch_late_rows_rescued_by_webhook(mocker):
    import src.ingestion.ingest_webhooks as ing
    from src.common.schemas import EXCEPTION_LATE_UNRESOLVED

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    _run_batch(ing, mocker, captured)
    merge_sql = _merge_sql(spark)
    assert EXCEPTION_LATE_UNRESOLVED in merge_sql


def _run_batch(ing, mocker, captured, *, empty=False, late_n=0, select_side_effect=None):
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = empty
    batch.cache.return_value = batch
    if select_side_effect is not None:
        batch.select.side_effect = select_side_effect
    else:
        batch.select.return_value.distinct.return_value.collect.return_value = [
            {"schema_id": "123"}
        ]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    batch.filter.side_effect = _filter_stubs(valid, invalid, late_n)
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
    assert _merge_sql(spark) is None


def test_write_batch_schema_id_check_skipped_warns(mocker, caplog):
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        _run_batch(ing, mocker, captured, select_side_effect=RuntimeError("boom"))
    assert any("schema-id check skipped" in r.message for r in caplog.records)
    assert "MERGE INTO" in _merge_sql(spark)


def test_write_batch_late_data_warns(mocker, caplog):
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        _run_batch(ing, mocker, captured, late_n=3)
    assert any("late-data" in r.message for r in caplog.records)
    assert "MERGE INTO" in _merge_sql(spark)


def test_ensure_webhook_table_issues_no_write_ordering(mocker, caplog):
    import logging

    import src.ingestion.ingest_webhooks as ing

    spark = MagicMock()
    with caplog.at_level(logging.WARNING, logger="src.ingestion.ingest_webhooks"):
        ing.ensure_webhook_table(spark, "nessie.db.webhooks")
    issued = [c.args[0] for c in spark.sql.call_args_list if c.args and c.args[0]]
    assert not any("WRITE ORDERED BY" in s for s in issued)
    assert not any("Write ordering skipped" in r.message for r in caplog.records)


def test_ensure_webhook_table_bucket_partitions_join_key():
    spark = MagicMock()
    import src.ingestion.ingest_webhooks as ing

    ing.ensure_webhook_table(spark, "nessie.db.webhooks")
    ddl = spark.sql.call_args_list[1][0][0]
    assert "PARTITIONED BY (bucket(16, transaction_id))" in ddl


def test_write_batch_merge_failure_propagates(mocker):
    import src.ingestion.ingest_webhooks as ing

    spark, parsed, ws, captured = MagicMock(), MagicMock(), MagicMock(), {}
    _mock_stream(spark, parsed, ws, captured)
    _patch_ingest(ing, mocker, spark)
    ing.run_ingestion()
    batch = MagicMock(name="batch")
    batch.isEmpty.return_value = False
    batch.cache.return_value = batch
    batch.select.return_value.distinct.return_value.collect.return_value = [{"schema_id": "123"}]
    valid = MagicMock(name="valid")
    valid.dropDuplicates.return_value = valid
    valid.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = MagicMock()
    invalid = MagicMock(name="invalid")
    invalid.dropDuplicates.return_value = invalid
    invalid.count.return_value = 0
    batch.filter.side_effect = _filter_stubs(valid, invalid, 0)
    mocker.patch.object(ing, "lit", return_value=MagicMock())
    mocker.patch.object(ing, "current_timestamp", return_value=MagicMock())
    mocker.patch.object(ing, "F", new=MagicMock())
    spark.sql.side_effect = RuntimeError("merge boom")
    with pytest.raises(RuntimeError, match="merge boom"):
        captured["fn"](batch, 0)


def test_run_ingestion_requires_sasl_when_flagged(mocker):
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
