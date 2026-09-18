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


def _mock_stream(spark, parsed, ws, captured):
    """Wire the readStream->foreachBatch chain; return hook capturing the batch fn."""
    rs = spark.readStream.format.return_value
    rs.option.return_value = rs
    rs.load.return_value.withColumn.return_value = MagicMock()
    rs.load.return_value.withColumn.return_value.select.return_value.select.return_value = parsed
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
    invalid.isEmpty.return_value = False
    late = MagicMock(name="late")
    late.count.return_value = 0
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
