import json
import logging
from unittest.mock import MagicMock, patch

from src.common.schemas import WEBHOOK_AVRO_SCHEMA as avro_schema_str
from src.ingestion.ingest_webhooks import _BatchProgressLogger


def test_batch_progress_line_is_key_value(caplog):
    event = MagicMock()
    event.progress.name = "webhooks_valid"
    event.progress.batchId = 3
    event.progress.numInputRows = 1200
    event.progress.durationMs = {"triggerExecution": 450}
    with caplog.at_level(logging.INFO, logger="src.ingestion.ingest_webhooks"):
        _BatchProgressLogger().onQueryProgress(event)
    line = caplog.text
    assert "ingest_batch query=webhooks_valid batch=3 rows=1200 duration_ms=450" in line


def test_avro_schema_valid_json():
    schema_dict = json.loads(avro_schema_str)
    assert schema_dict["type"] == "record"
    assert schema_dict["name"] == "WebhookEvent"
    fields = {f["name"]: f["type"] for f in schema_dict["fields"]}
    assert fields["transaction_id"] == "string"
    assert fields["amount_paise"] == "long"  # must stay 64-bit: matches reconcile LongType


class MockColumn:
    def __gt__(self, other):
        return self

    def __le__(self, other):
        return self

    def __and__(self, other):
        return self

    def __or__(self, other):
        return self

    def __invert__(self):
        return self

    def isNotNull(self):
        return self

    def isNull(self):
        return self

    def alias(self, name):
        return self

    def cast(self, type_str):
        return self


@patch("src.ingestion.ingest_webhooks.get_spark_session")
@patch(
    "src.ingestion.ingest_webhooks.from_avro",
    return_value=MagicMock(alias=MagicMock(return_value=MockColumn())),
)
@patch("src.ingestion.ingest_webhooks.current_timestamp", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.col", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.expr", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.lit", return_value=MockColumn())
def test_run_ingestion_wiring(
    mock_lit, mock_expr, mock_col, mock_ts, mock_from_avro, mock_get_spark
):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark

    mock_df = MagicMock()
    (
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value
    ) = mock_df

    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df

    mock_write = MagicMock()
    mock_df.writeStream.foreachBatch.return_value = mock_write
    mock_write.queryName.return_value = mock_write
    mock_write.trigger.return_value = mock_write
    mock_write.option.return_value = mock_write

    from src.ingestion.ingest_webhooks import run_ingestion

    run_ingestion()

    mock_spark.readStream.format.assert_called_with("kafka")
    # Single foreachBatch sink (not dual toTable): one consumer, NULL-safe split inside.
    mock_df.writeStream.foreachBatch.assert_called_once()
    mock_write.queryName.assert_called_with("webhooks_all")
    assert any("webhooks_all" in str(c) for c in mock_write.option.call_args_list), (
        "checkpoint must be the unified webhooks_all path"
    )
    mock_spark.streams.addListener.assert_called_once()


def test_run_ingestion_ddl_uses_qualified_tables():
    import src.ingestion.ingest_webhooks as mod

    assert hasattr(mod, "_qualified_table")
    with_unsafe = False
    try:
        mod._qualified_table("x; DROP TABLE --")
    except ValueError:
        with_unsafe = True
    assert with_unsafe, "_qualified_table must reject injection"


def test_run_ingestion_null_safe_split():
    """NULL 3VL regression: invalid filter must catch NULLs, not drop them."""
    import inspect

    from src.ingestion import ingest_webhooks as mod

    src = inspect.getsource(mod.run_ingestion)
    assert "coalesce" in src, "invalid split must be NULL-safe via coalesce"
    assert "awaitAnyTermination" not in src, "single query uses query.awaitTermination"
