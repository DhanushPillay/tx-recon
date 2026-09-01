import json
from unittest.mock import MagicMock, patch

from src.common.schemas import WEBHOOK_AVRO_SCHEMA as avro_schema_str


def test_avro_schema_valid_json():
    schema_dict = json.loads(avro_schema_str)
    assert schema_dict["type"] == "record"
    assert schema_dict["name"] == "WebhookEvent"
    fields = {f["name"]: f["type"] for f in schema_dict["fields"]}
    assert fields["transaction_id"] == "string"
    assert fields["amount_paise"] == "int"


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
    mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_df
    )

    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df

    mock_enriched = MagicMock()
    mock_enriched.withColumn.return_value = mock_enriched
    mock_enriched.withColumn.return_value.withColumn.return_value = mock_enriched
    mock_df.withColumn.return_value = mock_enriched

    mock_write = MagicMock()
    mock_enriched.writeStream.format.return_value = mock_write
    mock_write.outputMode.return_value = mock_write
    mock_write.trigger.return_value = mock_write
    mock_write.option.return_value = mock_write

    from src.ingestion.ingest_webhooks import run_ingestion

    run_ingestion()

    mock_spark.readStream.format.assert_called_with("kafka")
    mock_spark.streams.awaitAnyTermination.assert_called_once()


@patch("src.ingestion.ingest_webhooks.get_spark_session")
@patch(
    "src.ingestion.ingest_webhooks.from_avro",
    return_value=MagicMock(alias=MagicMock(return_value=MockColumn())),
)
@patch("src.ingestion.ingest_webhooks.current_timestamp", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.col", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.expr", return_value=MockColumn())
@patch("src.ingestion.ingest_webhooks.lit", return_value=MockColumn())
def test_run_ingestion_main_block(
    mock_lit, mock_expr, mock_col, mock_ts, mock_from_avro, mock_get_spark
):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark

    mock_df = MagicMock()
    mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_df
    )

    mock_enriched = MagicMock()
    mock_enriched.withColumn.return_value = mock_enriched
    mock_df.withColumn.return_value = mock_enriched

    mock_write = MagicMock()
    mock_enriched.writeStream.format.return_value = mock_write
    mock_write.outputMode.return_value = mock_write
    mock_write.trigger.return_value = mock_write
    mock_write.option.return_value = mock_write

    from src.ingestion.ingest_webhooks import run_ingestion

    run_ingestion()

    mock_spark.streams.awaitAnyTermination.assert_called_once()
