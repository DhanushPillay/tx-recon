from unittest.mock import MagicMock, patch

from src.processing.reconcile import run_reconciliation


@patch("src.processing.reconcile.get_spark_session")
def test_run_reconciliation_wiring(mock_get_spark):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark

    mock_bank_df = MagicMock()
    mock_spark.read.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_bank_df
    )

    run_reconciliation()

    mock_spark.read.format.assert_called_with("csv")
    mock_bank_df.createOrReplaceTempView.assert_called_once_with("bank_settlements")

    sql_call = mock_spark.sql.call_args[0][0]
    assert "MERGE INTO" in sql_call
    assert "nessie.db.webhooks" in sql_call
    assert "bank_settlements" in sql_call


@patch("src.processing.reconcile.get_spark_session")
def test_run_reconciliation_main_block(mock_get_spark):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark
    mock_spark.read.format.return_value.option.return_value.option.return_value.load.return_value = (
        MagicMock()
    )

    run_reconciliation()

    mock_spark.sql.assert_called_once()
