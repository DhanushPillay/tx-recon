"""Reconcile wiring tests: pure builder logic + MERGE SQL shape (mocked Spark)."""

from unittest.mock import MagicMock, patch

import pytest

from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import (
    _qualified_table,
    build_fee_case_sql,
    run_reconciliation,
)


def test_fee_case_sql_matches_fee_engine():
    engine = FeeEngine()
    fee_sql, gst_sql = build_fee_case_sql(engine)
    assert "CREDIT_CARD" in fee_sql and "UPI" in fee_sql
    assert "(t.amount_paise * 200 + 5000) DIV 10000" in fee_sql  # 200bps CC
    assert "(t.amount_paise * 0 + 5000) DIV 10000" in fee_sql  # 0bps UPI
    assert "DIV 10000" in gst_sql and "1800" in gst_sql  # 18% GST in bps


def test_fee_case_sql_custom_columns():
    engine = FeeEngine()
    fee_sql, _ = build_fee_case_sql(
        engine, amount_col="w.amount_paise", inst_col="w.instrument_type"
    )
    assert "w.amount_paise" in fee_sql
    assert "w.instrument_type" in fee_sql


def test_qualified_table_rejects_injection():
    with pytest.raises(ValueError):
        _qualified_table("nessie.db.webhooks; DROP TABLE x --")
    assert _qualified_table("nessie.db.webhooks") == "nessie.db.webhooks"


@patch("pyspark.sql.functions.row_number")
@patch("pyspark.sql.functions.col")
@patch("pyspark.sql.window.Window")
@patch("src.processing.reconcile.get_spark_session")
def test_run_reconciliation_wiring(mock_get_spark, mock_window, mock_col, mock_row_number):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark

    mock_bank_df = MagicMock()
    mock_spark.read.format.return_value.option.return_value.schema.return_value.load.return_value = mock_bank_df
    # counts path: 4 deduped settlement rows; table holds 3 MATCHED + 1 FEE_MISMATCH
    mock_bank_df.withColumn.return_value.filter.return_value.drop.return_value.count.return_value = 4
    mock_spark.sql.return_value.collect.return_value = [
        {"reconciliation_status": "MATCHED", "n": 3},
        {"reconciliation_status": "EXCEPTION_FEE_MISMATCH", "n": 1},
    ]

    counts = run_reconciliation()

    mock_spark.read.format.assert_called_with("csv")
    dedup_df = mock_bank_df.withColumn.return_value.filter.return_value.drop.return_value
    dedup_df.createOrReplaceTempView.assert_called_once_with("bank_settlements")

    merge_sql = mock_spark.sql.call_args_list[0][0][0]
    assert "MERGE INTO" in merge_sql
    assert "nessie.db.webhooks" in merge_sql
    assert "bank_settlements" in merge_sql
    assert "ABS(" in merge_sql  # tolerance-aware matching
    assert "EXCEPTION_MISSING_WEBHOOK" in merge_sql
    assert merge_sql.count("UPDATE SET") == 2  # matched + mismatched, never DELETE
    assert merge_sql.count("WHEN NOT MATCHED") == 1
    assert "DELETE" not in merge_sql
    assert counts == {
        "settlement_rows_deduped": 4,
        "MATCHED": 3,
        "EXCEPTION_FEE_MISMATCH": 1,
    }
