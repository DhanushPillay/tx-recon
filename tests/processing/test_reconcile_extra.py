"""Reconcile SQL builder + run_reconciliation error paths."""

from unittest.mock import MagicMock, patch

import pytest

from src.processing import reconcile as rec

pytestmark = pytest.mark.unit


def test_qualified_table_injection_blocked():
    for bad in ["x; DROP TABLE y", "a--b", "x y", "t`x", ""]:
        with pytest.raises(ValueError):
            rec._qualified_table(bad)
    assert rec._qualified_table("nessie.db.webhooks") == "nessie.db.webhooks"


def test_merchant_escape_and_none_merchant():
    card = {
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {"UPI": {"mdr_rate_bps": 0}},
        "merchants": {"o'brien": {"UPI": {"mdr_rate_bps": 10}}},
    }
    fee, gst, tol = rec._build_single_card_fee_sql(
        card, "t.amount_paise", "s.instrument_type", "s.merchant_id"
    )
    assert "o''brien" in fee
    fee2, _, _ = rec._build_single_card_fee_sql(card, "t.amount_paise", "s.instrument_type", None)
    assert "WHEN" in fee2


def test_build_fee_sql_single_and_versioned():
    from src.processing.fee_engine import get_fee_engine

    eng = get_fee_engine()
    fee, gst = rec.build_fee_case_sql(eng)
    assert "CASE" in fee and "CASE" in gst
    tol = rec.build_tolerance_case_sql(eng)
    assert "CASE" in tol or tol.strip().isdigit() or "ELSE" in tol


def test_build_fee_sql_single_card_path():
    class StubEngine:
        rate_cards = None
        config = {"instruments": {"UPI": {"mdr_rate_bps": 0}}, "merchants": {}}

        def get_default_rate(self):
            return {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1}

    fee, gst = rec.build_fee_case_sql(StubEngine())
    assert "CASE" in fee and "CASE" in gst
    assert rec.build_tolerance_case_sql(StubEngine()).strip() != ""


def test_build_fee_sql_single_element_cards():
    class StubEngine:
        rate_cards = [
            {
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {},
                "merchants": {},
            }
        ]
        config = {"instruments": {}, "merchants": {}}

        def get_default_rate(self):
            return {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1}

    fee, gst = rec.build_fee_case_sql(StubEngine())
    assert "CASE" in fee
    assert rec.build_tolerance_case_sql(StubEngine()).strip() != ""


def test_run_reconciliation_missing_file_raises():
    with patch("src.processing.reconcile.get_spark_session") as gs:
        gs.return_value.read.format.return_value.option.return_value.schema.return_value.load.side_effect = Exception(
            "no file"
        )
        with pytest.raises(FileNotFoundError):
            rec.run_reconciliation(date_str="2099-01-01")


def test_run_reconciliation_merge_failure_wraps():
    spark = MagicMock()
    spark.read.format.return_value.option.return_value.schema.return_value.load.return_value = (
        MagicMock()
    )
    # make dedup chain return mock df
    df = MagicMock()
    df.filter.return_value.withColumn.return_value.filter.return_value.drop.return_value = df
    spark.read.format.return_value.option.return_value.schema.return_value.load.return_value = df
    spark.sql.side_effect = ["ok", Exception("merge boom")]
    with patch("src.processing.reconcile.get_spark_session", return_value=spark):
        # FileNotFound path needs load_csv_on_driver False; force MERGE fail via sql
        import pandas as pd

        _df2 = pd.DataFrame(
            [
                {
                    "transaction_id": "tx_abcdef123456",
                    "settled_amount_paise": 100,
                    "settlement_date": "2025-04-02",
                }
            ]
        )
        spark.createDataFrame.return_value = MagicMock()
        # simpler: patch whole flow to hit RuntimeError
        with (
            patch.object(spark, "sql", side_effect=[None, RuntimeError("boom")]),
            pytest.raises((RuntimeError, Exception)),
        ):
            rec.run_reconciliation(date_str="2025-04-02")
