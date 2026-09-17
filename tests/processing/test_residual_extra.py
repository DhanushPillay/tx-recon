"""Residual scorer edges + apply_residual with mocked spark."""

from unittest.mock import MagicMock

import pytest

from src.processing.residual import _sigmoid, apply_residual, should_downgrade

pytestmark = pytest.mark.unit


def test_sigmoid_both_branches():
    assert 0.49 < _sigmoid(0) < 0.51
    assert _sigmoid(100) > 0.99
    assert _sigmoid(-100) < 0.01


def test_excess_zero_vs_positive():
    eng = MagicMock()
    eng.compute_expected_settled.return_value = 1000
    eng.get_rate_for_date.return_value = {"tolerance_paise": 1}
    assert should_downgrade(1000, 1000, "UPI", None, None, fee_engine=eng) is False
    # merchant disagreement forces 0.95 -> downgrade at default tau 0.9
    eng2 = MagicMock()
    eng2.compute_expected_settled.side_effect = [1000, 2000]
    eng2.get_rate_for_date.side_effect = [{"tolerance_paise": 1}, {"tolerance_paise": 1}]
    assert should_downgrade(1000, 1000, "UPI", "m1", None, fee_engine=eng2) is True


def test_apply_residual_no_rows_returns_zero():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"n": 0}]
    assert apply_residual(spark, "nessie.db.webhooks") == 0
    assert spark.sql.call_count == 1  # count only, no MERGE


def test_apply_residual_demotes_via_merge():
    from src.processing.fee_engine import FeeEngine

    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"n": 2}]
    assert apply_residual(spark, "nessie.db.webhooks", fee_engine=FeeEngine()) == 2
    assert spark.sql.call_count == 2
    merge_sql = spark.sql.call_args_list[1][0][0]
    assert "MERGE INTO nessie.db.webhooks" in merge_sql
    assert "bank_settlements" in merge_sql
    assert ">= 0.9" in merge_sql
    assert "IN (" not in merge_sql  # set-based: no tx-id literals


def test_apply_residual_count_failure_returns_zero():
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = RuntimeError("boom")
    assert apply_residual(spark, "nessie.db.webhooks") == 0


def test_score_match_sql_reuses_builders():
    from src.processing.fee_engine import FeeEngine
    from src.processing.residual import score_match_sql

    sql = score_match_sql(FeeEngine())
    assert "CAST(NULL AS STRING)" in sql  # default leg ignores merchant
    assert "s.merchant_id" in sql  # merchant leg
    assert "GREATEST" in sql and "EXP(" in sql
    assert "0.95" in sql and "0.75" in sql


def test_apply_residual_rejects_bad_table():
    with pytest.raises(ValueError):
        apply_residual(MagicMock(), "bad; DROP")
