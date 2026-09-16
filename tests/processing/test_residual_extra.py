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
    spark.sql.return_value.collect.return_value = []
    assert apply_residual(spark, "nessie.db.webhooks") == 0


def test_apply_residual_demotes_and_updates():
    spark = MagicMock()
    row = {
        "transaction_id": "tx_abcdef123456",
        "amount_paise": 100000,
        "settled_amount_paise": 100000,
        "instrument_type": "UPI",
        "merchant_id": None,
        "settlement_date": None,
    }
    spark.sql.return_value.collect.return_value = [row]
    eng = MagicMock()
    eng.compute_expected_settled.side_effect = [100000, 999000]
    eng.get_rate_for_date.side_effect = [{"tolerance_paise": 1}, {"tolerance_paise": 1}]
    import src.processing.residual as res

    orig = res.should_downgrade
    try:
        res.should_downgrade = lambda *a, **k: True
        assert apply_residual(spark, "nessie.db.webhooks") == 1
        assert spark.sql.call_count == 2
    finally:
        res.should_downgrade = orig


def test_apply_residual_rejects_bad_table():
    with pytest.raises(ValueError):
        apply_residual(MagicMock(), "bad; DROP")


def test_apply_residual_escapes_quotes_in_tx_ids():
    spark = MagicMock()
    row = {
        "transaction_id": "tx_o'brien",
        "amount_paise": 100000,
        "settled_amount_paise": 100000,
        "instrument_type": "UPI",
        "merchant_id": None,
        "settlement_date": None,
    }
    spark.sql.return_value.collect.return_value = [row]
    import src.processing.residual as res

    orig = res.should_downgrade
    try:
        res.should_downgrade = lambda *a, **k: True
        assert apply_residual(spark, "nessie.db.webhooks") == 1
        update_sql = spark.sql.call_args_list[1][0][0]
        assert "'tx_o''brien'" in update_sql
        assert "'tx_o'brien'" not in update_sql.replace("''", "")
    finally:
        res.should_downgrade = orig
