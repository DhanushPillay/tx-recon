"""Reconciliation logic tests: Spark for wiring, FeeEngine for money math.

Uses the real FeeEngine.check_match (tolerance-aware) instead of a
reimplemented formula, so these tests break when prod logic drifts.
"""

import pytest

from src.processing.fee_engine import FeeEngine

pytestmark = pytest.mark.skipif(
    __import__("platform").system() == "Windows",
    reason="PySpark Python worker crashes on Windows with Python 3.13",
)


def _reconcile_statuses(spark, webhooks_rows, bank_rows, instrument="CREDIT_CARD"):
    """Join in Spark (wiring), decide in FeeEngine (real logic)."""
    engine = FeeEngine()
    webhooks_df = spark.createDataFrame(webhooks_rows, ["transaction_id", "amount_paise"])
    bank_df = spark.createDataFrame(bank_rows, ["transaction_id", "settled_amount_paise"])
    joined = {
        r["transaction_id"]: (r["amount_paise"], r["settled_amount_paise"])
        for r in webhooks_df.join(bank_df, "transaction_id", "inner").collect()
    }
    statuses = {}
    for tx, (amount, settled) in joined.items():
        matched, _ = engine.check_match(amount, settled, instrument)
        statuses[tx] = "MATCHED" if matched else "EXCEPTION_FEE_MISMATCH"
    webhook_ids = {r["transaction_id"] for r in webhooks_rows}
    for tx in webhook_ids - set(statuses):
        statuses[tx] = "UNRECONCILED"
    return statuses


def test_fee_calculation_exact_match(spark):
    engine = FeeEngine()
    settled = engine.compute_expected_settled(100000, "CREDIT_CARD")
    results = _reconcile_statuses(spark, [("tx_1", 100000)], [("tx_1", settled)], "CREDIT_CARD")
    assert results["tx_1"] == "MATCHED"


def test_fee_calculation_mismatch(spark):
    results = _reconcile_statuses(spark, [("tx_1", 100000)], [("tx_1", 98000)], "CREDIT_CARD")
    assert results["tx_1"] == "EXCEPTION_FEE_MISMATCH"


def test_fee_calculation_no_bank_record(spark):
    results = _reconcile_statuses(spark, [("tx_1", 100000)], [("tx_2", 97640)], "CREDIT_CARD")
    assert results["tx_1"] == "UNRECONCILED"


def test_fee_calculation_mixed_results(spark):
    engine = FeeEngine()
    s1 = engine.compute_expected_settled(100000, "CREDIT_CARD")
    s3 = engine.compute_expected_settled(50000, "CREDIT_CARD")
    results = _reconcile_statuses(
        spark,
        [("tx_1", 100000), ("tx_2", 200000), ("tx_3", 50000)],
        [("tx_1", s1), ("tx_2", 196000), ("tx_3", s3)],
        "CREDIT_CARD",
    )
    assert results["tx_1"] == "MATCHED"
    assert results["tx_2"] == "EXCEPTION_FEE_MISMATCH"
    assert results["tx_3"] == "MATCHED"


def test_tolerance_boundary(spark):
    engine = FeeEngine()
    settled = engine.compute_expected_settled(100000, "CREDIT_CARD")
    # ±1 paise is within default tolerance -> MATCHED; ±2 is not.
    assert (
        _reconcile_statuses(spark, [("t", 100000)], [("t", settled + 1)], "CREDIT_CARD")["t"]
        == "MATCHED"
    )
    assert (
        _reconcile_statuses(spark, [("t", 100000)], [("t", settled + 2)], "CREDIT_CARD")["t"]
        == "EXCEPTION_FEE_MISMATCH"
    )


def test_upi_zero_fee_match(spark):
    results = _reconcile_statuses(spark, [("tx_1", 50000)], [("tx_1", 50000)], "UPI")
    assert results["tx_1"] == "MATCHED"
