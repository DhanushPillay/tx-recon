import pytest
from pyspark.sql.functions import col, lit, when

pytestmark = pytest.mark.skipif(
    __import__("platform").system() == "Windows",
    reason="PySpark Python worker crashes on Windows with Python 3.13",
)


def _apply_reconciliation(webhooks_df, bank_df):
    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")
    return joined_df.withColumn(
        "reconciliation_status",
        when(
            (col("amount_paise") - ((col("amount_paise") * 15) / lit(1000)).cast("int"))
            == col("settled_amount_paise"),
            "MATCHED",
        )
        .when(
            (col("amount_paise") - ((col("amount_paise") * 15) / lit(1000)).cast("int"))
            != col("settled_amount_paise"),
            "EXCEPTION_FEE_MISMATCH",
        )
        .otherwise("UNRECONCILED"),
    )


def test_fee_calculation_exact_match(spark):
    webhooks_df = spark.createDataFrame([("tx_1", 100000)], ["transaction_id", "amount_paise"])
    bank_df = spark.createDataFrame([("tx_1", 98500)], ["transaction_id", "settled_amount_paise"])
    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in _apply_reconciliation(webhooks_df, bank_df).collect()
    }
    assert results["tx_1"] == "MATCHED"


def test_fee_calculation_mismatch(spark):
    webhooks_df = spark.createDataFrame([("tx_1", 100000)], ["transaction_id", "amount_paise"])
    bank_df = spark.createDataFrame([("tx_1", 98000)], ["transaction_id", "settled_amount_paise"])
    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in _apply_reconciliation(webhooks_df, bank_df).collect()
    }
    assert results["tx_1"] == "EXCEPTION_FEE_MISMATCH"


def test_fee_calculation_no_bank_record(spark):
    webhooks_df = spark.createDataFrame([("tx_1", 100000)], ["transaction_id", "amount_paise"])
    bank_df = spark.createDataFrame([("tx_2", 98500)], ["transaction_id", "settled_amount_paise"])
    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in _apply_reconciliation(webhooks_df, bank_df).collect()
    }
    assert results["tx_1"] == "UNRECONCILED"


def test_fee_calculation_mixed_results(spark):
    webhooks_df = spark.createDataFrame(
        [("tx_1", 100000), ("tx_2", 200000), ("tx_3", 50000)],
        ["transaction_id", "amount_paise"],
    )
    bank_df = spark.createDataFrame(
        [("tx_1", 98500), ("tx_2", 196000), ("tx_3", 49250)],
        ["transaction_id", "settled_amount_paise"],
    )
    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in _apply_reconciliation(webhooks_df, bank_df).collect()
    }
    assert results["tx_1"] == "MATCHED"
    assert results["tx_2"] == "EXCEPTION_FEE_MISMATCH"
    assert results["tx_3"] == "MATCHED"


def test_fee_non_round_amount(spark):
    webhooks_df = spark.createDataFrame([("tx_1", 10001)], ["transaction_id", "amount_paise"])
    # 10001 * 15 = 150015, 150015 DIV 1000 = 150, expected = 10001 - 150 = 9851
    bank_df = spark.createDataFrame([("tx_1", 9851)], ["transaction_id", "settled_amount_paise"])
    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in _apply_reconciliation(webhooks_df, bank_df).collect()
    }
    assert results["tx_1"] == "MATCHED"
