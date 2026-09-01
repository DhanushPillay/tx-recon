import pytest
from pyspark.sql.functions import col, lit, when

pytestmark = pytest.mark.skipif(
    __import__("platform").system() == "Windows",
    reason="PySpark Python worker crashes on Windows with Python 3.13",
)


def _fee_div(amount_col):
    """Integer division fee calculation matching production MERGE SQL."""
    return (amount_col * 15) / lit(1000)


def test_fee_calculation_exact_match(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise"])

    bank_data = [("tx_1", 98500)]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise"])

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
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

    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}

    assert results["tx_1"] == "MATCHED"


def test_fee_calculation_mismatch(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise"])

    bank_data = [("tx_1", 98000)]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise"])

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
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

    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}

    assert results["tx_1"] == "EXCEPTION_FEE_MISMATCH"


def test_fee_calculation_no_bank_record(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise"])

    bank_data = [("tx_2", 98500)]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise"])

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
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

    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}

    assert results["tx_1"] == "UNRECONCILED"


def test_fee_calculation_mixed_results(spark):
    webhooks_data = [
        ("tx_1", 100000),
        ("tx_2", 200000),
        ("tx_3", 50000),
    ]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise"])

    bank_data = [
        ("tx_1", 98500),
        ("tx_2", 196000),
        ("tx_3", 49250),
    ]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise"])

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
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

    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}

    assert results["tx_1"] == "MATCHED"
    assert results["tx_2"] == "EXCEPTION_FEE_MISMATCH"
    assert results["tx_3"] == "MATCHED"


def test_fee_non_round_amount(spark):
    """Test with amount where 15 * amount is not evenly divisible by 1000."""
    webhooks_data = [("tx_1", 10001)]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise"])

    # 10001 * 15 = 150015, 150015 DIV 1000 = 150, expected = 10001 - 150 = 9851
    bank_data = [("tx_1", 9851)]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise"])

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
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

    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}

    assert results["tx_1"] == "MATCHED"
