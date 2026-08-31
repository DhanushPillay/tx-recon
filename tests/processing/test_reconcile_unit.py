import os
import platform
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, when

pytestmark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="PySpark Python worker crashes on Windows with Python 3.13",
)


@pytest.fixture(scope="session")
def spark():
    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.master("local[1]")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .appName("Reconcile-Unit-Tests")
        .getOrCreate()
    )


def test_fee_calculation_exact_match(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(
        webhooks_data, ["transaction_id", "amount_paise"]
    )

    bank_data = [("tx_1", 98500)]
    bank_df = spark.createDataFrame(
        bank_data, ["transaction_id", "settled_amount_paise"]
    )

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
        "reconciliation_status",
        when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            == col("settled_amount_paise"),
            "MATCHED",
        )
        .when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            != col("settled_amount_paise"),
            "EXCEPTION_FEE_MISMATCH",
        )
        .otherwise("UNRECONCILED"),
    )

    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in result_df.collect()
    }

    assert results["tx_1"] == "MATCHED"


def test_fee_calculation_mismatch(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(
        webhooks_data, ["transaction_id", "amount_paise"]
    )

    bank_data = [("tx_1", 98000)]
    bank_df = spark.createDataFrame(
        bank_data, ["transaction_id", "settled_amount_paise"]
    )

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
        "reconciliation_status",
        when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            == col("settled_amount_paise"),
            "MATCHED",
        )
        .when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            != col("settled_amount_paise"),
            "EXCEPTION_FEE_MISMATCH",
        )
        .otherwise("UNRECONCILED"),
    )

    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in result_df.collect()
    }

    assert results["tx_1"] == "EXCEPTION_FEE_MISMATCH"


def test_fee_calculation_no_bank_record(spark):
    webhooks_data = [("tx_1", 100000)]
    webhooks_df = spark.createDataFrame(
        webhooks_data, ["transaction_id", "amount_paise"]
    )

    bank_data = [("tx_2", 98500)]
    bank_df = spark.createDataFrame(
        bank_data, ["transaction_id", "settled_amount_paise"]
    )

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
        "reconciliation_status",
        when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            == col("settled_amount_paise"),
            "MATCHED",
        )
        .when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            != col("settled_amount_paise"),
            "EXCEPTION_FEE_MISMATCH",
        )
        .otherwise("UNRECONCILED"),
    )

    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in result_df.collect()
    }

    assert results["tx_1"] == "UNRECONCILED"


def test_fee_calculation_mixed_results(spark):
    webhooks_data = [
        ("tx_1", 100000),
        ("tx_2", 200000),
        ("tx_3", 50000),
    ]
    webhooks_df = spark.createDataFrame(
        webhooks_data, ["transaction_id", "amount_paise"]
    )

    bank_data = [
        ("tx_1", 98500),
        ("tx_2", 197000),
        ("tx_3", 49000),
    ]
    bank_df = spark.createDataFrame(
        bank_data, ["transaction_id", "settled_amount_paise"]
    )

    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")

    result_df = joined_df.withColumn(
        "reconciliation_status",
        when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            == col("settled_amount_paise"),
            "MATCHED",
        )
        .when(
            (col("amount_paise") - (col("amount_paise") * 15 / 1000))
            != col("settled_amount_paise"),
            "EXCEPTION_FEE_MISMATCH",
        )
        .otherwise("UNRECONCILED"),
    )

    results = {
        row["transaction_id"]: row["reconciliation_status"]
        for row in result_df.collect()
    }

    assert results["tx_1"] == "MATCHED"
    assert results["tx_2"] == "EXCEPTION_FEE_MISMATCH"
    assert results["tx_3"] == "MATCHED"
