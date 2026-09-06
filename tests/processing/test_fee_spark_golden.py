import platform

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import get_spark_session
from src.processing.fee_engine import get_fee_engine
from src.processing.reconcile import build_fee_case_sql

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        platform.system() == "Windows",
        reason="collect() needs Python workers, which crash on Windows with Python 3.13",
    ),
]


def test_spark_vs_python_golden_fee_calculation():
    """
    Ensures that the Spark SQL translation of fee logic exactly matches
    the canonical Python FeeEngine logic for all instruments and edges.
    Uses the SAME builder as production (no copy-paste).
    """
    spark = get_spark_session("GoldenFeeTest")
    engine = get_fee_engine()

    test_cases = [
        # amount_paise, instrument
        (100000, "UPI"),
        (100000, "CREDIT_CARD"),
        (100000, "DEBIT_CARD"),
        (100000, "INTERNATIONAL"),
        (123456, "CREDIT_CARD"),  # weird amount
        (9999, "INTERNATIONAL"),
        (1, "UPI"),
        (25000, "NETBANKING"),
        (75000, "WALLET"),
        (0, "UPI"),
        (100000000, "CREDIT_CARD"),
    ]

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("amount_paise", LongType(), True),
            StructField("instrument_type", StringType(), True),
        ]
    )

    data = [(f"tx_{i}", amt, inst) for i, (amt, inst) in enumerate(test_cases)]
    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("test_webhooks")

    fee_case_sql, gst_case_sql = build_fee_case_sql(engine, inst_col="t.instrument_type")

    result_df = spark.sql(f"""
        SELECT
            transaction_id,
            amount_paise,
            instrument_type,
            {fee_case_sql} AS spark_fee,
            {gst_case_sql} AS spark_gst
        FROM test_webhooks t
    """)

    rows = result_df.collect()

    for row in rows:
        py_result = engine.compute_fee(row.amount_paise, row.instrument_type)
        assert row.spark_fee == py_result.fee_paise - py_result.gst_paise, (
            f"Fee mismatch for {row.instrument_type} at {row.amount_paise}"
        )
        assert row.spark_gst == py_result.gst_paise, (
            f"GST mismatch for {row.instrument_type} at {row.amount_paise}"
        )
