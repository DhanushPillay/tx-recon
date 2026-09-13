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
    Uses the SAME builder as production (no copy-paste). Version and
    merchant aware: settlement_date and merchant_id are provided.
    """
    spark = get_spark_session("GoldenFeeTest")
    engine = get_fee_engine()

    test_cases = [
        # amount, instrument, settlement_date, merchant_id
        (100000, "UPI", "2024-06-15", None),
        (100000, "CREDIT_CARD", "2024-06-15", None),
        (100000, "CREDIT_CARD", "2025-06-15", None),
        (100000, "CREDIT_CARD", "2025-06-15", "merch_001"),  # v2 merchant 180bps vs 200
        (100000, "CREDIT_CARD", "2025-06-15", "merch_demo"),  # v2 190bps
        (100000, "DEBIT_CARD", "2025-06-15", None),
        (100000, "INTERNATIONAL", "2025-06-15", None),
        (123456, "CREDIT_CARD", "2025-06-15", None),
        (9999, "INTERNATIONAL", "2024-06-15", None),
        (1, "UPI", "2025-06-15", None),
        (25000, "NETBANKING", "2025-06-15", None),
        (75000, "WALLET", "2024-06-15", None),
        (0, "UPI", "2025-06-15", None),
        (100000000, "CREDIT_CARD", "2025-06-15", None),
    ]

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("amount_paise", LongType(), True),
            StructField("instrument_type", StringType(), True),
            StructField("settlement_date", StringType(), True),
            StructField("merchant_id", StringType(), True),
        ]
    )

    data = [
        (f"tx_{i}", amt, inst, sdate, merch)
        for i, (amt, inst, sdate, merch) in enumerate(test_cases)
    ]
    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("test_webhooks")

    fee_case_sql, gst_case_sql = build_fee_case_sql(
        engine,
        amount_col="t.amount_paise",
        inst_col="t.instrument_type",
        merchant_col="t.merchant_id",
        settlement_date_col="t.settlement_date",
    )

    result_df = spark.sql(f"""
        SELECT
            transaction_id,
            amount_paise,
            instrument_type,
            settlement_date,
            merchant_id,
            {fee_case_sql} AS spark_fee,
            {gst_case_sql} AS spark_gst
        FROM test_webhooks t
    """)

    rows = result_df.collect()

    for row in rows:
        py_result = engine.compute_fee(
            row.amount_paise,
            row.instrument_type,
            merchant_id=row.merchant_id,
            settlement_date=row.settlement_date,
        )
        assert row.spark_fee == py_result.fee_paise - py_result.gst_paise, (
            f"Fee mismatch for {row.instrument_type} at {row.amount_paise} "
            f"date {row.settlement_date} merchant {row.merchant_id}"
        )
        assert row.spark_gst == py_result.gst_paise, (
            f"GST mismatch for {row.instrument_type} at {row.amount_paise} "
            f"date {row.settlement_date} merchant {row.merchant_id}"
        )
