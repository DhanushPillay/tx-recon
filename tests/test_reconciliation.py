import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TxRecon-Tests") \
        .getOrCreate()

def test_data_quality_filter(spark):
    # Simulate the filter logic from ingest_webhooks.py
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("amount_paise", IntegerType(), True)
    ])
    
    data = [
        ("tx_1", 1000),    # Valid
        ("tx_2", -500),    # Invalid (negative)
        (None, 500),       # Invalid (missing ID)
        ("tx_4", 0)        # Invalid (zero)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Filter valid
    valid_df = df.filter((df.amount_paise > 0) & (df.transaction_id.isNotNull()))
    
    # Expected valid
    expected_valid_data = [("tx_1", 1000)]
    expected_valid_df = spark.createDataFrame(expected_valid_data, schema)
    
    assert_df_equality(valid_df, expected_valid_df, ignore_row_order=True)

def test_reconciliation_logic(spark):
    # Testing the core math of the MERGE statement (without actual iceberg MERGE)
    # If gateway amount * 0.985 == settled amount -> MATCHED
    
    # Gateway data
    webhooks_data = [
        ("tx_1", 100000, "SUCCESS"), # 98500 expected
        ("tx_2", 100000, "SUCCESS")  # 98500 expected
    ]
    webhooks_df = spark.createDataFrame(webhooks_data, ["transaction_id", "amount_paise", "gateway_status"])
    
    # Bank data
    bank_data = [
        ("tx_1", 98500, "bnk_1"), # Exact match
        ("tx_2", 98000, "bnk_2")  # Mismatch fee
    ]
    bank_df = spark.createDataFrame(bank_data, ["transaction_id", "settled_amount_paise", "bank_ref_id"])
    
    # Join to simulate merge logic
    joined_df = webhooks_df.join(bank_df, "transaction_id", "left")
    
    # Apply logic
    from pyspark.sql.functions import when, col
    
    result_df = joined_df.withColumn(
        "reconciliation_status",
        when((col("amount_paise") * 0.985) == col("settled_amount_paise"), "MATCHED")
        .when((col("amount_paise") * 0.985) != col("settled_amount_paise"), "EXCEPTION_FEE_MISMATCH")
        .otherwise("UNRECONCILED")
    )
    
    # Extract results
    results = {row["transaction_id"]: row["reconciliation_status"] for row in result_df.collect()}
    
    assert results["tx_1"] == "MATCHED"
    assert results["tx_2"] == "EXCEPTION_FEE_MISMATCH"
