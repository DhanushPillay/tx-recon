from pyspark.sql.functions import col
from config import get_spark_session

def run_reconciliation():
    spark = get_spark_session("ReconciliationJob")
    
    # 1. Read the late-arriving bank settlement CSVs
    bank_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("../data/*.csv")
        
    bank_df.createOrReplaceTempView("bank_settlements")
    
    # 2. Perform the MERGE INTO operation using Iceberg
    # This solves the T+1 update problem without rewriting the entire dataset
    merge_sql = """
    MERGE INTO local.db.webhooks t
    USING bank_settlements s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND (t.amount_paise * 0.985) = s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'MATCHED',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND (t.amount_paise * 0.985) != s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'EXCEPTION_FEE_MISMATCH',
            t.bank_ref_id = s.bank_ref_id
    """
    
    print("Running reconciliation MERGE...")
    spark.sql(merge_sql)
    print("Reconciliation complete. Updated Iceberg table.")

if __name__ == "__main__":
    run_reconciliation()
