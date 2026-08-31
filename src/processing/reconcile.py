import logging

from src.common.config import get_spark_session

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_reconciliation():
    logger.info("Initializing Spark Session for Batch Reconciliation")
    spark = get_spark_session("ReconciliationJob")

    # 1. Read the late-arriving bank settlement CSVs
    logger.info("Reading bank settlement CSVs from ../data/*.csv")
    bank_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("../data/*.csv")
    )

    bank_df.createOrReplaceTempView("bank_settlements")

    # 2. Perform the MERGE INTO operation using Iceberg via Nessie
    merge_sql = """
    MERGE INTO nessie.db.webhooks t
    USING bank_settlements s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND (t.amount_paise - (t.amount_paise * 15 / 1000)) = s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'MATCHED',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND (t.amount_paise - (t.amount_paise * 15 / 1000)) != s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'EXCEPTION_FEE_MISMATCH',
            t.bank_ref_id = s.bank_ref_id
    """

    logger.info("Executing MERGE INTO operation on nessie.db.webhooks")
    spark.sql(merge_sql)
    logger.info("Reconciliation batch complete.")


if __name__ == "__main__":
    run_reconciliation()
