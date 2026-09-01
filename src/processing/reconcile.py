import logging
import os

from src.common.config import get_spark_session
from src.common.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_reconciliation():
    settings = get_settings()
    project_root = settings.project_root
    data_path = os.path.join(project_root, "data", "*.csv")

    logger.info("Initializing Spark Session for Batch Reconciliation")
    spark = get_spark_session("ReconciliationJob")

    logger.info(f"Reading bank settlement CSVs from {data_path}")
    bank_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(data_path)
    )

    bank_df.createOrReplaceTempView("bank_settlements")

    # Instrument-aware MERGE: compute expected fee per row based on instrument type
    merge_sql = f"""
    MERGE INTO {settings.webhook_table} t
    USING (
        SELECT
            s.transaction_id,
            s.bank_ref_id,
            s.settled_amount_paise,
            s.instrument_type,
            CASE
                WHEN s.instrument_type = 'UPI' THEN 0
                WHEN s.instrument_type = 'CREDIT_CARD' THEN (t.amount_paise * 200) DIV 10000
                WHEN s.instrument_type = 'DEBIT_CARD' THEN (t.amount_paise * 100) DIV 10000
                WHEN s.instrument_type = 'INTERNATIONAL' THEN (t.amount_paise * 300) DIV 10000
                ELSE (t.amount_paise * 150) DIV 10000
            END AS expected_fee,
            CASE
                WHEN s.instrument_type = 'UPI' THEN 0
                WHEN s.instrument_type = 'CREDIT_CARD' THEN ((t.amount_paise * 200) DIV 10000 * 18) DIV 100
                WHEN s.instrument_type = 'DEBIT_CARD' THEN ((t.amount_paise * 100) DIV 10000 * 18) DIV 100
                WHEN s.instrument_type = 'INTERNATIONAL' THEN ((t.amount_paise * 300) DIV 10000 * 18) DIV 100
                ELSE ((t.amount_paise * 150) DIV 10000 * 18) DIV 100
            END AS expected_gst
        FROM bank_settlements s
        JOIN {settings.webhook_table} t ON t.transaction_id = s.transaction_id
    ) s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND (t.amount_paise - s.expected_fee - s.expected_gst) = s.settled_amount_paise THEN
        UPDATE SET
            t.reconciliation_status = 'MATCHED',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND (t.amount_paise - s.expected_fee - s.expected_gst) != s.settled_amount_paise THEN
        UPDATE SET
            t.reconciliation_status = 'EXCEPTION_FEE_MISMATCH',
            t.bank_ref_id = s.bank_ref_id
    WHEN NOT MATCHED THEN
        UPDATE SET
            t.reconciliation_status = 'EXCEPTION_NO_WEBHOOK'
    """

    logger.info("Executing MERGE INTO operation")
    spark.sql(merge_sql)
    logger.info("Reconciliation batch complete.")


if __name__ == "__main__":
    run_reconciliation()
