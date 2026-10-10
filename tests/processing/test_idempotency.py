import os

import pytest
from pyspark.errors import AnalysisException

from src.common.config import get_spark_session
from src.common.settings import get_settings
from src.processing.reconcile import run_reconciliation


def _statuses(df):
    rows = df.groupBy("reconciliation_status").count().collect()
    return {r["reconciliation_status"]: r["count"] for r in rows}


pytestmark = pytest.mark.integration

SEED_STAMP = "19990101"
SEED_ROWS = """bank_ref_id,transaction_id,settled_amount_paise,settlement_date
bnk_idem001,tx_idem_001,100000,1999-01-01
bnk_idem002,tx_idem_002,97640,1999-01-01
bnk_idem003,tx_idem_003,50000,1999-01-01
"""


def test_reconciliation_is_idempotent():
    settings = get_settings()
    spark = get_spark_session("IdempotencyTest")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {settings.webhook_table} (
            transaction_id string,
            amount_paise bigint,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            processing_run_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp,
            instrument_type string
        ) USING iceberg"""
    )

    spark.sql(
        f"""INSERT INTO {settings.webhook_table} (
            transaction_id, amount_paise, gateway_status, timestamp_utc,
            merchant_id, processing_run_id, reconciliation_status,
            bank_ref_id, ingested_at, instrument_type
        ) VALUES
        ('tx_idem_001', 101802, 'SUCCESS', '1999-01-01T00:00:00', 'UNKNOWN',
         'idem-seed', 'PENDING_SETTLEMENT', NULL, current_timestamp(), 'UPI'),
        ('tx_idem_002', 99399, 'SUCCESS', '1999-01-01T00:00:00', 'UNKNOWN',
         'idem-seed', 'PENDING_SETTLEMENT', NULL, current_timestamp(), 'UPI'),
        ('tx_idem_003', 50902, 'SUCCESS', '1999-01-01T00:00:00', 'UNKNOWN',
         'idem-seed', 'PENDING_SETTLEMENT', NULL, current_timestamp(), 'UPI')
        """
    )

    data_dir = os.path.join(settings.project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    seed_path = os.path.join(data_dir, f"settlement_{SEED_STAMP}.csv")
    with open(seed_path, "w") as f:
        f.write(SEED_ROWS)
    try:
        run_reconciliation(SEED_STAMP)

        try:
            df_run1 = spark.table(settings.webhook_table)
            count_run1 = df_run1.count()
            checksum_run1 = df_run1.orderBy("transaction_id").collect()
        except AnalysisException as e:
            pytest.skip(f"Iceberg table unreadable (is infra up?): {e}")

        assert _statuses(df_run1) == {"MATCHED": 3}, _statuses(df_run1)

        run_reconciliation(SEED_STAMP)

        df_run2 = spark.table(settings.webhook_table)
        count_run2 = df_run2.count()
        checksum_run2 = df_run2.orderBy("transaction_id").collect()

        assert count_run1 == count_run2, "Row count changed on second run!"
        assert checksum_run1 == checksum_run2, "Data changed on second run!"
    finally:
        os.remove(seed_path)
