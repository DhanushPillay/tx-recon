import os

import pytest

from src.common.config import get_spark_session
from src.common.settings import get_settings
from src.processing.reconcile import run_reconciliation

pytestmark = pytest.mark.integration

# ponytail: hermetic fixture — data/ is gitignored so CI has no CSVs;
# the test seeds its own and deletes it afterwards.
SEED_STAMP = "19990101"
SEED_ROWS = """bank_ref_id,transaction_id,settled_amount_paise,settlement_date
bnk_idem001,tx_idem_001,100000,1999-01-01
bnk_idem002,tx_idem_002,97640,1999-01-01
bnk_idem003,tx_idem_003,50000,1999-01-01
"""


def test_reconciliation_is_idempotent():
    """
    Integration test to ensure that running the reconciliation process twice
    yields the exact same row count and identical data in the target Iceberg table.
    """
    settings = get_settings()
    spark = get_spark_session("IdempotencyTest")

    # Self-seed: same DDL as src/ingestion/ingest_webhooks.py __main__.
    # The test needs the table to exist; the pipeline creates it at deploy.
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
            ingested_at timestamp
        ) USING iceberg"""
    )

    # Run once
    data_dir = os.path.join(settings.project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    seed_path = os.path.join(data_dir, f"settlement_{SEED_STAMP}.csv")
    with open(seed_path, "w") as f:
        f.write(SEED_ROWS)
    try:
        run_reconciliation(SEED_STAMP)

        # Capture state after first run
        try:
            df_run1 = spark.table(settings.webhook_table)
            count_run1 = df_run1.count()
            checksum_run1 = df_run1.orderBy("transaction_id").collect()
        except Exception as e:
            pytest.skip(f"Could not read Iceberg table (ensure infra is up): {e}")

        # Run second time
        run_reconciliation(SEED_STAMP)

        # Capture state after second run
        df_run2 = spark.table(settings.webhook_table)
        count_run2 = df_run2.count()
        checksum_run2 = df_run2.orderBy("transaction_id").collect()

        # Verify exact match
        assert count_run1 == count_run2, "Row count changed on second run!"
        assert checksum_run1 == checksum_run2, "Data changed on second run!"
    finally:
        os.remove(seed_path)
