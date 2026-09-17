-- fact_reconciliation: BI-ready mart, one row per transaction.
-- Grain: transaction_id. Source: webhooks Iceberg table after the MERGE.
-- Materialized by run_reconciliation() (src/processing/reconcile.py) as
-- {namespace}.fact_reconciliation on every batch; this file is the checked-in
-- copy of that same SELECT for hand-runs on Spark/Trino/Athena. Replace
-- nessie.db with glue.db for the AWS Glue catalog (TABLE_PREFIX in
-- settings.py only retargets the Python path, not this file).
CREATE OR REPLACE VIEW nessie.db.fact_reconciliation AS
SELECT
    transaction_id,
    amount_paise,
    merchant_id,
    instrument_type,
    gateway_status,
    reconciliation_status AS status,
    bank_ref_id,
    CAST(timestamp_utc AS TIMESTAMP) AS transacted_at,
    CASE
        WHEN reconciliation_status = 'MATCHED' THEN amount_paise
        ELSE NULL
    END AS matched_amount_paise
FROM nessie.db.webhooks
