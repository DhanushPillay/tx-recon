-- fact_reconciliation: BI-ready mart, one row per transaction.
-- Grain: transaction_id. Source: webhooks Iceberg table after the MERGE.
-- Runs on Spark/Trino/Athena unchanged; catalog prefix (nessie/glue) is
-- injected by TABLE_PREFIX in settings, so the same file targets local and cloud.
SELECT
    transaction_id,
    amount_paise,
    merchant_id,
    gateway_status,
    reconciliation_status AS status,
    bank_ref_id,
    CAST(timestamp_utc AS TIMESTAMP) AS transacted_at,
    CASE
        WHEN reconciliation_status = 'MATCHED' THEN amount_paise
        ELSE NULL
    END AS matched_amount_paise
FROM nessie.db.webhooks
