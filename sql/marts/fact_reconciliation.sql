-- One row per transaction. Replace nessie.db with glue.db for AWS Glue.
CREATE OR REPLACE TABLE nessie.db.fact_reconciliation USING iceberg
PARTITIONED BY (bucket(16, transaction_id))
TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd',
    'gc.enabled' = 'true'
) AS
SELECT
    transaction_id,
    amount_paise,
    merchant_id,
    instrument_type,
    gateway_status,
    reconciliation_status AS status,
    bank_ref_id,
    TRY_CAST(timestamp_utc AS TIMESTAMP) AS transacted_at,
    CASE
        WHEN reconciliation_status = 'MATCHED' THEN amount_paise
        ELSE NULL
    END AS matched_amount_paise
FROM nessie.db.webhooks
