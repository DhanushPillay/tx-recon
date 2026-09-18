-- V1: fact_reconciliation initial mart. __SOURCE_TABLE__ / __MART_TABLE__
-- are substituted by run_reconciliation(); for hand-runs replace them with
-- e.g. nessie.db.webhooks / nessie.db.fact_reconciliation (or glue.db.* on AWS).
-- New mart changes land as V2__, V3__... in this dir; the loader always picks
-- the highest version, and test_mart_migrations.py pins the rendered V1 copy
-- (sql/marts/fact_reconciliation.sql) to this file so they cannot drift.
CREATE OR REPLACE TABLE __MART_TABLE__ USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd'
) AS
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
FROM __SOURCE_TABLE__
