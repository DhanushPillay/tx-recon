-- __SOURCE_TABLE__ / __MART_TABLE__ / __TIMESTAMP_UTC__ are substituted by
-- refresh_mart(). __TIMESTAMP_UTC__ is the same parse the batch side uses, so
-- a timestamp that ages a row to LATE_UNRESOLVED also reads here.
CREATE OR REPLACE TABLE __MART_TABLE__ USING iceberg
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
    __TIMESTAMP_UTC__ AS transacted_at,
    CASE
        WHEN reconciliation_status = 'MATCHED' THEN amount_paise
        ELSE NULL
    END AS matched_amount_paise
FROM __SOURCE_TABLE__
