-- Volume and row counts per payment instrument.
SELECT
    instrument_type,
    COUNT(*) AS n,
    SUM(amount_paise) / 100.0 AS volume_rupees
FROM nessie.db.webhooks
WHERE instrument_type IS NOT NULL
GROUP BY instrument_type
ORDER BY n DESC
