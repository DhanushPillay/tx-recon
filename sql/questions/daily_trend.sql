-- Daily reconciliation trend by status.
SELECT
    CAST(timestamp_utc AS DATE) AS day,
    reconciliation_status AS status,
    COUNT(*) AS n
FROM nessie.db.webhooks
GROUP BY 1, 2
ORDER BY 1, 2;
