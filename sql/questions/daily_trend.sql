SELECT
    to_date(try_cast(timestamp_utc AS TIMESTAMP)) AS day,
    reconciliation_status AS status,
    COUNT(*) AS n
FROM nessie.db.webhooks
GROUP BY 1, 2
ORDER BY 1, 2
