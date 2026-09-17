-- Daily batch health: totals + match rate per day (table).
SELECT
    date(substr(timestamp_utc, 1, 10)) AS day,
    COUNT(*) AS total,
    SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched,
    ROUND(
        100.0 * SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS match_rate_pct
FROM nessie.db.webhooks
GROUP BY 1
ORDER BY 1
