-- Daily reconciliation trend by status.
-- substr(..,1,10): timestamp_utc mixes ISO 'T' and space formats in the wild;
-- plain CAST(.. AS DATE) fails on both, first-10-chars parses either.
SELECT
    date(substr(timestamp_utc, 1, 10)) AS day,
    reconciliation_status AS status,
    COUNT(*) AS n
FROM nessie.db.webhooks
GROUP BY 1, 2
ORDER BY 1, 2;
