-- Match rate: share of reconciled transactions per status.
-- Paste into a Metabase SQL question (Trino: catalog nessie, schema db).
SELECT
    reconciliation_status AS status,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM nessie.db.webhooks
GROUP BY reconciliation_status
ORDER BY n DESC;
