-- KPI: overall match rate (scalar). Compare against the PROOF accuracy gate.
SELECT ROUND(
    100.0 * SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END) / COUNT(*),
    2
) AS match_rate_pct
FROM nessie.db.webhooks
