-- Fee-mismatch exposure per merchant, in rupees.
SELECT
    merchant_id,
    COUNT(*) AS mismatches,
    SUM(amount_paise) / 100.0 AS rupees_at_risk
FROM nessie.db.webhooks
WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'
GROUP BY merchant_id
ORDER BY mismatches DESC;
