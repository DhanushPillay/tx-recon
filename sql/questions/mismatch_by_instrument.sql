-- Fee-mismatch exposure per payment instrument, in rupees.
-- Rows seeded before the instrument column land NULL and are excluded.
SELECT
    instrument_type,
    COUNT(*) AS mismatches,
    SUM(amount_paise) / 100.0 AS rupees_at_risk
FROM nessie.db.webhooks
WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'
  AND instrument_type IS NOT NULL
GROUP BY instrument_type
ORDER BY mismatches DESC
