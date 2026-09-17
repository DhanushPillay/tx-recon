-- Top 10 fee mismatches by amount: where to look first.
SELECT
    transaction_id,
    merchant_id,
    instrument_type,
    amount_paise / 100.0 AS amount_rupees,
    bank_ref_id
FROM nessie.db.webhooks
WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'
ORDER BY amount_paise DESC
LIMIT 10
