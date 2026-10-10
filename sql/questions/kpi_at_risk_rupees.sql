SELECT SUM(amount_paise) / 100.0 AS at_risk_rupees
FROM nessie.db.webhooks
WHERE reconciliation_status <> 'MATCHED'
