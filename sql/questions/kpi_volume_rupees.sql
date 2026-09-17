-- KPI: total reconciled volume in rupees (scalar).
SELECT SUM(amount_paise) / 100.0 AS volume_rupees
FROM nessie.db.webhooks
