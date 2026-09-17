-- KPI: dead-letter queue depth (scalar). Sustained growth means poison traffic.
SELECT COUNT(*) AS dlq_rows
FROM nessie.db.webhooks_dlq
