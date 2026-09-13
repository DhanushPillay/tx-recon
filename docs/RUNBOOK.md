# Runbook

This document details operations, troubleshooting, and error handling for the transaction reconciliation pipeline.

## Error handling

The pipeline separates bad data from good data rather than failing the entire process.

### Webhook Dead Letter Queue (DLQ)

When reading webhooks from Kafka, messages that cannot be parsed (e.g., malformed JSON or missing mandatory fields) are caught by the ingestion script.

*   **Behavior:** Invalid records are isolated and written to a separate DLQ table or logged.
*   **Action required:** Query the DLQ to inspect the raw payload. If the issue is a systemic change in the webhook payload structure from the payment gateway, update the ingestion schema.

### Settlement quarantine

Settlement CSVs are validated using Pandera before they reach Spark.

*   **Behavior:** Rows that violate schema contracts (e.g., negative settlement amounts, missing `bank_ref_id`, duplicate `transaction_id` within the same file) are removed from the processing DataFrame.
*   **Output:** The invalid rows are written to a `quarantine_*.csv` file in the output directory.
*   **Action required:** Review the quarantined records. These usually require manual investigation with the bank to correct the underlying data issue before they can be re-ingested.

## Failure injection (testing only)

Synthetic chaos is fixture-driven — no code path in prod randomly fails:

- **Duplicate webhook:** replay the same `transaction_id` twice in a Kafka micro-batch. Streaming `dropDuplicates` + `MERGE ... WHEN NOT MATCHED` keeps one row; the duplicate is a no-op. Verify `EXCEPTION_DUPLICATE_WEBHOOK` is never spuriously emitted.
- **Duplicate settlement:** add two rows with the same `transaction_id` and different `settlement_date` in one CSV. Pandera quarantines intra-file duplicates; pre-MERGE `WINDOW row_number() ORDER BY settlement_date DESC` keeps the latest. Check `count(batch_*) == settlement_rows_deduped`.
- **Late correction:** generate a day-T file, run MERGE, then re-run with an amendment file carrying the same `transaction_id` with a newer `settlement_date`. The second MERGE updates `bank_ref_id` and status in place; re-running again is idempotent.
- **Fee mismatch:** mutate one settlement `settled_amount_paise` by `+500` paise. Expect `batch_EXCEPTION_FEE_MISMATCH` increments by one, `batch_MATCHED` drops by one, `FP==0` preserved.
- **Missing webhook (orphan):** add a settlement `transaction_id` with no webhook row. Expect `EXCEPTION_MISSING_WEBHOOK` placeholder inserted; second run preserves its status (first `WHEN MATCHED` clause) and only refreshes `bank_ref_id`.

Run with `pytest tests/ -m "not integration" -k "chaos or failure or drift"` and inspect `pipeline.py` batch drift warning (`Batch drift: deduped X but batch statuses sum Y`).

## Troubleshooting

### Pipeline failures

If the `pipeline.py` script fails:

1.  **Check infrastructure:** Ensure Docker containers are running (`docker ps`). MinIO, Nessie, and Redpanda must be healthy.
2.  **Check logs:** Read the stack trace. The most common failures in local development are related to missing dependencies, wrong Python versions, or stale `JAVA_HOME` variables required by PySpark.

### Batch drift alert

`pipeline.py` logs `Batch drift` when `settlement_rows_deduped != sum(batch_*)`. This means the MERGE missed rows (NULL guards) or double counted. Treat it as a P1 — check the dedup window and `WHERE s.transaction_id IS NOT NULL` clause in `reconcile.py`.

### Replaying data

Because the pipeline relies on Iceberg `MERGE INTO`, re-running the reconciliation process is safe and idempotent.

*   **If a settlement file processing fails midway:** You can re-run the script against the same file. Successfully processed rows will update in place, and missed rows will be inserted.
*   **Late corrections:** If the bank issues an updated settlement file for a previous day, place it in the ingest directory and run the pipeline. The `MERGE` logic will overwrite the old values based on the `transaction_id`.

## Accessing data

To query the data manually:

1.  Connect a Spark SQL shell or a Python notebook configured with the Nessie catalog.
2.  Query the Iceberg tables:
    *   `SELECT * FROM tx_recon.webhooks LIMIT 10;`
    *   `SELECT * FROM tx_recon.reconciled WHERE status != 'MATCHED';`
