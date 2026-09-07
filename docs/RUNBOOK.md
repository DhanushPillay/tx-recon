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

## Troubleshooting

### Pipeline failures

If the `pipeline.py` script fails:

1.  **Check infrastructure:** Ensure Docker containers are running (`docker ps`). MinIO, Nessie, and Redpanda must be healthy.
2.  **Check logs:** Read the stack trace. The most common failures in local development are related to missing dependencies, wrong Python versions, or stale `JAVA_HOME` variables required by PySpark.

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
