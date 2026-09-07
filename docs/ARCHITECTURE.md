# Architecture

This document describes the system design for the Transaction Reconciliation pipeline.

## Overview

The system uses a dual-pipeline approach to reconcile real-time payment gateway webhooks against end-of-day bank settlement batch files.

1.  **Streaming ingestion:** Webhooks arrive in real time, are published to a Redpanda (Kafka) topic, and are ingested into an Apache Iceberg table using PySpark Structured Streaming.
2.  **Batch processing:** Settlement CSV files arrive periodically. They are validated against a strict schema, processed to calculate expected fees and taxes, and then merged against the existing webhook records.

## Key design decisions

### Apache Iceberg MERGE

We use Iceberg's `MERGE INTO` functionality instead of standard appends. The reconciliation is performed on the `transaction_id`.

*   **Idempotency:** Re-running the pipeline does not duplicate records. If a matching transaction already exists, it is updated rather than inserted twice.
*   **Late corrections:** If a bank issues a correction for a transaction in a subsequent settlement file, the `MERGE` operation updates the existing row in place with the corrected amounts and status.

### Write-Audit-Publish pattern

To prevent corrupted data from entering the Iceberg table, the batch pipeline applies the Write-Audit-Publish pattern at the ingestion boundary.

*   Settlement files are passed through a Pandera schema validation step.
*   Constraints (such as non-null transaction IDs and positive integer amounts) are enforced before the data reaches Spark.
*   Invalid rows are quarantined and excluded from the merge operation, ensuring that the main ledger remains accurate.

### Integer currency representation

All financial amounts are stored and processed as integers (paise).

*   Floating-point representations are subject to rounding errors during calculations.
*   By using integers, the Spark SQL arithmetic directly matches the canonical Python fee engine logic, ensuring exact precision for Merchant Discount Rate (MDR) and GST calculations.

## Component mapping

The local Docker Compose environment mirrors a typical cloud-native deployment:

*   **Redpanda** acts as the message broker (equivalent to Amazon MSK or Confluent Cloud).
*   **MinIO** provides S3-compatible object storage (equivalent to Amazon S3 or Google Cloud Storage).
*   **Nessie** serves as the transactional catalog for Iceberg (equivalent to AWS Glue Data Catalog or a Hive Metastore).
*   **PySpark** handles the compute for streaming and batch jobs (equivalent to Amazon EMR or Dataproc).
