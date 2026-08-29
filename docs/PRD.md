# Product Requirements Document: Payment Reconciliation Engine

## 1. Problem Statement
When a customer pays, the payment gateway fires a webhook and the internal database logs a success. But the actual money doesn't hit the bank account until a day or two later when the bank sends a batch settlement file. This file contains the net payout—the original amount minus gateway fees and taxes.

If nobody automates this, the finance team has to manually match millions of webhooks against bank statements in Excel to find missing funds or duplicate charges. This causes month-end delays and masks revenue leakage.

Our goal is to build an engine that handles this matching automatically.

## 2. Target Audience
- Finance & Accounting teams (who sign off on daily settlements).
- Data Analysts (who build revenue reports).

## 3. Functional Requirements
1. **Webhook Ingestion:** Read real-time JSON webhooks simulating gateway events (Order ID, Amount, Status).
2. **Bank File Ingestion:** Read daily end-of-day (EOD) CSV files from a nodal bank account.
3. **Automated Matching:** Match records exactly on `transaction_id`.
4. **Fee Deduction Logic:** Calculate the expected bank amount by deducting a flat 1.5% MDR from the gateway amount.
5. **Exception Routing:** Flag unmatched or mathematically incorrect records with exact statuses (e.g., `EXCEPTION_FEE_MISMATCH`, `PENDING_SETTLEMENT`).
6. **Data Contracts:** Validate incoming bank settlement files for schema integrity and business rules before processing.
7. **Dead Letter Queue (DLQ):** Route malformed real-time events to a DLQ without crashing the ingestion pipeline.

## 4. Non-Functional Requirements
1. **Idempotency:** Re-running the pipeline for the same day must not duplicate records or inflate balances.
2. **Precision:** No floating-point data types. All currency values must be stored as integers (e.g., ₹100.50 stored as `10050` paise) to prevent rounding errors.
3. **Scalability:** The processing engine must handle distributed scale.
4. **Resilience:** Late-arriving bank files must cleanly update yesterday's `PENDING` records without overwriting history.

## 5. Scope
**In Scope:** 
- Ingesting raw data (streaming and batch).
- Two-way reconciliation.
- Lakehouse storage with ACID upserts.
- Data Quality enforcement (Native Pandas).
- Dimensional data modeling (dbt).
- Infrastructure as Code (IaC) definitions.

**Out of Scope:**
- Three-way reconciliation (e.g., checking against an ERP system).
- Real-time stream processing of the bank files (the bank files are inherently batch).
