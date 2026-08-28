# ADR 003: Data Quality - Great Expectations for Data Contracts

**Status:** Accepted

## Context
The daily bank settlement CSV files are notoriously messy in the real world. They often contain negative amounts, missing transaction IDs, or duplicate rows. If this dirty data is merged directly into our Apache Iceberg table, it corrupts the lakehouse state, requiring painful and manual rollbacks.

## Alternatives Considered
1. **Raw PySpark Asserts / Filtering:**
    * *Pros:* No external dependencies, fast to implement in the existing `reconcile.py` script.
    * *Cons:* Hard to maintain rules, limited observability, and fails late in the pipeline (during the merge phase).
2. **dbt Tests:**
    * *Pros:* Excellent for SQL-based testing.
    * *Cons:* Operates *after* the data is loaded into the warehouse/lake. We want to prevent bad data from entering the lake in the first place (Shift-Left Data Quality).
3. **Great Expectations:**
    * *Pros:* Declarative framework for defining "Data Contracts". Runs as a gateway *before* data ingestion. Provides rich data documentation and native Airflow integration.
    * *Cons:* Steeper learning curve, requires managing Expectation Suites.

## Decision
We chose **Great Expectations** to enforce strict Data Contracts within our Airflow DAG *before* the Iceberg `MERGE INTO` operation.

## Rationale
In a production data engineering environment, preventing bad data from contaminating the data lake is critical. By placing a Great Expectations validation node in our Airflow DAG before the PySpark reconciliation job, we create a strict Data Contract. If the bank sends a file with missing IDs or negative amounts, the pipeline halts immediately and alerts the team, keeping our Iceberg table pristine.
