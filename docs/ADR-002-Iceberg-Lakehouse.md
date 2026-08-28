# ADR 002: Storage Layer - Apache Iceberg over Raw Parquet

**Status:** Accepted

## Context
Bank settlements run on a delay. A transaction happens on Monday, but the bank doesn't settle it until Wednesday.

This means on Monday night, we write a transaction as `PENDING`. On Wednesday, we need to update that exact record to `MATCHED`.

## Alternatives Considered
1.  **Raw Parquet Files on MinIO (S3):**
    *   *Pros:* Standard big data format, highly compressed.
    *   *Cons:* Immutable. To update a single `PENDING` record, we would have to read the entire historical Parquet file into memory, modify the row, and overwrite the whole file (write amplification).
2.  **Traditional RDBMS (PostgreSQL):**
    *   *Pros:* Full ACID transactions, row-level updates are native.
    *   *Cons:* Does not scale well for analytical processing of hundreds of millions of historical rows without expensive indexing.
3.  **Apache Iceberg (Lakehouse Format):**
    *   *Pros:* Brings ACID transactions to data lakes. Supports row-level updates and deletes (`MERGE INTO`). Eliminates write amplification.
    *   *Cons:* Slight learning curve for configuring catalog and metadata storage.

## Decision
We chose **Apache Iceberg** as the table format, stored locally to simulate S3/MinIO.

## Rationale
Doing row-level updates in a data lake is notoriously difficult. If we used raw Parquet on S3, we'd have to rewrite the entire historical file just to update one row. We chose Apache Iceberg because it supports native row-level updates via `MERGE INTO`. We can update historical statuses cleanly without rewriting terabytes of data, mirroring how modern data platforms actually work.
