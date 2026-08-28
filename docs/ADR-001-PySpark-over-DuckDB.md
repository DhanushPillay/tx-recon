# ADR 001: Processing Engine - PySpark over DuckDB

**Status:** Accepted

## Context
We need to join a continuous stream of webhooks against massive, daily batch CSVs from the bank.

## Alternatives Considered
1.  **DuckDB:** A fast, in-process analytical SQL engine.
    *   *Pros:* Extremely fast for single-node processing, zero dependencies, easy local setup.
    *   *Cons:* Limited to the memory of a single machine. Doesn't demonstrate distributed computing skills.
2.  **Pandas:** Python data manipulation library.
    *   *Pros:* Ubiquitous, easy to write.
    *   *Cons:* High memory overhead, not designed for distributed ETL, struggles with out-of-core datasets.
3.  **Apache Spark (PySpark):** A distributed processing framework.
    *   *Pros:* The industry standard for big data. Handles out-of-core processing via shuffling and partitioning. Native integration with Apache Iceberg.
    *   *Cons:* JVM overhead, heavier local setup.

## Decision
We chose **Apache Spark (PySpark)** as the core processing engine. 

## Rationale
DuckDB is faster for local development, but it runs on a single machine. We chose PySpark instead because we want to prove we can handle distributed data. PySpark forces us to handle partitioning, shuffle optimization, and streaming—the actual problems you hit at scale in production.
