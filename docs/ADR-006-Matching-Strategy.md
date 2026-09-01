# ADR-006: Multi-Status Reconciliation Strategy

## Status
Accepted

## Context
Reconciling payment webhooks against bank settlements is not a binary match/no-match problem. Transactions can fail at various stages, and each stage requires different handling. A simple "matched" vs "unmatched" status loses critical information for operations and finance teams.

## Decision
We define seven reconciliation statuses that capture the full lifecycle of a transaction:

| Status | Meaning | Operator Action |
| :--- | :--- | :--- |
| `MATCHED` | Webhook received, settlement confirmed, fees match | None — transaction complete |
| `EXCEPTION_FEE_MISMATCH` | Both exist but fee amounts differ | Investigate fee discrepancy |
| `EXCEPTION_NO_WEBHOOK` | Settlement received but no matching webhook | Check gateway delivery |
| `PENDING_SETTLEMENT` | Webhook received but bank settlement pending | Wait for T+1 settlement |
| `UNRECONCILED` | Webhook received but no settlement processing attempted | Trigger reconciliation |
| `SUCCESS` | Gateway returned success (settlement processing not yet started) | None — normal state |
| `FAILED` | Gateway returned failure | Check with payment processor |
| `DUPLICATE` | Duplicate transaction detected | Ignore — already processed |

### Status Transition Flow
```
SUCCESS → MATCHED (settlement confirmed, fees match)
SUCCESS → EXCEPTION_FEE_MISMATCH (settlement confirmed, fees differ)
SUCCESS → PENDING_SETTLEMENT (no settlement yet)
SUCCESS → UNRECONCILED (reconciliation not attempted)
FAILED → FAILED (terminal)
DUPLICATE → DUPLICATE (terminal)
```

## Implementation
- Iceberg `MERGE INTO` uses `WHEN MATCHED` and `WHEN NOT MATCHED` clauses
- `WHEN MATCHED` applies fee tolerance check to distinguish MATCHED from EXCEPTION_FEE_MISMATCH
- `WHEN NOT MATCHED` inserts with status `EXCEPTION_NO_WEBHOOK`
- The `reconcile.py` script performs the MERGE in a single atomic operation

## Consequences
- Operators can filter dashboards by exception type to prioritize investigation
- The DAG can be extended to auto-retry PENDING_SETTLEMENT transactions
- EXCEPTION_NO_WEBHOOK alerts can trigger gateway health checks
- Future: status can drive automated workflows (e.g., auto-escalate after 24h in PENDING_SETTLEMENT)
