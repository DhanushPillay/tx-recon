"""
Domain Contracts for TX Reconciliation

1. Canonical Fields:
   - transaction_id: String (UUID). The primary business key.
   - amount_paise / settled_amount_paise: Integer. All monetary units MUST be in integer paise.
   - merchant_id: String. Secondary grouping key.

2. Terminal Statuses:
   - MATCHED: webhook amount equals settled minus exact fee (within tolerance).
   - EXCEPTION_FEE_MISMATCH: webhook amount and settled amount differ beyond tolerance.
   - EXCEPTION_MISSING_WEBHOOK: settlement received but no matching webhook.
   - EXCEPTION_DUPLICATE_SETTLEMENT: multiple settlement records for the same transaction.
   - EXCEPTION_DUPLICATE_WEBHOOK: multiple webhooks for the same transaction.
   - EXCEPTION_INVALID: failed schema validation or quarantine.
   - EXCEPTION_LATE_UNRESOLVED: timebound SLA breached for resolution.

3. Duplicate Policy & Lineage:
   - Deduplication happens at ingestion and pre-reconciliation using exact `transaction_id`.
   - Lineage fields: `timestamp_utc` (ingestion time), `processing_run_id` (batch run identifier), `file_source` (for CSVs).
   - Precedence: A valid settlement overwrites a missing webhook status.

"""

import json

# Single source of truth for instrument types (fee_rates.yaml keys).
INSTRUMENT_TYPES = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]

# Canonical reconciliation statuses (terminal states for a webhook row).
MATCHED = "MATCHED"
EXCEPTION_FEE_MISMATCH = "EXCEPTION_FEE_MISMATCH"
EXCEPTION_MISSING_WEBHOOK = "EXCEPTION_MISSING_WEBHOOK"
EXCEPTION_DUPLICATE_SETTLEMENT = "EXCEPTION_DUPLICATE_SETTLEMENT"
EXCEPTION_DUPLICATE_WEBHOOK = "EXCEPTION_DUPLICATE_WEBHOOK"
EXCEPTION_INVALID = "EXCEPTION_INVALID"
EXCEPTION_LATE_UNRESOLVED = "EXCEPTION_LATE_UNRESOLVED"

WEBHOOK_AVRO_SCHEMA_DICT = {
    "type": "record",
    "name": "WebhookEvent",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "amount_paise", "type": "long"},
        {"name": "gateway_status", "type": "string"},
        {"name": "timestamp_utc", "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "processing_run_id", "type": ["null", "string"], "default": None},
    ],
}

WEBHOOK_AVRO_SCHEMA = json.dumps(WEBHOOK_AVRO_SCHEMA_DICT)
