import json

INSTRUMENT_TYPES = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]

MATCHED = "MATCHED"
EXCEPTION_FEE_MISMATCH = "EXCEPTION_FEE_MISMATCH"
EXCEPTION_MISSING_WEBHOOK = "EXCEPTION_MISSING_WEBHOOK"
EXCEPTION_DUPLICATE_SETTLEMENT = "EXCEPTION_DUPLICATE_SETTLEMENT"
EXCEPTION_DUPLICATE_WEBHOOK = "EXCEPTION_DUPLICATE_WEBHOOK"
EXCEPTION_INVALID = "EXCEPTION_INVALID"
EXCEPTION_LATE_UNRESOLVED = "EXCEPTION_LATE_UNRESOLVED"
EXCEPTION_MISSING_BANK_STATEMENT = "EXCEPTION_MISSING_BANK_STATEMENT"

# A settlement in a currency the webhook leg did not declare. The two amounts
# are not comparable, so the fee tolerance is meaningless here. Not terminal:
# a corrected settlement re-evaluates it on the next batch.
EXCEPTION_CURRENCY_MISMATCH = "EXCEPTION_CURRENCY_MISMATCH"

# late webhooks wait here so a later batch can still judge them
PENDING_RECONCILIATION = "PENDING_RECONCILIATION"

PLACEHOLDER_STATUSES = (EXCEPTION_MISSING_WEBHOOK, EXCEPTION_LATE_UNRESOLVED)

TERMINAL_STATUSES = (MATCHED, EXCEPTION_MISSING_BANK_STATEMENT)

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
        # Null means the gateway did not declare one. Left null rather than
        # defaulted to INR: a default is an assumption about money, and it made
        # the settlement-side INR check unable to ever fire on a blank.
        {"name": "currency", "type": ["null", "string"], "default": None},
    ],
}

WEBHOOK_AVRO_SCHEMA = json.dumps(WEBHOOK_AVRO_SCHEMA_DICT)
