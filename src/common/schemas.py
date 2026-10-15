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

# Money moving backwards: the gateway told us a refund, a reversal or a
# chargeback. These rows carry a non-positive amount, which is why they used to
# land in the DLQ - indistinguishable from a malformed record, and
# `replay_dlq.py` filters on `amount_paise > 0`, so they could never be replayed
# or purged either. They are real events and belong in the ledger.
#
# Known gap, stated rather than papered over: the settlement side cannot yet
# express the matching debit. `settlement_schema.Check.gt(0)` quarantines a
# negative settled amount, so a reversal webhook parks here until the
# settlement contract grows a direction field. It is a visible, counted wait
# rather than a silent DLQ entry nobody can drain.
#
# Not terminal: a later settlement or a corrected event re-evaluates it.
EXCEPTION_REVERSAL = "EXCEPTION_REVERSAL"

# Gateway status strings this repo reads as a backward money movement.
# Gateways disagree on spelling; this is the vocabulary, not a provider's
# spec. Extend it for the provider you integrate. Compared case-insensitively.
REVERSAL_GATEWAY_STATUSES = frozenset(
    {
        "REFUND",
        "REFUNDED",
        "REFUND_PENDING",
        "PARTIAL_REFUND",
        "REVERSED",
        "REVERSAL",
        "CHARGEBACK",
        "DISPUTE",
    }
)

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
