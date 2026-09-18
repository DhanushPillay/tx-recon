"""Shared webhook-auth helper: HMAC sign/verify for gateway webhooks.

Lives here (not in the producer) so the producer, the gateway edge, and
tests all share one implementation. Spark's Kafka source exposes no record
headers, so verification runs at the gateway edge / producer, not inside
ingest_webhooks.py — see SECURITY.md.
"""

import hashlib
import hmac


def sign_webhook(transaction_id: str, amount_paise: int, secret: str) -> str:
    return hmac.new(
        secret.encode(), f"{transaction_id}|{amount_paise}".encode(), hashlib.sha256
    ).hexdigest()


def verify_webhook(transaction_id: str, amount_paise: int, signature: str, secret: str) -> bool:
    return hmac.compare_digest(sign_webhook(transaction_id, amount_paise, secret), signature)
