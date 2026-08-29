import pytest
from src.generators.webhook_producer import generate_webhook_event
import datetime


def test_generate_webhook_event():
    event = generate_webhook_event()
    assert "transaction_id" in event
    assert event["transaction_id"].startswith("tx_")
    assert "amount_paise" in event
    assert isinstance(event["amount_paise"], int)
    assert 1000 <= event["amount_paise"] <= 1000000
    assert event["gateway_status"] == "SUCCESS"
    assert "timestamp_utc" in event
    assert "merchant_id" in event
