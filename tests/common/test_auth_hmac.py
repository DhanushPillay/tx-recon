"""HMAC webhook auth roundtrip (hermetic, no Kafka/Spark)."""

from src.common.auth import sign_webhook, verify_webhook


def test_sign_verify_roundtrip():
    sig = sign_webhook("tx_abc123", 97640, "test-secret")
    assert verify_webhook("tx_abc123", 97640, sig, "test-secret") is True


def test_tampered_amount_fails():
    sig = sign_webhook("tx_abc123", 97640, "test-secret")
    assert verify_webhook("tx_abc123", 97641, sig, "test-secret") is False


def test_tampered_id_fails():
    sig = sign_webhook("tx_abc123", 97640, "test-secret")
    assert verify_webhook("tx_xyz999", 97640, sig, "test-secret") is False


def test_wrong_secret_fails():
    sig = sign_webhook("tx_abc123", 97640, "test-secret")
    assert verify_webhook("tx_abc123", 97640, sig, "other-secret") is False
