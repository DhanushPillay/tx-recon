"""SASL/auth config surface (hermetic, no Spark session)."""

from src.common.settings import Settings


def test_kafka_plaintext_by_default():
    assert Settings().kafka_security_protocol == "PLAINTEXT"


def test_sasl_fields_present_and_empty_by_default():
    s = Settings()
    assert s.kafka_sasl_mechanism == ""
    assert s.kafka_sasl_username == ""
    assert s.kafka_sasl_password == ""


def test_sasl_config_accepted():
    s = Settings(
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-256",
        kafka_sasl_username="u",
        kafka_sasl_password="p",  # noqa: S106 — dummy test credential, not a secret
    )
    assert s.kafka_security_protocol == "SASL_SSL"
    assert s.kafka_sasl_username == "u"


def test_webhook_secret_defaults_empty():
    assert Settings().webhook_secret == ""
