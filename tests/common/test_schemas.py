"""Contract pins: domain constants must track config/schemas they claim to mirror.

A renamed instrument in fee_rates.yaml (or a dropped Avro field) silently
re-prices rows as UNKNOWN/quarantine. These tests fail loudly instead.
"""

import io
import json

import yaml


def _rate_config():
    import os

    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    with open(os.path.join(root, "config", "fee_rates.yaml")) as f:
        return yaml.safe_load(f)


def test_instrument_types_match_rate_card():
    from src.common.schemas import INSTRUMENT_TYPES

    cfg = _rate_config()
    assert set(INSTRUMENT_TYPES) == set(cfg.get("instruments", {}).keys()), (
        "INSTRUMENT_TYPES drifted from fee_rates.yaml instruments "
        f"(schemas={sorted(INSTRUMENT_TYPES)}, yaml={sorted(cfg.get('instruments', {}))})"
    )


def test_statuses_distinct_and_stable():
    import src.common.schemas as s

    statuses = [
        s.MATCHED,
        s.EXCEPTION_FEE_MISMATCH,
        s.EXCEPTION_MISSING_WEBHOOK,
        s.EXCEPTION_DUPLICATE_SETTLEMENT,
        s.EXCEPTION_DUPLICATE_WEBHOOK,
        s.EXCEPTION_INVALID,
        s.EXCEPTION_LATE_UNRESOLVED,
        s.EXCEPTION_MISSING_BANK_STATEMENT,
    ]
    assert len(set(statuses)) == len(statuses), "two status constants share a value"
    assert all(v and v == v.upper() for v in statuses)


def test_avro_schema_dict_matches_json():
    from src.common.schemas import WEBHOOK_AVRO_SCHEMA, WEBHOOK_AVRO_SCHEMA_DICT

    assert json.loads(WEBHOOK_AVRO_SCHEMA) == WEBHOOK_AVRO_SCHEMA_DICT


def test_avro_roundtrip():
    """Producer bytes must parse with the pinned schema (writer/reader agree)."""
    import fastavro

    from src.common.schemas import WEBHOOK_AVRO_SCHEMA_DICT

    record = {
        "transaction_id": "tx_test123",
        "amount_paise": 100000,
        "gateway_status": "SUCCESS",
        "timestamp_utc": "2025-04-02T00:00:00",
        "merchant_id": "merch_001",
        "processing_run_id": None,
    }
    buf = io.BytesIO()
    fastavro.writer(buf, fastavro.parse_schema(WEBHOOK_AVRO_SCHEMA_DICT), [record])
    buf.seek(0)
    out = list(fastavro.reader(buf))
    assert out[0]["transaction_id"] == "tx_test123"
    assert out[0]["amount_paise"] == 100000
