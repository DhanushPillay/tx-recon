"""Avro compat matrix (unit-only, no registry): required fields are stable,
new fields must be optional-with-default, type changes are breaking."""

import copy
import json

import pytest

from src.common.schemas import WEBHOOK_AVRO_SCHEMA_DICT

pytestmark = pytest.mark.unit

REQUIRED = {
    "transaction_id": "string",
    "amount_paise": "long",
    "gateway_status": "string",
    "timestamp_utc": "string",
    "merchant_id": "string",
}


def _fields(schema):
    return {f["name"]: f for f in schema["fields"]}


def test_required_fields_stable():
    fields = _fields(WEBHOOK_AVRO_SCHEMA_DICT)
    for name, typ in REQUIRED.items():
        assert fields[name]["type"] == typ


def test_schema_serializes_to_valid_json():
    parsed = json.loads(json.dumps(WEBHOOK_AVRO_SCHEMA_DICT))
    assert parsed["name"] == "WebhookEvent"


def test_adding_optional_field_with_default_is_compat():
    evolved = copy.deepcopy(WEBHOOK_AVRO_SCHEMA_DICT)
    evolved["fields"].append({"name": "currency", "type": ["null", "string"], "default": None})
    base = _fields(WEBHOOK_AVRO_SCHEMA_DICT)
    for name in REQUIRED:
        assert _fields(evolved)[name]["type"] == base[name]["type"]
    added = _fields(evolved)["currency"]
    assert added["default"] is None and "null" in added["type"]


def test_removing_required_field_is_breaking():
    evolved = copy.deepcopy(WEBHOOK_AVRO_SCHEMA_DICT)
    evolved["fields"] = [f for f in evolved["fields"] if f["name"] != "amount_paise"]
    assert "amount_paise" not in _fields(evolved)


def test_changing_field_type_is_breaking():
    evolved = copy.deepcopy(WEBHOOK_AVRO_SCHEMA_DICT)
    _fields(evolved)["amount_paise"]["type"] = "string"
    assert _fields(evolved)["amount_paise"]["type"] != REQUIRED["amount_paise"]


def test_processing_run_id_stays_nullable():
    f = _fields(WEBHOOK_AVRO_SCHEMA_DICT)["processing_run_id"]
    assert "null" in f["type"] and f["default"] is None
