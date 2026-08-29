import pytest
import json
from src.ingest_webhooks import avro_schema_str


def test_avro_schema_valid_json():
    # Verify the schema string is valid JSON
    schema_dict = json.loads(avro_schema_str)

    assert schema_dict["type"] == "record"
    assert schema_dict["name"] == "WebhookEvent"

    fields = {f["name"]: f["type"] for f in schema_dict["fields"]}
    assert fields["transaction_id"] == "string"
    assert fields["amount_paise"] == "int"
