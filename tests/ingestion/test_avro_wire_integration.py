"""Avro wire compat on live Spark (integration): real to_avro/from_avro bytes.

Unit tests pin the schema dict; these prove the bytes on the wire behave:
roundtrip equality, forward-compat reads (new optional field), breaking
reads fail closed, and the decode guard flags real binary frames.
"""

import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import get_spark_session
from src.common.schemas import WEBHOOK_AVRO_SCHEMA, WEBHOOK_AVRO_SCHEMA_DICT
from src.ingestion.ingest_webhooks import _decode_guard

pytestmark = pytest.mark.integration

COLS = [f["name"] for f in WEBHOOK_AVRO_SCHEMA_DICT["fields"]]

_ROWS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("amount_paise", LongType(), True),
        StructField("gateway_status", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("processing_run_id", StringType(), True),
        StructField("currency", StringType(), True),
    ]
)


def _rows_df(spark, rows):
    return spark.createDataFrame(rows, schema=_ROWS_SCHEMA)


def test_avro_wire_roundtrip_preserves_all_fields():
    spark = get_spark_session("AvroWireTest")
    rows = [
        (
            "tx_wire_001",
            100000,
            "SUCCESS",
            "2025-04-02T00:00:00Z",
            "merch_001",
            "run-1",
            "INR",
        ),
        ("tx_wire_002", 1, "SUCCESS", "2025-04-02T00:00:01Z", "UNKNOWN", None, None),
    ]
    df = _rows_df(spark, rows)
    wire = df.select(to_avro(F.struct(*COLS), WEBHOOK_AVRO_SCHEMA).alias("value"))
    back = wire.select(from_avro("value", WEBHOOK_AVRO_SCHEMA).alias("d")).select("d.*")
    assert (
        back.select(*COLS).orderBy("transaction_id").collect()
        == df.orderBy("transaction_id").collect()
    )


def test_avro_reader_with_old_schema_ignores_new_optional_field():
    spark = get_spark_session("AvroWireTest")
    evolved = {
        "type": "record",
        "name": "WebhookEvent",
        "fields": [
            *WEBHOOK_AVRO_SCHEMA_DICT["fields"],
            {"name": "routing_hint", "type": ["null", "string"], "default": None},
        ],
    }
    rows = _rows_df(
        spark,
        [("tx_wire_010", 500, "SUCCESS", "2025-04-02T00:00:00Z", "m", None, None)],
    ).withColumn("routing_hint", F.lit("hint"))
    wire = rows.select(
        to_avro(
            F.struct(*[*COLS, "routing_hint"]),
            json.dumps(evolved),
        ).alias("value")
    )
    back = wire.select(from_avro("value", WEBHOOK_AVRO_SCHEMA).alias("d")).select("d.*")
    got = back.collect()[0]
    assert got["transaction_id"] == "tx_wire_010"
    assert got["amount_paise"] == 500
    assert "routing_hint" not in back.columns


def test_avro_type_change_fails_closed_on_read():
    spark = get_spark_session("AvroWireTest")
    df = _rows_df(
        spark,
        [("tx_wire_020", 500, "SUCCESS", "2025-04-02T00:00:00Z", "m", None, None)],
    )
    wire = df.select(to_avro(F.struct(*COLS)).alias("value"))
    breaking = {
        "type": "record",
        "name": "WebhookEvent",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "amount_paise", "type": "string"},
            {"name": "gateway_status", "type": "string"},
            {"name": "timestamp_utc", "type": "string"},
            {"name": "merchant_id", "type": "string"},
            {"name": "processing_run_id", "type": ["null", "string"], "default": None},
        ],
    }
    with pytest.raises(Exception, match="(?i)(avro|schema|decode|incompatible|failed)"):
        wire.select(from_avro("value", json.dumps(breaking)).alias("d")).collect()


def test_decode_guard_flags_live_binary_frames():
    spark = get_spark_session("AvroWireTest")
    payload_hex = "7b2261223a317d"  # '{"a":1}'
    frames = [
        (bytearray.fromhex(f"00{'00000007'}{payload_hex}"),),  # good, schema_id 7
        (bytearray.fromhex(f"01{'00000007'}{payload_hex}"),),  # wrong magic byte
        (bytearray.fromhex("00"),),  # shorter than header
        (None,),  # kafka tombstone
        (bytearray.fromhex("00aabb"),),  # truncated payload, header ok
    ]
    schema = StructType([StructField("value", BinaryType(), True)])
    df = spark.createDataFrame(frames, schema=schema)
    got = _decode_guard(df).orderBy(F.col("decodable").desc()).collect()
    assert got[0]["decodable"] is True
    assert str(got[0]["schema_id"]) == "7"
    assert all(r["decodable"] is False for r in got[1:])
    assert all(r["fixed_value"] is None for r in got[1:])
