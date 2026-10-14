import io

import pytest
from fastavro import schemaless_writer

from src.common.schemas import WEBHOOK_AVRO_SCHEMA_DICT
from src.ingestion.ingest_webhooks import _DLQ_COLS, _decode_guard

GOOD_ID = 123


def _frame(schema_id=GOOD_ID, **overrides):
    rec = {
        "transaction_id": "tx_a",
        "amount_paise": 100000,
        "gateway_status": "SUCCESS",
        "timestamp_utc": "2026-09-04T10:00:00Z",
        "merchant_id": "merch_001",
        "processing_run_id": "run-1",
    }
    rec.update(overrides)
    buf = io.BytesIO()
    schemaless_writer(buf, WEBHOOK_AVRO_SCHEMA_DICT, rec)
    return b"\x00" + schema_id.to_bytes(4, "big") + buf.getvalue()


CASES = [
    ("well_formed", _frame(), True),
    ("wrong_magic_byte", b"\xff" + _frame()[1:], False),
    ("header_only_no_payload", b"\x00" + GOOD_ID.to_bytes(4, "big"), False),
    ("shorter_than_header", b"\x00\x00\x00", False),
    ("kafka_tombstone", None, False),
    ("truncated_payload", _frame()[:-4], True),
    (
        "raw_json_not_avro",
        b"\x00" + GOOD_ID.to_bytes(4, "big") + b'{"transaction_id":"tx_b"}',
        True,
    ),
]


@pytest.mark.parametrize("label,value,decodable", CASES, ids=[c[0] for c in CASES])
def test_decode_guard_classifies_frames(spark, label, value, decodable):
    df = spark.createDataFrame([(value,)], "value binary")
    row = _decode_guard(df).collect()[0]
    assert row["decodable"] is decodable
    assert (row["fixed_value"] is None) is (not decodable)


def test_schema_id_survives_the_guard_for_unchanged_rows(spark):
    rows = [(_frame(schema_id=7),), (_frame(schema_id=9),), (_frame(),)]
    out = _decode_guard(spark.createDataFrame(rows, "value binary")).collect()
    assert [r["schema_id"] for r in out] == ["7", "9", str(GOOD_ID)]
    assert all(r["decodable"] for r in out)


def test_dlq_keeps_every_corrupt_row(spark):
    import pyspark.sql.functions as F

    n_corrupt = 50
    rows = [(None, None, "SUCCESS", None, None, None) for _ in range(n_corrupt)]
    df = spark.createDataFrame(
        rows,
        "transaction_id string, amount_paise long, gateway_status string, "
        "timestamp_utc string, merchant_id string, processing_run_id string",
    )
    valid_cond = (df["amount_paise"] > 0) & df["transaction_id"].isNotNull()
    inv = df.filter(~F.coalesce(valid_cond, F.lit(False))).select(*_DLQ_COLS)

    assert inv.count() == n_corrupt
    assert inv.dropDuplicates(["transaction_id"]).count() == 1
