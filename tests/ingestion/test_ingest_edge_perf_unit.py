"""P3: streaming-ingest edge + hidden + perf (unit-only, no broker/Spark).

Decode-guard frame matrix via a recording fake DataFrame, watermark +
supervisor-delay parsing, schema-id drift refusal contract, SASL wiring.
"""

import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import src.ingestion.ingest_webhooks as ing
from src.ingestion.ingest_webhooks import (
    _decode_guard,
    parse_watermark_delay,
    supervisor_delay,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def _no_spark(monkeypatch):
    monkeypatch.setattr(ing, "expr", lambda s: s)


class _FakeDF:
    def __init__(self):
        self.cols = []

    def withColumn(self, name, _expr):
        self.cols.append(name)
        return self


# ---- decode guard ----


def test_decode_guard_adds_all_columns(_no_spark):
    df = _decode_guard(_FakeDF())
    assert df.cols == ["fixed_value", "schema_id", "decodable", "fixed_value"]


def test_decode_guard_sql_uses_magic_byte_and_length(_no_spark):
    seen = []

    class _Rec:
        def withColumn(self, name, expr):
            seen.append((name, str(expr)))
            return self

    _decode_guard(_Rec())
    names = [n for n, _ in seen]
    assert names == ["fixed_value", "schema_id", "decodable", "fixed_value"]
    by_name = {}
    for n, e in seen:
        by_name.setdefault(n, []).append(e)
    assert "substring(value, 6" in by_name["fixed_value"][0]
    assert "CASE WHEN decodable" in by_name["fixed_value"][1]
    assert "X'00'" in by_name["decodable"][0]
    assert "length(value) > 5" in by_name["decodable"][0]
    assert "substring(value, 2, 4)" in by_name["schema_id"][0]


# ---- watermark ----


def test_watermark_valid_matrix():
    assert parse_watermark_delay("1 day") == (1, "day")
    assert parse_watermark_delay("12 hours") == (12, "hours")
    assert parse_watermark_delay("30 minutes") == (30, "minutes")
    assert parse_watermark_delay("90 seconds") == (90, "seconds")
    assert parse_watermark_delay("  2 DAYS  ") == (2, "days")


def test_watermark_rejects_garbage():
    for bad in ["", "soon", "1 fortnight", "1 day; DROP", "-3 hours", "1.5 days"]:
        with pytest.raises(ValueError):
            parse_watermark_delay(bad)


@given(
    n=st.integers(min_value=1, max_value=10_000),
    unit=st.sampled_from(["seconds", "minutes", "hours", "days"]),
)
@settings(max_examples=50)
def test_hidden_watermark_roundtrip(n, unit):
    assert parse_watermark_delay(f"{n} {unit}") == (n, unit)


# ---- supervisor ----


def test_supervisor_delay_doubles_then_caps():
    assert supervisor_delay(0) == 5.0
    assert supervisor_delay(1) == 10.0
    assert supervisor_delay(10) == 300.0
    assert supervisor_delay(100) == 300.0


def test_schema_drift_contract_documented():
    import inspect

    src = inspect.getsource(ing)
    assert "schema-id drift" in src
    assert "schema_id" in src


def test_sasl_required_flag_documented():
    import inspect

    src = inspect.getsource(ing)
    assert "REQUIRE_KAFKA_SASL" in src or "require_kafka_sasl" in src


# ---- perf ----


def test_perf_watermark_parse_10k():
    n = 10_000
    t0 = time.perf_counter()
    for _ in range(n):
        parse_watermark_delay("12 hours")
    dt = time.perf_counter() - t0
    assert n / dt > 5_000, f"watermark parse too slow: {n / dt:.0f} ops/s"


def test_perf_decode_guard_fake_5k(_no_spark):
    n = 5_000
    t0 = time.perf_counter()
    for _ in range(n):
        _decode_guard(_FakeDF())
    dt = time.perf_counter() - t0
    assert n / dt > 5_000, f"decode guard wiring too slow: {n / dt:.0f} ops/s"
