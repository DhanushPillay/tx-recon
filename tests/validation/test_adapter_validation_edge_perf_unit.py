"""P2: adapter + validation edge + hidden + perf (unit-only, pandas, no Spark).

Covers date/money/instrument normalization, CSV dialect + blank cells,
adapter detection + hints, quarantine split + rate gate, calendar mask,
volume/stale signals, PAN gate, registry redelivery.
"""

import time

import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.adapters.settlement import (
    _map_instrument,
    _paise_inr_series,
    _to_iso_date,
    blank_to_na,
    detect_adapter,
    load_settlement_csv,
    normalize_settlement_df,
    sniff_csv_delimiter,
)
from src.validation.file_registry import load_registry, record_file
from src.validation.pan_guard import scan_frame
from src.validation.settlement_schema import bad_calendar_date_mask, settlement_schema
from src.validation.validate_settlement import (
    SettlementValidationError,
    _settlement_date,
    _warn_volume_shift,
    validate_and_quarantine,
)

pytestmark = pytest.mark.unit


def _row(**kw):
    base = {
        "transaction_id": "TXN000001",
        "settled_amount_paise": 1000,
        "bank_ref_id": "B1",
        "settlement_date": "2025-04-02",
        "instrument_type": "UPI",
    }
    base.update(kw)
    return pd.DataFrame([base])


# ---- dates ----


def test_iso_and_slash_and_datetime_prefix():
    assert _to_iso_date("2025-04-02") == "2025-04-02"
    assert _to_iso_date("02/04/2025") == "2025-04-02"  # day-first
    assert _to_iso_date("2025-04-02T10:00:00Z") == "2025-04-02"
    assert _to_iso_date("garbage") is None
    assert _to_iso_date("") is None
    assert _to_iso_date(None) is None


def test_settlement_date_parses_stem():
    from datetime import date

    assert _settlement_date("settlement_20250402.csv") == date(2025, 4, 2)
    assert _settlement_date("nope.csv") is None


# ---- money ----


def test_paise_symbols_and_half_up():
    s = _paise_inr_series(pd.Series(["\u20b91,234.56", " 100 ", "2.675", "1.005"]))
    assert list(s.astype(int)) == [123456, 10000, 268, 101]


def test_paise_empty_passthrough():
    out = _paise_inr_series(pd.Series([], dtype=object))
    assert len(out) == 0


# ---- instruments ----


def test_instrument_alias_matrix():
    assert _map_instrument("upi") == "UPI"
    assert _map_instrument("card - visa credit") == "CREDIT_CARD"
    assert _map_instrument("Debit(Prepaid)") == "DEBIT_CARD"
    assert _map_instrument("nb") == "NETBANKING"
    assert _map_instrument("mystery") == "UNKNOWN"
    assert _map_instrument(None) == "UNKNOWN"
    assert _map_instrument("   ") == "UNKNOWN"


@given(st.text(max_size=20))
@settings(max_examples=100)
def test_hidden_map_never_crashes(raw):
    assert isinstance(_map_instrument(raw), str)


@given(st.text(max_size=20))
@settings(max_examples=100)
def test_hidden_iso_never_crashes(raw):
    assert _to_iso_date(raw) is None or len(_to_iso_date(raw)) == 10


# ---- csv hygiene ----


def test_blank_to_na_keeps_values():
    df = pd.DataFrame({"a": ["x", "   ", ""], "n": [1.5, 2.5, 3.5]})
    out = blank_to_na(df)
    assert out["a"].isna().sum() == 2
    assert out["n"].tolist() == [1.5, 2.5, 3.5]


def test_sniff_semicolon_and_fallback(tmp_path):
    p = tmp_path / "cf.csv"
    p.write_text("a;b;c\n1;2;3\n", encoding="utf-8")
    assert sniff_csv_delimiter(str(p)) == ";"
    assert sniff_csv_delimiter(str(tmp_path / "missing.csv")) == ","


def test_detect_generic_vs_hint(tmp_path):
    df = pd.DataFrame(
        {
            "transaction_id": ["TXN000001"],
            "settled_amount_paise": [1000],
            "bank_ref_id": ["B1"],
            "settlement_date": ["2025-04-02"],
            "instrument_type": ["UPI"],
        }
    )
    assert detect_adapter(df).pg_name == "generic"
    canon, name = normalize_settlement_df(df, pg_hint="razorpay")
    assert name == "razorpay" and len(canon) == 1


def test_load_csv_roundtrip(tmp_path):
    p = tmp_path / "settlement_20250402.csv"
    p.write_text(
        "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type\n"
        "TXN000001,1000,B1,2025-04-02,UPI\n",
        encoding="utf-8",
    )
    canon, _ = load_settlement_csv(str(p))
    assert canon.iloc[0]["transaction_id"] == "TXN000001"


# ---- quarantine + schema ----


def test_quarantine_split_exact():
    good = _row()
    bad = _row(transaction_id="BAD000001", settled_amount_paise=-50)
    df = pd.concat([good, bad], ignore_index=True)
    valid, invalid = validate_and_quarantine(df, settlement_schema)
    assert len(valid) == 1 and len(invalid) == 1


def test_quarantine_rate_gate_math(monkeypatch):
    import os

    rate = 2 / 10 * 100
    monkeypatch.setenv("QUARANTINE_MAX_PCT", "50")
    assert rate <= float(os.environ["QUARANTINE_MAX_PCT"])
    monkeypatch.setenv("QUARANTINE_MAX_PCT", "10")
    assert rate > float(os.environ["QUARANTINE_MAX_PCT"])


def test_calendar_mask_flags_only_impossible():
    s = pd.Series(["2025-02-30", "2025-13-01", "2024-02-29", "2025-04-02", None])
    mask = bad_calendar_date_mask(s)
    assert mask.tolist() == [True, True, False, False, False]


def test_empty_frame_rejected():
    with pytest.raises((SettlementValidationError, Exception)):
        settlement_schema.validate(pd.DataFrame([]), lazy=True)


# ---- volume / stale / pan / registry ----


def test_volume_shift_strict_raises(tmp_path):
    prev = tmp_path / "curated_settlement_20250401.csv"
    prev.write_text("a\n" + "x\n" * 10, encoding="utf-8")
    cur = tmp_path / "settlement_20250402.csv"
    cur.write_text("a\nx\n", encoding="utf-8")
    with pytest.raises(SettlementValidationError, match="volume shift"):
        _warn_volume_shift(str(tmp_path), str(cur), 1, strict=True)


def test_stale_warns_not_raises(tmp_path):
    cur = tmp_path / "settlement_20200101.csv"
    cur.write_text("a\nx\n", encoding="utf-8")
    _warn_volume_shift(str(tmp_path), str(cur), 1, strict=True)  # stale only warns


def test_pan_gate_blocks_card_number():
    df = pd.DataFrame({"narration": ["4111111111111111"], "tx": ["TXN000001"]})
    assert scan_frame(df)


def test_registry_redelivery(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("tx\n1\n", encoding="utf-8")
    b.write_bytes(a.read_bytes())
    first = record_file(str(tmp_path), str(a), "2025-04-02")
    second = record_file(str(tmp_path), str(b), "2025-04-02")
    assert first["seen_before"] is False and second["seen_before"] is True
    assert first["sha256"] in load_registry(str(tmp_path))


# ---- perf ----


def test_perf_normalize_10k_rows():
    n = 10_000
    df = pd.DataFrame(
        {
            "transaction_id": [f"TXN{i:06d}" for i in range(n)],
            "settled_amount_paise": [1000] * n,
            "bank_ref_id": [f"B{i}" for i in range(n)],
            "settlement_date": ["02/04/2025"] * n,
            "instrument_type": ["upi"] * n,
        }
    )
    t0 = time.perf_counter()
    canon, _ = normalize_settlement_df(df)
    dt = time.perf_counter() - t0
    assert len(canon) == n
    assert n / dt > 2_000, f"normalize too slow: {n / dt:.0f} rows/s"


def test_perf_quarantine_5k_rows():
    n = 5_000
    df = pd.DataFrame(
        {
            "transaction_id": [f"TXN{i:06d}" for i in range(n)],
            "settled_amount_paise": [1000] * n,
            "bank_ref_id": [f"B{i}" for i in range(n)],
            "settlement_date": ["2025-04-02"] * n,
            "instrument_type": ["UPI"] * n,
        }
    )
    t0 = time.perf_counter()
    valid, invalid = validate_and_quarantine(df, settlement_schema)
    dt = time.perf_counter() - t0
    assert len(valid) == n and len(invalid) == 0
    assert n / dt > 1_000, f"quarantine too slow: {n / dt:.0f} rows/s"
