"""Adapter edge cases + property tests."""

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from src.adapters.settlement import (
    _map_instrument,
    _paise_inr_series,
    _to_iso_date,
    _to_iso_series,
    load_settlement_csv,
    normalize_settlement_df,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2025-04-01", "2025-04-01"),
        ("02/04/2025", "2025-04-02"),
        ("02-04-2025", "2025-04-02"),
        ("2025-04-01T10:00:00Z", "2025-04-01"),
        ("2025-04-01 10:00:00", "2025-04-01"),
        ("not-a-date", None),
        ("", None),
        (None, None),
    ],
)
def test_to_iso_date_formats(raw, expected):
    assert _to_iso_date(raw) == expected


def test_to_iso_series_row_fallback():
    out = _to_iso_series(pd.Series(["2025-04-01", "02/04/2025", "junk"])).tolist()
    assert out[0] == "2025-04-01"
    assert out[1] == "2025-04-02"
    assert out[2] is None or pd.isna(out[2])


def test_unknown_provider_falls_back_to_generic(tmp_path):
    f = tmp_path / "weird_pg.csv"
    f.write_text(
        "transaction_id,bank_ref_id,settled_amount_paise,settlement_date,instrument_type\n"
        "tx_abcdef123456,b1,100000,2025-04-02,UPI\n"
    )
    df, pg_name = load_settlement_csv(str(f))
    assert pg_name == "generic"
    assert df["provider"].iloc[0] == "generic"


def test_map_instrument_unknown_quarantined():
    # Unknown methods map to UNKNOWN (not a real instrument): the Pandera isin
    # check quarantines the row instead of mispricing it as CREDIT_CARD.
    assert _map_instrument("something-weird-xyz") == "UNKNOWN"
    assert _map_instrument(None) == "UNKNOWN"
    assert _map_instrument("card - visa credit") == "CREDIT_CARD"


def test_paise_inr_series_symbols():
    s = _paise_inr_series(pd.Series(["₹1,234.56", " 100 ", None, ""]))
    assert s.iloc[0] == 123456
    assert s.iloc[1] == 10000


def test_paise_inr_series_half_up():
    # 2.675 Rs is unrepresentable below .5 in float64; half-even .round()
    # gives 267. Money rounds half up -> 268.
    s = _paise_inr_series(pd.Series(["2.675", "1.005", "100.00"]))
    assert s.iloc[0] == 268
    assert s.iloc[1] == 101
    assert s.iloc[2] == 10000


def test_iso_series_mixed_and_empty():
    assert len(_to_iso_series(pd.Series([], dtype=object))) == 0
    out = _to_iso_series(pd.Series(["2025-04-02", "02/04/2025", "bad-date"]))
    assert out.iloc[0] == "2025-04-02"
    assert out.iloc[1] == "2025-04-02"


def test_semicolon_csv(tmp_path):
    f = tmp_path / "s.csv"
    f.write_text(
        "payment_id;settlement_id;settlement_amount;utr;payment_method;settlement_date\np1;s1;100.00;u1;upi;2025-04-02\n"
    )
    df, name = load_settlement_csv(str(f))
    assert name == "cashfree"
    assert df.iloc[0]["settled_amount_paise"] == 10000


def test_generic_case_insensitive():
    df = pd.DataFrame(
        [
            {
                "Transaction_ID": "tx_abcdef123456",
                "Bank_Ref_ID": "b1",
                "Settled_Amount_Paise": 100,
                "Settlement_Date": "2025-04-02",
                "Instrument_Type": "upi",
            }
        ]
    )
    out, _ = normalize_settlement_df(df)
    assert out.iloc[0]["instrument_type"] == "UPI"


@given(st.text(max_size=20))
def test_map_never_crashes(s):
    assert isinstance(_map_instrument(s), str)


@given(st.text(max_size=20))
def test_iso_never_crashes(s):
    out = _to_iso_series(pd.Series([s]))
    assert len(out) == 1


def test_iso_date_arms():
    from src.adapters.settlement import _to_iso_date

    assert _to_iso_date(None) is None
    assert _to_iso_date("   ") is None
    assert _to_iso_date("2025-04-02") == "2025-04-02"
    assert _to_iso_date("02/04/2025") == "2025-04-02"
    assert _to_iso_date("2-4-2025") == "2025-04-02"
    assert _to_iso_date("2025-04-01T10:00:00Z") == "2025-04-01"
    assert _to_iso_date("garbage-xyz") is None


def test_empty_series_passthrough():
    import pandas as pd

    from src.adapters.settlement import _map_instrument_series

    e = pd.Series([], dtype=object)
    assert len(_paise_inr_series(e)) == 0
    assert len(_to_iso_series(e)) == 0
    assert len(_map_instrument_series(e)) == 0


def test_direct_alias_and_series_substring():
    from src.adapters.settlement import _map_instrument_series

    assert _map_instrument("upi") == "UPI"
    out = _map_instrument_series(pd.Series(["upi", "card - visa credit", "mystery-xyz"]))
    assert list(out) == ["UPI", "CREDIT_CARD", "UNKNOWN"]


def test_unknown_instrument_quarantined_by_schema():
    """UNKNOWN is not in INSTRUMENT_TYPES: validation quarantines, never merges."""
    import pandera.pandas as pa
    from pandera.errors import SchemaErrors

    from src.validation.settlement_schema import settlement_schema

    df = pd.DataFrame(
        [
            {
                "transaction_id": "tx_abcdef123456",
                "bank_ref_id": "bnk_001",
                "settled_amount_paise": 97640,
                "settlement_date": "2025-04-02",
                "instrument_type": "UNKNOWN",
            }
        ]
    )
    with pytest.raises((SchemaErrors, pa.errors.SchemaError)):
        settlement_schema.validate(df, lazy=True)


def test_date_fallback_fills_partial_nulls():
    """Fallback fills per-row NULLs, not only all-NULL columns."""
    from src.adapters.settlement import _date_with_fallback, _lc

    df = pd.DataFrame(
        {"settlementdate": ["2025-04-02", None], "created_at": ["2025-01-01", "2025-04-03"]}
    )
    out = _date_with_fallback(
        df, _lc(df), ("settlementdate", "settlement_date"), ("created_at",)
    ).tolist()
    assert out[0] == "2025-04-02"
    assert out[1] == "2025-04-03"


def test_payu_space_variant_amount():
    """'Settlement Amount' header (space variant) normalizes like underscore."""
    df = pd.DataFrame({"mihpayid": ["a1"], "Settlement Amount": ["100.00"]})
    norm, pg = normalize_settlement_df(df)
    assert pg == "payu"
    assert norm["settled_amount_paise"].iloc[0] == 10000


def test_chunked_and_direct_reads_agree_on_nulls(tmp_path):
    """Chunked (>256MB) and direct reads treat NA/null literals identically."""
    from src.validation.validate_settlement import _validate_large_file

    p = tmp_path / "s.csv"
    p.write_text(
        "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type,currency\n"
        "tx1,100,b1,2025-04-02,UPI,null\n"
    )
    direct, _ = load_settlement_csv(str(p))
    chunked, _ = _validate_large_file(str(p), normalize_settlement_df)
    assert direct["currency"].iloc[0] == "INR"
    assert chunked["currency"].iloc[0] == direct["currency"].iloc[0]
