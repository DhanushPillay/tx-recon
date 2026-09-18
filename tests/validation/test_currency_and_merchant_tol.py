import pandas as pd
import pytest
import yaml
from pandera.errors import SchemaError

from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import build_tolerance_case_sql
from src.validation.settlement_schema import settlement_schema

_MERCH_TOL_YAML = {
    "version": "v9.0.0",
    "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
    "instruments": {},
    "merchants": {"merch_loose": {"UPI": {"mdr_rate_bps": 0, "tolerance_paise": 5}}},
}


def _engine_with(tmp_path, cfg: dict) -> FeeEngine:
    p = tmp_path / "fee_rates.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return FeeEngine(config_path=str(p))


def _row(**kw):
    base = {
        "transaction_id": "tx001",
        "settled_amount_paise": 100000,
        "bank_ref_id": "b1",
        "settlement_date": "2026-09-01",
        "instrument_type": "UPI",
    }
    return pd.DataFrame([{**base, **kw}])


def test_currency_inr_and_null_pass():
    settlement_schema.validate(_row(currency="INR"))
    settlement_schema.validate(_row())  # absent -> treated as INR


def test_currency_non_inr_quarantined():
    with pytest.raises(SchemaError):
        settlement_schema.validate(_row(currency="USD"))


def test_per_merchant_tolerance_python(tmp_path):
    eng = _engine_with(tmp_path, _MERCH_TOL_YAML)
    ok_default, _ = eng.check_match(100000, 100002, "UPI")  # diff 2 > tol 1
    assert not ok_default
    ok_merch, _ = eng.check_match(100000, 100005, "UPI", merchant_id="merch_loose")
    assert ok_merch  # diff 5 <= merchant tol 5


def test_per_merchant_tolerance_sql(tmp_path):
    eng = _engine_with(tmp_path, _MERCH_TOL_YAML)
    sql = build_tolerance_case_sql(eng)
    assert "merch_loose" in sql and "THEN 5" in sql


def test_negative_merchant_tolerance_rejected(tmp_path):
    bad = {
        "version": "v9.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {},
        "merchants": {"m": {"UPI": {"mdr_rate_bps": 0, "tolerance_paise": -2}}},
    }
    with pytest.raises(ValueError, match="tolerance_paise"):
        _engine_with(tmp_path, bad)
