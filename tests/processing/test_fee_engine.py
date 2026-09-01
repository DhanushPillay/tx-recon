import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.processing.fee_engine import FeeEngine


def test_default_mdr_rate():
    engine = FeeEngine()
    result = engine.compute_fee(100000, "UPI")
    assert result.fee_paise == 0
    assert result.net_paise == 100000
    assert result.instrument_type == "UPI"


def test_credit_card_fee():
    engine = FeeEngine()
    result = engine.compute_fee(100000, "CREDIT_CARD")
    # 100000 * 200 / 10000 = 2000 fee, GST = 2000 * 18 / 100 = 360
    assert result.fee_paise == 2360
    assert result.net_paise == 97640
    assert result.rate_bps == 200
    assert result.gst_paise == 360


def test_debit_card_fee():
    engine = FeeEngine()
    result = engine.compute_fee(100000, "DEBIT_CARD")
    # 100000 * 100 / 10000 = 1000 fee, GST = 1000 * 18 / 100 = 180
    assert result.fee_paise == 1180
    assert result.net_paise == 98820


def test_international_fee():
    engine = FeeEngine()
    result = engine.compute_fee(100000, "INTERNATIONAL")
    # 100000 * 300 / 10000 = 3000 fee, GST = 3000 * 18 / 100 = 540
    assert result.fee_paise == 3540
    assert result.net_paise == 96460


def test_check_match_exact():
    engine = FeeEngine()
    # UPI: no fee, net = amount
    matched, result = engine.check_match(100000, 100000, "UPI")
    assert matched is True


def test_check_match_with_tolerance():
    engine = FeeEngine()
    # CREDIT_CARD: 100000 -> net 97640, tolerance 1
    matched, result = engine.check_match(100000, 97640, "CREDIT_CARD")
    assert matched is True


def test_check_match_mismatch():
    engine = FeeEngine()
    matched, result = engine.check_match(100000, 99000, "CREDIT_CARD")
    assert matched is False


def test_compute_expected_settled():
    engine = FeeEngine()
    assert engine.compute_expected_settled(100000, "UPI") == 100000
    assert engine.compute_expected_settled(100000, "CREDIT_CARD") == 97640
    assert engine.compute_expected_settled(100000, "DEBIT_CARD") == 98820


def test_unknown_instrument_uses_default():
    engine = FeeEngine()
    result = engine.compute_fee(100000, "UNKNOWN_TYPE")
    # Default is 150 bps = 1.5%, fee = 1500, GST = 270
    assert result.fee_paise == 1770
    assert result.rate_bps == 150
