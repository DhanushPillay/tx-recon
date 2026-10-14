"""P1: fee-engine edge + hidden + perf (unit-only, no Spark).

Visible: dust/large amounts, negative rejection, half-up pins, zero-GST,
tolerance +/-1/2, unknown instrument, merchant override, date routing.
Hidden: Hypothesis randomized amounts/instruments/dates vs invariants.
Perf: compute_fee / check_match throughput floors (loose, CI-safe).
"""

import time

import pytest
import yaml
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from src.processing.fee_engine import FeeEngine, gst_pct_to_bps

_ENGINE = FeeEngine()  # shipped config; shared so @given tests need no fixtures

pytestmark = pytest.mark.unit

INSTRUMENTS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]


def _write_cfg(tmp_path, cfg):
    p = tmp_path / "fee_rates.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return str(p)


def _base_cfg():
    return {
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {
            "UPI": {"mdr_rate_bps": 0, "gst_on_mdr": 0, "tolerance_paise": 1},
            "CREDIT_CARD": {"mdr_rate_bps": 200, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        },
        "merchants": {"merch_loose": {"CREDIT_CARD": {"tolerance_paise": 5}}},
    }


# ---- visible edge cases ----


def test_dust_amount_one_paisa(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    r = eng.compute_fee(1, "CREDIT_CARD")
    assert r.fee_paise >= 0 and r.net_paise == 1 - r.fee_paise


def test_large_amount_no_overflow(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    r = eng.compute_fee(100_000_00, "CREDIT_CARD")
    assert r.net_paise == 100_000_00 - r.fee_paise >= 0


def test_negative_amount_rejected(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    with pytest.raises(ValueError):
        eng.compute_fee(-1, "UPI")


def test_half_up_pins(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    assert gst_pct_to_bps(2.675) == 268
    assert gst_pct_to_bps(1.005) == 101
    r = eng.compute_fee(100000, "CREDIT_CARD")
    assert (r.fee_paise, r.gst_paise, r.net_paise) == (2360, 360, 97640)


def test_zero_gst_branch(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    r = eng.compute_fee(100000, "UPI")
    assert r.gst_paise == 0 and r.fee_paise == 0 and r.net_paise == 100000


def test_tolerance_edges_all_instruments(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    for inst in INSTRUMENTS:
        net = eng.compute_expected_settled(100000, inst)
        ok, _ = eng.check_match(100000, net + 1, inst)
        bad, _ = eng.check_match(100000, net + 2, inst)
        assert ok is True and bad is False


def test_merchant_tolerance_hidden_path(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    net = eng.compute_expected_settled(100000, "CREDIT_CARD", merchant_id="merch_loose")
    ok, _ = eng.check_match(100000, net + 5, "CREDIT_CARD", merchant_id="merch_loose")
    bad, _ = eng.check_match(100000, net + 6, "CREDIT_CARD", merchant_id="merch_loose")
    assert ok is True and bad is False


def test_unknown_instrument_and_empty_merchant_fall_back(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    a = eng.compute_fee(100000, "MYSTERY")
    b = eng.compute_fee(100000, "CREDIT_CARD", merchant_id="")
    assert a.rate_bps == 150
    assert b.rate_bps == 200


def test_history_effective_to_boundary_inclusive(tmp_path):
    cfg = _base_cfg()
    cfg["history"] = [
        {
            "version": "v1",
            "effective_from": "2025-01-01",
            "effective_to": "2025-03-31",
            "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        },
        {
            "version": "v2",
            "effective_from": "2025-04-01",
            "default": {"mdr_rate_bps": 200, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        },
    ]
    eng = FeeEngine(_write_cfg(tmp_path, cfg))
    assert eng.get_rate_for_date("UNKNOWN_X", settlement_date="2025-03-31")["mdr_rate_bps"] == 100
    assert eng.get_rate_for_date("UNKNOWN_X", settlement_date="2025-04-01")["mdr_rate_bps"] == 200
    # malformed / missing dates route to latest, never crash
    assert eng.get_rate_for_date("UNKNOWN_X", settlement_date="not-a-date")["mdr_rate_bps"] == 200
    assert eng.get_rate_for_date("UNKNOWN_X", settlement_date=None)["mdr_rate_bps"] == 200


# ---- hidden (randomized) ----


@settings(max_examples=60, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(amount=st.integers(min_value=0, max_value=100_000_00), inst=st.sampled_from(INSTRUMENTS))
def test_hidden_net_identity_and_bounds(amount, inst):
    r = _ENGINE.compute_fee(amount, inst)
    assert r.net_paise == amount - r.fee_paise
    assert 0 <= r.fee_paise <= amount
    assert 0 <= r.gst_paise <= r.fee_paise


@settings(max_examples=40, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    amount=st.integers(min_value=0, max_value=1_000_000),
    delta=st.integers(min_value=0, max_value=500),
)
def test_hidden_self_match_consistent(amount, delta):
    net = _ENGINE.compute_expected_settled(amount, "CREDIT_CARD")
    matched, _ = _ENGINE.check_match(amount, net + delta, "CREDIT_CARD")
    tol = _ENGINE.get_rate("CREDIT_CARD").get("tolerance_paise", 1)
    assert matched == (delta <= tol)


# ---- perf ----


def test_perf_compute_fee_throughput(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    n = 20_000
    t0 = time.perf_counter()
    for i in range(n):
        eng.compute_fee(100000 + (i % 997), INSTRUMENTS[i % len(INSTRUMENTS)])
    dt = time.perf_counter() - t0
    assert n / dt > 2_000, f"compute_fee too slow: {n / dt:.0f} ops/s"


def test_perf_check_match_throughput(tmp_path):
    eng = FeeEngine(_write_cfg(tmp_path, _base_cfg()))
    n = 5_000
    t0 = time.perf_counter()
    for _ in range(n):
        eng.check_match(100000, 97640, "CREDIT_CARD")
    dt = time.perf_counter() - t0
    assert n / dt > 1_000, f"check_match too slow: {n / dt:.0f} ops/s"
