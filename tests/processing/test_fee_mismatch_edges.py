"""Tolerance edges per instrument: +-1 paise MATCHES, +-2 MISMATCHES.

Catches tolerance inflation (e.g. someone bumping tolerance_paise to hide
fee drift). Hermetic: FeeEngine only, no Spark.
"""

import pytest

from src.processing.fee_engine import FeeEngine

INSTRUMENTS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]


@pytest.mark.parametrize("instrument", INSTRUMENTS)
@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_within_tolerance_matches(instrument, delta):
    engine = FeeEngine()
    net = engine.compute_expected_settled(100000, instrument)
    matched, _ = engine.check_match(100000, net + delta, instrument)
    assert matched is True


@pytest.mark.parametrize("instrument", INSTRUMENTS)
@pytest.mark.parametrize("delta", [-2, 2])
def test_outside_tolerance_mismatches(instrument, delta):
    engine = FeeEngine()
    net = engine.compute_expected_settled(100000, instrument)
    matched, _ = engine.check_match(100000, net + delta, instrument)
    assert matched is False
