from recon_accuracy import MATCH_GATE, load, match, run

from src.adapters.real_data import _bucket
from src.common.schemas import (
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_MISSING_WEBHOOK,
    MATCHED,
)


def test_real_accuracy_gate():
    rep = run()
    assert rep["n"] > 0
    assert rep["match_rate"] >= MATCH_GATE, (
        f"match_rate {rep['match_rate']:.2%} < 85%: fee calibration or drift suspect"
    )


def test_orphans_visible_in_real_sample():
    rep = run()
    assert rep["orphans"] > 0


def test_fixture_exceptions_are_all_injected_genuine():
    webhooks, latest = load(
        "data/samples/real_10k_hooks.csv", "data/samples/real_10k_settlement.csv"
    )
    predicted = match(webhooks, latest)
    assert len(predicted) > 0
    for tx, status in predicted.items():
        bucket = _bucket(f"exc|{tx}", 7)
        if status == EXCEPTION_FEE_MISMATCH:
            assert bucket < 5, f"{tx} mismatched outside the injected set"
        elif status == EXCEPTION_MISSING_WEBHOOK:
            assert 7 <= bucket < 12, f"{tx} orphan outside the injected set"
        elif status == MATCHED:
            assert bucket in (5, 6) or bucket >= 12, f"{tx} matched an injected row"
        else:
            raise AssertionError(f"{tx} has unexpected status {status!r}")
