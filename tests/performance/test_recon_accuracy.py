"""What the 10k sample can and cannot tell us.

These three pin the mix and prove where it comes from. None of them is a
correctness measurement, and test_fixture_exceptions_are_all_injected_genuine
is the proof: it re-derives the generator's own bucket for every row and shows
the pipeline's output lands exactly where the generator put it. A broken fee
engine would still pass all three, which is why
tests/fixtures/recon_cases.yaml exists.
"""

from recon_accuracy import MATCH_GATE, load, match, mix_drift, run

from src.adapters.real_data import _bucket
from src.common.schemas import (
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_MISSING_WEBHOOK,
    MATCHED,
)


def test_the_sample_mix_is_what_it_was_when_it_was_checked_in():
    rep = run()
    assert rep["n"] > 0
    drift = mix_drift(rep)
    assert not drift, "the sample or the rate card moved:\n" + "\n".join(drift)


def test_the_mix_floor_still_holds():
    rep = run()
    assert rep["match_rate"] >= MATCH_GATE, (
        f"match_rate {rep['match_rate']:.2%} < 85%. This is the generator's mix, not accuracy, "
        f"so a collapse here means the loader or the rate card changed."
    )


def test_orphans_visible_in_real_sample():
    rep = run()
    assert rep["orphans"] > 0


def test_the_report_says_what_it_is_and_is_not():
    rep = run()
    assert rep["is_not"], "the report has to state that it is not an accuracy measurement"
    assert rep["measures"], "the report has to say what it does measure"


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
