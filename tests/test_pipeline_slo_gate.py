"""The publish gate must judge the rate the batch actually produced.

``batch_match_rate`` used to be rounded to four decimals on the way into the
counts dict, and the gate read the rounded value. A batch at 0.94997 therefore
failed the comparison inside reconcile.py, logged a warning, and then sailed
through the publish gate because 0.95 >= 0.95.
"""

import pytest

from src.pipeline import _for_metrics, _slo_ok

pytestmark = pytest.mark.unit

SLO = 0.95


def _gate(evaluated: int, matched: int, slo_env=None, monkeypatch=None) -> bool:
    """Reproduce the rate reconcile.py computes, then ask the publish gate."""
    if slo_env is not None:
        monkeypatch.setenv("MATCH_RATE_SLO", slo_env)
    counts = {
        "settlement_rows_evaluated": evaluated,
        "batch_MATCHED": matched,
        "batch_match_rate": matched / evaluated,
    }
    return _slo_ok(counts)


# Rounding to four decimals can only lift a rate to exactly 0.95 if the rate is
# in [0.94995, 0.95), and it can only land on a four-decimal boundary if the
# denominator reaches 100000. Enumerated over denominators up to 10 million,
# the first slip is 94996/100000. At 10k rows the window is empty, which is
# why this went unnoticed on the sample batches and bites at full scale.
SLIPPING = [
    (100000, 94996, "0.94996"),
    (100000, 94997, "0.94997"),
    (100000, 94999, "0.94999"),
    (1000000, 949951, "0.949951"),
    (12635227, 12003465, "the 12.6M production batch, 7 rows under the bar"),
]


@pytest.mark.parametrize(("evaluated", "matched", "why"), SLIPPING)
def test_gate_refuses_a_rate_that_rounds_up_to_the_slo(evaluated, matched, why):
    """The whole regression: the rate is under the bar, rounding hid it."""
    assert matched / evaluated < SLO, f"{why}: premise, the rate is not actually below"
    assert round(matched / evaluated, 4) >= SLO, f"{why}: premise, rounding no longer lifts it"
    assert _gate(evaluated, matched) is False, why


@pytest.mark.parametrize(
    ("evaluated", "matched"),
    [(100000, 94995), (100000, 94990), (10000, 9497), (1000, 949), (10000, 9000)],
)
def test_gate_refuses_a_rate_that_is_plainly_below_the_slo(evaluated, matched):
    assert matched / evaluated < SLO, "premise: this rate really is under the bar"
    assert _gate(evaluated, matched) is False


@pytest.mark.parametrize(
    ("evaluated", "matched"),
    [(100000, 95000), (1000000, 950000), (10000, 9500), (1000, 999), (3, 3), (10000, 10000)],
)
def test_gate_admits_a_rate_at_or_above_the_slo(evaluated, matched):
    assert matched / evaluated >= SLO
    assert _gate(evaluated, matched) is True


def test_boundary_is_exact_not_rounded(monkeypatch):
    """A rate that is exactly the SLO passes, and only exactly."""
    monkeypatch.setenv("MATCH_RATE_SLO", "0.95")
    assert _slo_ok({"batch_match_rate": 0.95}) is True
    assert _slo_ok({"batch_match_rate": 0.94999999}) is False
    assert _slo_ok({"batch_match_rate": 0.9499999}) is False


def test_gate_honours_a_configured_slo(monkeypatch):
    monkeypatch.setenv("MATCH_RATE_SLO", "0.99")
    assert _slo_ok({"batch_match_rate": 0.96}) is False
    monkeypatch.setenv("MATCH_RATE_SLO", "0.90")
    assert _slo_ok({"batch_match_rate": 0.92}) is True


def test_absent_rate_is_not_a_pass_by_accident():
    """No key means the batch never computed a rate; the caller decides."""
    assert _slo_ok({}) is True
    assert _slo_ok({"batch_match_rate": None}) is True


def test_metrics_file_rounds_without_touching_the_gate():
    """Rounding moves to the record so the metrics file stays readable."""
    counts = {"batch_match_rate": 0.9499712, "batch_MATCHED": 9499}
    assert _for_metrics(counts)["batch_match_rate"] == 0.95
    assert _for_metrics(counts)["batch_MATCHED"] == 9499
    assert counts["batch_match_rate"] == 0.9499712, "the gate's copy must not change"
    assert _slo_ok(counts) is False


@pytest.mark.parametrize("value", [1, 0, None, "0.5", [0.5]])
def test_for_metrics_passes_through_non_float_rates_untouched(value):
    out = _for_metrics({"batch_match_rate": value})
    assert out["batch_match_rate"] == value


def test_for_metrics_copies_rather_than_mutates():
    counts = {"batch_match_rate": 0.123456789}
    out = _for_metrics(counts)
    assert out is not counts
    assert counts["batch_match_rate"] == 0.123456789
