"""Accuracy gate: the matcher must score perfectly on seeded synthetic breaks.

Normal profile (no adversarial amounts): precision 1.0, recall 1.0, FP == 0.
"""

from recon_accuracy import build_case, run

from src.common.schemas import EXCEPTION_MISSING_WEBHOOK


def test_recon_accuracy_normal():
    rep = run(n=500, seed=42)
    assert rep["precision"] == 1.0
    assert rep["recall"] == 1.0
    assert rep["f1"] == 1.0
    assert rep["false_positives"] == 0
    assert rep["fanout_max"] == 1
    for cls, recall in rep["per_class_recall"].items():
        assert recall == 1.0, f"class {cls} recall {recall}"


def test_recon_accuracy_second_seed():
    rep = run(n=500, seed=7)
    assert rep["precision"] == 1.0
    assert rep["recall"] == 1.0
    assert rep["false_positives"] == 0


def test_harness_catches_fee_engine_bug(monkeypatch):
    # Falsifiability pin: ground truth comes from the YAML rate card, not
    # FeeEngine, so a broken matcher must score below 1.0.
    from recon_accuracy import build_case, match, score
    from src.processing.fee_engine import FeeEngine, FeeResult

    def _broken_fee(self, amount_paise, instrument_type="UPI", merchant_id=None):
        return FeeResult(
            fee_paise=0,
            net_paise=amount_paise,
            rate_bps=0,
            gst_paise=0,
            instrument_type=instrument_type,
            rate_version="broken",
        )

    webhooks, settlements, answer = build_case(n=200, seed=1)
    monkeypatch.setattr(FeeEngine, "compute_fee", _broken_fee)
    rep = score(answer, match(webhooks, settlements))
    assert rep["f1"] < 1.0
    assert rep["false_positives"] > 0 or rep["recall"] < 1.0


def test_orphan_class_reachable():
    # Regression pin: the orphan branch was once shadowed by a duplicate
    # `elif r < 0.90`, making EXCEPTION_MISSING_WEBHOOK unreachable.
    _, _, answer = build_case(n=2000, seed=123)
    assert EXCEPTION_MISSING_WEBHOOK in answer.values()
    rep = run(n=2000, seed=123)
    assert rep["precision"] == 1.0
    assert rep["recall"] == 1.0
    assert rep["f1"] == 1.0
    assert rep["false_positives"] == 0
    assert rep["fanout_max"] == 1
