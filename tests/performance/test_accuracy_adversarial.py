"""Adversarial checks on real data: determinism + tolerance edge.

Hermetic: pure-Python harness, no Spark/Docker. Real sample only.
"""

from recon_accuracy import _default_paths, load, match, run


def test_rerun_identical():
    h, s = _default_paths()
    webhooks, latest = load(h, s)
    assert match(webhooks, latest) == match(webhooks, latest)


def test_tolerance_edge_flips_to_mismatch():
    # Shift one MATCHED settlement by tolerance+1 -> must flip to MISMATCH.
    from src.common.schemas import EXCEPTION_FEE_MISMATCH, MATCHED
    from src.processing.fee_engine import get_fee_engine

    h, s = _default_paths()
    webhooks, latest = load(h, s)
    predicted = match(webhooks, latest)
    target = next(tx for tx, v in predicted.items() if v == MATCHED)
    engine = get_fee_engine()
    amount, inst, wmerch, _ = webhooks[target]
    _, day0, smerch0 = latest[target]
    tol = engine.get_rate_for_date(inst, smerch0 or wmerch, day0).get("tolerance_paise", 1)
    settled, day, smerch = latest[target]
    shifted = dict(latest)
    shifted[target] = (settled + tol + 1, day, smerch)
    assert match(webhooks, shifted)[target] == EXCEPTION_FEE_MISMATCH


def test_gate_reports_match_rate():
    rep = run()
    assert 0.0 <= rep["match_rate"] <= 1.0
    assert rep["matched"] + rep["mismatched"] + rep["orphans"] == rep["n"]
