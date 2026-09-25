"""Accuracy gate on real data: match_rate >= 85% on the 10k sample.

No synthetic breaks. A drop means fee miscalibration or data drift.
"""

from recon_accuracy import MATCH_GATE, run


def test_real_accuracy_gate():
    rep = run()
    assert rep["n"] > 0
    assert rep["match_rate"] >= MATCH_GATE, (
        f"match_rate {rep['match_rate']:.2%} < 85%: fee calibration or drift suspect"
    )


def test_orphans_visible_in_real_sample():
    # The 10k sample carries ~5% orphan settlements; the gate must see them.
    rep = run()
    assert rep["orphans"] > 0
