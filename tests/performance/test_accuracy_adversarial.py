"""Adversarial accuracy: off-by-two must MISMATCH; reruns must be identical.

Hermetic: pure-Python harness, no Spark/Docker.
"""

from recon_accuracy import build_case, match, score

from src.common.schemas import EXCEPTION_FEE_MISMATCH


def test_off_by_two_is_mismatch_zero_fp():
    webhooks, settlements, answer = build_case(n=500, seed=7)
    # shift one known-MATCHED settlement by tolerance+1 -> must flip to MISMATCH
    target = next(tx for tx, y in answer.items() if y == "MATCHED")
    shifted = [(tx, s + 2, d) if tx == target else (tx, s, d) for tx, s, d in settlements]
    predicted = match(webhooks, shifted)
    assert predicted[target] == EXCEPTION_FEE_MISMATCH
    assert score(answer, predicted)["false_positives"] == 0


def test_rerun_identical():
    webhooks, settlements, _ = build_case(n=500, seed=11)
    assert match(webhooks, settlements) == match(webhooks, settlements)
