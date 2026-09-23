"""Adversarial accuracy: off-by-two must MISMATCH; reruns must be identical.

Hermetic: pure-Python harness, no Spark/Docker.
"""

from recon_accuracy import _expected_net_raw, build_case, match, score

from src.common.schemas import EXCEPTION_FEE_MISMATCH


def test_off_by_two_is_mismatch_zero_fp():
    webhooks, settlements, answer = build_case(n=500, seed=7)
    # shift one known-MATCHED settlement by tolerance+1 -> must flip to MISMATCH.
    # Target an exact-net row (not rounding noise): net-1 noise + 2 lands back
    # inside tolerance and would not flip. Tuple shape is (tx, settled, day,
    # merchant, settlement_date); trailing fields pass through untouched.
    target = next(
        tx
        for tx, y in answer.items()
        if y == "MATCHED"
        and all(s == _expected_net_raw(*webhooks[tx]) for t, s, *_ in settlements if t == tx)
    )
    shifted = [
        (tx, s + 2, *rest) if tx == target else (tx, s, *rest) for tx, s, *rest in settlements
    ]
    predicted = match(webhooks, shifted)
    assert predicted[target] == EXCEPTION_FEE_MISMATCH
    assert score(answer, predicted)["false_positives"] == 0


def test_rerun_identical():
    webhooks, settlements, _ = build_case(n=500, seed=11)
    assert match(webhooks, settlements) == match(webhooks, settlements)
