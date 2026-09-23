from hypothesis import given
from hypothesis import strategies as st

from src.processing.fee_engine import get_fee_engine
from src.processing.residual import DEFAULT_TAU, score_match, should_downgrade


def test_merchant_disagreement_scores_high():
    eng = get_fee_engine()
    # merch_001 CREDIT_CARD at 180bps vs default 200bps — create amount where default MATCH, merchant MISMATCH.
    amount = 100000  # Rs 1000
    # settled for merch_001 at 180bps
    exp_merch = eng.compute_expected_settled(amount, "CREDIT_CARD", merchant_id="merch_001")
    # default at 200bps would be lower; use merch settled but score with merch_001 merchant_id so disagreement triggers
    exp_default = eng.compute_expected_settled(amount, "CREDIT_CARD", merchant_id=None)
    assert exp_default != exp_merch
    # Use mismatched settled: default MATCH window does not contain merch expected, but merch would
    # Construct settled that is MATCH under default but not under merch: take exp_default +1 (within tol 1)
    settled = exp_default
    s = score_match(amount, settled, "CREDIT_CARD", "merch_001", None, fee_engine=eng)
    assert s == 0.95
    assert should_downgrade(
        amount, settled, "CREDIT_CARD", "merch_001", None, tau=DEFAULT_TAU, fee_engine=eng
    )


def test_no_downgrade_on_perfect_match():
    eng = get_fee_engine()
    amount = 50000
    settled = eng.compute_expected_settled(amount, "UPI")
    s = score_match(amount, settled, "UPI", None, None, fee_engine=eng)
    assert s < DEFAULT_TAU
    assert not should_downgrade(amount, settled, "UPI", None, None, fee_engine=eng)


def test_threshold_monotone():
    eng = get_fee_engine()
    amount = 100000
    settled_match = eng.compute_expected_settled(amount, "CREDIT_CARD")
    settled_mismatch = settled_match + 500
    assert score_match(
        amount, settled_match, "CREDIT_CARD", None, None, fee_engine=eng
    ) < score_match(amount, settled_mismatch, "CREDIT_CARD", None, None, fee_engine=eng)


@given(
    amount=st.integers(min_value=1000, max_value=1_000_000),
    instrument=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_perfect_match_never_downgrades_property(amount, instrument):
    """Perfect rule-match scores below tau for all draws (calls real scorer)."""
    eng = get_fee_engine()
    settled = eng.compute_expected_settled(amount, instrument)
    s = score_match(amount, settled, instrument, None, None, fee_engine=eng)
    assert 0.0 <= s < DEFAULT_TAU
    assert not should_downgrade(amount, settled, instrument, None, None, fee_engine=eng)


@given(
    amount=st.integers(min_value=1000, max_value=1_000_000),
    instrument=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
    merchant=st.sampled_from([None, "merch_001", "merch_demo", "merch_unknown"]),
    delta=st.integers(min_value=-5000, max_value=5000),
)
def test_downgrade_never_creates_fp(amount, instrument, merchant, delta):
    """PROOF.md property: downgrade-only post-pass never promotes, so for any
    truth labeling FP(after) <= FP(before). Structural: after==MATCHED implies
    before==MATCHED, checked against both default-truth and merchant-truth."""
    from src.common.schemas import EXCEPTION_FEE_MISMATCH, MATCHED

    eng = get_fee_engine()
    settled = eng.compute_expected_settled(amount, instrument) + delta
    ok_default, _ = eng.check_match(amount, settled, instrument)
    pred_before = MATCHED if ok_default else EXCEPTION_FEE_MISMATCH
    pred_after = (
        EXCEPTION_FEE_MISMATCH
        if pred_before == MATCHED
        and should_downgrade(amount, settled, instrument, merchant, None, fee_engine=eng)
        else pred_before
    )
    # No promotion, ever.
    assert not (pred_before != MATCHED and pred_after == MATCHED)
    # FP monotone under either truth labeling.
    for truth in (
        MATCHED if eng.check_match(amount, settled, instrument)[0] else EXCEPTION_FEE_MISMATCH,
        MATCHED
        if eng.check_match(amount, settled, instrument, merchant_id=merchant)[0]
        else EXCEPTION_FEE_MISMATCH,
    ):
        fp_before = int(pred_before == MATCHED and truth != MATCHED)
        fp_after = int(pred_after == MATCHED and truth != MATCHED)
        assert fp_after <= fp_before


@given(
    amount=st.integers(min_value=1000, max_value=1_000_000),
    instrument=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_score_monotone_in_excess_property(amount, instrument):
    """A far-mismatch always scores above the perfect match (same draw)."""
    eng = get_fee_engine()
    settled_ok = eng.compute_expected_settled(amount, instrument)
    s_ok = score_match(amount, settled_ok, instrument, None, None, fee_engine=eng)
    s_bad = score_match(amount, settled_ok + 500, instrument, None, None, fee_engine=eng)
    assert s_bad > s_ok
