from hypothesis import given
from hypothesis import strategies as st

from src.processing.fee_engine import get_fee_engine

# Property 1: Conservation of Value
# Gross Amount == Net Settled Amount + MDR Fee + GST


@given(
    amount_paise=st.integers(min_value=100, max_value=100000000),  # 1 INR to 1M INR
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_settlement_fee_decomposition_invariant(amount_paise, instrument_type):
    fee_engine = get_fee_engine()
    result = fee_engine.compute_fee(amount_paise, instrument_type)

    # Invariant: Gross must exactly equal Net + Fees + Tax
    # Total fee computed inside FeeEngine includes GST, so:
    assert amount_paise == result.net_paise + result.fee_paise


# Property 2: Non-negative amounts
@given(
    amount_paise=st.integers(min_value=0, max_value=100000000),
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_fees_are_non_negative(amount_paise, instrument_type):
    fee_engine = get_fee_engine()
    result = fee_engine.compute_fee(amount_paise, instrument_type)

    assert result.fee_paise >= 0
    assert result.net_paise >= 0
    assert result.gst_paise >= 0


# Property 3: Fee monotonicity — bigger amount never yields a smaller fee
@given(
    a=st.integers(min_value=0, max_value=100000000),
    b=st.integers(min_value=0, max_value=100000000),
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_fee_monotonic(a, b, instrument_type):
    fee_engine = get_fee_engine()
    fa = fee_engine.compute_fee(min(a, b), instrument_type).fee_paise
    fb = fee_engine.compute_fee(max(a, b), instrument_type).fee_paise
    assert fa <= fb


# Property 4: check_match agrees with compute_fee (self-consistency)
@given(
    amount_paise=st.integers(min_value=100, max_value=100000000),
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_check_match_self_consistent(amount_paise, instrument_type):
    fee_engine = get_fee_engine()
    net = fee_engine.compute_expected_settled(amount_paise, instrument_type)
    matched, _ = fee_engine.check_match(amount_paise, net, instrument_type)
    assert matched is True
