from hypothesis import given
from hypothesis import strategies as st

from src.processing.fee_engine import get_fee_engine


@given(
    amount_paise=st.integers(min_value=100, max_value=100000000),
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_fee_rounding_never_drifts_half_a_paisa(amount_paise, instrument_type):
    fee_engine = get_fee_engine()
    result = fee_engine.compute_fee(amount_paise, instrument_type)

    mdr_paise = result.fee_paise - result.gst_paise
    assert abs(mdr_paise * 10000 - amount_paise * result.rate_bps) <= 5000


@given(
    amount_paise=st.integers(min_value=100, max_value=100000000),
    instrument_type=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_gst_never_exceeds_the_fee_it_is_charged_on(amount_paise, instrument_type):
    result = get_fee_engine().compute_fee(amount_paise, instrument_type)

    assert result.gst_paise <= result.fee_paise - result.gst_paise


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
