"""One rounding rule for rupees to paise, shared by every reader.

The pipeline converts money in three places and they did not agree. What
pins them together is ``Decimal`` HALF_UP, which is the rule the fee engine's
rate arithmetic already assumes and the rule an auditor would expect from a
ledger: a half paisa goes away from zero, not towards even.
"""

from decimal import Decimal

import pandas as pd
import pytest

from src.adapters.bank_statement import _paise
from src.adapters.settlement import _paise_inr_series

pytestmark = pytest.mark.unit


def _decimal_half_up(rupees: str) -> int:
    return int((Decimal(rupees) * 100).to_integral_value(rounding="ROUND_HALF_UP"))


# Half-paise ties are where the three readers used to part company. Positive
# ties already worked; negative ties are the regression.
TIES = ["0.125", "-0.125", "0.145", "-0.145", "0.005", "-0.005", "100.50", "-100.50"]
# Non-ties, to prove the fix did not disturb the ordinary path.
ORDINARY = ["0", "-0", "1", "-1", "99.999", "1234.56", "-1234.56", "1234567.89", "-1234567.89"]


@pytest.mark.parametrize("rupees", TIES + ORDINARY)
def test_series_converter_matches_decimal_half_up(rupees):
    got = _paise_inr_series(pd.Series([rupees], dtype=object)).iloc[0]
    assert got == _decimal_half_up(rupees)


@pytest.mark.parametrize("rupees", TIES + ORDINARY)
def test_bank_converter_matches_decimal_half_up(rupees):
    assert _paise(Decimal(rupees)) == _decimal_half_up(rupees)


def test_negative_tie_rounds_away_from_zero_not_towards_even():
    """The regression, stated as one line.

    ``floor(x + 0.5)`` sends a tie towards positive infinity, so -0.125 rupees
    became -12 paise. Half-to-even would give the same wrong answer, which is
    why the reference is an explicit rounding mode and not another float.
    """
    assert _paise_inr_series(pd.Series(["-0.125"], dtype=object)).iloc[0] == -13
    assert _paise_inr_series(pd.Series(["0.125"], dtype=object)).iloc[0] == 13
    assert round(12.5) == 12, "if this ever changes the test above is measuring the wrong thing"


@pytest.mark.parametrize("raw", ["abc", "nan", "None", "", None, "Rs 500.25", "-", "1.2.3"])
def test_unparseable_amounts_stay_missing(raw):
    """A value the converter cannot read stays NA; it never becomes a zero."""
    assert pd.isna(_paise_inr_series(pd.Series([raw], dtype=object)).iloc[0]), raw


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1,234.56", 123456), (" 12.34 ", 1234), ("₹99.99", 9999), ("-0.001", 0)],
)
def test_formatted_amounts_are_read_as_written(raw, expected):
    """Thousand separators, currency marks and padding are stripped first."""
    assert _paise_inr_series(pd.Series([raw], dtype=object)).iloc[0] == expected


def test_converter_keeps_nullable_int64_dtype():
    out = _paise_inr_series(pd.Series(["1.00", "abc", "-2.50"], dtype=object))
    assert str(out.dtype) == "Int64"
    assert out.isna().sum() == 1


def test_empty_input_is_not_an_error():
    out = _paise_inr_series(pd.Series([], dtype=object))
    assert len(out) == 0


@pytest.mark.parametrize("rupees", TIES + ORDINARY)
def test_integer_fee_formula_agrees_with_decimal_on_the_rounded_gross(rupees):
    """The fee table multiplies by basis points on an integer.

    Rounding the gross and rounding the fee are separate operations, so pin
    them to each other: the bps formula has to land on the same paise as the
    Decimal reference once the gross is settled.
    """
    gross = _paise_inr_series(pd.Series([rupees], dtype=object)).iloc[0]
    by_bps = int((int(gross) * 200 + 5000) // 10000)
    by_decimal = int(
        (Decimal(int(gross)) * Decimal(200) / Decimal(10000)).to_integral_value(
            rounding="ROUND_HALF_UP"
        )
    )
    assert by_bps == by_decimal, rupees
