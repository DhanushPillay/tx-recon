"""The bps rule must not depend on the sign of the amount.

`reconcile._bps_sql` emitted `(x * bps + 5000) DIV 10000`. Spark's `DIV`
truncates toward zero, so that expression is round-half-up only while x is
positive; on a negative x it comes out one paisa short of what
`fee_engine.bps_amount` returns in Python. The two halves of the same formula
therefore disagreed on 23580 of 24003 negative amounts in the grid below, not
on the rare tie I first assumed.

Nothing in the pipeline prices a negative gross amount today:
`settlement_schema.Check.gt(0)` quarantines them, and `compute_fee` raises on
a negative amount_paise. That is why the disagreement went unnoticed. These
tests pin the arithmetic so the two halves cannot drift again when a refund
or a chargeback reaches the fee path; they are not a claim about how any
provider actually charges a fee on a reversal.
"""

import pandas as pd
import pytest

from src.processing.fee_engine import FeeEngine, bps_amount, gst_pct_to_bps
from src.processing.reconcile import _bps_sql, build_fee_case_sql

pytestmark = pytest.mark.unit

_BPS = [0, 1, 7, 100, 150, 180, 200, 1200, 1800, 10000]
_AMOUNTS = [
    0,
    1,
    7,
    24,
    25,
    26,
    4999,
    5000,
    5001,
    99999,
    100000,
    100001,
    -1,
    -7,
    -24,
    -25,
    -26,
    -4999,
    -5000,
    -5001,
    -99999,
    -100000,
    -100001,
]

# amount * bps % 10000 == 5000 puts the product exactly on a half-paisa tie.
_TIE_AMOUNTS = [5000, 15000, 25000, -5000, -15000, -25000]


def _reference(amount: int, bps: int) -> int:
    """Half away from zero, written out longhand and independent of bps_amount."""
    product = amount * bps
    whole, remainder = divmod(abs(product), 10000)
    if remainder * 2 >= 10000:
        whole += 1
    return -whole if product < 0 else whole


def _run_sql(spark, expression: str, amounts: list[int], bps: int, column: str = "a"):
    df = spark.createDataFrame(
        pd.DataFrame([(a,) for a in amounts], columns=[column]), f"{column} bigint"
    )
    rows = df.selectExpr(column, f"{expression} AS out").collect()
    return {r[0]: r["out"] for r in rows}


# --- the documented rule itself ------------------------------------------------


@pytest.mark.parametrize("bps", _BPS)
@pytest.mark.parametrize("amount", _AMOUNTS + _TIE_AMOUNTS)
def test_bps_amount_matches_an_independent_reference(amount, bps):
    assert bps_amount(amount, bps) == _reference(amount, bps)


def test_bps_amount_is_symmetric_about_zero():
    for bps in (7, 100, 150, 1800):
        for amount in (1, 24, 25, 4999, 5001, 99999, 100001):
            assert bps_amount(amount, bps) == -bps_amount(-amount, bps)


def test_bps_amount_is_exactly_half_when_the_product_lands_on_a_tie():
    # 5000 paise at 100 bps = 50 paise exactly: no tie.
    assert bps_amount(5000, 100) == 50
    # 5000 paise at 200 bps = 100 paise exactly: no tie either.
    assert bps_amount(5000, 200) == 100
    # 25 paise at 200 bps = 0.5 paise -> away from zero, either sign.
    assert bps_amount(25, 200) == 1
    assert bps_amount(-25, 200) == -1


def test_zero_rate_costs_nothing_in_either_direction():
    assert bps_amount(123456, 0) == 0
    assert bps_amount(-123456, 0) == 0


def test_a_zero_fee_stays_a_zero_fee_after_the_fix():
    """compute_fee on the smallest chargeable amount, so the case is reachable."""
    assert bps_amount(1, 150) == 0
    assert bps_amount(1, 150) == bps_amount(-1, 150) - 0  # symmetry, not a subtraction


# --- the SQL half --------------------------------------------------------------


@pytest.mark.parametrize("bps", _BPS)
def test_sql_rounds_the_same_way_as_python(spark, bps):
    amounts = _AMOUNTS + _TIE_AMOUNTS
    got = _run_sql(spark, _bps_sql("a", bps), amounts, bps)
    wrong = {a: (got[a], bps_amount(a, bps)) for a in amounts if got[a] != bps_amount(a, bps)}
    assert not wrong, f"SQL != engine for {bps} bps: {wrong}"


def test_sql_agrees_on_every_positive_amount(spark):
    """The pre-fix expression was already right here; keep it pinned."""
    amounts = list(range(0, 500))
    got = _run_sql(spark, _bps_sql("a", 180), amounts, 180)
    assert all(got[a] == bps_amount(a, 180) for a in amounts)


def test_sql_agrees_on_every_negative_amount_in_a_wide_band(spark):
    """Denser than the tie set: the old expression was wrong on nearly all of these."""
    amounts = list(range(-500, 0))
    got = _run_sql(spark, _bps_sql("a", 200), amounts, 200)
    wrong = [a for a in amounts if got[a] != bps_amount(a, 200)]
    assert not wrong, f"{len(wrong)} negatives disagree, first 5: {wrong[:5]}"


def test_a_debit_sized_charge_costs_the_same_magnitude_as_its_credit(spark):
    """The user-visible consequence: a 10-rupee reversal is priced like the 10-rupee payment."""
    got = _run_sql(spark, _bps_sql("a", 200), [1000, -1000], 200)
    assert got[1000] == 20
    assert got[-1000] == -20


def test_sql_keeps_working_when_the_input_is_an_expression(spark):
    """The GST leg nests the fee expression, so the helper has to compose."""
    fee = _bps_sql("a", 200)
    got = _run_sql(spark, _bps_sql(fee, 1800), [100000, -100000], 1800)
    assert got[100000] == bps_amount(bps_amount(100000, 200), 1800)
    assert got[-100000] == bps_amount(bps_amount(-100000, 200), 1800)


def test_sql_leaves_a_null_amount_null(spark):
    df = spark.createDataFrame(pd.DataFrame([(None,)], columns=["a"]), "a bigint")
    assert df.selectExpr(f"{_bps_sql('a', 200)} AS out").collect()[0].out is None


def test_no_arm_in_the_generated_fee_sql_uses_the_bare_div_form(spark):
    """A grep-level guard: `(... * bps + 5000) DIV 10000` outside the helper is a regression."""
    fee_sql, gst_sql = build_fee_case_sql(FeeEngine(), "a", "i", "m", "d")
    for fragment in (fee_sql, gst_sql):
        guards = fragment.count(" THEN -(((-(")
        assert guards, "the sign guard is missing entirely"
        # The helper emits one `THEN -(((-(` and two `+ 5000) DIV 10000`, and
        # neither substring appears anywhere else, so a stray bare DIV anywhere
        # else in the generated SQL shows up as an odd count.
        assert fragment.count("+ 5000) DIV 10000") == 2 * guards


# --- and the two together ------------------------------------------------------


def test_engine_fee_and_sql_fee_agree_across_the_sign_boundary(spark):
    """compute_fee still refuses a negative amount; this pins the arithmetic below it."""
    engine = FeeEngine()
    bps = engine.get_rate("WALLET")["mdr_rate_bps"]
    assert bps > 0, "WALLET is expected to carry a non-zero rate in config/fee_rates.yaml"
    amounts = [a for a in _AMOUNTS if a > 0]
    got = _run_sql(spark, _bps_sql("a", bps), amounts, bps)
    for a in amounts:
        assert got[a] == bps_amount(a, bps), f"amount {a} at {bps} bps"


def test_gst_bps_conversion_and_the_gst_leg_use_the_same_rule():
    gst_bps = gst_pct_to_bps(18.0)
    assert gst_bps == 1800
    fee = bps_amount(100000, 200)
    assert bps_amount(fee, gst_bps) == bps_amount(bps_amount(100000, 200), 1800)
