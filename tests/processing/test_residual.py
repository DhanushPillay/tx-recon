from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from src.common.schemas import EXCEPTION_FEE_MISMATCH, MATCHED
from src.processing.fee_engine import get_fee_engine
from src.processing.residual import (
    DEFAULT_TAU,
    apply_residual,
    score_match,
    score_match_sql,
    should_downgrade,
)


def _sql_score(spark, eng, amount, settled, instrument, merchant):
    t2 = spark.createDataFrame([(amount,)], "amount_paise bigint").alias("t2")
    s = spark.createDataFrame(
        [(settled, instrument, merchant, None)],
        "settled_amount_paise bigint, instrument_type string, "
        "merchant_id string, settlement_date string",
    ).alias("s")
    expr = score_match_sql(eng)
    return float(t2.crossJoin(s).selectExpr(f"{expr} AS score").collect()[0]["score"])


def test_merchant_disagreement_scores_high():
    eng = get_fee_engine()
    amount = 100000
    exp_merch = eng.compute_expected_settled(amount, "CREDIT_CARD", merchant_id="merch_001")
    exp_default = eng.compute_expected_settled(amount, "CREDIT_CARD", merchant_id=None)
    assert exp_default != exp_merch
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
    eng = get_fee_engine()
    settled = eng.compute_expected_settled(amount, instrument)
    s = score_match(amount, settled, instrument, None, None, fee_engine=eng)
    assert 0.0 <= s < DEFAULT_TAU
    assert not should_downgrade(amount, settled, instrument, None, None, fee_engine=eng)


@settings(
    max_examples=15, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    amount=st.integers(min_value=1000, max_value=1_000_000),
    instrument=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
    merchant=st.sampled_from([None, "merch_001", "merch_demo", "merch_unknown"]),
    delta=st.integers(min_value=-5000, max_value=5000),
    tau=st.floats(min_value=0.5, max_value=0.999, allow_nan=False, allow_infinity=False),
)
def test_downgrade_never_creates_fp(amount, instrument, merchant, delta, tau, spark):
    eng = get_fee_engine()
    settled = eng.compute_expected_settled(amount, instrument) + delta
    ok_before, _ = eng.check_match(amount, settled, instrument)
    pred_before = MATCHED if ok_before else EXCEPTION_FEE_MISMATCH

    score = _sql_score(spark, eng, amount, settled, instrument, merchant)
    assert score == pytest.approx(
        score_match(amount, settled, instrument, merchant, None, fee_engine=eng)
    ), "SQL and Python scorers disagree, so the proof is about a scorer nobody runs"

    pred_after = EXCEPTION_FEE_MISMATCH if pred_before == MATCHED and score >= tau else pred_before
    assert not (pred_before != MATCHED and pred_after == MATCHED)
    for truth in (
        MATCHED if eng.check_match(amount, settled, instrument)[0] else EXCEPTION_FEE_MISMATCH,
        MATCHED
        if eng.check_match(amount, settled, instrument, merchant_id=merchant)[0]
        else EXCEPTION_FEE_MISMATCH,
    ):
        fp_before = int(pred_before == MATCHED and truth != MATCHED)
        fp_after = int(pred_after == MATCHED and truth != MATCHED)
        assert fp_after <= fp_before


def test_apply_residual_can_only_demote_a_matched_row():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"n": 0}]
    apply_residual(spark, "nessie.db.webhooks")

    source = spark.sql.call_args_list[0][0][0]
    assert f"t2.reconciliation_status = '{MATCHED}'" in source

    spark.sql.reset_mock()
    spark.sql.return_value.collect.return_value = [{"n": 3}]
    apply_residual(spark, "nessie.db.webhooks")
    merge = spark.sql.call_args_list[1][0][0]

    assert "WHEN NOT MATCHED" not in merge
    set_clause = merge.split("UPDATE SET", 1)[1]
    assert f"= '{EXCEPTION_FEE_MISMATCH}'" in set_clause
    assert f"= '{MATCHED}'" not in set_clause


@given(
    amount=st.integers(min_value=1000, max_value=1_000_000),
    instrument=st.sampled_from(
        ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
    ),
)
def test_score_monotone_in_excess_property(amount, instrument):
    eng = get_fee_engine()
    settled_ok = eng.compute_expected_settled(amount, instrument)
    s_ok = score_match(amount, settled_ok, instrument, None, None, fee_engine=eng)
    s_bad = score_match(amount, settled_ok + 500, instrument, None, None, fee_engine=eng)
    assert s_bad > s_ok
