"""The publish gate must judge the rate that survives the residual pass.

run_reconciliation counts statuses and computes batch_match_rate, then
apply_residual moves MATCHED rows to EXCEPTION_FEE_MISMATCH. Nothing recomputed
the counts afterwards, so _slo_ok compared a rate the batch had before a second
opinion acted on it.

check_batch_drift did not notice, and that is the reason this went unnoticed: a
demotion moves a row between two batch_* buckets, so the sum the drift check
tiles against is unchanged. The check passed while the rate went stale.
"""

import pytest

from src.pipeline import _fold_residual, _slo_ok
from src.processing.reconcile import batch_match_rate

pytestmark = pytest.mark.unit

SLO = 0.95


def _counts(matched: int, mismatched: int = 0, evaluated: int = 100, missing_lag: int = 0) -> dict:
    """The counts dict reconciliation produces for a batch."""
    counts = {
        "settlement_rows_total": evaluated,
        "settlement_rows_deduped": evaluated,
        "settlement_rows_evaluated": evaluated,
        "missing_within_lag": missing_lag,
        "batch_MATCHED": matched,
        "batch_EXCEPTION_FEE_MISMATCH": mismatched,
    }
    rate = batch_match_rate(counts)
    if rate is not None:
        counts["batch_match_rate"] = rate
    return counts


def test_demotion_lowers_the_rate_the_gate_reads():
    counts = _counts(matched=97, mismatched=3)
    assert counts["batch_match_rate"] == 0.97
    _fold_residual(counts, 2)
    assert counts["batch_MATCHED"] == 95
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 5
    assert counts["batch_match_rate"] == 0.95


def test_demotion_can_breach_the_gate_that_previously_passed():
    """The regression: this batch published with a rate below SLO.

    97 of 100 matched, so reconciliation passed the gate. The residual demoted 3
    more and the batch was still published at 94%, which is under the 95% SLO.
    """
    counts = _counts(matched=97, mismatched=3)
    assert _slo_ok(counts) is True
    _fold_residual(counts, 3)
    assert counts["batch_match_rate"] == 0.94
    assert _slo_ok(counts) is False


def test_batch_just_inside_the_slo_stays_publishable():
    counts = _counts(matched=97, mismatched=3)
    _fold_residual(counts, 2)
    assert _slo_ok(counts) is True


def _status_total(counts: dict) -> int:
    """Sum the way check_batch_drift does: batch_* keys holding ints.

    batch_match_rate is a float and is excluded by the same isinstance filter.
    """
    return sum(v for k, v in counts.items() if k.startswith("batch_") and isinstance(v, int))


def test_demotion_keeps_the_status_sum_intact():
    """The drift gate's tiling is unaffected, which is why this hid so well."""
    counts = _counts(matched=97, mismatched=3)
    before = _status_total(counts)
    _fold_residual(counts, 4)
    assert _status_total(counts) == before == 100


def test_zero_demotion_is_a_no_op():
    counts = _counts(matched=97, mismatched=3)
    before = dict(counts)
    _fold_residual(counts, 0)
    assert counts == before


def test_fold_returns_none_so_it_cannot_be_mistaken_for_a_count():
    """counts["residual"] holds the demotion count, so the fold must not
    be assignable in its place."""
    counts = _counts(matched=97, mismatched=3)
    assert _fold_residual(counts, 2) is None


def test_negative_demotion_is_a_no_op():
    """apply_residual returns a count; a negative one means the caller is wrong."""
    counts = _counts(matched=97, mismatched=3)
    before = dict(counts)
    _fold_residual(counts, -5)
    assert counts == before


def test_matched_never_goes_negative():
    counts = _counts(matched=2, mismatched=98)
    _fold_residual(counts, 10)
    assert counts["batch_MATCHED"] == 0
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 108


def test_absent_matched_bucket_is_treated_as_zero():
    counts = {"settlement_rows_evaluated": 10, "missing_within_lag": 0}
    _fold_residual(counts, 3)
    assert counts["batch_MATCHED"] == 0
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 3
    assert counts["batch_match_rate"] == 0.0


def test_absent_mismatched_bucket_is_created():
    counts = {"settlement_rows_evaluated": 10, "batch_MATCHED": 10, "batch_match_rate": 1.0}
    _fold_residual(counts, 4)
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 4
    assert counts["batch_match_rate"] == 0.6


def test_rate_is_not_recomputed_when_there_is_no_denominator():
    """No settlement row evaluated means there is no rate to recompute.

    The pre-existing key is left alone rather than overwritten with a value the
    shared helper declines to produce.
    """
    counts = {"settlement_rows_evaluated": 0, "missing_within_lag": 0, "batch_MATCHED": 0}
    counts["batch_match_rate"] = "untouched"
    _fold_residual(counts, 3)
    assert counts["batch_match_rate"] == "untouched"
    assert counts["batch_MATCHED"] == 0


def test_missing_within_lag_still_comes_out_of_the_denominator():
    """Late orphans are not something a settlement could have matched."""
    counts = _counts(matched=90, mismatched=5, evaluated=100, missing_lag=10)
    assert counts["batch_match_rate"] == 1.0
    _fold_residual(counts, 1)
    assert counts["batch_match_rate"] == 0.9888888888888889


def test_real_batch_shape_from_the_shipped_metrics():
    """The 9 Oct run, which data/metrics_20260904.json recorded as 0.8988.

    8813 MATCHED over 9805 evaluated, with no missing_within_lag. This is the
    MATCHED/deduped rate, not the 94.63% that recon_accuracy reports over a
    matched-plus-mismatched denominator.
    """
    counts = _counts(matched=8813, mismatched=500, evaluated=9805)
    assert counts["batch_match_rate"] == 8813 / 9805
    assert round(counts["batch_match_rate"], 4) == 0.8988
    _fold_residual(counts, 12)
    assert counts["batch_MATCHED"] == 8801
    assert round(counts["batch_match_rate"], 4) == 0.8976


# --- the shared formula -------------------------------------------------


@pytest.mark.parametrize(
    ("matched", "evaluated", "missing_lag", "expected"),
    [
        (0, 0, 0, None),
        (5, 0, 0, None),
        (5, 10, 10, None),
        (5, 10, 20, None),
        (0, 10, 0, 0.0),
        (10, 10, 0, 1.0),
        (5, 10, 0, 0.5),
        (9, 10, 1, 1.0),
    ],
)
def test_batch_match_rate(matched, evaluated, missing_lag, expected):
    counts = {
        "batch_MATCHED": matched,
        "settlement_rows_evaluated": evaluated,
        "missing_within_lag": missing_lag,
    }
    assert batch_match_rate(counts) == expected


def test_batch_match_rate_on_an_empty_dict_is_none():
    assert batch_match_rate({}) is None


# --- the wiring, through run_daily --------------------------------------


def test_run_daily_publishes_counts_the_residual_already_moved(tmp_path):
    """The published metrics must describe the batch as it ended.

    98 of 100 matched before the residual. It demoted 2, leaving 96%, still
    inside the SLO, so the batch publishes -- but what it publishes has to be
    the 96% batch, not the 98% one.
    """
    from unittest.mock import MagicMock, patch

    from src.pipeline import run_daily

    counts = _counts(matched=98, mismatched=2, evaluated=100)
    assert _slo_ok(counts) is True
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=dict(counts)),
        patch("src.processing.residual.apply_residual", return_value=2),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
    ):
        out = run_daily(date_str="2025-04-02")

    assert out["residual"] == 2
    assert out["batch_MATCHED"] == 96
    assert out["batch_EXCEPTION_FEE_MISMATCH"] == 4
    assert out["batch_match_rate"] == 0.96
    assert _status_total(out) == 100
    assert any("fast_forward" in c[0][0] for c in spark.sql.call_args_list)


def test_run_daily_blocks_publish_when_the_residual_breaks_the_slo(tmp_path):
    """The demotion moves the batch under SLO, so nothing may be published."""
    from unittest.mock import MagicMock, patch

    from src.pipeline import run_daily

    counts = _counts(matched=96, mismatched=4, evaluated=100)
    spark = MagicMock()
    with (
        patch(
            "src.validation.validate_settlement.validate_latest_settlement",
            return_value=str(tmp_path / "curated_settlement_20250402.csv"),
        ),
        patch("src.common.config.get_spark_session", return_value=spark),
        patch("src.processing.reconcile.run_reconciliation", return_value=dict(counts)),
        patch("src.processing.residual.apply_residual", return_value=2),
        patch("src.processing.reconcile.maintain_tables", return_value={}),
        pytest.raises(RuntimeError, match="publish gate failed"),
    ):
        run_daily(date_str="2025-04-02")

    stmts = [c[0][0] for c in spark.sql.call_args_list]
    assert not any("fast_forward" in s for s in stmts)
