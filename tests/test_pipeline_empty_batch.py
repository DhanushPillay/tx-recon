"""A batch with nothing to reconcile must warn, not raise a drift error.

``check_batch_drift`` read its row count as
``counts.get("settlement_rows_evaluated") or counts.get("settlement_rows_deduped", 0)``.
``or`` treats a genuine 0 as absent, so a batch where every settlement row has a
NULL ``settled_amount_paise`` — reported by reconcile.py as 0 evaluated and N
deduped — was scored against N, and the function raised "Batch drift" naming
dedup and the dedup window as the suspects. The empty-batch branch three lines
below it could never fire for exactly the case it was written for.
"""

import pytest

from src.pipeline import check_batch_drift

pytestmark = pytest.mark.unit


def _evaluated_zero_batch(evaluated: int = 0, deduped: int = 5) -> dict:
    """What reconciliation reports when no settlement row carries an amount.

    Every settlement row survives dedup, but reconcile.py:353 filters on
    ``settled_amount_paise IS NOT NULL`` before counting, so nothing is
    evaluated and the MERGE matches nothing.
    """
    return {
        "settlement_rows_total": deduped,
        "settlement_rows_deduped": deduped,
        "settlement_rows_evaluated": evaluated,
        "batch_MATCHED": 0,
    }


def test_zero_evaluated_warns_instead_of_raising(caplog):
    """The regression: this used to raise Batch drift with a misleading cause."""
    with caplog.at_level("WARNING"):
        check_batch_drift(_evaluated_zero_batch(), ds="2026-09-04")
    assert "Empty settlement batch" in caplog.text
    assert "ds=2026-09-04" in caplog.text
    assert not [r for r in caplog.records if r.levelname == "ERROR"]


def test_zero_evaluated_says_no_check_was_possible(caplog):
    with caplog.at_level("WARNING"):
        check_batch_drift(_evaluated_zero_batch(deduped=9805))
    assert "0 rows evaluated" in caplog.text
    assert "no drift or SLO check possible" in caplog.text


@pytest.mark.parametrize("deduped", [1, 5, 9805])
def test_zero_evaluated_never_raises_regardless_of_deduped_count(deduped):
    """Any N above zero used to trip the drift check, since tile(0) != N."""
    check_batch_drift(_evaluated_zero_batch(deduped=deduped))


def test_absent_evaluated_key_still_falls_back_to_deduped():
    """The fallback the `or` was written for must survive the fix.

    Counts dicts built before reconcile.py reported the key, and several tests
    hand-roll one with only the deduped count.
    """
    check_batch_drift(
        {"settlement_rows_deduped": 2, "batch_MATCHED": 1, "batch_EXCEPTION_MISSING_WEBHOOK": 1}
    )


def test_absent_key_with_deduped_zero_still_warns(caplog):
    with caplog.at_level("WARNING"):
        check_batch_drift({"settlement_rows_deduped": 0, "batch_MATCHED": 0})
    assert "Empty settlement batch" in caplog.text


def test_absent_both_keys_is_treated_as_empty(caplog):
    with caplog.at_level("WARNING"):
        check_batch_drift({"batch_MATCHED": 3})
    assert "Empty settlement batch" in caplog.text


def test_none_valued_key_falls_back_rather_than_comparing_none():
    """``None`` means the same as absent here; a real 0 does not."""
    check_batch_drift(
        {
            "settlement_rows_evaluated": None,
            "settlement_rows_deduped": 3,
            "batch_MATCHED": 3,
        }
    )


def test_real_batch_still_raises_on_genuine_drift():
    """The gate must keep its teeth for the case it exists to catch."""
    with pytest.raises(RuntimeError, match="Batch drift"):
        check_batch_drift(
            {
                "settlement_rows_evaluated": 10,
                "settlement_rows_deduped": 10,
                "batch_MATCHED": 6,
                "batch_EXCEPTION_MISSING_WEBHOOK": 3,
            }
        )


def test_zero_evaluated_returns_before_the_tiling_check(caplog):
    """Pins the guard's early return, which predates this fix.

    Nothing can join the MERGE when no settlement row carries an amount, so
    statuses alongside a 0 evaluated count are impossible in practice. The
    guard returns before the tiling check either way; it warns and does not
    raise. This is existing behaviour, recorded here so a future change to the
    guard order is a visible decision rather than a silent drift.
    """
    with caplog.at_level("WARNING"):
        check_batch_drift(
            {
                "settlement_rows_evaluated": 0,
                "settlement_rows_deduped": 0,
                "batch_MATCHED": 4,
            }
        )
    assert "Empty settlement batch" in caplog.text
