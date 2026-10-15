"""A settlement paid in two instalments is not a duplicate settlement.

fold_settlements used to take row_number() == 1 per transaction and discard
everything else. For a provider re-sending the same line that is right. For a
merchant paid 400 then 600 it silently threw 400 away, compared 1000 - fees
against 600, and if the tolerance covered the gap marked the row MATCHED with
a fifth of the revenue missing from the ledger. The discarded rows were then
reported as duplicate_settlement_rows, which files an instalment plan as a
delivery fault.

These run against a real local Spark session because the behaviour is a
grouping and a window, not a string.
"""

from __future__ import annotations

import pytest

from src.processing.reconcile import fold_settlements

pytestmark = pytest.mark.unit

HEADER = [
    "bank_ref_id",
    "transaction_id",
    "settled_amount_paise",
    "settlement_date",
    "instrument_type",
    "merchant_id",
    "fee_paise",
    "gst_paise",
    "settlement_id",
    "utr",
    "currency",
    "gross_amount_paise",
    "provider",
]


def _rows(*specs: dict) -> list[tuple]:
    base = {
        "bank_ref_id": "b",
        "transaction_id": "t",
        "settled_amount_paise": 100,
        "settlement_date": "2025-04-02",
        "instrument_type": "UPI",
        "merchant_id": "m1",
        "fee_paise": 0,
        "gst_paise": 0,
        "settlement_id": "s",
        "utr": "u",
        "currency": "INR",
        "gross_amount_paise": 100,
        "provider": "generic",
    }
    out = []
    for spec in specs:
        row = {**base, **spec}
        out.append(tuple(row[h] for h in HEADER))
    return out


def _frame(spark, *specs: dict):
    return spark.createDataFrame(_rows(*specs), schema=HEADER)


def _one(spark, *specs: dict):
    """Fold, then return {transaction_id: (settled, date, bank_ref)}."""
    distinct, folded, partial = fold_settlements(_frame(spark, *specs))
    got = {
        r["transaction_id"]: (r["settled_amount_paise"], r["settlement_date"], r["bank_ref_id"])
        for r in folded.collect()
    }
    return got, partial, distinct.count()


# --- the defect ------------------------------------------------------------


def test_two_instalments_are_summed_not_discarded(spark):
    got, partial, _ = _one(
        spark,
        {
            "settled_amount_paise": 400,
            "settlement_date": "2025-04-02",
            "bank_ref_id": "b1",
            "settlement_id": "s1",
        },
        {
            "settled_amount_paise": 600,
            "settlement_date": "2025-04-03",
            "bank_ref_id": "b2",
            "settlement_id": "s2",
        },
    )
    # Before the fix this returned 600 and dropped 400.
    assert got["t"] == (1000, "2025-04-03", "b2")
    assert partial == 1


def test_a_split_payout_is_not_counted_as_a_duplicate(spark):
    _, partial, distinct_rows = _one(
        spark,
        {"settled_amount_paise": 400, "settlement_date": "2025-04-02", "bank_ref_id": "b1"},
        {"settled_amount_paise": 600, "settlement_date": "2025-04-03", "bank_ref_id": "b2"},
    )
    assert partial == 1
    assert distinct_rows == 2, "both instalments are distinct content, not redeliveries"


def test_three_instalments_sum(spark):
    got, partial, _ = _one(
        spark,
        {"settled_amount_paise": 200, "settlement_date": "2025-04-01", "bank_ref_id": "b1"},
        {"settled_amount_paise": 300, "settlement_date": "2025-04-02", "bank_ref_id": "b2"},
        {"settled_amount_paise": 500, "settlement_date": "2025-04-03", "bank_ref_id": "b3"},
    )
    assert got["t"] == (1000, "2025-04-03", "b3")
    assert partial == 1


# --- redeliveries still collapse -------------------------------------------


def test_a_byte_identical_redelivery_collapses_to_one(spark):
    got, partial, distinct_rows = _one(
        spark,
        {"settled_amount_paise": 900, "bank_ref_id": "b1", "settlement_id": "s1"},
        {"settled_amount_paise": 900, "bank_ref_id": "b1", "settlement_id": "s1"},
    )
    assert got["t"][0] == 900, "doubling a redelivery would overstate the settlement"
    assert partial == 0, "a redelivery is not an instalment"
    assert distinct_rows == 1


def test_a_redelivery_arriving_three_times_still_collapses(spark):
    row = {"settled_amount_paise": 900, "bank_ref_id": "b1", "settlement_id": "s1"}
    got, partial, distinct_rows = _one(spark, row, dict(row), dict(row))
    assert got["t"][0] == 900
    assert partial == 0
    assert distinct_rows == 1


def test_the_same_amount_on_a_different_date_is_an_instalment(spark):
    # Same bank reference, same amount, next day. A provider re-sending the
    # line changes neither of those, so content has to include the date.
    got, partial, _ = _one(
        spark,
        {"settled_amount_paise": 500, "settlement_date": "2025-04-02", "bank_ref_id": "b1"},
        {"settled_amount_paise": 500, "settlement_date": "2025-04-03", "bank_ref_id": "b1"},
    )
    assert got["t"][0] == 1000
    assert partial == 1


def test_the_same_amount_and_date_under_a_different_reference_is_an_instalment(spark):
    got, _, _ = _one(
        spark,
        {"settled_amount_paise": 500, "settlement_date": "2025-04-02", "bank_ref_id": "b1"},
        {"settled_amount_paise": 500, "settlement_date": "2025-04-02", "bank_ref_id": "b2"},
    )
    assert got["t"][0] == 1000


# --- which row survives the fold -------------------------------------------


def test_the_effective_date_is_the_last_instalment(spark):
    got, _, _ = _one(
        spark,
        {"settlement_date": "2025-04-09", "settled_amount_paise": 100, "bank_ref_id": "b9"},
        {"settlement_date": "2025-04-02", "settled_amount_paise": 100, "bank_ref_id": "b2"},
    )
    assert got["t"][1] == "2025-04-09"


def test_the_bank_reference_comes_from_the_completing_instalment(spark):
    # The MERGE writes bank_ref_id onto the webhook row, so it has to be the
    # reference of the instalment that completed the payout.
    got, _, _ = _one(
        spark,
        {"settlement_date": "2025-04-02", "bank_ref_id": "first"},
        {"settlement_date": "2025-04-05", "bank_ref_id": "completing"},
    )
    assert got["t"][2] == "completing"


def test_a_date_tie_breaks_on_the_bank_reference(spark):
    got, _, _ = _one(
        spark,
        {"settlement_date": "2025-04-02", "bank_ref_id": "b_a", "settled_amount_paise": 100},
        {"settlement_date": "2025-04-02", "bank_ref_id": "b_b", "settled_amount_paise": 100},
    )
    assert got["t"] == (200, "2025-04-02", "b_b"), "a tie must not resolve by chance"


def test_fees_and_gst_accumulate_across_instalments(spark):
    _distinct, folded, _ = fold_settlements(
        _frame(
            spark,
            {"fee_paise": 10, "gst_paise": 2, "bank_ref_id": "b1", "settled_amount_paise": 1},
            {"fee_paise": 20, "gst_paise": 4, "bank_ref_id": "b2", "settled_amount_paise": 1},
        )
    )
    row = folded.collect()[0]
    assert row["fee_paise"] == 30
    assert row["gst_paise"] == 6


def test_a_null_merchant_on_one_instalment_does_not_erase_the_other(spark):
    _distinct, folded, _ = fold_settlements(
        _frame(
            spark,
            {"merchant_id": None, "bank_ref_id": "b1", "settled_amount_paise": 1},
            {"merchant_id": "m9", "bank_ref_id": "b2", "settled_amount_paise": 1},
        )
    )
    # The MERGE coalesces this to UNKNOWN, which would drop the negotiated rate.
    assert folded.collect()[0]["merchant_id"] == "m9"


def test_a_null_currency_on_one_instalment_does_not_erase_the_other(spark):
    _distinct, folded, _ = fold_settlements(
        _frame(
            spark,
            {"currency": None, "bank_ref_id": "b1", "settled_amount_paise": 1},
            {"currency": "INR", "bank_ref_id": "b2", "settled_amount_paise": 1},
        )
    )
    assert folded.collect()[0]["currency"] == "INR"


def test_the_provider_survives_so_the_lag_window_still_applies(spark):
    _distinct, folded, _ = fold_settlements(
        _frame(
            spark,
            {"provider": None, "bank_ref_id": "b1", "settled_amount_paise": 1},
            {"provider": "razorpay", "bank_ref_id": "b2", "settled_amount_paise": 1},
        )
    )
    assert folded.collect()[0]["provider"] == "razorpay"


# --- shape -----------------------------------------------------------------


def test_the_folded_frame_keeps_the_columns_the_merge_projects(spark):
    _, folded, _ = fold_settlements(_frame(spark, {}))
    cols = set(folded.columns)
    assert {
        "transaction_id",
        "bank_ref_id",
        "settled_amount_paise",
        "instrument_type",
        "merchant_id",
        "settlement_date",
        "currency",
        "provider",
    } <= cols


def test_the_helper_count_column_does_not_leak_into_the_view(spark):
    # installment_rows exists to count partials; it is not part of the contract.
    _, folded, _ = fold_settlements(_frame(spark, {}))
    assert "installment_rows" not in folded.columns


def test_transactions_are_kept_separate(spark):
    got, partial, _ = _one(
        spark,
        {"transaction_id": "t1", "settled_amount_paise": 100},
        {"transaction_id": "t2", "settled_amount_paise": 300},
    )
    assert got == {
        "t1": (100, "2025-04-02", "b"),
        "t2": (300, "2025-04-02", "b"),
    }
    assert partial == 0


def test_a_single_row_passes_through_unchanged(spark):
    got, partial, distinct_rows = _one(spark, {"settled_amount_paise": 742})
    assert got["t"] == (742, "2025-04-02", "b")
    assert partial == 0
    assert distinct_rows == 1


def test_two_transactions_each_split_count_two_partials(spark):
    _distinct, folded, partial = fold_settlements(
        _frame(
            spark,
            {"transaction_id": "t1", "settled_amount_paise": 100, "bank_ref_id": "a1"},
            {"transaction_id": "t1", "settled_amount_paise": 200, "bank_ref_id": "a2"},
            {"transaction_id": "t2", "settled_amount_paise": 300, "bank_ref_id": "b1"},
            {"transaction_id": "t2", "settled_amount_paise": 400, "bank_ref_id": "b2"},
        )
    )
    assert partial == 2
    assert folded.count() == 2


# --- the counters on the batch record --------------------------------------


def test_duplicate_settlement_rows_means_redeliveries_not_installments():
    import inspect

    from src.processing import reconcile as rc

    src = inspect.getsource(rc.run_reconciliation)
    assert "settlement_rows_total - settlement_rows_distinct" in src
    assert "partial_settlement_txns" in src


def test_neither_partial_counter_is_batch_prefixed():
    # check_batch_drift sums every batch_ int against settlement_rows_evaluated.
    # A partial payout is a normal thing for a merchant to do, not drift.
    import inspect

    from src.processing import reconcile as rc

    src = inspect.getsource(rc.run_reconciliation)
    assert "batch_partial_settlement_txns" not in src
    assert 'counts["batch_EXCEPTION_DUPLICATE_SETTLEMENT"]' in src
