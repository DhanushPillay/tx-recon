"""A fee applied off a rate nobody agreed is a wrong fee, and it looks clean.

The fee CASE has four branches that fall through to a default instead of
failing: a date that does not parse, a date older than the earliest rate card,
a merchant with no negotiated rate, and an instrument outside the configured
vocabulary. Every one of those produced a fee and a normal-looking row, so the
batch reported green while the amount had been priced off a guess.

These tests pin the classifier, the four counters, and the opt-in refusal.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import (
    FEE_FALLBACK_COUNT_KEYS,
    earliest_rate_card_date,
    fee_fallback_sql,
    fold_fallback_rows,
    known_fee_merchants,
)
from tests.processing.test_reconcile import _run_with_collects

_ALL_FOUR = sorted(FEE_FALLBACK_COUNT_KEYS.values())


@pytest.fixture
def engine() -> FeeEngine:
    return FeeEngine()


# --- the classifier --------------------------------------------------------


def test_a_well_formed_row_is_not_flagged(engine):
    sql = fee_fallback_sql(engine)
    assert sql.rstrip().endswith("ELSE NULL END")
    assert "2024-01-01" in sql, "the earliest rate card date has to be in the comparison"


def test_an_unparseable_date_is_its_own_reason(engine):
    # to_date(..., 'yyyy-MM-dd') IS NULL is the same predicate _versioned_wrap
    # uses to pick the newest card, so the two cannot disagree.
    sql = fee_fallback_sql(engine)
    assert sql.startswith("CASE WHEN to_date(settlement_date, 'yyyy-MM-dd') IS NULL")
    assert sql.index("card_unparseable_date") < sql.index("card_out_of_range")


def test_an_out_of_range_date_is_named_against_the_earliest_card(engine):
    first = earliest_rate_card_date(engine)
    assert first == "2024-01-01"
    assert f"< DATE '{first}'" in fee_fallback_sql(engine)


def test_a_pre_2024_settlement_is_classified_out_of_range_not_silently_priced():
    # The checked-in sample data is dated 2010, so every row in the shipped
    # 10k batch takes this branch today.
    assert earliest_rate_card_date(FeeEngine()) > "2010-01-01"


def test_a_known_merchant_is_not_flagged(engine):
    known = known_fee_merchants(engine)
    assert "merch_001" in known
    for m in known:
        assert f"'{m}'" in fee_fallback_sql(engine)


def test_an_unknown_merchant_is_flagged(engine):
    sql = fee_fallback_sql(engine)
    assert "merchant_unknown" in sql
    assert "NOT IN (" in sql


def test_the_merchant_check_ignores_an_unknown_marker(engine):
    # Coalesce-to-UNKNOWN is what the MERGE does; a row that never named a
    # merchant is a gap in the data, not an unpriced merchant.
    assert "<> 'UNKNOWN'" in fee_fallback_sql(engine)


def test_the_instrument_vocabulary_is_the_contract_one(engine):
    sql = fee_fallback_sql(engine)
    for inst in ("UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"):
        assert f"'{inst}'" in sql


def test_a_config_with_no_merchants_still_produces_valid_sql(engine):
    # NOT IN () is a parse error, so an empty list has to be handled.
    bare = MagicMock(rate_cards=[{"effective_from": "2024-01-01", "merchants": {}}])
    assert "''" in fee_fallback_sql(bare)
    assert earliest_rate_card_date(bare) == "2024-01-01"


def test_a_config_with_no_cards_falls_back_to_the_epoch(engine):
    bare = MagicMock(rate_cards=[])
    assert known_fee_merchants(bare) == set()
    assert earliest_rate_card_date(bare) == "1970-01-01"


def test_merchant_ids_are_escaped_not_interpolated_raw():
    nasty = MagicMock(
        rate_cards=[{"effective_from": "2024-01-01", "merchants": {"a'b": {}}}],
    )
    assert "'a''b'" in fee_fallback_sql(nasty)


# --- the counters ----------------------------------------------------------


def test_all_four_counters_are_always_present():
    # A counter that only shows up when it is non-zero cannot be alerted on.
    folded = fold_fallback_rows([])
    assert sorted(folded) == _ALL_FOUR
    assert set(folded.values()) == {0}


def test_a_reason_maps_to_its_own_counter():
    folded = fold_fallback_rows([{"reason": "card_out_of_range", "n": 7}])
    assert folded["fee_card_out_of_range_rows"] == 7
    assert folded["fee_merchant_unknown_rows"] == 0


def test_an_unrecognised_reason_is_ignored_not_counted():
    assert fold_fallback_rows([{"reason": "something_new", "n": 3}]) == {k: 0 for k in _ALL_FOUR}


def test_a_null_count_reads_zero():
    folded = fold_fallback_rows([{"reason": "merchant_unknown", "n": None}])
    assert folded["fee_merchant_unknown_rows"] == 0


def test_every_reason_can_be_counted_at_once():
    rows = [{"reason": r, "n": i + 1} for i, r in enumerate(sorted(FEE_FALLBACK_COUNT_KEYS))]
    folded = fold_fallback_rows(rows)
    assert all(v > 0 for v in folded.values())


def test_the_counters_reach_the_batch_record():
    import inspect

    from src.processing import reconcile as rc

    assert "counts.update(fee_fallbacks)" in inspect.getsource(rc.run_reconciliation)


def test_no_counter_is_batch_prefixed():
    # check_batch_drift sums every batch_-prefixed int and compares the total
    # with settlement_rows_evaluated. Four pricing signals landing there would
    # read as batch drift.
    assert not any(k.startswith("batch_") for k in FEE_FALLBACK_COUNT_KEYS.values())


# --- the opt-in refusal ----------------------------------------------------


def test_a_clean_batch_reports_zero_for_all_four():
    counts, spark = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
    )
    assert {k: counts[k] for k in _ALL_FOUR} == dict.fromkeys(_ALL_FOUR, 0)


def test_a_fallback_is_counted_and_the_batch_still_publishes(monkeypatch):
    monkeypatch.delenv("STRICT_FEE_RATES", raising=False)
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
        fallback_rows=[{"reason": "card_out_of_range", "n": 4}],
    )
    assert counts["fee_card_out_of_range_rows"] == 4
    assert counts["fee_merchant_unknown_rows"] == 0


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", " 1 "])
def test_strict_fee_rates_refuses_the_batch(monkeypatch, value):
    monkeypatch.setenv("STRICT_FEE_RATES", value)
    with pytest.raises(RuntimeError, match="STRICT_FEE_RATES=1"):
        _run_with_collects(
            batch_collect=[{"st": "MATCHED", "n": 4}],
            table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
            fallback_rows=[{"reason": "merchant_unknown", "n": 2}],
        )


@pytest.mark.parametrize("value", ["0", "false", "no", "", "off"])
def test_the_flag_is_off_by_default(monkeypatch, value):
    monkeypatch.setenv("STRICT_FEE_RATES", value)
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
        fallback_rows=[{"reason": "merchant_unknown", "n": 2}],
    )
    assert counts["fee_merchant_unknown_rows"] == 2


def test_the_refusal_happens_before_the_merge(monkeypatch):
    # Refusing after the MERGE would leave the work half done and the webhook
    # table mutated.
    monkeypatch.setenv("STRICT_FEE_RATES", "1")
    sqls: list[str] = []
    with pytest.raises(RuntimeError, match="STRICT_FEE_RATES=1"):
        _run_with_collects(
            batch_collect=[{"st": "MATCHED", "n": 4}],
            table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
            fallback_rows=[{"reason": "card_out_of_range", "n": 1}],
            sql_log=sqls,
        )
    assert any("AS reason FROM bank_settlements" in s for s in sqls), "the scan did run"
    assert not any("MERGE INTO" in s for s in sqls)
