"""The two legs of a reconciliation must agree on the unit before we subtract.

The webhook leg used to carry no currency at all and the settlement leg carried
one that was validated and then thrown away, so the MERGE joined on
transaction_id alone. A settlement denominated in USD matched an INR webhook and
the fee tolerance was applied across two currencies without complaint.

These tests pin the four pieces that make a cross-currency match visible:
the Avro field, the table column, the settlement-side normalisation, and the
arm ordering in the MERGE.
"""

from __future__ import annotations

import inspect

import pandas as pd
import pytest

from src.adapters.settlement import _currency
from src.common.schemas import (
    EXCEPTION_CURRENCY_MISMATCH,
    TERMINAL_STATUSES,
    WEBHOOK_AVRO_SCHEMA_DICT,
)
from src.ingestion import ingest_webhooks as iw
from src.processing import reconcile as rc

_MERGE_SRC = inspect.getsource(rc.run_reconciliation)


def _avro_field(name: str) -> dict:
    return next(f for f in WEBHOOK_AVRO_SCHEMA_DICT["fields"] if f["name"] == name)


# --- the wire format -------------------------------------------------------


def test_the_webhook_wire_format_carries_a_currency():
    assert "currency" in [f["name"] for f in WEBHOOK_AVRO_SCHEMA_DICT["fields"]]


def test_currency_is_optional_so_an_old_producer_keeps_working():
    # A gateway that never sends one must not break the stream. That is what
    # makes "unknown" a representable value rather than a default.
    f = _avro_field("currency")
    assert "null" in f["type"]
    assert f.get("default") is None


def test_currency_is_added_last_so_existing_field_order_is_untouched():
    names = [f["name"] for f in WEBHOOK_AVRO_SCHEMA_DICT["fields"]]
    assert names[-1] == "currency"


def test_the_webhook_table_has_the_column():
    assert "currency" in iw._WEBHOOK_COLS


def test_the_table_ddl_declares_and_backfills_currency():
    spark = _MagicSpark()
    iw.ensure_webhook_table(spark, "nessie.db.webhooks")
    stmts = [c[0][0] for c in spark.sql.call_args_list]
    create = next(s for s in stmts if s.lstrip().startswith("CREATE TABLE"))
    alters = "\n".join(s for s in stmts if s.lstrip().startswith("ALTER TABLE"))
    assert "currency string" in create
    assert "currency string" in alters


# --- the settlement side ---------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("INR", "INR"),
        ("inr", "INR"),
        ("  inr  ", "INR"),
        ("usd", "USD"),
        ("USD", "USD"),
    ],
)
def test_a_declared_currency_is_normalised_not_guessed(raw, expected):
    df = pd.DataFrame({"currency": [raw]})
    assert _currency(df, {"currency": "currency"}).iloc[0] == expected


@pytest.mark.parametrize("missing", ["", "   ", None, float("nan"), "NaN", "None", "<NA>"])
def test_an_undeclared_currency_stays_undeclared(missing):
    # The defect: this used to become "INR", which made the settlement schema's
    # isin(["INR"]) check unable to ever fire on a blank value.
    df = pd.DataFrame({"currency": [missing]})
    assert pd.isna(_currency(df, {"currency": "currency"}).iloc[0])


def test_a_missing_currency_column_is_undeclared_not_inr():
    df = pd.DataFrame({"settled_amount_paise": [100]})
    assert pd.isna(_currency(df, {}).iloc[0])


def test_only_the_declared_rows_keep_a_value():
    df = pd.DataFrame({"currency": ["INR", "", "USD"]})
    out = _currency(df, {"currency": "currency"})
    assert out.iloc[0] == "INR"
    assert pd.isna(out.iloc[1])
    assert out.iloc[2] == "USD"


# --- the merge -------------------------------------------------------------


def test_the_merge_projects_the_settlement_currency():
    assert "s.currency" in _MERGE_SRC


def test_a_conflicting_currency_gets_its_own_status():
    assert EXCEPTION_CURRENCY_MISMATCH in _MERGE_SRC


def test_the_conflict_test_needs_both_sides_declared():
    # One side unknown is not a conflict, it is an absence of evidence. The
    # settlement_currency_missing_rows counter covers that case instead. The
    # predicate itself lives in currency_mismatch_guard, which both the MERGE
    # arm and the decision SELECT call.
    guard = rc.currency_mismatch_guard("'MATCHED'")
    assert "t.currency IS NOT NULL" in guard
    assert "s.currency IS NOT NULL" in guard
    assert "t.currency <> s.currency" in guard
    assert "currency_mismatch_guard(" in _arm(EXCEPTION_CURRENCY_MISMATCH)


def test_a_currency_conflict_is_not_terminal():
    # Terminal rows are frozen by the NOT IN guard, so marking this one
    # terminal would mean a corrected settlement could never heal it.
    assert EXCEPTION_CURRENCY_MISMATCH not in TERMINAL_STATUSES


def test_the_currency_arm_precedes_the_tolerance_case():
    # First matching clause wins. If the tolerance arm ran first a USD
    # settlement would be priced as rupees and marked MATCHED. The verdict
    # expression itself moved into match_decision_sql, so the order to check is
    # the order the two are called in.
    assert _MERGE_SRC.index("currency_mismatch_guard(") < _MERGE_SRC.index("match_decision_sql(")


def test_the_currency_arm_carries_the_same_terminal_guard_as_the_tolerance_arm():
    # Both guards come from non_terminal_guard, so the currency arm cannot drift
    # away from the tolerance arm by copying an edited literal: it is handed the
    # same argument. Check that it is the same argument.
    assert "non_terminal_guard" in rc.currency_mismatch_guard.__code__.co_names
    assert _arm(EXCEPTION_CURRENCY_MISMATCH).count("_terminal_not_in") == 1
    tolerance_arm = next(b for b in _MERGE_SRC.split("WHEN MATCHED") if "match_decision_sql(" in b)
    assert "_terminal_not_in" in tolerance_arm, "the tolerance arm stopped using the shared guard"
    assert "NOT IN" in rc.currency_mismatch_guard("'MATCHED'")


def test_a_new_placeholder_row_records_the_settlement_currency():
    # No webhook to compare against, but the next batch will have one, so the
    # declared currency has to survive onto the row we insert.
    insert = _MERGE_SRC.split("WHEN NOT MATCHED")[1]
    assert "currency)" in insert
    assert "s.currency" in insert


def test_undeclared_currency_is_counted_not_assumed():
    assert "settlement_currency_missing_rows" in inspect.getsource(rc.run_reconciliation)


def test_the_missing_currency_counter_is_not_batch_prefixed():
    # check_batch_drift sums every batch_-prefixed int and compares it with the
    # evaluated row count. A data-quality signal there would read as batch drift.
    assert "batch_settlement_currency_missing_rows" not in _MERGE_SRC


# --- helpers ---------------------------------------------------------------


def _arm(status: str) -> str:
    """The MERGE block of the WHEN MATCHED arm that produces a status.

    The MERGE is built by an f-string, so inspect.getsource shows the
    interpolation rather than the value it resolves to.
    """
    marker = f"{{{status}}}"
    assert marker in _MERGE_SRC, f"{status} is not interpolated into the MERGE"
    blocks = _MERGE_SRC.split("WHEN MATCHED")
    owning = [b for b in blocks if marker in b]
    assert len(owning) == 1, f"{status} should appear in exactly one arm, got {len(owning)}"
    return owning[0]


class _MagicSpark:
    def __init__(self) -> None:
        from unittest.mock import MagicMock

        self.sql = MagicMock()
        self.table = MagicMock()
        self.table.return_value.columns = []
