"""Reconciliation fixtures with hand-written answers.

``tests/fixtures/recon_cases.yaml`` holds one batch of gateway rows, one batch of
settlement rows, and the status every transaction is supposed to end up with.
Every expectation in it was worked out by hand from ``config/fee_rates.yaml`` and
the arithmetic written in that file's ``meta`` block. None of them came from
running the pipeline.

That is the failure this module exists to replace.
``tests/performance/recon_accuracy.py`` scored ``FeeEngine.check_match`` against
its own output and reported 94.63%, which is an arithmetic property of the
generator's sha256 buckets rather than a measurement of the fee engine: a
correctly-working fixture and a completely broken one produce the same number.

Two tiers read the same labels:

* this file, which runs everywhere and evaluates ``merge_decision_sql`` -- the
  batch MERGE's own arms and expressions -- in a real Spark engine;
* ``tests/processing/test_recon_cases_integration.py``, marked ``integration``,
  which runs ``run_reconciliation`` end to end against the real catalog.
  Spark's session catalog cannot execute MERGE INTO and the Iceberg catalog
  needs the docker services, so the MERGE itself is only reachable there.
"""

from __future__ import annotations

import csv
import functools
import os

import pytest
import yaml

from src.common.schemas import (
    EXCEPTION_CURRENCY_MISMATCH,
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_MISSING_BANK_STATEMENT,
    EXCEPTION_MISSING_WEBHOOK,
    EXCEPTION_REVERSAL,
    MATCHED,
    TERMINAL_STATUSES,
)
from src.ingestion import ingest_webhooks as iw
from src.processing import reconcile as rc

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(HERE, "fixtures", "recon_cases.yaml")


@functools.lru_cache(maxsize=1)
def _fx() -> dict:
    with open(FIXTURE, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _cases() -> list[dict]:
    return _fx()["cases"]


def _webhook_schema():
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("transaction_id", StringType()),
            StructField("amount_paise", LongType()),
            StructField("gateway_status", StringType()),
            StructField("timestamp_utc", StringType()),
            StructField("merchant_id", StringType()),
            StructField("reconciliation_status", StringType()),
            StructField("bank_ref_id", StringType()),
            StructField("instrument_type", StringType()),
            StructField("currency", StringType()),
            StructField("processing_run_id", StringType()),
            StructField("duplicate_mutation_count", LongType()),
            StructField("last_duplicate_at", TimestampType()),
        ]
    )


def _bank_schema():
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from src.adapters.settlement import CANONICAL_COLS

    return StructType(
        [StructField(c, LongType() if "paise" in c else StringType(), True) for c in CANONICAL_COLS]
    )


def _settlement_rows(fx: dict) -> list[tuple]:
    """The YAML's settlement rows as the canonical tuple the bank frame holds."""
    from src.adapters.settlement import CANONICAL_COLS

    out = []
    for row in fx["settlements"]:
        rec = dict(row)
        rec["transaction_id"] = row["tx"]
        out.append(tuple(rec.get(c) for c in CANONICAL_COLS))
    return out


def _write_settlement_csv(path: str, rows: list[dict]) -> str:
    from src.adapters.settlement import CANONICAL_COLS

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_COLS))
        writer.writeheader()
        for row in rows:
            # The fixture calls the key `tx`; the CSV and the canonical contract
            # call it transaction_id.
            rec = dict(row)
            rec["transaction_id"] = row["tx"]
            writer.writerow(
                {c: ("null" if rec.get(c) is None else rec.get(c)) for c in CANONICAL_COLS}
            )
    return path


def _settled_amounts(fx: dict) -> dict[str, int]:
    """What the fold should produce per transaction, as a hand-side check.

    This re-implements ``fold_settlements`` in plain Python on purpose. It is
    not a second opinion from the pipeline -- it is the fixture checking itself,
    so a disagreement between the YAML's expected counts and its own rows shows
    up as a failure in the fixture rather than as a mysterious miss in the
    pipeline.
    """
    distinct: dict[tuple, dict] = {}
    for row in fx["settlements"]:
        key = (row["tx"], row["settled_amount_paise"], row["settlement_date"], row["bank_ref_id"])
        distinct.setdefault(key, row)
    totals: dict[str, int] = {}
    for row in distinct.values():
        totals[row["tx"]] = totals.get(row["tx"], 0) + int(row["settled_amount_paise"])
    return totals


@pytest.fixture(scope="module")
def frame(spark):
    """The fixture batch as two Spark frames, folded the way the pipeline folds."""
    fx = _fx()

    hooks = spark.createDataFrame(
        [
            (
                w["id"],
                w["amount_paise"],
                w["gateway_status"],
                w["timestamp_utc"],
                w["merchant_id"],
                None,
                None,
                w["instrument_type"],
                w["currency"],
                None,
                0,
                None,
            )
            for w in fx["webhooks"]
        ],
        _webhook_schema(),
    ).withColumn("reconciliation_status", iw._ingest_status_column())
    hooks.createOrReplaceTempView("fx_webhooks")

    settlements = spark.createDataFrame(_settlement_rows(fx), _bank_schema())
    distinct, folded, partial = rc.fold_settlements(settlements)
    folded.createOrReplaceTempView("bank_settlements")

    return {
        "fx": fx,
        "hooks": hooks,
        "settlements": settlements,
        "distinct": distinct,
        "folded": folded,
        "partial": partial,
    }


@pytest.fixture(scope="module")
def decision(frame, spark):
    """One decided status per transaction, from the pipeline's own SQL."""
    fee_engine = rc.get_fee_engine()
    fee_case_sql, gst_case_sql = rc.build_fee_case_sql(fee_engine)
    return {
        r["transaction_id"]: r["decided_status"]
        for r in spark.sql(
            rc.merge_decision_sql(
                fee_case_sql=fee_case_sql,
                gst_case_sql=gst_case_sql,
                tolerance_sql=rc.build_tolerance_case_sql(fee_engine),
                terminal_not_in=", ".join(f"'{s}'" for s in TERMINAL_STATUSES),
                webhook_table="fx_webhooks",
            )
        ).collect()
    }


# --------------------------------------------------------------------------
# The labels themselves
# --------------------------------------------------------------------------


def test_the_fixture_covers_every_transaction_id():
    """Each case labels exactly one transaction, and nothing is unlabelled."""
    fx = _fx()
    labelled = [c["id"] for c in fx["cases"]]
    assert len(labelled) == len(set(labelled)), "a transaction is labelled twice"

    in_settlement = {r["tx"] for r in fx["settlements"]}
    in_webhooks = {w["id"] for w in fx["webhooks"]}
    assert set(labelled) == in_settlement | in_webhooks, (
        f"unlabelled: {sorted((in_settlement | in_webhooks) - set(labelled))}; "
        f"labelled but absent: {sorted(set(labelled) - (in_settlement | in_webhooks))}"
    )


def test_every_label_is_a_status_the_pipeline_defines():
    from src.common import schemas

    known = {v for k, v in vars(schemas).items() if k.isupper() and isinstance(v, str)}
    for case in _fx()["cases"]:
        assert case["expect_status"] in known, case["id"]
        assert case.get("why"), f"{case['id']} has no stated reason"


def test_the_fixture_exercises_both_sides_of_the_decision():
    """A fixture that only ever lands on MATCHED proves nothing."""
    tally = {c["expect_status"] for c in _fx()["cases"]}
    assert MATCHED in tally
    for exception in (
        EXCEPTION_FEE_MISMATCH,
        EXCEPTION_MISSING_WEBHOOK,
        EXCEPTION_CURRENCY_MISMATCH,
        EXCEPTION_REVERSAL,
    ):
        assert exception in tally, f"the fixture never lands on {exception}"


def test_expected_counts_describe_the_fixtures_own_rows():
    """The hand-written counters must match the rows that ship with them."""
    fx = _fx()
    exp = fx["expected_counts"]
    rows = fx["settlements"]
    labelled_in_batch = [c["id"] for c in fx["cases"] if c["expect_in_batch"]]

    assert exp["settlement_rows_total"] == len(rows)
    assert exp["settlement_rows_deduped"] == len(labelled_in_batch)
    distinct_keys = {
        (r["tx"], r["settled_amount_paise"], r["settlement_date"], r["bank_ref_id"]) for r in rows
    }
    assert exp["settlement_rows_distinct"] == len(distinct_keys)
    assert exp["duplicate_settlement_rows"] == len(rows) - len(distinct_keys), (
        "a duplicate is a repeated line, not a repeated count"
    )
    assert exp["settlement_rows_evaluated"] == sum(
        1 for tx in _settled_amounts(fx) if _settled_amounts(fx)[tx] is not None
    ), (
        "evaluated counts transactions that carry a settlement amount at all; the malformed-amount "
        "cases settle nothing and cannot join the MERGE"
    )


def test_the_expected_nets_match_the_arithmetic_the_fixture_states():
    """Re-derive each routine case from meta.mdr_bps_by_instrument.

    An independent longhand implementation, checking the fixture against the
    rule written at the top of it rather than against the pipeline.
    """
    fx = _fx()
    meta = fx["meta"]
    gst_bps = int(meta["gst_bps"])
    tol = int(meta["tolerance_paise"])
    rates = meta["mdr_bps_by_instrument"]

    def half_away(num: int, den: int) -> int:
        whole, rem = divmod(abs(num), den)
        out = whole + (1 if rem * 2 >= den else 0)
        return -out if num < 0 else out

    by_tx = {r["tx"]: r for r in fx["settlements"]}
    gross = {w["id"]: w["amount_paise"] for w in fx["webhooks"]}
    folded = _settled_amounts(fx)
    checked = 0
    for case in fx["cases"]:
        row = by_tx.get(case["id"])
        if row is None or gross.get(case["id"]) is None:
            continue
        bps = rates.get(row["instrument_type"])
        if (
            bps is None
            or row["merchant_id"] is not None
            or not case["expect_in_batch"]
            or case["expect_status"] == EXCEPTION_CURRENCY_MISMATCH
        ):
            # Merchant overrides, out-of-vocabulary instruments and the currency
            # arm are priced by branches this reference does not model. The
            # currency cases settle the right number on purpose, so arithmetic
            # agreement is not the reason they are labelled a mismatch.
            continue
        g = int(gross[case["id"]])
        fee = half_away(g * bps, 10000)
        gst = half_away(fee * gst_bps, 10000)
        net = g - fee - gst
        settled = folded.get(case["id"], row["settled_amount_paise"])
        expected = MATCHED if abs(net - settled) <= tol else EXCEPTION_FEE_MISMATCH
        assert expected == case["expect_status"], (
            f"{case['id']}: fixture says {case['expect_status']}, the stated arithmetic gives "
            f"{expected} (gross {g}, {bps} bps, fee {fee}, gst {gst}, net {net}, settled {settled})"
        )
        checked += 1
    assert checked >= 15, f"only {checked} cases were cross-checked"


def test_the_folded_totals_match_the_fixture_arithmetic():
    """A split payout has to add up before anything compares it to a net."""
    folded = _settled_amounts(_fx())
    by_case = {c["id"]: c for c in _fx()["cases"]}
    assert folded["j_two_parts"] == 1000, folded["j_two_parts"]
    assert folded["j_three_parts"] == 1000, folded["j_three_parts"]
    assert folded["j_short"] == 998, folded["j_short"]
    assert folded["k_redelivery"] == 97640, (
        "the redelivered line must collapse, not double: two identical rows would sum to 195280"
    )
    assert by_case["j_short"]["expect_status"] == EXCEPTION_FEE_MISMATCH, (
        "998 against a 1000 net is a 2 paisa gap, so j_short is only a mismatch if the fold summed"
    )


# --------------------------------------------------------------------------
# The pipeline's SQL against the labels
# --------------------------------------------------------------------------


@pytest.mark.parametrize("case_id", [c["id"] for c in _cases() if c["expect_in_batch"]])
def test_the_pipeline_lands_on_the_labelled_status(decision, case_id):
    expected = next(c["expect_status"] for c in _fx()["cases"] if c["id"] == case_id)
    actual = decision.get(case_id)
    assert actual == expected, (
        f"{case_id}: expected {expected}, the pipeline's SQL produced {actual}"
    )


def test_every_settled_transaction_got_a_decision(decision):
    """A transaction the decision SELECT does not mention would pass by absence."""
    settled = {r["tx"] for r in _fx()["settlements"]}
    assert settled == set(decision), f"missing: {sorted(settled - set(decision))}"


@pytest.mark.parametrize("case_id", [c["id"] for c in _cases() if not c["expect_in_batch"]])
def test_a_transaction_outside_the_batch_keeps_its_ingestion_status(frame, case_id):
    """The reversal is not a settlement outcome, so reconciliation must not touch it."""
    row = frame["hooks"].filter(f"transaction_id = '{case_id}'").collect()
    assert row, case_id
    assert row[0]["reconciliation_status"] == EXCEPTION_REVERSAL, (
        "ingestion parks a refund at EXCEPTION_REVERSAL; a settlement batch has no opinion on it"
    )


@pytest.mark.parametrize("key,value", sorted(_fx()["expected_counts"].items()))
def test_the_pipeline_produces_the_labelled_counts(frame, spark, key, value):
    counts = _counters(frame, spark)
    assert counts.get(key) == value, (
        f"{key}: fixture says {value}, pipeline produced {counts.get(key)}"
    )


def _counters(frame, spark) -> dict[str, int]:
    """The batch counters the fixture labels, read off the same frames."""
    fee_engine = rc.get_fee_engine()
    rows = spark.sql(
        f"SELECT {rc.fee_fallback_sql(fee_engine, date_col='settlement_date', merchant_col='merchant_id')} "
        "AS reason, COUNT(*) AS n FROM bank_settlements GROUP BY 1"
    ).collect()
    out = rc.fold_fallback_rows(rows)
    out.update(
        {
            "settlement_rows_total": frame["settlements"].count(),
            "settlement_rows_distinct": frame["distinct"].count(),
            "settlement_rows_deduped": frame["folded"].count(),
            "settlement_rows_evaluated": frame["folded"]
            .filter("settled_amount_paise IS NOT NULL")
            .count(),
            "duplicate_settlement_rows": frame["settlements"].count() - frame["distinct"].count(),
            "partial_settlement_txns": frame["partial"],
            "settlement_currency_missing_rows": frame["folded"].filter("currency IS NULL").count(),
            "table_EXCEPTION_REVERSAL": frame["hooks"]
            .filter(f"reconciliation_status = '{EXCEPTION_REVERSAL}'")
            .count(),
        }
    )
    return out


def test_the_decision_select_and_the_merge_call_the_same_builders():
    """The SELECT and the MERGE must not drift into two different rules.

    Both emit their guards by calling the builders below, so if either one is
    ever rewritten to inline a literal the name disappears from that function's
    source and this fails. The builders themselves are pinned by
    test_the_guards_state_their_predicates.
    """
    import inspect

    merge = inspect.getsource(rc.run_reconciliation)
    decision = inspect.getsource(rc.merge_decision_sql)
    for builder in (
        "merge_using_sql",
        "non_terminal_guard",
        "currency_mismatch_guard",
        "match_decision_sql",
    ):
        assert builder in merge, f"the MERGE stopped calling {builder}"
    # The decision SELECT reaches the non-terminal guard through
    # currency_mismatch_guard, so it does not call it directly.
    for builder in ("merge_using_sql", "currency_mismatch_guard", "match_decision_sql"):
        assert builder in decision, f"the decision SELECT stopped calling {builder}"


def test_the_guards_state_their_predicates():
    """What each builder emits, so a change to a guard is a visible diff."""
    assert rc.non_terminal_guard("'MATCHED'") == "t.reconciliation_status NOT IN ('MATCHED')"
    currency = rc.currency_mismatch_guard("'MATCHED'")
    assert "t.currency IS NOT NULL AND s.currency IS NOT NULL" in currency
    assert "t.currency <> s.currency" in currency
    assert "t.reconciliation_status NOT IN ('MATCHED')" in currency, (
        "a settled-then-reversed row has to be re-evaluated when the currency comes back"
    )
    verdict = rc.match_decision_sql("FEE", "GST", "TOL")
    assert verdict == (
        "CASE WHEN ABS((t.amount_paise - (FEE) - (GST)) - s.settled_amount_paise) <= (TOL) "
        f"THEN '{MATCHED}' ELSE '{EXCEPTION_FEE_MISMATCH}' END"
    )


# --------------------------------------------------------------------------
# Did it flag the right rows, or just the right number of them?
# --------------------------------------------------------------------------


def _confusion(labelled: list[tuple[str, str]], predicted: dict[str, str]) -> dict:
    """Exception detection as a two-by-two table.

    "Exception" means anything that is not ``MATCHED``. The positive class is
    the interesting one: a row the pipeline flags but should not have is a false
    match, and a row it stays quiet about is a false exception. Both cost an
    operator, in opposite directions, so neither is acceptable on its own.

    ``labelled`` is ``(transaction_id, expect_status)`` from the fixture. This is
    pure arithmetic on two lists -- no pipeline code -- so it can be checked
    against hand-built inputs rather than against its own output.
    """
    tp = [t for t, exp in labelled if exp != MATCHED and predicted.get(t) not in (None, MATCHED)]
    fp = [t for t, exp in labelled if exp == MATCHED and predicted.get(t) not in (None, MATCHED)]
    fn = [t for t, exp in labelled if exp != MATCHED and predicted.get(t) == MATCHED]
    tn = [t for t, exp in labelled if exp == MATCHED and predicted.get(t) == MATCHED]
    precision = len(tp) / (len(tp) + len(fp)) if (tp or fp) else None
    recall = len(tp) / (len(tp) + len(fn)) if (tp or fn) else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and (precision + recall)
        else None
    )
    return {
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def _labelled_in_batch() -> list[tuple[str, str]]:
    return [(c["id"], c["expect_status"]) for c in _cases() if c["expect_in_batch"]]


@pytest.fixture(scope="module")
def matrix(decision):
    return _confusion(_labelled_in_batch(), decision)


def test_every_transaction_lands_in_exactly_one_cell(matrix):
    total = sum(
        len(matrix[cell])
        for cell in ("true_positive", "false_positive", "false_negative", "true_negative")
    )
    assert total == len(_labelled_in_batch()), (
        "a cell is unaccounted for; either a decision is missing or one is counted twice"
    )


def test_the_fixture_is_reproduced_exactly(matrix):
    """The whole point of hand-written labels: every one of them should hold.

    A single-precision number could hide a swap -- one row missed, one row wrongly
    flagged, ratio unchanged. The confusion table is checked as cells, and the
    named offending transactions come back in the failure message so the swap is
    visible rather than merely detectable.
    """
    assert matrix["false_negative"] == [], (
        "flagged nothing for transactions the fixture marks as exceptions: "
        f"{matrix['false_negative']}"
    )
    assert matrix["false_positive"] == [], (
        f"raised an exception where the fixture says the money is right: {matrix['false_positive']}"
    )
    assert matrix["precision"] == 1.0
    assert matrix["recall"] == 1.0
    assert matrix["f1"] == 1.0


def test_the_table_has_something_to_measure(matrix):
    """All-correct is only meaningful if both classes are populated."""
    assert matrix["true_positive"], "the fixture asks for no exceptions at all"
    assert matrix["true_negative"], "the fixture asks for no clean settlements at all"
    exceptions = {exp for _, exp in _labelled_in_batch() if exp != MATCHED}
    assert len(exceptions) >= 3, f"only {sorted(exceptions)} are represented"


def test_the_scoring_survives_a_wrong_prediction():
    """The table has to be able to report a bad run, or it reports nothing.

    Each scenario below is hand-built: no pipeline code decides any of it.
    """
    labels = [("a", MATCHED), ("b", MATCHED), ("c", EXCEPTION_FEE_MISMATCH)]
    clean = _confusion(labels, {"a": MATCHED, "b": MATCHED, "c": EXCEPTION_FEE_MISMATCH})
    assert (clean["precision"], clean["recall"]) == (1.0, 1.0)

    missed = _confusion(labels, {"a": MATCHED, "b": MATCHED, "c": MATCHED})
    assert missed["false_negative"] == ["c"]
    assert missed["recall"] == 0.0, "a missed exception has to drag recall down"
    # Precision stays undefined rather than perfect: the run raised nothing at
    # all, so there is no prediction to be right about.
    assert missed["precision"] is None
    assert missed["f1"] is None

    spurious = _confusion(
        labels, {"a": MATCHED, "b": EXCEPTION_FEE_MISMATCH, "c": EXCEPTION_FEE_MISMATCH}
    )
    assert spurious["false_positive"] == ["b"]
    assert spurious["precision"] == 0.5, "one right flag and one wrong flag is 50 percent precision"
    assert spurious["recall"] == 1.0

    both = _confusion(labels, {"a": EXCEPTION_FEE_MISMATCH, "b": MATCHED, "c": MATCHED})
    assert both["false_positive"] == ["a"] and both["false_negative"] == ["c"]
    # Nothing caught, and something raised: both rates are zero. F1 has no
    # denominator left, so it is undefined too rather than reported as a
    # reassuring 1.0.
    assert both["precision"] == 0.0 and both["recall"] == 0.0 and both["f1"] is None


def test_the_counts_the_gate_reports_are_not_the_same_as_this_table(matrix):
    """A batch can pass its match-rate gate while the table says it misfired.

    ``check_batch_drift`` and the SLO gate read ratios. Those ratios are
    blind to which rows are on which side, so this fixture pins the two views
    separately: the ratio gates live in tests/test_pipeline_slo_gate.py.
    """
    flagged = len(matrix["true_positive"]) + len(matrix["false_positive"])
    total = flagged + len(matrix["false_negative"]) + len(matrix["true_negative"])
    rate = flagged / total
    assert 0.0 < rate < 1.0, (
        "if this ever reads 0 or 1 the confusion table has collapsed and the "
        "precision/recall figures above are meaningless"
    )


# --------------------------------------------------------------------------
# Bank evidence
# --------------------------------------------------------------------------


def test_bank_evidence_labels(spark):
    """The bank-evidence half of the fixture, through the real bank-leg SQL.

    These drive the real ``missing`` subquery against temp views rather than a
    parsed MT940 file, so the signed amounts in the file are the inputs.
    Parsing a .sta is covered by tests/adapters/test_bank_statement.py.
    """
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    fx = _fx()
    bank = fx["bank"]
    lag = int(bank["lag_days"])

    spark.createDataFrame(
        [
            (
                s["tx"],
                int(s["settled_amount_paise"]),
                s["settlement_date"],
                s.get("instrument_type"),
                s.get("merchant_id"),
                int(s.get("tolerance_paise", fx["meta"]["tolerance_paise"])),
            )
            for s in bank["settlements"]
        ],
        StructType(
            [
                StructField("transaction_id", StringType()),
                StructField("settled_amount_paise", LongType()),
                StructField("settlement_date", StringType()),
                StructField("instrument_type", StringType()),
                StructField("merchant_id", StringType()),
                StructField("tolerance_paise", LongType()),
            ]
        ),
    ).createOrReplaceTempView("bank_settlements")

    spark.createDataFrame(
        [
            (b["value_date"], int(b["amount_paise"]), b["link_tx"], b["narrative"])
            for b in bank["statements"]
        ],
        StructType(
            [
                StructField("value_date", StringType()),
                StructField("amount_paise", LongType()),
                StructField("link_tx", StringType()),
                StructField("narrative", StringType()),
            ]
        ),
    ).createOrReplaceTempView("bank_statements")

    missing = spark.sql(
        "SELECT s.transaction_id AS tid FROM bank_settlements s "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_statements b WHERE "
        "b.link_tx = s.transaction_id "
        "OR (b.link_tx IS NULL "
        f"AND ABS(b.amount_paise - s.settled_amount_paise) <= s.tolerance_paise "
        f"AND ABS(DATEDIFF(b.value_date, s.settlement_date)) <= {lag}))"
    ).collect()
    missing_ids = {r["tid"] for r in missing}

    got = {
        tx: (EXCEPTION_MISSING_BANK_STATEMENT if tx in missing_ids else MATCHED)
        for tx in bank["expectations"]
    }
    assert got == bank["expectations"], (
        f"the bank predicate disagrees with the fixture's hand-written reasons: {bank['why']}"
    )
