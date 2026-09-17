"""Downgrade-only residual scorer for tx-recon.

Base MERGE is rule-based with F1=1.0 on the sealed harness. The residual
may only demote MATCHED -> EXCEPTION_FEE_MISMATCH, never promote.
Hence FP (hallucinated MATCH) is monotone non-increasing — proven in
docs/PROOF.md.

Scorer is pure Python, no new deps, so it is auditable. A learned
logistic regression would replace score_match() internals with fit(),
but the wrapper and proof stay identical.
"""

from __future__ import annotations

import math

from src.common.schemas import EXCEPTION_FEE_MISMATCH, MATCHED

DEFAULT_TAU = 0.9


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    e = math.exp(x)
    return e / (1.0 + e)


def score_match(
    amount_paise: int,
    settled_amount_paise: int,
    instrument_type: str,
    merchant_id: str | None,
    settlement_date: str | None,
    *,
    fee_engine=None,
) -> float:
    """Downgrade probability in [0,1] for a MATCHED row.

    High score = mercy match that should be demoted. Strongest signal is
    merchant-rate disagreement: default says MATCH but merchant says MISMATCH.
    """
    if fee_engine is None:
        from src.processing.fee_engine import get_fee_engine

        fee_engine = get_fee_engine()

    exp_default = fee_engine.compute_expected_settled(
        amount_paise, instrument_type=instrument_type, settlement_date=settlement_date
    )
    diff_default = abs(exp_default - settled_amount_paise)
    tol_default = fee_engine.get_rate_for_date(
        instrument_type, merchant_id=None, settlement_date=settlement_date
    ).get("tolerance_paise", 1)

    exp_merch = fee_engine.compute_expected_settled(
        amount_paise,
        instrument_type=instrument_type,
        merchant_id=merchant_id,
        settlement_date=settlement_date,
    )
    diff_merch = abs(exp_merch - settled_amount_paise)
    tol_merch = fee_engine.get_rate_for_date(
        instrument_type, merchant_id=merchant_id, settlement_date=settlement_date
    ).get("tolerance_paise", tol_default)

    default_match = diff_default <= tol_default
    merch_match = diff_merch <= tol_merch
    if default_match and not merch_match:
        return 0.95
    excess = max(diff_default - tol_default, diff_merch - tol_merch)
    if excess <= 0:
        return 0.05 + 0.1 * _sigmoid(excess / 10.0)
    return 0.2 + 0.75 * _sigmoid((excess - 10) / 20.0)


def should_downgrade(
    amount_paise: int,
    settled_amount_paise: int,
    instrument_type: str,
    merchant_id: str | None,
    settlement_date: str | None,
    *,
    tau: float = DEFAULT_TAU,
    fee_engine=None,
) -> bool:
    """True iff score >= tau. Caller must only call on MATCHED rows."""
    return (
        score_match(
            amount_paise,
            settled_amount_paise,
            instrument_type,
            merchant_id,
            settlement_date,
            fee_engine=fee_engine,
        )
        >= tau
    )


def score_match_sql(
    fee_engine,
    w_amount="t2.amount_paise",
    s_settled="s.settled_amount_paise",
    s_inst="s.instrument_type",
    s_merch="s.merchant_id",
    s_date="s.settlement_date",
) -> str:
    """SQL port of score_match(): same curve, set-based (no driver collect).

    Reuses build_fee_case_sql/build_tolerance_case_sql so fee math stays
    single-sourced. The default leg passes CAST(NULL AS STRING) as the
    merchant column so merchant WHENs never match (merchant-agnostic rates).
    """
    from src.processing.reconcile import build_fee_case_sql, build_tolerance_case_sql

    null_merch = "CAST(NULL AS STRING)"
    fee_d, gst_d = build_fee_case_sql(
        fee_engine,
        amount_col=w_amount,
        inst_col=s_inst,
        merchant_col=null_merch,
        settlement_date_col=s_date,
    )
    tol_d = build_tolerance_case_sql(
        fee_engine, inst_col=s_inst, merchant_col=null_merch, settlement_date_col=s_date
    )
    fee_m, gst_m = build_fee_case_sql(
        fee_engine,
        amount_col=w_amount,
        inst_col=s_inst,
        merchant_col=s_merch,
        settlement_date_col=s_date,
    )
    tol_m = build_tolerance_case_sql(
        fee_engine, inst_col=s_inst, merchant_col=s_merch, settlement_date_col=s_date
    )
    exp_d = f"({w_amount} - ({fee_d}) - ({gst_d}))"
    exp_m = f"({w_amount} - ({fee_m}) - ({gst_m}))"
    diff_d = f"ABS({exp_d} - {s_settled})"
    diff_m = f"ABS({exp_m} - {s_settled})"
    excess = f"GREATEST(({diff_d} - ({tol_d})), ({diff_m} - ({tol_m})))"
    return (
        f"CASE WHEN ({diff_d} <= ({tol_d})) AND ({diff_m} > ({tol_m})) THEN 0.95 "
        f"WHEN ({excess}) <= 0 THEN 0.05 + 0.1 * (1.0 / (1.0 + EXP(-(({excess}) / 10.0)))) "
        f"ELSE 0.2 + 0.75 * (1.0 / (1.0 + EXP(-((({excess}) - 10.0) / 20.0)))) END"
    )


def apply_residual(
    spark,
    table: str,
    *,
    tau: float = DEFAULT_TAU,
    fee_engine=None,
) -> int:
    """Batch post-pass: demote scored MATCHED rows. Returns rows demoted.

    Set-based MERGE (no driver collect): scores every MATCHED row in Spark
    via score_match_sql and demotes those >= tau. Never promotes.
    Requires a temp view bank_settlements with
    (transaction_id, settled_amount_paise, instrument_type, merchant_id,
    settlement_date).
    """
    from src.processing.reconcile import _qualified_table

    table = _qualified_table(table)

    if fee_engine is None:
        from src.processing.fee_engine import get_fee_engine

        fee_engine = get_fee_engine()

    score_expr = score_match_sql(fee_engine)
    scored_source = (
        "SELECT s.transaction_id AS tid FROM bank_settlements s "
        f"JOIN {table} t2 ON t2.transaction_id = s.transaction_id "
        f"WHERE t2.reconciliation_status = '{MATCHED}' "
        "AND t2.amount_paise IS NOT NULL AND s.settled_amount_paise IS NOT NULL "
        f"AND ({score_expr}) >= {float(tau)}"
    )
    try:
        to_demote = spark.sql(f"SELECT COUNT(*) AS n FROM ({scored_source})").collect()[0]["n"]
    except Exception:
        return 0
    if not to_demote:
        return 0

    spark.sql(
        f"MERGE INTO {table} t USING ({scored_source}) scored "
        "ON t.transaction_id = scored.tid "
        f"WHEN MATCHED THEN UPDATE SET t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}'"
    )
    return int(to_demote)
