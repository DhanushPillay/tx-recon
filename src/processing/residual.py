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
    excess = diff_default - tol_default
    if excess <= 0:
        return 0.05 + 0.1 * _sigmoid(excess / 10.0)
    return 0.2 + 0.7 * _sigmoid((excess - 10) / 20.0)


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
    """True iff score > tau. Caller must only call on MATCHED rows."""
    return (
        score_match(
            amount_paise,
            settled_amount_paise,
            instrument_type,
            merchant_id,
            settlement_date,
            fee_engine=fee_engine,
        )
        > tau
    )


def apply_residual(
    spark,
    table: str,
    *,
    tau: float = DEFAULT_TAU,
) -> int:
    """Batch post-pass: demote scored MATCHED rows. Returns rows demoted.

    Requires a temp view bank_settlements with
    (transaction_id, settled_amount_paise, instrument_type, merchant_id,
    settlement_date). Never promotes MISMATCH -> MATCHED.
    """
    from src.processing.reconcile import _qualified_table

    _qualified_table(table)

    rows = spark.sql(
        f"""
        SELECT
            t.transaction_id, t.amount_paise,
            s.settled_amount_paise, s.instrument_type, s.merchant_id, s.settlement_date
        FROM {table} t
        JOIN bank_settlements s ON t.transaction_id = s.transaction_id
        WHERE t.reconciliation_status = '{MATCHED}'
        """
    ).collect()

    to_demote: list[str] = []
    for r in rows:
        if should_downgrade(
            r["amount_paise"],
            r["settled_amount_paise"],
            r["instrument_type"],
            r["merchant_id"],
            r["settlement_date"],
            tau=tau,
        ):
            to_demote.append(r["transaction_id"])

    if not to_demote:
        return 0

    in_list = ", ".join(f"'{tx}'" for tx in to_demote)
    spark.sql(
        f"UPDATE {table} SET reconciliation_status = '{EXCEPTION_FEE_MISMATCH}' "
        f"WHERE transaction_id IN ({in_list})"
    )
    return len(to_demote)
