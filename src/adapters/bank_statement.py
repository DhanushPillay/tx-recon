"""Third settlement leg: bank statement (MT940) -> canonical bank rows.

Gateways can be consistently wrong; the bank credit is the independent
truth. Parsing is delegated to the `mt-940` package (typed, fixture-tested)
— never hand-rolled SWIFT. Output links to settlements by extracted TXN id
when the narration carries one, else by amount + value date.
"""

import logging
import re

logger = logging.getLogger(__name__)

_TXN_LINK = re.compile(r"TXN\d{6,}")


def _paise(amount) -> int:
    from decimal import Decimal

    raw = getattr(amount, "amount", amount)  # mt940 Amount wraps a Decimal
    return int((Decimal(str(raw)) * 100).to_integral_value(rounding="ROUND_HALF_UP"))


def parse_mt940(path: str):
    """Parse an MT940 file into a pandas DataFrame of bank statement rows.

    Columns: bank_ref, amount_paise (signed: credit +, debit -), value_date
    (YYYY-MM-DD), direction (CREDIT/DEBIT), link_tx (TXN id from narration or
    None), narration. Raises on unparseable files (fail closed, like PG CSVs).
    """
    import pandas as pd

    try:
        import mt940
    except ImportError as exc:
        raise RuntimeError("mt-940 package required for bank statements") from exc

    try:
        txns = mt940.parse(path)
    except Exception as exc:
        raise ValueError(f"cannot parse MT940 {path}: {exc}") from exc
    if not txns:
        raise ValueError(f"cannot parse MT940 {path}: zero statement lines")

    rows = []
    for t in txns:
        d = t.data
        day = d.get("date")
        if day is None:
            raise ValueError(f"cannot parse MT940 {path}: missing value date")
        day_iso = day.strftime("%Y-%m-%d") if hasattr(day, "strftime") else str(day)[:10]  # type: ignore[union-attr]
        direction = "CREDIT" if str(d.get("status", "")).upper().startswith("C") else "DEBIT"
        narration = " ".join(
            str(d.get(k) or "") for k in ("transaction_details", "extra_details")
        ).strip()
        m = _TXN_LINK.search(narration)
        amt = _paise(d.get("amount", 0))
        rows.append(
            {
                "bank_ref": str(d.get("customer_reference") or d.get("bank_reference") or ""),
                "amount_paise": amt if direction == "CREDIT" else -amt,
                "value_date": day_iso,
                "direction": direction,
                "link_tx": m.group(0) if m else None,
                "narration": narration[:256],
            }
        )
    df = pd.DataFrame(
        rows,
        columns=["bank_ref", "amount_paise", "value_date", "direction", "link_tx", "narration"],
    )
    # No-PAN gate (same as PG CSVs): narration is free text and would put the
    # lake in PCI scope. Fail the file, never merge it.
    from src.validation.pan_guard import scan_frame

    pan_hits = scan_frame(df)
    if pan_hits:
        raise ValueError(
            f"PAN detected in bank statement {path} {pan_hits}: refusing file "
            "(store last-4 + issuer only)"
        )
    logger.info(f"bank statement {path}: {len(df)} rows, {df['link_tx'].notna().sum()} linked")
    return df
