"""Derived bank statement: MT940 built from real settlement rows.

No public dataset links webhooks + settlements + bank credits (real bank
statements carry real account numbers, banks never publish them), so the
third leg is derived from our own real settlement file: every row becomes
one :61: credit line with its real bank_ref, settled amount, value date and
transaction id (behind an explicit ReconRef marker the parser extracts).
All three legs are then real-anchored and mutually consistent.

Streaming: two passes over the CSV (sum, then emit), constant memory.
Usage: python -m src.adapters.build_bank_statement data/real_thiru_full_settlement.csv data/real_thiru_full_statement.sta
"""

import csv
import logging

logger = logging.getLogger(__name__)

_CURRENCY = "INR"  # source currency column is empty; PG data is INR paise


def _inr(amount_paise: int) -> str:
    rupees, paise = divmod(abs(int(amount_paise)), 100)
    return f"{rupees},{paise:02d}"


def _yymmdd(iso_date: str) -> str:
    y, m, d = iso_date.split("-")
    return f"{y[2:]}{m}{d}"


def build_bank_statement(input_csv: str, output_sta: str) -> int:
    """Write one :61:/:86: credit line per settlement row. Returns line count."""
    total = 0
    first_date = last_date = None
    with open(input_csv, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                total += int(row.get("settled_amount_paise") or 0)
            except (TypeError, ValueError):
                continue
            day = (row.get("settlement_date") or "")[:10]
            if len(day) == 10:
                first_date = first_date or day
                last_date = day

    n = skipped = 0
    with (
        open(input_csv, newline="", encoding="utf-8") as fh,
        open(output_sta, "w", encoding="utf-8", newline="") as out,
    ):
        out.write(":20:TXRECON-FULL\n")
        out.write(":25:12345678901234567890\n")
        out.write(":28C:00001/001\n")
        start = _yymmdd(first_date) if first_date else "100101"
        out.write(f":60F:C{start}{_CURRENCY}0,00\n")
        for row in csv.DictReader(fh):
            tx = (row.get("transaction_id") or "").strip()
            day = (row.get("settlement_date") or "")[:10]
            try:
                amount = int(row.get("settled_amount_paise") or 0)
            except (TypeError, ValueError):
                amount = 0
            if not tx or len(day) != 10 or amount <= 0:
                skipped += 1
                continue
            ymd = _yymmdd(day)
            ref = (row.get("bank_ref_id") or "")[:16]
            merch = (row.get("merchant_id") or "")[:16]
            inst = (row.get("instrument_type") or "")[:16]
            out.write(f":61:{ymd}{ymd[2:]}C{_inr(amount)}NTRF{ref}\n")
            out.write(f":86:NEFT Cr ReconRef {tx} M{merch} {inst}\n".rstrip() + "\n")
            n += 1
        end = _yymmdd(last_date) if last_date else start
        out.write(f":62F:C{end}{_CURRENCY}{_inr(total)}\n")
    logger.info(f"bank statement {output_sta}: {n} lines, {skipped} skipped")
    return n


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv")
    ap.add_argument("output_sta")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    print(f"wrote {build_bank_statement(args.input_csv, args.output_sta)} lines")
