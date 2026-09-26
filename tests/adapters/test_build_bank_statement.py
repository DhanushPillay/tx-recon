"""Derived bank statement: MT940 built from real settlement rows.

Covers the RED test driving the builder (tmp in -> parse back -> links hold).
"""

import csv

from src.adapters.build_bank_statement import build_bank_statement


def _settlement_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "bank_ref_id",
                "transaction_id",
                "settled_amount_paise",
                "settlement_date",
                "instrument_type",
                "merchant_id",
            ],
        )
        w.writeheader()
        w.writerows(rows)


def test_build_links_hold_parse_back(tmp_path):
    from src.adapters.bank_statement import parse_mt940

    src = tmp_path / "settle.csv"
    out = tmp_path / "stmt.sta"
    _settlement_csv(
        str(src),
        [
            {
                "bank_ref_id": "bnk_7475328",
                "transaction_id": "7475328",
                "settled_amount_paise": "1423",
                "settlement_date": "2010-01-01",
                "instrument_type": "CREDIT_CARD",
                "merchant_id": "67570",
            },
            {
                "bank_ref_id": "bnk_7475329",
                "transaction_id": "7475329",
                "settled_amount_paise": "100000",
                "settlement_date": "2010-01-02",
                "instrument_type": "UPI",
                "merchant_id": "27092",
            },
        ],
    )
    n = build_bank_statement(str(src), str(out))
    assert n == 2
    df = parse_mt940(str(out))
    assert len(df) == 2
    assert sorted(df["link_tx"].tolist()) == ["7475328", "7475329"]
    assert sorted(df["amount_paise"].tolist()) == [1423, 100000]
    assert (df["direction"] == "CREDIT").all()
