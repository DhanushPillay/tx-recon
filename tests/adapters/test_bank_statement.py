import os

import pytest

from src.adapters.bank_statement import parse_mt940
from src.common.schemas import EXCEPTION_MISSING_BANK_STATEMENT
from src.processing.reconcile import build_bank_leg_sql

pytestmark = pytest.mark.unit

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "bank_sample.sta")


def _sta(lines: list[str], tmp_path) -> str:
    """Wrap bare ``:61:``/``:86:`` pairs in the minimum MT940 envelope."""
    body = ":20:STARTUMS\n:25:12345678901234567890\n:28C:00001/001\n:60F:C160401INR100000,00\n"
    body += "".join(lines)
    body += ":62F:C160402INR90000,00\n"
    p = tmp_path / "stmt.sta"
    p.write_text(body)
    return str(p)


def test_parse_mt940_fixture():
    df = parse_mt940(FIXTURE)
    assert len(df) == 2
    assert list(df.columns) == [
        "bank_ref",
        "amount_paise",
        "value_date",
        "direction",
        "link_tx",
        "narration",
    ]
    r1 = df.iloc[0]
    assert r1["amount_paise"] == 976400
    assert r1["value_date"] == "2016-04-02"
    assert r1["direction"] == "CREDIT"
    assert r1["link_tx"] == "TXN000000001"
    assert r1["bank_ref"] == "REF000000000001"


def test_parse_mt940_garbage_fails_closed():
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".sta", delete=False) as fh:
        fh.write("this is not swift\n")
        path = fh.name
    try:
        with pytest.raises(ValueError, match="cannot parse MT940"):
            parse_mt940(path)
    finally:
        os.unlink(path)


def test_parse_mt940_zero_lines_fails_closed(tmp_path):
    p = tmp_path / "empty.sta"
    p.write_text(":20:STARTUMS\n:25:12345678901234567890\n:28C:00001/001\n")
    with pytest.raises(ValueError, match="zero statement lines"):
        parse_mt940(str(p))


def test_bank_leg_sql_shape():
    merge_sql, orphan_sql = build_bank_leg_sql("nessie.db.webhooks", lag_days=2)
    assert "MERGE INTO nessie.db.webhooks" in merge_sql
    assert EXCEPTION_MISSING_BANK_STATEMENT in merge_sql
    assert "bank_statements" in merge_sql and "bank_settlements" in merge_sql
    assert "DATEDIFF" in merge_sql
    assert merge_sql.count("WHEN MATCHED") == 1
    assert "WHEN NOT MATCHED" not in merge_sql
    assert "COUNT(*)" in orphan_sql


def test_bank_leg_sql_rejects_injection():
    with pytest.raises(ValueError):
        build_bank_leg_sql("x; DROP TABLE y --")


def test_parse_mt940_missing_package_fails_closed(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "mt940", None)
    with pytest.raises(RuntimeError, match="mt-940 package required"):
        parse_mt940(FIXTURE)


def test_parse_mt940_missing_value_date_fails_closed():
    from unittest.mock import MagicMock, patch

    txn = MagicMock()
    txn.data = {"status": "C", "amount": 100}
    with (
        patch("mt940.parse", return_value=[txn]),
        pytest.raises(ValueError, match="missing value date"),
    ):
        parse_mt940("dummy.sta")


def test_parse_mt940_pan_in_narration_fails_closed(tmp_path):
    p = tmp_path / "pan.sta"
    p.write_text(
        ":20:STARTUMS\n"
        ":25:12345678901234567890\n"
        ":28C:00001/001\n"
        ":60F:C160401INR100000,00\n"
        ":61:1604020402C9764,00NTRFREF000000000001\n"
        "TXN000000001\n"
        ":86:NEFT Cr card 4111111111111111 TXN000000001\n"
        ":62F:C160402INR100000,00\n"
    )
    with pytest.raises(ValueError, match="PAN detected"):
        parse_mt940(str(p))


# Every mark below carries the same 9764,00 magnitude so the sign is the only
# thing under test. The MT940 :61: regex accepts C, D, RC and RD only: C is
# money in, D money out, RC undoes a credit, RD undoes a debit.
_SIGNS = [
    ("C", 976400, "CREDIT"),
    ("D", -976400, "DEBIT"),
    ("RC", -976400, "DEBIT"),
    ("RD", 976400, "CREDIT"),
    ("d", -976400, "DEBIT"),
]


@pytest.mark.parametrize("mark,expected,expected_direction", _SIGNS)
def test_parse_mt940_signs_each_mark_once(tmp_path, mark, expected, expected_direction):
    """Regression: the mark was applied by mt940 and then applied again here.

    A DEBIT of 9764,00 used to land as +976400 paise, so a chargeback on a
    transaction read as positive settlement evidence.
    """
    path = _sta(
        [
            f":61:1604020402{mark}9764,00NTRFREF000000000001\n",
            "TXN000000001\n",
            ":86:NEFT txn TXN000000001\n",
        ],
        tmp_path,
    )
    row = parse_mt940(path).iloc[0]
    assert row["amount_paise"] == expected
    assert row["direction"] == expected_direction


def test_parse_mt940_direction_never_contradicts_the_sign(tmp_path):
    path = _sta(
        [
            f":61:1604020402{mark}9764,00NTRFREF{i:06d}\nTXN000000{i}\n:86:t TXN000000{i}\n"
            for i, (mark, _, _) in enumerate(_SIGNS, start=1)
        ],
        tmp_path,
    )
    df = parse_mt940(path)
    assert len(df) == len(_SIGNS)
    for _, row in df.iterrows():
        expected = "CREDIT" if row["amount_paise"] >= 0 else "DEBIT"
        assert row["direction"] == expected, row["narration"]


def test_debit_line_cannot_satisfy_bank_evidence(tmp_path):
    """A chargeback must not stand in for the payout it reversed.

    ``reconcile.py`` accepts bank evidence when a line sits within tolerance of
    the settled amount, so a same-sized DEBIT would otherwise confirm a
    settlement the bank actually pulled back.
    """
    settled = 976400
    debit = parse_mt940(
        _sta(
            [
                ":61:1604020402D9764,00NTRFREF000000000001\n",
                "TXN000000001\n",
                ":86:NEFT chargeback TXN000000001\n",
            ],
            tmp_path,
        )
    ).iloc[0]["amount_paise"]
    assert abs(debit - settled) > 1, "a DEBIT of the same size would match the settlement"


def test_parse_mt940_still_signs_the_credit_leg_unchanged(tmp_path):
    """The fix must not disturb the credit path every batch depends on."""
    path = _sta(
        [
            ":61:1604020402C9764,00NTRFREF000000000001\n",
            "TXN000000001\n",
            ":86:NEFT payout TXN000000001\n",
        ],
        tmp_path,
    )
    row = parse_mt940(path).iloc[0]
    assert row["amount_paise"] == 976400
    assert row["direction"] == "CREDIT"
    assert row["link_tx"] == "TXN000000001"
