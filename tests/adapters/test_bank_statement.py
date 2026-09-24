"""Bank-statement leg: MT940 parse + third-leg MERGE SQL shape."""

import os

import pytest

from src.adapters.bank_statement import parse_mt940
from src.common.schemas import EXCEPTION_MISSING_BANK_STATEMENT
from src.processing.reconcile import build_bank_leg_sql

pytestmark = pytest.mark.unit

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "bank_sample.sta")


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
    assert r1["amount_paise"] == 976400  # 9764.00 credit
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
    """Headers but no :61: lines: nothing to match against, fail loudly."""
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
    # Only MATCHED rows demote; other statuses are terminal for this leg.
    assert merge_sql.count("WHEN MATCHED") == 1
    assert "WHEN NOT MATCHED" not in merge_sql
    assert "COUNT(*)" in orphan_sql


def test_bank_leg_sql_rejects_injection():
    with pytest.raises(ValueError):
        build_bank_leg_sql("x; DROP TABLE y --")
