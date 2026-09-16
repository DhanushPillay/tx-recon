"""Settlement generator branches."""

import csv

import pytest
from freezegun import freeze_time

from src.generators.settlement_generator import generate_settlement_file

pytestmark = pytest.mark.unit


def test_num_records_zero_raises():
    with pytest.raises(ValueError):
        generate_settlement_file(num_records=0)


def test_planned_3tuple_and_4tuple(tmp_path):
    planned3 = [("tx_abcdef123456", 100000, "CREDIT_CARD")]
    f1 = generate_settlement_file(
        num_records=1, output_dir=str(tmp_path), planned=planned3, date_str="2025-04-02"
    )
    assert "20250402" in f1
    planned4 = [("tx_abcdef123457", 100000, "UPI", "merch_001")]
    f2 = generate_settlement_file(
        num_records=1, output_dir=str(tmp_path), planned=planned4, date_str="2025-04-03"
    )
    assert "20250403" in f2


def test_random_path_with_webhook_ids(tmp_path):
    f = generate_settlement_file(
        num_records=5,
        output_dir=str(tmp_path),
        webhook_ids=["tx_abcdef123458"],
        seed=7,
        date_str="2025-04-04",
    )
    import csv

    with open(f) as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 5
    assert all(r["settled_amount_paise"].isdigit() for r in rows)


def test_merchant_override(tmp_path):
    f = generate_settlement_file(
        num_records=3, output_dir=str(tmp_path), seed=1, date_str="2025-04-05", merchant_id="mX"
    )
    import csv

    with open(f) as fh:
        rows = list(csv.DictReader(fh))
    assert {r["merchant_id"] for r in rows} == {"mX"}


@freeze_time("2025-04-01 12:00:00")
def test_default_date_is_frozen_tomorrow(tmp_path):
    # No date_str -> tomorrow's date from the clock; frozen so deterministic.
    f = generate_settlement_file(num_records=2, output_dir=str(tmp_path), seed=7)
    assert f.endswith("settlement_20250402.csv")
    with open(f) as fh:
        rows = list(csv.DictReader(fh))
    assert {r["settlement_date"] for r in rows} == {"2025-04-02"}
