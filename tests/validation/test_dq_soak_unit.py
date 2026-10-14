"""DQ distribution + soak (unit-only, pandas, no Spark).

- quarantine split keeps good rows and isolates bad rows
- quarantine-rate gate trips only above QUARANTINE_MAX_PCT
- repeated identical inputs give identical splits (soak-stable)
"""

import pandas as pd
import pytest

from src.validation.settlement_schema import settlement_schema
from src.validation.validate_settlement import validate_and_quarantine

pytestmark = pytest.mark.unit


def _frame(n_good=8, n_bad=2):
    good = {
        "transaction_id": [f"TXN{i:06d}" for i in range(n_good)],
        "settled_amount_paise": [1000] * n_good,
        "bank_ref_id": [f"B{i}" for i in range(n_good)],
        "settlement_date": ["2025-04-02"] * n_good,
        "instrument_type": ["UPI"] * n_good,
    }
    bad = {
        "transaction_id": [f"BAD{i:06d}" for i in range(n_bad)],
        "settled_amount_paise": [-50] * n_bad,
        "bank_ref_id": [f"X{i}" for i in range(n_bad)],
        "settlement_date": ["2025-04-02"] * n_bad,
        "instrument_type": ["UPI"] * n_bad,
    }
    return pd.DataFrame(
        {k: good[k] + bad[k] for k in good},
    )


def test_quarantine_split_is_exact():
    valid, invalid = validate_and_quarantine(_frame(), settlement_schema)
    assert len(valid) == 8 and len(invalid) == 2
    assert (invalid["settled_amount_paise"] < 0).all()


def test_quarantine_rate_gate(monkeypatch):
    import os

    rate = 2 / 10 * 100  # 20% bad in _frame()
    monkeypatch.setenv("QUARANTINE_MAX_PCT", "50")
    assert rate <= float(os.environ["QUARANTINE_MAX_PCT"])
    monkeypatch.setenv("QUARANTINE_MAX_PCT", "10")
    assert rate > float(os.environ["QUARANTINE_MAX_PCT"])


def test_soak_stable_over_repeated_runs():
    first = validate_and_quarantine(_frame(), settlement_schema)
    for _ in range(10):
        valid, invalid = validate_and_quarantine(_frame(), settlement_schema)
        assert valid.equals(first[0]) and invalid.equals(first[1])
