"""Curated contract: N-1 good + 1 bad -> raises, quarantine=1, curated=N-1.

Hermetic: pandas/Pandera only, tmp dir as project_root, no Spark.
"""

import os

import pandas as pd
import pytest

from src.validation.validate_settlement import (
    SettlementValidationError,
    validate_latest_settlement,
)


def _row(tx, amount=97640, date="2026-09-01", inst="CREDIT_CARD"):
    return {
        "transaction_id": tx,
        "settled_amount_paise": amount,
        "bank_ref_id": f"bank_{tx}",
        "settlement_date": date,
        "instrument_type": inst,
    }


def test_quarantine_plus_curated(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    rows = [_row(f"tx_good{i:03d}") for i in range(4)]
    rows.append(_row("tx_bad1", amount=-50))  # violates gt(0)
    pd.DataFrame(rows).to_csv(data / "settlement_20260901.csv", index=False)

    with pytest.raises(SettlementValidationError):
        validate_latest_settlement(project_root=str(tmp_path))

    quarantine = data / "quarantine_20260901.csv"
    curated = data / "curated_settlement_20260901.csv"
    assert quarantine.exists()
    assert curated.exists()
    assert len(pd.read_csv(quarantine)) == 1
    assert len(pd.read_csv(curated)) == 4
    # curated must not match the settlement_*.csv glob on rerun
    assert [f for f in os.listdir(data) if f.startswith("settlement_")] == [
        "settlement_20260901.csv"
    ]
