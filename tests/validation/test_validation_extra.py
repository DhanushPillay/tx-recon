"""Polars schema + validate_and_quarantine edge branches."""

import pandas as pd
import pytest

from src.validation.settlement_schema import settlement_schema
from src.validation.settlement_schema_pl import settlement_schema_pl
from src.validation.validate_settlement import (
    SettlementValidationError,
    _warn_volume_shift,
    validate_and_quarantine,
)

pytestmark = pytest.mark.unit


def test_pl_schema_rejects_bad_rows():
    import polars as pl
    from pandera.errors import SchemaError, SchemaErrors

    df = pl.DataFrame(
        [
            {
                "transaction_id": "bad",
                "settled_amount_paise": -5,
                "bank_ref_id": "b",
                "settlement_date": "2025-13-99",
                "instrument_type": "NOPE",
            }
        ]
    )
    with pytest.raises((SchemaError, SchemaErrors)):
        settlement_schema_pl.validate(df)


def test_pl_schema_accepts_minimal():
    import polars as pl

    df = pl.DataFrame(
        [
            {
                "transaction_id": "tx_abcdef123456",
                "settled_amount_paise": 100,
                "bank_ref_id": "b1",
                "settlement_date": "2025-04-02",
                "instrument_type": "UPI",
            }
        ]
    )
    out = settlement_schema_pl.validate(df)
    assert len(out) == 1


def test_quarantine_unexpected_failure_shape_raises(monkeypatch):
    from pandera.errors import SchemaErrors

    df = pd.DataFrame([{"a": 1}])

    class FakeExc(SchemaErrors):
        def __init__(self):
            self.failure_cases = pd.DataFrame([{"foo": 1}])

    monkeypatch.setattr(
        settlement_schema,
        "validate",
        lambda *a, **k: (_ for _ in ()).throw(FakeExc()),
    )
    with pytest.raises((SettlementValidationError, Exception)):
        validate_and_quarantine(df, settlement_schema)


def test_bad_calendar_date_passes_regex_only():
    # Schema pins format (regex) not calendar validity: 2024-02-30 matches
    # YYYY-MM-DD so it validates — documents a known residual gap.
    df = pd.DataFrame(
        [
            {
                "transaction_id": "tx_abcdef123456",
                "settled_amount_paise": 100,
                "bank_ref_id": "b1",
                "settlement_date": "2024-02-30",
                "instrument_type": "UPI",
            }
        ]
    )
    valid, invalid = validate_and_quarantine(df, settlement_schema)
    assert len(valid) == 1 and len(invalid) == 0


def test_validate_latest_empty_file_raises(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs

    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250402.csv").write_text("transaction_id,settled_amount_paise\n")
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises((SettlementValidationError, FileNotFoundError)):
        vs.validate_latest_settlement(project_root=str(tmp_path), date_str="2025-04-02")


def test_quarantine_path_nonstandard_basename(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs

    d = tmp_path / "data"
    d.mkdir()
    f = d / "settlement_20250402.csv"
    f.write_text(
        "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type\ntx_abcdef123456,-5,b1,2025-04-02,UPI\n"
    )
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises(SettlementValidationError):
        vs.validate_latest_settlement(project_root=str(tmp_path), date_str="2025-04-02")
    assert (d / "quarantine_20250402.csv").exists()


def test_warn_volume_shift_flags_collapse(tmp_path, caplog):
    """Previous curated batch 10 rows, current 2 -> >50% drop warning."""
    import logging

    d = tmp_path / "data"
    d.mkdir()
    prev = d / "curated_settlement_20250101.csv"
    prev.write_text("a\n" + "1\n" * 10)
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n1\n")
    with caplog.at_level(logging.WARNING, logger="src.validation.validate_settlement"):
        _warn_volume_shift(str(d), str(cur), 2)
    assert any("volume shift" in r.message for r in caplog.records)


def test_warn_volume_shift_first_run_silent(tmp_path, caplog):
    """No previous curated batch -> no warning, never raises."""
    import logging

    d = tmp_path / "data"
    d.mkdir()
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n1\n")
    with caplog.at_level(logging.WARNING, logger="src.validation.validate_settlement"):
        _warn_volume_shift(str(d), str(cur), 1)
    assert not [r for r in caplog.records if "volume shift" in r.message]


def test_warn_volume_shift_flags_stale_file(tmp_path, caplog):
    """Current file >48h old -> stale warning even when volume is normal."""
    import logging
    import os
    import time

    d = tmp_path / "data"
    d.mkdir()
    prev = d / "curated_settlement_20250101.csv"
    prev.write_text("a\n" + "1\n" * 10)
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n" + "1\n" * 10)
    old = time.time() - 72 * 3600
    os.utime(cur, (old, old))
    with caplog.at_level(logging.WARNING, logger="src.validation.validate_settlement"):
        _warn_volume_shift(str(d), str(cur), 10)
    assert any("stale settlement file" in r.message for r in caplog.records)
    assert not [r for r in caplog.records if "volume shift" in r.message]
