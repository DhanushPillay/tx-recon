import logging

import pandas as pd
import pytest

from src.validation.pan_guard import scan_frame
from src.validation.settlement_schema import bad_calendar_date_mask, settlement_schema
from src.validation.settlement_schema_pl import settlement_schema_pl
from src.validation.validate_settlement import (
    SettlementValidationError,
    _warn_volume_shift,
    validate_and_quarantine,
    validate_latest_settlement,
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


def test_pl_schema_matches_pandas_twin_columns():
    from src.validation.settlement_schema import settlement_schema

    assert set(settlement_schema_pl.columns) == set(settlement_schema.columns)


def test_pl_schema_rejects_non_inr_currency():
    import polars as pl
    from pandera.errors import SchemaError, SchemaErrors

    base = {
        "transaction_id": "tx_abcdef123456",
        "settled_amount_paise": 100,
        "bank_ref_id": "b1",
        "settlement_date": "2025-04-02",
        "instrument_type": "UPI",
    }
    with pytest.raises((SchemaError, SchemaErrors)):
        settlement_schema_pl.validate(pl.DataFrame([{**base, "currency": "USD"}]))
    out = settlement_schema_pl.validate(pl.DataFrame([{**base, "currency": "INR"}]))
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


def test_calendar_mask_flags_only_impossible_dates():
    s = pd.Series(["2024-02-30", "2025-13-01", "2024-02-29", "2025-04-02", None, "2024-02-29"])
    assert list(bad_calendar_date_mask(s)) == [True, True, False, False, False, False]


def test_impossible_calendar_date_is_quarantined(tmp_path):
    data = tmp_path / "data"
    data.mkdir()

    def _row(tx, date):
        return {
            "transaction_id": tx,
            "settled_amount_paise": 100,
            "bank_ref_id": f"bank_{tx}",
            "settlement_date": date,
            "instrument_type": "UPI",
        }

    rows = [
        _row("tx_fakeday1", "2024-02-30"),
        _row("tx_fakemonth1", "2025-13-01"),
        _row("tx_leapday01", "2024-02-29"),
    ]
    pd.DataFrame(rows).to_csv(data / "settlement_20260901.csv", index=False)

    with pytest.raises(SettlementValidationError, match="2 rows quarantined"):
        validate_latest_settlement(project_root=str(tmp_path))

    quarantine = pd.read_csv(data / "quarantine_20260901.csv")
    assert sorted(quarantine["transaction_id"]) == ["tx_fakeday1", "tx_fakemonth1"]
    assert not (data / "curated_settlement_20260901.csv").exists()


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


def test_bad_date_str_rejected_before_glob(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs

    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    for bad in ("../secret", "/abs/path", "2025-04*", "2025-04-02;rm", "2025040x"):
        with pytest.raises(ValueError, match="Bad date_str"):
            vs.validate_latest_settlement(project_root=str(tmp_path), date_str=bad)


def test_quarantined_batch_not_recorded_in_registry(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs
    from src.validation.file_registry import load_registry

    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250402.csv").write_text(
        "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type\ntx_abcdef123456,-5,b1,2025-04-02,UPI\n"
    )
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises(SettlementValidationError):
        vs.validate_latest_settlement(project_root=str(tmp_path), date_str="2025-04-02")
    assert load_registry(str(d)) == {}


def test_warn_volume_shift_flags_collapse(tmp_path, caplog):
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
    import logging

    d = tmp_path / "data"
    d.mkdir()
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n1\n")
    with caplog.at_level(logging.WARNING, logger="src.validation.validate_settlement"):
        _warn_volume_shift(str(d), str(cur), 1)
    assert not [r for r in caplog.records if "volume shift" in r.message]


def test_warn_volume_shift_flags_stale_file(tmp_path, caplog):
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


def test_pan_gate_rejects_card_number(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs

    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250102.csv").write_text(
        "bank_ref_id,transaction_id,settled_amount_paise,settlement_date,instrument_type\n"
        "b1,tx_abcdef123456,97640,2025-01-02,CREDIT_CARD\n"
        "4111111111111111,tx_abcdef123457,97640,2025-01-02,CREDIT_CARD\n"
    )
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises(vs.SettlementValidationError, match="PAN detected"):
        vs.validate_latest_settlement(project_root=str(tmp_path), date_str="2025-01-02")


def test_pan_guard_unit():
    from src.validation.pan_guard import find_pans, scan_frame

    assert find_pans("4111111111111111") == ["4111111111111111"]
    assert find_pans("4111 1111 1111 1111") == ["4111111111111111"]
    assert find_pans("tx_abcdef123456") == []
    assert find_pans("12345") == []
    import pandas as pd

    df = pd.DataFrame({"transaction_id": ["tx_1", "4111111111111111"], "x": ["a", "b"]})
    assert scan_frame(df, ["transaction_id"]) == {"transaction_id": 1}
    assert scan_frame(df, ["x"]) == {}
    df2 = pd.DataFrame(
        {
            "notes": ["ok", "card 4111111111111111 here"],
            "settled_amount_paise": [100, 200],
        }
    )
    assert scan_frame(df2) == {"notes": 1}
    assert scan_frame(pd.DataFrame({"a": ["clean"], "n": [1]})) == {}


def test_file_registry_redelivery(tmp_path):
    from src.validation.file_registry import load_registry, record_file

    d = str(tmp_path)
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("same-bytes")
    b.write_text("same-bytes")
    first = record_file(d, str(a), "2025-01-01")
    assert first["seen_before"] is False
    second = record_file(d, str(b), "2025-01-01")
    assert second["seen_before"] is True
    assert second["sha256"] == first["sha256"]
    assert len(load_registry(d)) == 1


def test_file_registry_write_failure_warns_but_returns(tmp_path, caplog):
    import logging

    from src.validation.file_registry import record_file

    blocker = tmp_path / "blocker"
    blocker.write_text("not-a-dir")
    f = tmp_path / "a.csv"
    f.write_text("bytes")
    with caplog.at_level(logging.WARNING):
        out = record_file(str(blocker), str(f), "2025-01-01")
    assert out["seen_before"] is False
    assert len(out["sha256"]) == 64


def test_pan_guard_edge_cases():
    from src.validation.pan_guard import find_pans, scan_frame

    assert find_pans("") == []
    assert find_pans("no digits here") == []
    assert find_pans("411111111114") == []
    import pandas as pd

    df = pd.DataFrame({"transaction_id": ["tx_1"]})
    assert scan_frame(df, ["missing_col"]) == {}
    assert scan_frame(df, []) == {}


def test_adapter_failure_fails_closed(tmp_path, monkeypatch):
    import src.validation.validate_settlement as vs

    d = tmp_path / "data"
    d.mkdir()
    (d / "settlement_20250102.csv").write_bytes(b"\xff\xfe\x00not-a-csv\xff")
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    with pytest.raises(vs.SettlementValidationError, match="Adapter normalization failed"):
        vs.validate_latest_settlement(project_root=str(tmp_path), date_str="2025-01-02")


def test_volume_shift_raises_when_strict(tmp_path):
    import pytest as _pytest

    from src.validation.validate_settlement import SettlementValidationError

    d = tmp_path / "data"
    d.mkdir()
    prev = d / "curated_settlement_20250101.csv"
    prev.write_text("a\n" + "1\n" * 10)
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n1\n")
    with _pytest.raises(SettlementValidationError, match="volume shift"):
        _warn_volume_shift(str(d), str(cur), 2, strict=True)


def test_pan_gate_requires_an_issuer_prefix():
    from src.validation.pan_guard import find_pans

    for pan in (
        "4111111111111111",
        "5500005555555559",
        "2223003122003222",
        "378282246310005",
        "6011111111111117",
        "6500000000000002",
        "8100000000000002",
        "4111 1111 1111 1111",
        "4111-1111-1111-1111",
    ):
        assert find_pans(pan) == [pan.replace(" ", "").replace("-", "")], pan

    for not_a_pan in (
        "1234567890123460",
        "0000000000000000",
        "9000000000000000",
        "1234567890123456789012",
    ):
        assert find_pans(not_a_pan) == [], not_a_pan

    assert scan_frame(pd.DataFrame({"transaction_id": ["tx_4111111111111111"]})) == {
        "transaction_id": 1
    }


def test_latest_settlement_file_is_the_newest_date_not_the_newest_mtime(tmp_path, monkeypatch):
    import os
    import time

    d = tmp_path / "data"
    d.mkdir()
    old = d / "settlement_20250101.csv"
    new = d / "settlement_20250102.csv"
    for f in (old, new):
        f.write_text(
            "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type\n"
            "tx_abcdef123456,100,b1,2025-01-01,UPI\n"
        )
    stale_but_touched = time.time()
    os.utime(old, (stale_but_touched, stale_but_touched))

    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    validate_latest_settlement(project_root=str(tmp_path))

    assert (d / "curated_settlement_20250102.csv").exists()
    assert not (d / "curated_settlement_20250101.csv").exists()


def test_redelivery_of_the_same_date_is_volume_checked(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    cols = "transaction_id,settled_amount_paise,bank_ref_id,settlement_date,instrument_type"
    body10 = "".join(f"tx_abcdef1234{i:02d},100,b1,2025-01-02,UPI\n" for i in range(10))
    (d / "settlement_20250102.csv").write_text(cols + "\n" + body10)
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    validate_latest_settlement(project_root=str(tmp_path), date_str="2025-01-02")

    (d / "settlement_20250102.csv").write_text(cols + "\n" + body10[:110])
    with pytest.raises(SettlementValidationError, match="volume shift") as exc:
        validate_latest_settlement(project_root=str(tmp_path), date_str="2025-01-02")

    assert "curated_settlement_20250102.csv" in str(exc.value)


def test_stale_file_warns_even_when_strict(tmp_path, caplog):
    from src.validation.validate_settlement import _warn_volume_shift

    d = tmp_path / "data"
    d.mkdir()
    prev = d / "curated_settlement_20250101.csv"
    prev.write_text("a\n" + "1\n" * 10)
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n" + "1\n" * 10)

    with caplog.at_level(logging.WARNING):
        _warn_volume_shift(str(d), str(cur), 10, strict=True)

    assert "stale settlement file" in caplog.text
    assert "2025-01-02" in caplog.text


def test_stale_file_uses_filename_date_not_mtime(tmp_path, caplog):
    from src.validation.validate_settlement import _warn_volume_shift

    d = tmp_path / "data"
    d.mkdir()
    cur = d / "settlement_20250102.csv"
    cur.write_text("a\n1\n")

    with caplog.at_level(logging.WARNING):
        _warn_volume_shift(str(d), str(cur), 1, strict=False)

    assert "stale settlement file" in caplog.text
