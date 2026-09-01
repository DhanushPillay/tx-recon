import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from src.validation.settlement_schema import settlement_schema
from src.validation.validate_settlement import (
    SettlementValidation_error,
    validate_and_quarantine,
    validate_latest_settlement,
)


@pytest.fixture
def valid_csv(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_112233445566", "tx_aabbccddeeff"],
            "settled_amount_paise": [1000, 2000, 3000],
            "bank_ref_id": ["bnk_1", "bnk_2", "bnk_3"],
            "settlement_date": ["2024-01-16", "2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "CREDIT_CARD", "DEBIT_CARD"],
        }
    )
    path = data_dir / "settlement_valid.csv"
    df.to_csv(path, index=False)
    return tmp_path


@pytest.fixture
def invalid_csv_duplicate_ids(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_a1b2c3d4e5f6", "tx_aabbccddeeff"],
            "settled_amount_paise": [1000, 2000, 3000],
            "bank_ref_id": ["bnk_1", "bnk_2", "bnk_3"],
            "settlement_date": ["2024-01-16", "2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "UPI", "UPI"],
        }
    )
    path = data_dir / "settlement_dupes.csv"
    df.to_csv(path, index=False)
    return tmp_path


@pytest.fixture
def invalid_csv_negative_amount(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_112233445566"],
            "settled_amount_paise": [1000, -500],
            "bank_ref_id": ["bnk_1", "bnk_2"],
            "settlement_date": ["2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "UPI"],
        }
    )
    path = data_dir / "settlement_negative.csv"
    df.to_csv(path, index=False)
    return tmp_path


@pytest.fixture
def invalid_csv_null_ref(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_112233445566"],
            "settled_amount_paise": [1000, 2000],
            "bank_ref_id": ["bnk_1", None],
            "settlement_date": ["2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "UPI"],
        }
    )
    path = data_dir / "settlement_null.csv"
    df.to_csv(path, index=False)
    return tmp_path


def test_schema_validates_correct_data():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_112233445566"],
            "settled_amount_paise": [1000, 2000],
            "bank_ref_id": ["bnk_1", "bnk_2"],
            "settlement_date": ["2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "CREDIT_CARD"],
        }
    )
    settlement_schema.validate(df, lazy=True)


def test_schema_rejects_duplicate_ids(invalid_csv_duplicate_ids):
    df = pd.read_csv(invalid_csv_duplicate_ids / "data" / "settlement_dupes.csv")
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_schema_rejects_negative_amount(invalid_csv_negative_amount):
    df = pd.read_csv(invalid_csv_negative_amount / "data" / "settlement_negative.csv")
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_schema_rejects_null_ref(invalid_csv_null_ref):
    df = pd.read_csv(invalid_csv_null_ref / "data" / "settlement_null.csv")
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_validate_latest_settlement_success(valid_csv):
    validate_latest_settlement(project_root=str(valid_csv))


def test_validate_latest_settlement_failure(invalid_csv_duplicate_ids):
    with pytest.raises(SettlementValidation_error):
        validate_latest_settlement(project_root=str(invalid_csv_duplicate_ids))


def test_validate_latest_settlement_no_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_latest_settlement(project_root=str(tmp_path))


def test_validate_and_quarantine_direct():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_112233445566", "tx_aabbccddeeff"],
            "settled_amount_paise": [1000, 2000, 3000],
            "bank_ref_id": ["bnk_1", "bnk_2", "bnk_3"],
            "settlement_date": ["2024-01-16", "2024-01-16", "2024-01-16"],
            "instrument_type": ["UPI", "CREDIT_CARD", "DEBIT_CARD"],
        }
    )
    valid, invalid = validate_and_quarantine(df, settlement_schema)
    assert len(valid) == 3
    assert len(invalid) == 0


def test_schema_yaml_roundtrip(tmp_path):
    yaml_path = tmp_path / "settlement_schema.yaml"
    settlement_schema.to_yaml(str(yaml_path))
    assert yaml_path.exists()

    import pandera.pandas as pa

    loaded = pa.DataFrameSchema.from_yaml(str(yaml_path))
    assert "transaction_id" in loaded.columns
    assert "settled_amount_paise" in loaded.columns
    assert "bank_ref_id" in loaded.columns
    assert "instrument_type" in loaded.columns
