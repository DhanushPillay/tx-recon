from unittest.mock import patch

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from src.validation.settlement_schema import settlement_schema
from src.validation.validate_settlement import validate_latest_settlement


@pytest.fixture
def valid_csv(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_1", "tx_2", "tx_3"],
            "settled_amount_paise": [1000, 2000, 3000],
            "bank_ref_id": ["bnk_1", "bnk_2", "bnk_3"],
        }
    )
    path = data_dir / "settlement_valid.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def invalid_csv_duplicate_ids(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_1", "tx_1", "tx_3"],
            "settled_amount_paise": [1000, 2000, 3000],
            "bank_ref_id": ["bnk_1", "bnk_2", "bnk_3"],
        }
    )
    path = data_dir / "settlement_dupes.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def invalid_csv_negative_amount(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_1", "tx_2"],
            "settled_amount_paise": [1000, -500],
            "bank_ref_id": ["bnk_1", "bnk_2"],
        }
    )
    path = data_dir / "settlement_negative.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def invalid_csv_null_ref(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_1", "tx_2"],
            "settled_amount_paise": [1000, 2000],
            "bank_ref_id": ["bnk_1", None],
        }
    )
    path = data_dir / "settlement_null.csv"
    df.to_csv(path, index=False)
    return path


def test_schema_validates_correct_data():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx_1", "tx_2"],
            "settled_amount_paise": [1000, 2000],
            "bank_ref_id": ["bnk_1", "bnk_2"],
        }
    )
    settlement_schema.validate(df, lazy=True)


def test_schema_rejects_duplicate_ids(invalid_csv_duplicate_ids):
    df = pd.read_csv(invalid_csv_duplicate_ids)
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_schema_rejects_negative_amount(invalid_csv_negative_amount):
    df = pd.read_csv(invalid_csv_negative_amount)
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_schema_rejects_null_ref(invalid_csv_null_ref):
    df = pd.read_csv(invalid_csv_null_ref)
    with pytest.raises(SchemaErrors):
        settlement_schema.validate(df, lazy=True)


def test_validate_latest_settlement_success(valid_csv, monkeypatch):
    monkeypatch.chdir(valid_csv.parent.parent)
    with patch("src.validation.validate_settlement.sys.exit") as mock_exit:
        validate_latest_settlement()
        mock_exit.assert_not_called()


def test_validate_latest_settlement_failure(invalid_csv_duplicate_ids, monkeypatch):
    monkeypatch.chdir(invalid_csv_duplicate_ids.parent.parent)
    with patch("src.validation.validate_settlement.sys.exit") as mock_exit:
        validate_latest_settlement()
        mock_exit.assert_called_once_with(1)


def test_validate_latest_settlement_no_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch("src.validation.validate_settlement.sys.exit") as mock_exit:
        validate_latest_settlement()
        mock_exit.assert_called_once_with(1)


def test_schema_yaml_roundtrip(tmp_path):
    yaml_path = tmp_path / "settlement_schema.yaml"
    settlement_schema.to_yaml(str(yaml_path))
    assert yaml_path.exists()

    import pandera.pandas as pa

    loaded = pa.DataFrameSchema.from_yaml(str(yaml_path))
    assert "transaction_id" in loaded.columns
    assert "settled_amount_paise" in loaded.columns
    assert "bank_ref_id" in loaded.columns
