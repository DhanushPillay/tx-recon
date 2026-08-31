import importlib
import os
from unittest.mock import MagicMock, patch

import src.common.config as config  # noqa: PLR0402


def test_hosts_default_to_localhost(monkeypatch):
    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    importlib.reload(config)
    assert config.nessie_host == "localhost"
    assert config.minio_host == "localhost"
    assert config.redpanda_host == "localhost"
    assert config.is_airflow is False


def test_hosts_switch_when_airflow_present(monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", "/opt/airflow")
    importlib.reload(config)
    assert config.nessie_host == "nessie"
    assert config.minio_host == "minio"
    assert config.redpanda_host == "redpanda"
    assert config.is_airflow is True


@patch("src.common.config.SparkSession")
def test_get_spark_session_clears_spark_home(mock_spark_cls, monkeypatch):
    monkeypatch.setenv("SPARK_HOME", "/bad/path")
    mock_builder = MagicMock()
    mock_spark_cls.builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config.get_spark_session("TestApp")

    assert "SPARK_HOME" not in os.environ


def test_get_spark_session_config_values(monkeypatch):
    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    importlib.reload(config)

    with patch("src.common.config.SparkSession") as mock_spark_cls:
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        config.get_spark_session("TestApp")

        mock_spark_cls.builder.appName.assert_called_once_with("TestApp")

        all_config_args = [call[0] for call in mock_builder.config.call_args_list]
        config_keys = [args[0] for args in all_config_args]

        assert "spark.sql.catalog.nessie" in config_keys
        assert "spark.sql.catalog.nessie.uri" in config_keys
        assert "spark.hadoop.fs.s3a.access.key" in config_keys
        assert "spark.hadoop.fs.s3a.secret.key" in config_keys

        uri_calls = [
            args
            for args in all_config_args
            if args[0] == "spark.sql.catalog.nessie.uri"
        ]
        assert "http://localhost:19120/api/v1" == uri_calls[0][1]

        key_calls = [
            args
            for args in all_config_args
            if args[0] == "spark.hadoop.fs.s3a.access.key"
        ]
        assert "admin" == key_calls[0][1]
