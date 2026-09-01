import os
from unittest.mock import MagicMock, patch

from src.common import settings as settings_mod
from src.common.settings import Settings


def test_settings_loads_defaults(monkeypatch):
    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    settings_mod._settings = None
    s = Settings()
    assert s.nessie_host == "localhost"
    assert "localhost" in s.minio_endpoint
    assert s.redpanda_host == "localhost"
    assert s.is_airflow is False


def test_settings_switch_when_airflow_present(monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", "/opt/airflow")
    settings_mod._settings = None
    s = Settings.for_airflow()
    assert s.nessie_host == "nessie"
    assert "minio" in s.minio_endpoint
    assert s.redpanda_host == "redpanda"
    assert s.is_airflow is True


@patch("src.common.config.SparkSession")
def test_get_spark_session_clears_spark_home(mock_spark_cls, monkeypatch):
    from src.common import config

    monkeypatch.setenv("SPARK_HOME", "/bad/path")
    mock_builder = MagicMock()
    mock_spark_cls.builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config.get_spark_session("TestApp")

    assert "SPARK_HOME" not in os.environ


def test_get_spark_session_config_values(monkeypatch):
    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    settings_mod._settings = None
    from src.common import config

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

        uri_calls = [args for args in all_config_args if args[0] == "spark.sql.catalog.nessie.uri"]
        assert uri_calls[0][1] == "http://localhost:19120/api/v1"

        key_calls = [
            args for args in all_config_args if args[0] == "spark.hadoop.fs.s3a.access.key"
        ]
        assert key_calls[0][1] == "admin"
