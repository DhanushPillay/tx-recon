"""Settings matrix + table prefix + bench overrides."""

import pytest

from src.common.settings import Settings

pytestmark = pytest.mark.unit


def test_for_airflow_rewrites_hosts():
    s = Settings.for_airflow(Settings())
    assert s.nessie_host == "nessie"
    assert "redpanda" in s.kafka_broker


def test_for_cluster_and_yarn():
    assert Settings.for_cluster(Settings()).spark_master.startswith("spark://")
    assert Settings.for_yarn(Settings()).spark_master == "yarn"


def test_for_local_bench_file_warehouse(monkeypatch):
    monkeypatch.setenv("BENCH_MODE", "local")
    s = Settings.for_local(Settings())
    assert s.spark_shuffle_partitions == 32


def test_table_prefix_rewrite():
    s = Settings(table_prefix="glue")
    assert s.webhook_table.startswith("glue.")
    assert s.dlq_table.startswith("glue.")


def test_bench_warehouse_override(monkeypatch):
    from src.common import settings as mod

    monkeypatch.setenv("BENCH_WAREHOUSE", "file:///tmp/wh")
    mod._settings = None
    assert mod.get_settings().iceberg_warehouse == "file:///tmp/wh"
    mod._settings = None


def test_check_java_home_no_env_noop(monkeypatch):
    from src.common.config import _check_java_home

    monkeypatch.delenv("JAVA_HOME", raising=False)
    _check_java_home()  # must not raise


def test_check_java_home_valid_kept(monkeypatch, tmp_path):
    import os

    from src.common.config import _check_java_home

    bindir = tmp_path / "bin"
    bindir.mkdir()
    exe = "java.exe" if os.name == "nt" else "java"
    (bindir / exe).write_text("x")
    monkeypatch.setenv("JAVA_HOME", str(tmp_path))
    _check_java_home()
    assert os.environ["JAVA_HOME"] == str(tmp_path)


def test_check_java_home_dangling_popped(monkeypatch):
    import os

    from src.common.config import _check_java_home

    monkeypatch.setenv("JAVA_HOME", "/nonexistent-jdk-xyz")
    _check_java_home()
    assert "JAVA_HOME" not in os.environ


def test_get_spark_session_yarn_branch(monkeypatch):
    from unittest.mock import MagicMock, patch

    from src.common import config
    from src.common import settings as settings_mod

    monkeypatch.setenv("SPARK_MODE", "yarn")
    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    settings_mod._settings = None
    with patch.object(config, "SparkSession") as mock_cls:
        builder = MagicMock()
        mock_cls.builder.appName.return_value = builder
        builder.master.return_value = builder
        builder.config.return_value = builder
        builder.getOrCreate.return_value = MagicMock()
        config.get_spark_session("YarnApp")
        keys = [c[0][0] for c in builder.config.call_args_list]
        assert "spark.hadoop.fs.defaultFS" in keys
        assert "spark.submit.deployMode" in keys
    settings_mod._settings = None


def test_get_spark_session_cluster_branch(monkeypatch):
    from unittest.mock import MagicMock, patch

    from src.common import config
    from src.common import settings as settings_mod

    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    settings_mod._settings = None
    with patch.object(config, "SparkSession") as mock_cls:
        builder = MagicMock()
        mock_cls.builder.appName.return_value = builder
        builder.master.return_value = builder
        builder.config.return_value = builder
        builder.getOrCreate.return_value = MagicMock()
        s = settings_mod.get_settings()
        s = s.model_copy(update={"spark_master": "spark://spark-master:7077"})
        settings_mod._settings = s
        config.get_spark_session("ClusterApp")
        keys = [c[0][0] for c in builder.config.call_args_list]
        assert "spark.driver.host" in keys
    settings_mod._settings = None
