"""Settings matrix + table prefix + bench overrides."""

import pytest

from src.common.settings import Settings

pytestmark = pytest.mark.unit


def test_for_airflow_rewrites_hosts():
    s = Settings.for_airflow(Settings())
    assert s.nessie_host == "nessie"
    assert "redpanda" in s.kafka_broker


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


def test_fail_closed_defaults():
    """Prod posture out of the box: strict SLO, no destructive seeding."""
    s = Settings()
    assert s.strict_slo is True
    assert s.allow_destructive_seed is False
    assert s.require_schema_registry is False
    assert s.require_kafka_sasl is False


def test_secret_file_resolution(tmp_path, monkeypatch):
    """*_FILE convention loads mounted secrets; explicit env wins."""
    pw = tmp_path / "pw"
    pw.write_text("s3cr3t\n")
    monkeypatch.setenv("MINIO_SECRET_KEY", "")  # .env may provide one; force the FILE path
    monkeypatch.setenv("MINIO_SECRET_KEY_FILE", str(pw))
    assert Settings().minio_secret_key == "s3cr3t"  # noqa: S105 (test fixture)
    monkeypatch.setenv("MINIO_SECRET_KEY", "explicit")
    assert Settings().minio_secret_key == "explicit"  # noqa: S105 (test fixture)


def test_secret_file_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("MINIO_SECRET_KEY", "")
    monkeypatch.setenv("MINIO_SECRET_KEY_FILE", str(tmp_path / "nope"))
    with pytest.raises(ValueError, match="cannot read secret file"):
        Settings()


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
