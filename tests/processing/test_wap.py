"""WAP branch helpers: SQL shapes (live-verified statements in module docstring)."""

from unittest.mock import MagicMock

import pytest

from src.processing import wap

pytestmark = pytest.mark.unit


def test_branch_name():
    assert wap.branch_name("2025-04-02") == "ingest/2025-04-02"


def test_create_drop_merge_branch_sql():
    spark = MagicMock()
    wap.create_branch(spark, "nessie.db.webhooks", "ingest/2025-04-02")
    assert "CREATE BRANCH IF NOT EXISTS ingest/2025-04-02" in spark.sql.call_args[0][0]
    wap.drop_branch(spark, "nessie.db.webhooks", "ingest/2025-04-02")
    assert "DROP BRANCH IF EXISTS ingest/2025-04-02" in spark.sql.call_args[0][0]
    wap.merge_branch(spark, "nessie.db.webhooks", "ingest/2025-04-02")
    sql = spark.sql.call_args[0][0]
    assert "fast_forward" in sql and "to => 'ingest/2025-04-02'" in sql


def test_enable_wap_sets_property():
    spark = MagicMock()
    wap.enable_wap(spark, "nessie.db.webhooks")
    assert "write.wap.enabled" in spark.sql.call_args[0][0]


def test_use_branch_conf():
    spark = MagicMock()
    wap.use_branch(spark, "ingest/2025-04-02")
    spark.conf.set.assert_called_once_with("spark.wap.branch", "ingest/2025-04-02")
    wap.use_branch(spark, None)
    spark.conf.unset.assert_called_once_with("spark.wap.branch")


def test_branch_rejects_injection():
    spark = MagicMock()
    with pytest.raises(ValueError):
        wap.create_branch(spark, "x; DROP", "b")


def test_publish_branch_refuses_unvalidated():
    """Publish gate: merge never runs without validated=True (fail closed)."""
    spark = MagicMock()
    with pytest.raises(RuntimeError, match="unvalidated"):
        wap.publish_branch(spark, "nessie.db.webhooks", "ingest/2025-04-02")
    spark.sql.assert_not_called()


def test_publish_branch_merges_when_validated():
    spark = MagicMock()
    wap.publish_branch(spark, "nessie.db.webhooks", "ingest/2025-04-02", validated=True)
    sql = spark.sql.call_args[0][0]
    assert "fast_forward" in sql and "to => 'ingest/2025-04-02'" in sql
