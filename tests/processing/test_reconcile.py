"""Reconcile wiring tests: pure builder logic + MERGE SQL shape (mocked Spark)."""

from unittest.mock import MagicMock, patch

import pytest

from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import (
    _qualified_table,
    build_fee_case_sql,
    maintain_tables,
    reconcile_bank_leg,
    run_reconciliation,
)


def _run_with_collects(
    batch_collect, table_collect, late_n=0, dlq_n=0, late_total_n=0, strict_slo=False
):
    """Drive run_reconciliation with a mocked Spark; returns (counts, mock_spark)."""
    with (
        patch("pyspark.sql.functions.row_number"),
        patch("pyspark.sql.functions.col"),
        patch("pyspark.sql.window.Window"),
        patch("src.processing.reconcile.get_settings") as mock_get_settings,
        patch("src.processing.reconcile.get_spark_session") as mock_get_spark,
        patch(
            "src.processing.reconcile._settlement_source",
            return_value=("data/settlement_20250402.csv", ["data/settlement_20250402.csv"]),
        ),
    ):
        mock_get_settings.return_value = MagicMock(
            project_root=".",
            load_csv_on_driver=False,
            webhook_table="nessie.db.webhooks",
            dlq_table="nessie.db.webhooks_dlq",
            spark_shuffle_partitions=32,
            strict_slo=strict_slo,
        )
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        mock_bank_df = MagicMock()
        mock_spark.read.format.return_value.option.return_value.schema.return_value.load.return_value = mock_bank_df
        _valid = mock_bank_df.filter.return_value
        _valid.cache.return_value = _valid
        _valid.count.return_value = 4
        _dedup = _valid.withColumn.return_value.filter.return_value.drop.return_value
        _dedup.repartition.return_value.count.return_value = 4
        # Late UPDATE runs only when late_n > 0; gauges are one UNION collect.
        # Provider-aware late block first resolves DISTINCT providers, then the
        # side count, the within-lag gauge, and the optional late MERGE.
        _side = [
            MagicMock(),  # MERGE
            MagicMock(),  # mart CTAS (runs before counts)
            MagicMock(collect=MagicMock(return_value=table_collect)),  # table-level
            MagicMock(collect=MagicMock(return_value=batch_collect)),  # batch-scoped
            MagicMock(collect=MagicMock(return_value=[{"provider": "generic"}])),
            MagicMock(collect=MagicMock(return_value=[{"n": late_n}])),  # late-SLA side count
            MagicMock(collect=MagicMock(return_value=[{"n": 0}])),  # within-lag gauge
        ]
        if late_n:
            _side.append(MagicMock())  # late-SLA MERGE
        _side.append(
            MagicMock(
                collect=MagicMock(
                    return_value=[{"k": "dlq", "n": dlq_n}, {"k": "late", "n": late_total_n}]
                )
            )
        )
        mock_spark.sql.side_effect = _side
        return run_reconciliation(date_str="2025-04-02"), mock_spark


def test_run_reconciliation_requires_date():
    with pytest.raises(ValueError, match="requires date_str"):
        run_reconciliation()
    with pytest.raises(ValueError, match="requires date_str"):
        run_reconciliation(date_str=None)


def test_run_reconciliation_slo_breach_raises_strict(monkeypatch):
    """Strict mode (prod default): sub-SLO batch fails instead of warning."""
    monkeypatch.setenv("MATCH_RATE_SLO", "0.95")
    with pytest.raises(RuntimeError, match="SLO breach"):
        _run_with_collects(
            batch_collect=[{"st": "MATCHED", "n": 1}],
            table_collect=[{"reconciliation_status": "MATCHED", "n": 1}],
            strict_slo=True,
        )


def test_fee_case_sql_matches_fee_engine():
    engine = FeeEngine()
    fee_sql, gst_sql = build_fee_case_sql(engine)
    assert "CREDIT_CARD" in fee_sql and "UPI" in fee_sql
    assert "(t.amount_paise * 200 + 5000) DIV 10000" in fee_sql  # 200bps CC
    assert "(t.amount_paise * 0 + 5000) DIV 10000" in fee_sql  # 0bps UPI
    assert "DIV 10000" in gst_sql and "1800" in gst_sql  # 18% GST in bps
    for inst in ("DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"):
        assert inst in fee_sql
    assert "ELSE" in fee_sql, "unknown instruments must fall back to default rate"


def test_fee_case_sql_custom_columns():
    engine = FeeEngine()
    fee_sql, _ = build_fee_case_sql(
        engine, amount_col="w.amount_paise", inst_col="w.instrument_type"
    )
    assert "w.amount_paise" in fee_sql
    assert "w.instrument_type" in fee_sql


def test_qualified_table_rejects_injection():
    with pytest.raises(ValueError):
        _qualified_table("nessie.db.webhooks; DROP TABLE x --")
    for bad in ("", "a b", "a-b", None):
        with pytest.raises((ValueError, TypeError, AttributeError)):
            _qualified_table(bad)
    assert _qualified_table("nessie.db.webhooks") == "nessie.db.webhooks"


@patch("pyspark.sql.functions.row_number")
@patch("pyspark.sql.functions.col")
@patch("pyspark.sql.window.Window")
@patch("src.processing.reconcile.get_settings")
@patch("src.processing.reconcile.get_spark_session")
@patch(
    "src.processing.reconcile._settlement_source",
    return_value=("data/settlement_20250402.csv", ["data/settlement_20250402.csv"]),
)
def test_run_reconciliation_wiring(
    mock_source,
    mock_get_spark,
    mock_get_settings,
    mock_window,
    mock_col,
    mock_row_number,
    monkeypatch,
):
    monkeypatch.setenv("MATCH_RATE_SLO", "0.95")
    mock_get_settings.return_value = MagicMock(
        project_root=".",
        load_csv_on_driver=False,
        webhook_table="nessie.db.webhooks",
        dlq_table="nessie.db.webhooks_dlq",
        spark_shuffle_partitions=32,
        strict_slo=False,
    )
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark

    mock_bank_df = MagicMock()
    mock_spark.read.format.return_value.option.return_value.schema.return_value.load.return_value = mock_bank_df
    # counts path: 4 deduped settlement rows; table holds 3 MATCHED + 1 FEE_MISMATCH
    _valid = mock_bank_df.filter.return_value
    _valid.cache.return_value = _valid
    _valid.count.return_value = 4
    _valid.withColumn.return_value.filter.return_value.drop.return_value.repartition.return_value.count.return_value = 4
    table_collect = [
        {"reconciliation_status": "MATCHED", "n": 3},
        {"reconciliation_status": "EXCEPTION_FEE_MISMATCH", "n": 1},
    ]
    batch_collect = [
        {"st": "MATCHED", "n": 3},
        {"st": "EXCEPTION_FEE_MISMATCH", "n": 1},
    ]
    mock_spark.sql.side_effect = [
        MagicMock(),  # MERGE
        MagicMock(),  # mart table (runs before counts)
        MagicMock(collect=MagicMock(return_value=table_collect)),  # table-level
        MagicMock(collect=MagicMock(return_value=batch_collect)),  # batch-scoped
        MagicMock(collect=MagicMock(return_value=[{"provider": "generic"}])),
        MagicMock(collect=MagicMock(return_value=[{"n": 2}])),  # late-SLA side count
        MagicMock(collect=MagicMock(return_value=[{"n": 1}])),  # within-lag gauge
        MagicMock(),  # late-SLA MERGE (late_n=2 > 0)
        MagicMock(
            collect=MagicMock(return_value=[{"k": "dlq", "n": 1}, {"k": "late", "n": 2}])
        ),  # gauges UNION
    ]

    counts = run_reconciliation(date_str="2025-04-02")

    mock_spark.read.format.assert_called_with("csv")
    dedup_df = mock_bank_df.filter.return_value.withColumn.return_value.filter.return_value.drop.return_value.repartition.return_value
    dedup_df.createOrReplaceTempView.assert_called_once_with("bank_settlements")

    merge_sql = mock_spark.sql.call_args_list[0][0][0]
    assert "MERGE INTO" in merge_sql
    assert "nessie.db.webhooks" in merge_sql
    assert "bank_settlements" in merge_sql
    assert "ABS(" in merge_sql  # tolerance-aware matching
    assert "EXCEPTION_MISSING_WEBHOOK" in merge_sql
    # USING source must be settlements only: joining the target filters out
    # orphans and makes WHEN NOT MATCHED dead (regression pin for the P0 bug).
    using_clause = merge_sql.split("ON t.transaction_id")[0]
    assert "JOIN" not in using_clause
    assert "IS NOT NULL" in merge_sql, "null guards required on both branches"
    assert (
        merge_sql.count("UPDATE SET") == 4
    )  # placeholder-keep + late-keep + null-amount + single-pass CASE (matched/mismatched)
    assert merge_sql.count("WHEN NOT MATCHED") == 1
    assert "CASE WHEN ABS(" in merge_sql, "single-pass CASE replaces double ABS eval"
    # Placeholder rows (inserted by WHEN NOT MATCHED on an earlier run) must
    # keep EXCEPTION_MISSING_WEBHOOK on rerun, not flip to FEE_MISMATCH.
    placeholder_clause = merge_sql.split("WHEN MATCHED AND t.amount_paise IS NULL")[0]
    assert "EXCEPTION_MISSING_WEBHOOK" in placeholder_clause
    assert "DELETE" not in merge_sql
    # Late-SLA rows must survive rerun: LATE_UNRESOLVED is terminal for the
    # batch MERGE (only the late-SLA UPDATE writes it), else reruns flap
    # LATE -> MATCHED -> LATE and idempotency breaks.
    assert "EXCEPTION_LATE_UNRESOLVED" in merge_sql
    assert counts["settlement_rows_deduped"] == 4
    assert counts["MATCHED"] == 3
    assert counts["EXCEPTION_FEE_MISMATCH"] == 1
    assert counts["batch_MATCHED"] == 3
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 1
    # Match-rate SLO: 3/4 = 0.75 < 0.95 -> breach recorded as float rate.
    assert counts["batch_match_rate"] == 0.75
    # Late-SLA side-output count + ops depth gauges.
    assert counts["late_unresolved_marked"] == 2
    assert counts["missing_within_lag"] == 1
    assert counts["dlq_depth"] == 1
    assert counts["late_unresolved_total"] == 2
    update_sqls = [c[0][0] for c in mock_spark.sql.call_args_list if c[0][0].startswith("UPDATE ")]
    assert len(update_sqls) == 0  # late pass is MERGE (Iceberg has no UPDATE..FROM)
    late_merges = [
        c[0][0]
        for c in mock_spark.sql.call_args_list
        if c[0][0].startswith("MERGE INTO") and "EXCEPTION_LATE_UNRESOLVED" in c[0][0]
    ]
    assert len(late_merges) == 1
    # Mart table must be materialized on the same namespace as the target table.
    mart_sqls = [c[0][0] for c in mock_spark.sql.call_args_list if "fact_reconciliation" in c[0][0]]
    assert len(mart_sqls) == 1
    assert "CREATE OR REPLACE TABLE" in mart_sqls[0]
    assert "nessie.db.fact_reconciliation" in mart_sqls[0]
    assert "FROM nessie.db.webhooks" in mart_sqls[0]


def test_run_reconciliation_slo_satisfied(monkeypatch):
    """4/4 batch MATCHED -> rate 1.0, no breach; zero late rows -> no late warning."""
    monkeypatch.setenv("MATCH_RATE_SLO", "0.95")
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
        late_n=0,
    )
    assert counts["batch_match_rate"] == 1.0
    assert counts["late_unresolved_marked"] == 0


def test_maintain_tables_own_session(monkeypatch):
    """spark=None opens its own session and stops it (own_session branch)."""
    monkeypatch.setenv("MAINTAIN_RETAIN_LAST", "7")
    with (
        patch("src.common.config.get_spark_session") as mock_get_spark,
        patch("src.common.settings.get_settings") as mock_get_settings,
    ):
        mock_get_settings.return_value = MagicMock(
            webhook_table="nessie.db.webhooks", dlq_table="nessie.db.webhooks_dlq"
        )
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value = MagicMock(collect=MagicMock(return_value=[{"n": 5}]))
        out = maintain_tables()
    mock_get_spark.assert_called_once_with("Maintenance")
    mock_spark.stop.assert_called_once()
    assert out["nessie.db.webhooks"]["status"] == "ok"
    assert out["nessie.db.webhooks"]["files_before"] == 5
    assert out["nessie.db.webhooks_dlq"]["files_after"] == 5


def test_reconcile_bank_leg_counts():
    """Bank leg arithmetic: evidence kept, gap demoted, orphans counted."""
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [[{"n": 10}], [{"n": 7}], [{"n": 2}]]
    bank_df = MagicMock()
    out = reconcile_bank_leg(spark, "nessie.db.webhooks", bank_df, lag_days=2)
    assert out == {"bank_evidence": 7, "bank_missing": 3, "bank_orphans": 2}
    bank_df.createOrReplaceTempView.assert_called_once_with("bank_statements")
    merge_sql = spark.sql.call_args_list[1][0][0]
    assert "MERGE INTO nessie.db.webhooks" in merge_sql
    assert "EXCEPTION_MISSING_BANK_STATEMENT" in merge_sql


def test_maintain_tables_statement_order(monkeypatch):
    """Expire -> orphan -> binpack -> manifests. Reversed order wastes work
    (compacting files about to expire) or corrupts (sweeping live files)."""
    monkeypatch.setenv("MAINTAIN_RETAIN_LAST", "7")
    with (
        patch("src.common.config.get_spark_session") as mock_get_spark,
        patch("src.common.settings.get_settings") as mock_get_settings,
    ):
        mock_get_settings.return_value = MagicMock(
            webhook_table="nessie.db.webhooks", dlq_table="nessie.db.webhooks_dlq"
        )
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value = MagicMock(collect=MagicMock(return_value=[{"n": 5}]))
        maintain_tables()
    stmts = [c[0][0] for c in mock_spark.sql.call_args_list]
    ops = [s for s in stmts if s.startswith("CALL ")]
    import re as _re

    assert [_re.search(r"system\.(\w+)\s*\(", o).group(1) for o in ops] == [
        "expire_snapshots",
        "remove_orphan_files",
        "rewrite_data_files",
        "rewrite_manifests",
    ] * 2  # once per table (webhooks + dlq)
