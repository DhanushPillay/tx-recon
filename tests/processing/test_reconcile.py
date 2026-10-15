import logging
from unittest.mock import MagicMock, patch

import pytest

from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import (
    _qualified_table,
    _run_bank_leg,
    _settlement_source,
    build_bank_leg_sql,
    build_fee_case_sql,
    maintain_tables,
    reconcile_bank_leg,
    refresh_mart,
    run_reconciliation,
)


def _run_with_collects(
    batch_collect,
    table_collect,
    late_n=0,
    dlq_n=0,
    late_total_n=0,
    strict_slo=False,
    lag_n=0,
    fail_at=None,
    fail_exc=None,
):
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
        _valid.repartition.return_value = _valid
        _valid.cache.return_value = _valid
        _valid.count.return_value = 4
        _dedup = _valid.withColumn.return_value.filter.return_value.drop.return_value
        _dedup.count.return_value = 4
        _dedup.filter.return_value.count.return_value = 4
        count_collect = [
            {"scope": "table", "st": r["reconciliation_status"], "n": r["n"]} for r in table_collect
        ] + [{"scope": "batch", "st": r["st"], "n": r["n"]} for r in batch_collect]
        _stages = {
            "MERGE INTO": None,
            "CREATE OR REPLACE": None,
            "SELECT DISTINCT provider": MagicMock(
                collect=MagicMock(return_value=[{"provider": "generic"}])
            ),
            "UPDATE ": None,
            "date_sub(current_date()": MagicMock(collect=MagicMock(return_value=[{"n": late_n}])),
            "BETWEEN 0 AND": MagicMock(collect=MagicMock(return_value=[{"n": lag_n}])),
            "AS scope": MagicMock(collect=MagicMock(return_value=count_collect)),
            "'dlq'": MagicMock(
                collect=MagicMock(
                    return_value=[{"k": "dlq", "n": dlq_n}, {"k": "late", "n": late_total_n}]
                )
            ),
        }

        def _sql(_query, _stages=_stages, _fail_at=fail_at, _fail_exc=fail_exc):
            for _needle, _mock in _stages.items():
                if _needle in _query:
                    if _fail_at is not None and _fail_at == _needle:
                        raise _fail_exc or RuntimeError("boom")
                    return _mock or MagicMock()
            raise AssertionError(f"unmatched query in test: {_query[:120]}")

        mock_spark.sql.side_effect = _sql
        return run_reconciliation(date_str="2025-04-02"), mock_spark


def test_run_reconciliation_requires_date():
    with pytest.raises(ValueError, match="requires date_str"):
        run_reconciliation()
    with pytest.raises(ValueError, match="requires date_str"):
        run_reconciliation(date_str=None)


def test_run_reconciliation_slo_breach_raises_strict(monkeypatch):
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
    assert "(t.amount_paise * 200 + 5000) DIV 10000" in fee_sql
    assert "(t.amount_paise * 0 + 5000) DIV 10000" in fee_sql
    assert "DIV 10000" in gst_sql and "1800" in gst_sql
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
    _valid = mock_bank_df.filter.return_value
    _valid.repartition.return_value = _valid
    _valid.cache.return_value = _valid
    _valid.count.return_value = 4
    _dedup = _valid.withColumn.return_value.filter.return_value.drop.return_value
    _dedup.count.return_value = 4
    _dedup.filter.return_value.count.return_value = 4
    table_collect = [
        {"reconciliation_status": "MATCHED", "n": 3},
        {"reconciliation_status": "EXCEPTION_FEE_MISMATCH", "n": 1},
    ]
    batch_collect = [
        {"st": "MATCHED", "n": 3},
        {"st": "EXCEPTION_FEE_MISMATCH", "n": 1},
    ]
    count_collect = [
        {"scope": "table", "st": r["reconciliation_status"], "n": r["n"]} for r in table_collect
    ] + [{"scope": "batch", "st": r["st"], "n": r["n"]} for r in batch_collect]
    # Positional side_effect: one entry per spark.sql() call, in call order.
    # MERGE, the late-SLA count, the late-SLA UPDATE, the counts query, the
    # depth gauges. There is deliberately no entry for the mart -- run_daily
    # refreshes it after the gates, not run_reconciliation.
    mock_spark.sql.side_effect = [
        MagicMock(),
        MagicMock(collect=MagicMock(return_value=[{"provider": "generic"}])),
        MagicMock(collect=MagicMock(return_value=[{"n": 2}])),
        MagicMock(),
        MagicMock(collect=MagicMock(return_value=[{"n": 0}])),
        MagicMock(collect=MagicMock(return_value=count_collect)),
        MagicMock(collect=MagicMock(return_value=[{"k": "dlq", "n": 1}, {"k": "late", "n": 2}])),
    ]

    counts = run_reconciliation(date_str="2025-04-02")

    mock_spark.read.format.assert_called_with("csv")
    dedup_df = mock_bank_df.filter.return_value.withColumn.return_value.filter.return_value.drop.return_value
    dedup_df.createOrReplaceTempView.assert_called_once_with("bank_settlements")

    merge_sql = mock_spark.sql.call_args_list[0][0][0]
    assert "MERGE INTO" in merge_sql
    assert "nessie.db.webhooks" in merge_sql
    assert "bank_settlements" in merge_sql
    assert "ABS(" in merge_sql
    assert "EXCEPTION_MISSING_WEBHOOK" in merge_sql
    using_clause = merge_sql.split("ON t.transaction_id")[0]
    assert "JOIN" not in using_clause
    assert "IS NOT NULL" in merge_sql, "null guards required on both branches"
    assert merge_sql.count("UPDATE SET") == 3
    assert merge_sql.count("WHEN NOT MATCHED") == 1
    assert "CASE WHEN ABS(" in merge_sql, "single-pass CASE replaces double ABS eval"
    placeholder_clause = merge_sql.split("WHEN MATCHED AND t.amount_paise IS NULL")[0]
    assert "EXCEPTION_MISSING_WEBHOOK" in placeholder_clause
    assert "DELETE" not in merge_sql
    assert "EXCEPTION_LATE_UNRESOLVED" in merge_sql
    assert counts["settlement_rows_deduped"] == 4
    assert counts["MATCHED"] == 3
    assert counts["EXCEPTION_FEE_MISMATCH"] == 1
    assert counts["batch_MATCHED"] == 3
    assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 1
    assert counts["batch_match_rate"] == 0.75
    assert counts["late_unresolved_marked"] == 2
    assert counts["missing_within_lag"] == 0
    assert counts["dlq_depth"] == 1
    assert counts["late_unresolved_total"] == 2
    update_sqls = [c[0][0] for c in mock_spark.sql.call_args_list if c[0][0].startswith("UPDATE ")]
    assert len(update_sqls) == 1
    assert "EXCEPTION_LATE_UNRESOLVED" in update_sqls[0]
    assert "PENDING_RECONCILIATION" in update_sqls[0]
    late_merges = [
        c[0][0]
        for c in mock_spark.sql.call_args_list
        if c[0][0].startswith("MERGE INTO") and "EXCEPTION_LATE_UNRESOLVED" in c[0][0]
    ]
    assert len(late_merges) == 0
    # run_reconciliation must not touch fact_reconciliation. The mart is a
    # published artifact that WAP never branched, so refreshing it here handed a
    # gate-failing batch to every Trino and Metabase reader. run_daily calls
    # refresh_mart after the gates instead; see tests/test_mart_publish_gate.py.
    mart_sqls = [c[0][0] for c in mock_spark.sql.call_args_list if "fact_reconciliation" in c[0][0]]
    assert mart_sqls == []
    count_sqls = [c[0][0] for c in mock_spark.sql.call_args_list if " AS scope" in c[0][0]]
    assert len(count_sqls) == 1
    # three scopes: the whole table, the rows this settlement touched, and the
    # running duplicate-mutation total riding along on the same scan
    assert count_sqls[0].count("UNION ALL") == 2
    assert "SELECT 'table' AS scope" in count_sqls[0]
    assert "SELECT 'batch' AS scope" in count_sqls[0]
    assert "SELECT 'dupmut' AS scope" in count_sqls[0]
    assert "SUM(duplicate_mutation_count)" in count_sqls[0]
    assert "IN (SELECT transaction_id FROM bank_settlements)" in count_sqls[0]
    assert "JOIN bank_settlements" not in count_sqls[0]


def test_run_reconciliation_slo_satisfied(monkeypatch):
    monkeypatch.setenv("MATCH_RATE_SLO", "0.95")
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
        late_n=0,
    )
    assert counts["batch_match_rate"] == 1.0
    assert counts["late_unresolved_marked"] == 0


def test_maintain_tables_own_session(monkeypatch):
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
    mock_spark.stop.assert_not_called()  # shared singleton: stopping kills the caller's session
    assert out["nessie.db.webhooks"]["status"] == "ok"
    assert out["nessie.db.webhooks"]["files_before"] == 5
    assert out["nessie.db.webhooks_dlq"]["files_after"] == 5


def test_maintain_tables_skips_before_opening_session():
    with (
        patch("src.processing.reconcile.get_settings") as mock_get_settings,
        patch("src.processing.reconcile.get_spark_session") as mock_get_spark,
    ):
        mock_get_settings.return_value = MagicMock(maintain_after_merge=False)
        out = maintain_tables()

    mock_get_spark.assert_not_called()
    assert out == {"status": "skipped", "reason": "MAINTAIN_AFTER_MERGE=0"}


def test_reconcile_bank_leg_counts():
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [
        [{"k": "matched", "n": 10}, {"k": "orphans", "n": 2}],
        [{"n": 7}],
    ]
    bank_df = MagicMock()
    out = reconcile_bank_leg(spark, "nessie.db.webhooks", bank_df, lag_days=2)
    assert out == {"bank_evidence": 7, "bank_missing": 3, "bank_orphans": 2}
    bank_df.createOrReplaceTempView.assert_called_once_with("bank_statements")
    merge_sql = spark.sql.call_args_list[1][0][0]
    assert "MERGE INTO nessie.db.webhooks" in merge_sql
    assert "EXCEPTION_MISSING_BANK_STATEMENT" in merge_sql


def test_bank_leg_tolerance_threaded():
    default_merge, default_orphan = build_bank_leg_sql("nessie.db.webhooks")
    for sql in (default_merge, default_orphan):
        assert "CASE" in sql
        assert "'merch_001'" in sql
        assert "CREDIT_CARD" in sql
    merge, orphan = build_bank_leg_sql("nessie.db.webhooks", lag_days=2, tolerance_paise=5)
    assert "<= 5" in merge
    assert "<= 5" in orphan
    assert "CASE" not in merge


def test_late_rows_preserved_not_self_assigned():
    from src.common.schemas import EXCEPTION_LATE_UNRESOLVED, EXCEPTION_MISSING_WEBHOOK

    counts, mock_spark = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 4}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 4}],
    )
    merges = [c[0][0] for c in mock_spark.sql.call_args_list if "MERGE INTO" in c[0][0]]
    assert merges, "expected a batch MERGE"
    assert not any("t.bank_ref_id = t.bank_ref_id" in m for m in merges)
    assert any(EXCEPTION_MISSING_WEBHOOK in m and EXCEPTION_LATE_UNRESOLVED in m for m in merges), (
        "MISSING + LATE must share one preserve clause"
    )


def test_bank_leg_single_union_for_before_and_orphans():
    spark = MagicMock()
    bank_df = MagicMock()

    def _sql(q):
        m = MagicMock()
        if "UNION ALL" in q:
            m.collect.return_value = [{"k": "matched", "n": 10}, {"k": "orphans", "n": 2}]
        else:
            m.collect.return_value = [{"n": 7}]
        return m

    spark.sql.side_effect = _sql
    out = reconcile_bank_leg(spark, "nessie.db.webhooks", bank_df, lag_days=2)
    assert out == {"bank_evidence": 7, "bank_missing": 3, "bank_orphans": 2}
    union_qs = [c[0][0] for c in spark.sql.call_args_list if "UNION ALL" in c[0][0]]
    assert len(union_qs) == 1
    assert "'matched'" in union_qs[0] and "'orphans'" in union_qs[0]


def test_maintain_tables_statement_order(monkeypatch):
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
    ] * 3


def test_maintain_tables_expires_the_mart_to_one_generation(monkeypatch):
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
    mart = [s for s in stmts if "'db.fact_reconciliation'" in s and "expire_snapshots" in s]
    assert mart, "mart is never expired, so every batch's orphaned files leak"
    assert "retain_last => 1" in mart[0]
    hooks = [s for s in stmts if "'db.webhooks'" in s and "expire_snapshots" in s]
    assert "retain_last => 7" in hooks[0]


def test_fee_sql_gst_zero_branch():
    from src.processing.reconcile import _build_single_card_fee_sql

    card = {"default": {"mdr_rate_bps": 150, "gst_on_mdr": 0, "tolerance_paise": 1}}
    _fee, gst, _tol = _build_single_card_fee_sql(
        card, "t.amount_paise", "s.instrument_type", "s.merchant_id"
    )
    assert "THEN 0" in gst


def test_maintain_tables_records_statement_failure(monkeypatch):
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
        ok = MagicMock(collect=MagicMock(return_value=[{"n": 5}]))

        def _sql(q):
            if "expire_snapshots" in q:
                raise RuntimeError("boom")
            return ok

        mock_spark.sql.side_effect = _sql
        out = maintain_tables()
    assert out["nessie.db.webhooks"]["status"] == "failed"
    assert out["nessie.db.webhooks"]["error"] == "statement: boom"
    assert out["nessie.db.webhooks_dlq"]["status"] == "failed"


def test_settlement_source_rejects_a_date_that_escapes_the_data_dir(tmp_path):
    for bad in ("../../etc/passwd", "2025-04-02/../../x", "20250402; DROP", "not-a-date"):
        with pytest.raises(ValueError, match="Bad date_str"):
            _settlement_source(str(tmp_path), bad)


def test_settlement_source_fails_closed_without_a_curated_file(tmp_path, monkeypatch):
    monkeypatch.delenv("ALLOW_UNVALIDATED_SETTLEMENT", raising=False)
    with pytest.raises(FileNotFoundError, match="curated settlement not found"):
        _settlement_source(str(tmp_path), "2025-04-02")


def test_settlement_source_escape_hatch_is_opt_in_only(tmp_path, monkeypatch):
    raw = tmp_path / "settlement_20250402.csv"
    raw.write_text("transaction_id\ntx_1\n")
    for value in ("0", "", "yes", "TRUEISH"):
        monkeypatch.setenv("ALLOW_UNVALIDATED_SETTLEMENT", value)
        with pytest.raises(FileNotFoundError):
            _settlement_source(str(tmp_path), "2025-04-02")
    for value in ("1", "true", "TRUE"):
        monkeypatch.setenv("ALLOW_UNVALIDATED_SETTLEMENT", value)
        path, files = _settlement_source(str(tmp_path), "2025-04-02")
        assert path.endswith("settlement_20250402.csv")
        assert len(files) == 1


def test_settlement_source_prefers_the_curated_file_over_the_raw_drop(tmp_path):
    (tmp_path / "settlement_20250402.csv").write_text("transaction_id\ntx_1\n")
    curated = tmp_path / "curated_settlement_20250402.csv"
    curated.write_text("transaction_id\ntx_1\n")
    path, files = _settlement_source(str(tmp_path), "2025-04-02")
    assert path.endswith("curated_settlement_20250402.csv")
    assert files == [str(curated)]


def test_merge_failure_raises_a_runtime_error_naming_the_merge():
    with pytest.raises(RuntimeError, match="Reconciliation MERGE failed"):
        _run_with_collects(
            batch_collect=[],
            table_collect=[],
            fail_at="MERGE INTO",
            fail_exc=ValueError("spark exploded"),
        )


def _mart_settings(strict_slo: bool) -> MagicMock:
    return MagicMock(
        project_root=".",
        webhook_table="nessie.db.webhooks",
        strict_slo=strict_slo,
    )


def _mart_spark() -> MagicMock:
    spark = MagicMock()
    spark.sql.side_effect = ValueError("spark exploded")
    return spark


# These two used to run through _run_with_collects, because the mart refresh
# lived inside run_reconciliation. It moved out to refresh_mart so it can run
# after the publish gates; the strict/non-strict contract came with it.
def test_mart_refresh_failure_fails_the_batch_under_strict():
    with pytest.raises(RuntimeError, match="Could not refresh fact_reconciliation"):
        refresh_mart(_mart_spark(), _mart_settings(True))


def test_mart_refresh_failure_only_warns_when_not_strict():
    refresh_mart(_mart_spark(), _mart_settings(False))


def test_mart_refresh_issues_one_create_against_the_mart_table():
    spark = MagicMock()
    refresh_mart(spark, _mart_settings(True))
    (sql,) = [c[0][0] for c in spark.sql.call_args_list]
    assert "CREATE OR REPLACE TABLE nessie.db.fact_reconciliation" in sql
    assert "FROM nessie.db.webhooks" in sql


def test_late_sweep_failure_publishes_the_flag_the_alert_reads():
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 1}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 1}],
        late_n=1,
        fail_at="UPDATE ",
    )
    assert counts["late_sla_failed"] == 1
    assert "late_unresolved_marked" not in counts


def test_status_count_failure_raises():
    with pytest.raises(RuntimeError, match="Could not fetch reconciliation counts"):
        _run_with_collects(batch_collect=[], table_collect=[], fail_at="AS scope")


def test_depth_gauge_failure_degrades_to_defaults():
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 1}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 1}],
        fail_at="'dlq'",
    )
    assert counts["dlq_depth"] == 0
    assert counts["late_unresolved_total"] == 0


def test_unparseable_env_knobs_fall_back_instead_of_crashing(monkeypatch):
    monkeypatch.setenv("MATCH_RATE_SLO", "not-a-number")
    monkeypatch.setenv("LATE_SLA_DAYS", "seven")
    counts, _ = _run_with_collects(
        batch_collect=[{"st": "MATCHED", "n": 1}],
        table_collect=[{"reconciliation_status": "MATCHED", "n": 1}],
    )
    assert counts["batch_MATCHED"] == 1


def test_bad_maintain_retain_last_falls_back(monkeypatch):
    monkeypatch.setenv("MAINTAIN_RETAIN_LAST", "forever")
    with (
        patch("src.common.config.get_spark_session") as mock_get_spark,
        patch("src.common.settings.get_settings") as mock_get_settings,
    ):
        mock_get_settings.return_value = MagicMock(
            webhook_table="nessie.db.webhooks", dlq_table="nessie.db.webhooks_dlq"
        )
        mock_spark = MagicMock()
        mock_spark.sql.return_value = MagicMock(collect=MagicMock(return_value=[{"n": 3}]))
        mock_get_spark.return_value = mock_spark
        maintain_tables()
    expires = [c.args[0] for c in mock_spark.sql.call_args_list if "expire_snapshots" in c.args[0]]
    assert any("retain_last => 7" in q for q in expires)


def _bank_leg_settings(**kw):
    base = dict(
        project_root=".",
        webhook_table="nessie.db.webhooks",
        dlq_table="nessie.db.webhooks_dlq",
        strict_slo=False,
        wired_legs="",
    )
    base.update(kw)
    return MagicMock(**base)


def test_bank_leg_is_skipped_when_not_in_wired_legs(tmp_path):
    with patch("src.processing.reconcile.get_settings") as g:
        g.return_value = _bank_leg_settings(wired_legs="wap,residual")
        out = _run_bank_leg(
            MagicMock(), "nessie.db.webhooks", str(tmp_path), "2025-04-02", g.return_value
        )
    assert out == {"bank_statement": 0}


def test_missing_statement_file_keeps_the_batch_rather_than_failing(tmp_path, caplog):
    settings = _bank_leg_settings()
    with patch("src.processing.reconcile.get_settings") as g:
        g.return_value = settings
        with caplog.at_level(logging.WARNING):
            out = _run_bank_leg(
                MagicMock(), "nessie.db.webhooks", str(tmp_path), "2025-04-02", settings
            )
    assert out == {"bank_statement": 0}
    assert "no statement file" in caplog.text


def test_bank_leg_failure_fails_the_batch_only_under_strict(tmp_path):
    settings = _bank_leg_settings(strict_slo=True)
    (tmp_path / "statement_20250402.sta").write_text("garbage\n")
    with patch("src.processing.reconcile.get_settings") as g:
        g.return_value = settings
        with pytest.raises(RuntimeError, match="Bank leg failed"):
            _run_bank_leg(MagicMock(), "nessie.db.webhooks", str(tmp_path), "2025-04-02", settings)


def test_bank_leg_failure_only_warns_when_not_strict(tmp_path):
    settings = _bank_leg_settings()
    (tmp_path / "statement_20250402.sta").write_text("garbage\n")
    with patch("src.processing.reconcile.get_settings") as g:
        g.return_value = settings
        out = _run_bank_leg(
            MagicMock(), "nessie.db.webhooks", str(tmp_path), "2025-04-02", settings
        )
    assert out == {"bank_statement": 0}


_BANK_PDF_ROWS = [
    {
        "bank_ref": "b1",
        "amount_paise": 100000,
        "value_date": "2025-04-02",
        "direction": "CREDIT",
        "link_tx": "tx_abcdef",
        "narration": "TXN123456",
    }
]


def test_bank_leg_bridges_pandas_statements_to_spark():
    import pandas as pd

    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [
        [{"k": "matched", "n": 1}, {"k": "orphans", "n": 0}],
        [{"n": 1}],
    ]
    pdf = pd.DataFrame(_BANK_PDF_ROWS)
    view = MagicMock(name="bank_view")
    spark.createDataFrame.return_value = view
    out = reconcile_bank_leg(spark, "nessie.db.webhooks", pdf, lag_days=2)
    spark.createDataFrame.assert_called_once_with(pdf)
    view.createOrReplaceTempView.assert_called_once_with("bank_statements")
    assert out == {"bank_evidence": 1, "bank_missing": 0, "bank_orphans": 0}


def test_run_bank_leg_hands_parse_output_to_reconcile(tmp_path):
    import pandas as pd

    pdf = pd.DataFrame(_BANK_PDF_ROWS)
    (tmp_path / "statement_20250402.sta").write_text("dummy\n")
    settings = _bank_leg_settings()
    with (
        patch("src.adapters.bank_statement.parse_mt940", return_value=pdf),
        patch("src.processing.batches.load_providers", return_value={}),
        patch("src.processing.batches.provider_spec", return_value={"lag_days": 2}),
        patch(
            "src.processing.reconcile.reconcile_bank_leg",
            return_value={"bank_evidence": 1, "bank_missing": 0, "bank_orphans": 0},
        ) as rec,
        patch("src.processing.reconcile.get_settings", return_value=settings),
    ):
        out = _run_bank_leg(
            MagicMock(), "nessie.db.webhooks", str(tmp_path), "2025-04-02", settings
        )
    assert rec.call_args[0][2] is pdf
    assert out == {
        "bank_statement": 1,
        "bank_evidence": 1,
        "bank_missing": 0,
        "bank_orphans": 0,
    }
