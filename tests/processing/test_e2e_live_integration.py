# ruff: noqa: S608 -- fixtures below are literal test SQL, not input.
"""Live end-to-end on real Spark + Nessie + MinIO (integration).

validate -> MERGE -> residual -> drift/SLO -> mart, three times identical
(soak); a mixed-outcome batch tiles exactly; DLQ replay moves rows and the
purge is maker-checker journaled; a PAN row refuses the replay (chaos); the
Trino dashboard query sees the mart. Needs `docker compose up` infra.
"""

import os
import sys
from unittest.mock import patch

import pytest

from src.common.config import get_spark_session
from src.common.settings import get_settings
from src.ingestion.ingest_webhooks import ensure_dlq_table, ensure_webhook_table
from src.processing.reconcile import _mart_table, refresh_mart, run_reconciliation

pytestmark = pytest.mark.integration

CANONICAL = (
    "bank_ref_id,transaction_id,settled_amount_paise,settlement_date,"
    "instrument_type,merchant_id,fee_paise,gst_paise,settlement_id,utr,"
    "currency,gross_amount_paise,provider"
)

BI_COLUMNS = {
    "transaction_id",
    "amount_paise",
    "merchant_id",
    "instrument_type",
    "gateway_status",
    "status",
    "bank_ref_id",
    "transacted_at",
    "matched_amount_paise",
}


def _write_curated(data_dir, stamp, rows):
    path = os.path.join(data_dir, f"curated_settlement_{stamp}.csv")
    with open(path, "w") as fh:
        fh.write(CANONICAL + "\n")
        for r in rows:
            fh.write(",".join(map(str, r)) + "\n")
    return path


def _seed_webhooks(spark, table, rows):
    vals = ",".join(
        f"('{t}', {a}, 'SUCCESS', '{ts}', 'UNKNOWN', 'live-seed', "
        f"'PENDING_SETTLEMENT', NULL, current_timestamp(), '{i}', NULL, NULL, 'INR')"
        for t, a, ts, i in rows
    )
    spark.sql(
        f"INSERT INTO {table} (transaction_id, amount_paise, gateway_status, "
        f"timestamp_utc, merchant_id, processing_run_id, reconciliation_status, "
        f"bank_ref_id, ingested_at, instrument_type, duplicate_mutation_count, "
        f"last_duplicate_at, currency) VALUES {vals}"
    )


def test_live_green_batch_soaks_stable_and_builds_mart():
    settings = get_settings()
    spark = get_spark_session("LiveE2ETest")
    table = settings.webhook_table
    ensure_webhook_table(spark, table)
    data_dir = os.path.join(settings.project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    path = _write_curated(
        data_dir,
        "19990202",
        [
            (
                "bnk_live_a1",
                "tx_live_a1",
                100000,
                "1999-02-02",
                "UPI",
                "UNKNOWN",
                0,
                0,
                "st_a1",
                "u_a1",
                "INR",
                100000,
                "gateway",
            ),
            (
                "bnk_live_a2",
                "tx_live_a2",
                50000,
                "1999-02-02",
                "UPI",
                "UNKNOWN",
                0,
                0,
                "st_a2",
                "u_a2",
                "INR",
                50000,
                "gateway",
            ),
        ],
    )
    try:
        _seed_webhooks(
            spark,
            table,
            [
                ("tx_live_a1", 100000, "1999-02-02T00:00:00", "UPI"),
                ("tx_live_a2", 50000, "1999-02-02T00:00:00", "UPI"),
            ],
        )
        runs = [run_reconciliation("1999-02-02") for _ in range(3)]
        assert runs[0] == runs[1] == runs[2]
        assert runs[0]["settlement_rows_evaluated"] == 2
        assert runs[0]["batch_MATCHED"] == 2
        assert runs[0]["batch_match_rate"] == 1.0

        # The pipeline refreshes the mart only after its gates pass, so a
        # direct call to run_reconciliation has to ask for it explicitly.
        refresh_mart(spark, settings)

        mart = spark.table(_mart_table(table))
        assert set(mart.columns) >= BI_COLUMNS
        got = {
            r["transaction_id"]: (r["status"], r["matched_amount_paise"])
            for r in mart.filter("transaction_id LIKE 'tx_live_a%'").collect()
        }
        assert got == {
            "tx_live_a1": ("MATCHED", 100000),
            "tx_live_a2": ("MATCHED", 50000),
        }
    finally:
        os.remove(path)


def test_live_mixed_batch_tiles_exactly(monkeypatch):
    monkeypatch.setenv("MATCH_RATE_SLO", "0.0")
    settings = get_settings()
    spark = get_spark_session("LiveE2ETest")
    table = settings.webhook_table
    ensure_webhook_table(spark, table)
    data_dir = os.path.join(settings.project_root, "data")
    path = _write_curated(
        data_dir,
        "19990303",
        [
            (
                "bnk_live_b1",
                "tx_live_b1",
                100000,
                "1999-03-03",
                "UPI",
                "UNKNOWN",
                0,
                0,
                "st_b1",
                "u_b1",
                "INR",
                100000,
                "gateway",
            ),
            (
                "bnk_live_b2",
                "tx_live_b2",
                99900,
                "1999-03-03",
                "UPI",
                "UNKNOWN",
                0,
                0,
                "st_b2",
                "u_b2",
                "INR",
                100000,
                "gateway",
            ),
            (
                "bnk_live_b3",
                "tx_live_b3",
                75000,
                "1999-03-03",
                "UPI",
                "UNKNOWN",
                0,
                0,
                "st_b3",
                "u_b3",
                "INR",
                75000,
                "gateway",
            ),
        ],
    )
    try:
        _seed_webhooks(
            spark,
            table,
            [
                ("tx_live_b1", 100000, "1999-03-03T00:00:00", "UPI"),
                ("tx_live_b2", 100000, "1999-03-03T00:00:00", "UPI"),
            ],
        )
        counts = run_reconciliation("1999-03-03")
        assert counts["settlement_rows_evaluated"] == 3
        assert counts["batch_MATCHED"] == 1
        assert counts["batch_EXCEPTION_FEE_MISMATCH"] == 1
        orphan = counts.get("batch_EXCEPTION_MISSING_WEBHOOK", 0) + counts.get(
            "batch_EXCEPTION_LATE_UNRESOLVED", 0
        )
        assert orphan == 1
        tiled = counts["batch_MATCHED"] + counts["batch_EXCEPTION_FEE_MISMATCH"] + orphan
        assert tiled == 3
    finally:
        os.remove(path)


def _journal_path(settings):
    return os.path.join(settings.project_root, "data", "corrections.jsonl")


def _journal_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return fh.readlines()


def test_live_dlq_replay_moves_rows_and_purge_journals():
    import scripts.replay_dlq as replay_dlq

    settings = get_settings()
    spark = get_spark_session("LiveE2ETest")
    ensure_webhook_table(spark, settings.webhook_table)
    ensure_dlq_table(spark, settings.dlq_table)
    spark.sql(
        f"INSERT INTO {settings.dlq_table} (transaction_id, amount_paise, gateway_status, "
        "timestamp_utc, merchant_id, processing_run_id, payload_hash) VALUES "
        "('tx_dlq_001', 1000, 'SUCCESS', '1999-04-04T00:00:00', 'UNKNOWN', 'run-dlq', NULL),"
        "(NULL, -5, 'SUCCESS', '1999-04-04T00:00:00', 'UNKNOWN', 'run-dlq', NULL)"
    )
    journal = _journal_path(settings)
    before = _journal_lines(journal)
    try:
        with patch.object(sys, "argv", ["replay_dlq.py"]):
            out = replay_dlq.main()
        assert out == {
            "replayable": 1,
            "inserted": 1,
            "deleted": False,
            "reversal_stranded": 0,
        }
        pending = spark.sql(
            f"SELECT reconciliation_status FROM {settings.webhook_table} "
            "WHERE transaction_id = 'tx_dlq_001'"
        ).collect()
        assert [r["reconciliation_status"] for r in pending] == ["PENDING_RECONCILIATION"]

        with patch.object(
            sys,
            "argv",
            ["replay_dlq.py", "--delete", "--reason", "T-LIVE-1", "--approved-by", "ci"],
        ):
            out = replay_dlq.main()
        assert out["deleted"] is True
        left = spark.sql(
            f"SELECT COUNT(*) AS n FROM {settings.dlq_table} WHERE transaction_id = 'tx_dlq_001'"
        ).collect()[0]["n"]
        assert left == 0
        after = _journal_lines(journal)
        assert len(after) == len(before) + 1
        assert '"dlq_purge"' in after[-1] and "T-LIVE-1" in after[-1]
    finally:
        spark.sql(f"DELETE FROM {settings.webhook_table} WHERE transaction_id = 'tx_dlq_001'")
        spark.sql(f"DELETE FROM {settings.dlq_table} WHERE processing_run_id = 'run-dlq'")
        with open(journal, "w", encoding="utf-8") as fh:
            fh.writelines(before)


def test_live_dlq_replay_refuses_pan_row():
    import scripts.replay_dlq as replay_dlq

    settings = get_settings()
    spark = get_spark_session("LiveE2ETest")
    ensure_dlq_table(spark, settings.dlq_table)
    spark.sql(
        f"INSERT INTO {settings.dlq_table} (transaction_id, amount_paise, gateway_status, "
        "timestamp_utc, merchant_id, processing_run_id, payload_hash) VALUES "
        "('tx_dlq_pan', 1000, 'SUCCESS', '1999-04-04T00:00:00', "
        "'4111111111111111', 'run-pan', NULL)"
    )
    try:
        with (
            patch.object(sys, "argv", ["replay_dlq.py"]),
            pytest.raises(RuntimeError, match="PAN detected"),
        ):
            replay_dlq.main()
        assert (
            spark.table(settings.webhook_table).filter("transaction_id = 'tx_dlq_pan'").count() == 0
        )
    finally:
        spark.sql(f"DELETE FROM {settings.dlq_table} WHERE processing_run_id = 'run-pan'")


def _trino_query(client, base, headers, sql):
    r = client.post(f"{base}/v1/statement", headers=headers, content=sql)
    r.raise_for_status()
    info = r.json()
    for _ in range(60):
        if "data" in info or "error" in info:
            break
        if "nextUri" not in info:
            break
        r = client.get(info["nextUri"])
        r.raise_for_status()
        info = r.json()
    return info


def test_live_trino_dashboard_sees_the_mart():
    import time

    import httpx

    trino = os.environ.get("TRINO_URL", "http://localhost:8080")
    headers = {"X-Trino-User": "ci", "X-Trino-Catalog": "nessie", "X-Trino-Schema": "db"}
    sql = "SELECT status, COUNT(*) AS n FROM fact_reconciliation GROUP BY status"
    try:
        with httpx.Client(timeout=30) as c:
            # Trino can lag behind Nessie right after CREATE OR REPLACE; poll.
            info = _trino_query(c, trino, headers, sql)
            deadline = time.time() + 180
            while time.time() < deadline:
                if "error" in info:
                    break
                rows = info.get("data", [])
                if rows and "MATCHED" in {row[0] for row in rows}:
                    break
                time.sleep(10)
                info = _trino_query(c, trino, headers, sql)
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        pytest.skip(f"trino not reachable: {exc}")
    assert "error" not in info, info.get("error")
    rows = info.get("data", [])
    assert rows, "dashboard query returned no rows (run the green batch first)"
    assert "MATCHED" in {row[0] for row in rows}
