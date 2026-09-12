import logging
import os
import re

from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import get_spark_session
from src.common.schemas import (
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_MISSING_WEBHOOK,
    INSTRUMENT_TYPES,
    MATCHED,
)
from src.common.settings import get_settings
from src.processing.fee_engine import get_fee_engine

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z0-9_.]+$")


def _qualified_table(name: str) -> str:
    """Guard against SQL injection via table config: allow only catalog.db.table chars."""
    if not _TABLE_RE.match(name):
        raise ValueError(f"Unsafe table identifier: {name!r}")
    return name


def build_fee_case_sql(fee_engine, amount_col="t.amount_paise", inst_col="s.instrument_type"):
    """Build instrument-aware fee/gst CASE expressions (pure, Spark-testable).

    Integer-only math mirroring FeeEngine: fee=(amt*bps+5000)DIV 10000,
    gst=(fee*gst_bps+5000)DIV 10000 with gst_bps=gst_pct*100 (exact for 18.0).
    Uses DIV on integers only — Spark DIV rejects float operands.
    """
    fee_cases, gst_cases = [], []
    for inst in INSTRUMENT_TYPES:
        rate = fee_engine.get_rate(inst)
        mdr_bps = int(rate.get("mdr_rate_bps", 150))
        gst_bps = int(round(float(rate.get("gst_on_mdr", 0)) * 100))
        fee_cases.append(
            f"WHEN {inst_col} = '{inst}' THEN ({amount_col} * {mdr_bps} + 5000) DIV 10000"
        )
        gst_cases.append(
            f"WHEN {inst_col} = '{inst}' THEN ((({amount_col} * {mdr_bps} + 5000) DIV 10000 * {gst_bps} + 5000) DIV 10000)"
        )
    default_rate = fee_engine.get_default_rate()
    default_mdr = int(default_rate.get("mdr_rate_bps", 150))
    default_gst_bps = int(round(float(default_rate.get("gst_on_mdr", 18.0)) * 100))
    fee_sql = (
        "CASE " + " ".join(fee_cases) + f" ELSE ({amount_col} * {default_mdr} + 5000) DIV 10000 END"
    )
    gst_sql = (
        "CASE "
        + " ".join(gst_cases)
        + f" ELSE ((({amount_col} * {default_mdr} + 5000) DIV 10000 * {default_gst_bps} + 5000) DIV 10000) END"
    )
    return fee_sql, gst_sql


def run_reconciliation(date_str: str | None = None) -> dict[str, int]:
    """Run batch MERGE of settlement CSVs into the webhooks Iceberg table.

    Returns a dict of outcome counts: {"MATCHED": n, "EXCEPTION_FEE_MISMATCH": n,
    "EXCEPTION_MISSING_WEBHOOK": n, "settlement_rows_deduped": n}. The status
    counts are TABLE-LEVEL distribution (cumulative across all batches ever
    merged, not batch-only) — compare against settlement_rows_deduped (this
    batch's distinct settlement ids) to judge the batch, not raw MATCHED.
    Idempotent: the MERGE only UPDATEs rows keyed on transaction_id (matched) or
    INSERTs unseen ids (not matched) — it never appends a second row for the same
    id, so re-running over the same settlement files converges to identical state.
    """
    settings = get_settings()
    project_root = settings.project_root
    data_dir = os.path.join(project_root, "data")
    if date_str:
        file_pattern = f"settlement_{date_str.replace('-', '')}.csv"
        data_path = os.path.join(data_dir, file_pattern)
    else:
        data_path = os.path.join(data_dir, "*.csv")
    data_path = data_path.replace("\\", "/")

    logger.info("Initializing Spark Session for Batch Reconciliation")
    spark = get_spark_session("ReconciliationJob")

    logger.info(f"Reading bank settlement CSVs from {data_path}")
    bank_schema = StructType(
        [
            StructField("bank_ref_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("settled_amount_paise", LongType(), True),
            StructField("settlement_date", StringType(), True),
            StructField("instrument_type", StringType(), True),
        ]
    )

    try:
        if settings.load_csv_on_driver:
            import glob

            import pandas as pd

            host_path = os.path.join(project_root, "data", file_pattern if date_str else "*.csv")
            bank_df = spark.createDataFrame(
                pd.read_csv(host_path)
                if date_str
                else pd.concat([pd.read_csv(f) for f in glob.glob(host_path)], ignore_index=True)
            )
        else:
            bank_df = (
                spark.read.format("csv")
                .option("header", "true")
                .schema(bank_schema)
                .load(data_path)
            )
    except Exception as exc:
        raise FileNotFoundError(f"No settlement file readable at {data_path}: {exc}") from exc

    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("transaction_id").orderBy(F.col("settlement_date").desc())
    bank_df_dedup = (
        bank_df.filter(F.col("transaction_id").isNotNull())
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    bank_df_dedup.createOrReplaceTempView("bank_settlements")

    fee_engine = get_fee_engine()
    if fee_engine.config.get("merchants"):
        logger.warning(
            "Per-merchant rate overrides are configured but the SQL MERGE is "
            "instrument-only; merchant rows will use instrument rates. "
            "Add merchant_id to settlements to enable parity."
        )
    fee_case_sql, gst_case_sql = build_fee_case_sql(
        fee_engine, amount_col="t.amount_paise", inst_col="s.instrument_type"
    )
    tolerance = fee_engine.default_tolerance_paise

    # Instrument-aware MERGE: fee CASE is inlined in the MATCHED clause so it can
    # use the webhook amount (t.amount_paise) plus the settlement instrument
    # (s.instrument_type). The USING source is settlements only — joining the
    # target here would filter out orphans and make WHEN NOT MATCHED dead.
    table = _qualified_table(settings.webhook_table)
    merge_sql = f"""
    MERGE INTO {table} t
    USING (
        SELECT
            s.transaction_id,
            s.bank_ref_id,
            s.settled_amount_paise,
            s.instrument_type
        FROM bank_settlements s
        WHERE s.transaction_id IS NOT NULL AND s.settled_amount_paise IS NOT NULL
    ) s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND t.amount_paise IS NULL THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= {tolerance} THEN
        UPDATE SET
            t.reconciliation_status = '{MATCHED}',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) > {tolerance} THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, reconciliation_status, bank_ref_id)
        VALUES (s.transaction_id, s.settled_amount_paise, 'UNKNOWN', current_timestamp(), 'UNKNOWN', '{EXCEPTION_MISSING_WEBHOOK}', s.bank_ref_id)
    """

    logger.info("Executing MERGE INTO operation")
    try:
        spark.sql(merge_sql)
    except Exception as exc:
        raise RuntimeError(f"Reconciliation MERGE failed: {exc}") from exc

    # Observability: report outcome distribution (FAANG expects match-rate metrics).
    # NOTE: status counts below are table-level (cumulative); settlement_rows_deduped
    # scopes the batch so MATCHED can be judged per-run.
    counts = {}
    try:
        counts["settlement_rows_deduped"] = bank_df_dedup.count()
        for row in spark.sql(
            f"SELECT reconciliation_status, COUNT(*) AS n FROM {table} GROUP BY reconciliation_status"
        ).collect():
            counts[row["reconciliation_status"]] = row["n"]
        # Batch-scoped outcomes: join this batch's settlement ids back to the
        # target so a stale cumulative MATCHED can't mask a failing batch.
        for row in spark.sql(
            f"SELECT t.reconciliation_status AS st, COUNT(*) AS n FROM {table} t "
            f"JOIN bank_settlements s ON t.transaction_id = s.transaction_id "
            f"GROUP BY t.reconciliation_status"
        ).collect():
            counts[f"batch_{row['st']}"] = row["n"]
    except Exception as exc:  # metrics must never fail the job
        logger.warning(f"Could not fetch reconciliation counts: {exc}")
    logger.info(f"Reconciliation batch complete: {counts}")
    return counts


if __name__ == "__main__":
    run_reconciliation()
