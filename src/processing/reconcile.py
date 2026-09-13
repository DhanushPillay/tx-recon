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


def _build_single_card_fee_sql(
    card: dict, amount_col: str, inst_col: str, merchant_col: str | None
):
    """Build fee/gst/tolerance CASE for a single rate card (merchant-aware)."""
    fee_cases, gst_cases, tol_cases = [], [], []
    default = card.get("default", {})
    default_mdr = int(default.get("mdr_rate_bps", 150))
    default_gst_bps = int(round(float(default.get("gst_on_mdr", 18.0)) * 100))
    default_tol = int(default.get("tolerance_paise", 1))

    # Merchant overrides first (most specific)
    merchants = card.get("merchants", {}) or {}
    for merch, inst_map in merchants.items():
        merch_esc = merch.replace("'", "''")
        for inst, rate in (inst_map or {}).items():
            mdr_bps = int(rate.get("mdr_rate_bps", default_mdr))
            gst_pct = rate.get("gst_on_mdr", default.get("gst_on_mdr", 18.0))
            gst_bps = int(round(float(gst_pct) * 100))
            tol = int(rate.get("tolerance_paise", default_tol))
            inst_esc = inst.replace("'", "''")
            if merchant_col:
                fee_cases.append(
                    f"WHEN {merchant_col} = '{merch_esc}' AND {inst_col} = '{inst_esc}' THEN ({amount_col} * {mdr_bps} + 5000) DIV 10000"
                )
                gst_cases.append(
                    f"WHEN {merchant_col} = '{merch_esc}' AND {inst_col} = '{inst_esc}' THEN ((({amount_col} * {mdr_bps} + 5000) DIV 10000 * {gst_bps} + 5000) DIV 10000)"
                )
                tol_cases.append(
                    f"WHEN {merchant_col} = '{merch_esc}' AND {inst_col} = '{inst_esc}' THEN {tol}"
                )

    instruments = card.get("instruments", {}) or {}
    for inst in INSTRUMENT_TYPES:
        rate = instruments.get(inst, {})
        mdr_bps = int(rate.get("mdr_rate_bps", default_mdr))
        gst_pct = rate.get("gst_on_mdr", default.get("gst_on_mdr", 18.0))
        gst_bps = int(round(float(gst_pct) * 100))
        tol = int(rate.get("tolerance_paise", default_tol))
        inst_esc = inst.replace("'", "''")
        fee_cases.append(
            f"WHEN {inst_col} = '{inst_esc}' THEN ({amount_col} * {mdr_bps} + 5000) DIV 10000"
        )
        gst_cases.append(
            f"WHEN {inst_col} = '{inst_esc}' THEN ((({amount_col} * {mdr_bps} + 5000) DIV 10000 * {gst_bps} + 5000) DIV 10000)"
        )
        tol_cases.append(f"WHEN {inst_col} = '{inst_esc}' THEN {tol}")

    fee_sql = (
        "CASE " + " ".join(fee_cases) + f" ELSE ({amount_col} * {default_mdr} + 5000) DIV 10000 END"
    )
    gst_sql = (
        "CASE "
        + " ".join(gst_cases)
        + f" ELSE ((({amount_col} * {default_mdr} + 5000) DIV 10000 * {default_gst_bps} + 5000) DIV 10000) END"
    )
    tol_sql = "CASE " + " ".join(tol_cases) + f" ELSE {default_tol} END"
    return fee_sql, gst_sql, tol_sql


def build_fee_case_sql(
    fee_engine,
    amount_col="t.amount_paise",
    inst_col="s.instrument_type",
    merchant_col="s.merchant_id",
    settlement_date_col="s.settlement_date",
):
    """Build instrument-aware (and merchant-aware) fee/gst CASE expressions.

    Integer-only math mirroring FeeEngine. When the engine has versioned
    rate_cards, wraps per-version CASEs by settlement_date.
    Returns (fee_sql, gst_sql). Tolerance is available via build_tolerance_case_sql().
    """
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) > 1:
        # Versioned: CASE on settlement_date (use caller-provided column, so golden test can use t.*)
        fee_parts, gst_parts = [], []
        # Latest first so WHEN matches most recent applicable
        for card in reversed(cards):
            eff_from = card.get("effective_from", "1970-01-01")
            eff_from_esc = str(eff_from).replace("'", "''")
            f_sql, g_sql, _ = _build_single_card_fee_sql(card, amount_col, inst_col, merchant_col)
            fee_parts.append(f"WHEN {settlement_date_col} >= '{eff_from_esc}' THEN {f_sql}")
            gst_parts.append(f"WHEN {settlement_date_col} >= '{eff_from_esc}' THEN {g_sql}")
        # Fallback to earliest card's inner CASE
        earliest = cards[0]
        f0, g0, _ = _build_single_card_fee_sql(earliest, amount_col, inst_col, merchant_col)
        fee_sql = "CASE " + " ".join(fee_parts) + f" ELSE {f0} END"
        gst_sql = "CASE " + " ".join(gst_parts) + f" ELSE {g0} END"
        return fee_sql, gst_sql

    # Single card path
    card = (
        cards[0]
        if cards
        else {
            "default": fee_engine.get_default_rate(),
            "instruments": fee_engine.config.get("instruments", {}),
            "merchants": fee_engine.config.get("merchants", {}),
        }
    )
    fee_sql, gst_sql, _ = _build_single_card_fee_sql(card, amount_col, inst_col, merchant_col)
    return fee_sql, gst_sql


def build_tolerance_case_sql(
    fee_engine,
    inst_col="s.instrument_type",
    merchant_col="s.merchant_id",
    settlement_date_col="s.settlement_date",
) -> str:
    """Build CASE that yields per-row tolerance_paise (merchant + instrument + version aware)."""
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) > 1:
        parts = []
        for card in reversed(cards):
            eff_from = card.get("effective_from", "1970-01-01")
            eff_from_esc = str(eff_from).replace("'", "''")
            _, _, tol_sql = _build_single_card_fee_sql(
                card, "t.amount_paise", inst_col, merchant_col
            )
            parts.append(f"WHEN {settlement_date_col} >= '{eff_from_esc}' THEN {tol_sql}")
        _, _, tol0 = _build_single_card_fee_sql(cards[0], "t.amount_paise", inst_col, merchant_col)
        return "CASE " + " ".join(parts) + f" ELSE {tol0} END"
    card = (
        cards[0]
        if cards
        else {
            "default": fee_engine.get_default_rate(),
            "instruments": fee_engine.config.get("instruments", {}),
            "merchants": fee_engine.config.get("merchants", {}),
        }
    )
    _, _, tol_sql = _build_single_card_fee_sql(card, "t.amount_paise", inst_col, merchant_col)
    return tol_sql


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
            StructField("merchant_id", StringType(), True),
            StructField("fee_paise", LongType(), True),
            StructField("gst_paise", LongType(), True),
            StructField("settlement_id", StringType(), True),
            StructField("utr", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("gross_amount_paise", LongType(), True),
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
    bank_df_dedup.cache()
    bank_df_dedup.createOrReplaceTempView("bank_settlements")

    fee_engine = get_fee_engine()
    fee_case_sql, gst_case_sql = build_fee_case_sql(
        fee_engine,
        amount_col="t.amount_paise",
        inst_col="s.instrument_type",
        merchant_col="s.merchant_id",
    )
    tolerance_sql = build_tolerance_case_sql(
        fee_engine, inst_col="s.instrument_type", merchant_col="s.merchant_id"
    )

    # Instrument-aware MERGE: fee CASE is inlined in the MATCHED clause so it can
    # use the webhook amount (t.amount_paise) plus the settlement instrument
    # (s.instrument_type). The USING source is settlements only — joining the
    # target here would filter out orphans and make WHEN NOT MATCHED dead.
    # First MATCHED clause preserves EXCEPTION_MISSING_WEBHOOK placeholders:
    # their amount_paise equals the settled amount, so fee math would
    # misread them as FEE_MISMATCH on rerun and break idempotency.
    table = _qualified_table(settings.webhook_table)
    merge_sql = f"""
    MERGE INTO {table} t
    USING (
        SELECT
            s.transaction_id,
            s.bank_ref_id,
            s.settled_amount_paise,
            s.instrument_type,
            COALESCE(s.merchant_id, 'UNKNOWN') AS merchant_id,
            s.settlement_date
        FROM bank_settlements s
        WHERE s.transaction_id IS NOT NULL AND s.settled_amount_paise IS NOT NULL
    ) s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' THEN
        UPDATE SET
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND t.amount_paise IS NULL THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= ({tolerance_sql}) THEN
        UPDATE SET
            t.reconciliation_status = '{MATCHED}',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) > ({tolerance_sql}) THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, reconciliation_status, bank_ref_id)
        VALUES (s.transaction_id, s.settled_amount_paise, 'UNKNOWN', current_timestamp(), COALESCE(s.merchant_id, 'UNKNOWN'), '{EXCEPTION_MISSING_WEBHOOK}', s.bank_ref_id)
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
    finally:
        import contextlib

        with contextlib.suppress(Exception):
            bank_df_dedup.unpersist()
    logger.info(f"Reconciliation batch complete: {counts}")
    return counts


if __name__ == "__main__":
    run_reconciliation()
