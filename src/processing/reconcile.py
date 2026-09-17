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


def _append_leg(fee_cases, gst_cases, tol_cases, pred, amount_col, mdr_bps, gst_bps, tol):
    """Append one WHEN leg to fee/gst/tolerance CASE lists (single source for leg shape)."""
    fee_cases.append(f"WHEN {pred} THEN (({amount_col} * {mdr_bps} + 5000) DIV 10000)")
    if gst_bps > 0:
        gst_cases.append(
            f"WHEN {pred} THEN ((((({amount_col} * {mdr_bps} + 5000) DIV 10000) * {gst_bps} + 5000) DIV 10000))"
        )
    else:
        gst_cases.append(f"WHEN {pred} THEN 0")
    tol_cases.append(f"WHEN {pred} THEN {tol}")


def _versioned_wrap(cards, top_merchants, date_col, build):
    """Wrap per-card SQL fragments in settlement_date range WHENs (latest-first, ELSE latest).

    build(card_eff) -> tuple of fragments; returns tuple of versioned CASEs.
    Mirrors FeeEngine._card_for_date: NULL/malformed dates match nothing -> latest.
    """
    per = []
    for card in reversed(cards):
        eff_from_esc = str(card.get("effective_from", "1970-01-01")).replace("'", "''")
        eff_to = card.get("effective_to")
        card_eff = dict(card)
        card_eff["merchants"] = {**top_merchants, **(card.get("merchants", {}) or {})}
        if eff_to:
            eff_to_esc = str(eff_to).replace("'", "''")
            cond = f"{date_col} >= '{eff_from_esc}' AND {date_col} <= '{eff_to_esc}'"
        else:
            cond = f"{date_col} >= '{eff_from_esc}'"
        per.append((cond, build(card_eff)))
    latest_eff = dict(cards[-1])
    latest_eff["merchants"] = {**top_merchants, **(cards[-1].get("merchants", {}) or {})}
    latest = build(latest_eff)
    return tuple(
        "CASE "
        + " ".join(f"WHEN {cond} THEN {frag[i]}" for cond, frag in per)
        + f" ELSE {latest[i]} END"
        for i in range(len(latest))
    )


def _build_single_card_fee_sql(
    card: dict, amount_col: str, inst_col: str, merchant_col: str | None
):
    """Build fee/gst/tolerance CASE for a single rate card (merchant-aware)."""
    fee_cases: list[str] = []
    gst_cases: list[str] = []
    tol_cases: list[str] = []
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
                _append_leg(
                    fee_cases,
                    gst_cases,
                    tol_cases,
                    f"{merchant_col} = '{merch_esc}' AND {inst_col} = '{inst_esc}'",
                    amount_col,
                    mdr_bps,
                    gst_bps,
                    tol,
                )

    instruments = card.get("instruments", {}) or {}
    for inst in INSTRUMENT_TYPES:
        rate = instruments.get(inst, {})
        mdr_bps = int(rate.get("mdr_rate_bps", default_mdr))
        gst_pct = rate.get("gst_on_mdr", default.get("gst_on_mdr", 18.0))
        gst_bps = int(round(float(gst_pct) * 100))
        tol = int(rate.get("tolerance_paise", default_tol))
        inst_esc = inst.replace("'", "''")
        _append_leg(
            fee_cases,
            gst_cases,
            tol_cases,
            f"{inst_col} = '{inst_esc}'",
            amount_col,
            mdr_bps,
            gst_bps,
            tol,
        )

    fee_sql = (
        "CASE "
        + " ".join(fee_cases)
        + f" ELSE (({amount_col} * {default_mdr} + 5000) DIV 10000) END"
    )
    if default_gst_bps > 0:
        gst_sql = (
            "CASE "
            + " ".join(gst_cases)
            + f" ELSE ((((({amount_col} * {default_mdr} + 5000) DIV 10000) * {default_gst_bps} + 5000) DIV 10000)) END"
        )
    else:
        gst_sql = "CASE " + " ".join(gst_cases) + " ELSE 0 END"
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
        # Versioned: CASE on settlement_date range [effective_from, effective_to].
        # NULL/malformed dates match nothing -> ELSE latest (mirrors FeeEngine._card_for_date).
        # Top-level merchants inherit into every card (mirrors FeeEngine get_rate_for_date).
        top_merchants = (getattr(fee_engine, "config", {}) or {}).get("merchants", {}) or {}
        return _versioned_wrap(
            cards,
            top_merchants,
            settlement_date_col,
            lambda c: _build_single_card_fee_sql(c, amount_col, inst_col, merchant_col)[:2],
        )

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
        top_merchants = (getattr(fee_engine, "config", {}) or {}).get("merchants", {}) or {}
        return _versioned_wrap(
            cards,
            top_merchants,
            settlement_date_col,
            lambda c: _build_single_card_fee_sql(c, "t.amount_paise", inst_col, merchant_col)[2:],
        )[0]
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


def _settlement_source(data_dir: str, date_str: str | None) -> tuple[str, list[str]]:
    """Prefer curated (normalized) files from validate; fall back to raw.

    Curated files are written by validate_latest_settlement as
    curated_settlement_*.csv so PG INR->paise/date/instrument normalization
    survives into the MERGE. Uses settlement_*.csv (never *.csv) so
    quarantine_*.csv is never merged.
    """
    import glob as _glob

    if date_str:
        stem = date_str.replace("-", "")
        curated = os.path.join(data_dir, f"curated_settlement_{stem}.csv")
        if os.path.exists(curated):
            return curated.replace("\\", "/"), [curated]
        raw = os.path.join(data_dir, f"settlement_{stem}.csv")
        return raw.replace("\\", "/"), [raw]
    curated_files = sorted(_glob.glob(os.path.join(data_dir, "curated_settlement_*.csv")))
    if curated_files:
        pattern = os.path.join(data_dir, "curated_settlement_*.csv")
        return pattern.replace("\\", "/"), curated_files
    pattern = os.path.join(data_dir, "settlement_*.csv")
    return pattern.replace("\\", "/"), sorted(_glob.glob(pattern))


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
    data_path, settlement_files = _settlement_source(data_dir, date_str)

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
            import pandas as pd

            if date_str:
                bank_df = spark.createDataFrame(pd.read_csv(settlement_files[0]))
            else:
                bank_df = spark.createDataFrame(
                    pd.concat([pd.read_csv(f) for f in settlement_files], ignore_index=True)
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

    window_spec = Window.partitionBy("transaction_id").orderBy(
        F.col("settlement_date").desc(),
        F.col("bank_ref_id").asc(),
        F.col("settlement_id").asc(),
    )
    settlement_rows_total = bank_df.filter(F.col("transaction_id").isNotNull()).count()
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
    # Single-pass fee evaluation: one ABS(...) + CASE per row (was 2x).
    # First two guards preserve idempotency (MISSING placeholder + null amount).
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
    WHEN MATCHED THEN
        UPDATE SET
            t.reconciliation_status = CASE WHEN ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= ({tolerance_sql}) THEN '{MATCHED}' ELSE '{EXCEPTION_FEE_MISMATCH}' END,
            t.bank_ref_id = s.bank_ref_id
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, reconciliation_status, bank_ref_id)
        VALUES (s.transaction_id, s.settled_amount_paise, 'UNKNOWN', current_timestamp(), COALESCE(s.merchant_id, 'UNKNOWN'), '{EXCEPTION_MISSING_WEBHOOK}', s.bank_ref_id)
    """

    logger.info("Executing MERGE INTO operation")
    try:
        spark.sql(merge_sql)
    except Exception as exc:
        import contextlib as _ctx

        with _ctx.suppress(Exception):
            bank_df_dedup.unpersist()
        raise RuntimeError(f"Reconciliation MERGE failed: {exc}") from exc

    # Mart: refresh the BI-ready view over the reconciled table so Trino/Metabase
    # never query a stale or missing object. Same SELECT as sql/marts/fact_reconciliation.sql.
    namespace = table.rsplit(".", 1)[0]
    try:
        spark.sql(
            f"CREATE OR REPLACE VIEW {namespace}.fact_reconciliation AS "
            "SELECT transaction_id, amount_paise, merchant_id, gateway_status, "
            "reconciliation_status AS status, bank_ref_id, "
            "CAST(timestamp_utc AS TIMESTAMP) AS transacted_at, "
            "CASE WHEN reconciliation_status = 'MATCHED' THEN amount_paise "
            "ELSE NULL END AS matched_amount_paise "
            f"FROM {table}"
        )
    except Exception as exc:  # mart must never fail the job
        logger.warning(f"Could not refresh fact_reconciliation view: {exc}")

    # Observability: report outcome distribution (FAANG expects match-rate metrics).
    # NOTE: status counts below are table-level (cumulative); settlement_rows_deduped
    # scopes the batch so MATCHED can be judged per-run.
    # Fail closed: dedup count must succeed, else the batch is unknown (never return {}).
    counts: dict[str, int] = {"settlement_rows_total": settlement_rows_total}
    counts["settlement_rows_deduped"] = bank_df_dedup.count()
    counts["duplicate_settlement_rows"] = max(
        0, settlement_rows_total - counts["settlement_rows_deduped"]
    )
    if counts["duplicate_settlement_rows"]:
        counts["batch_EXCEPTION_DUPLICATE_SETTLEMENT"] = counts["duplicate_settlement_rows"]
        logger.warning(
            "Duplicate settlements deduped: %d rows collapsed (deterministic tiebreak "
            "settlement_date DESC, bank_ref_id/settlement_id ASC)",
            counts["duplicate_settlement_rows"],
        )
    try:
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
    except Exception as exc:
        raise RuntimeError(f"Could not fetch reconciliation counts: {exc}") from exc
    finally:
        import contextlib

        with contextlib.suppress(Exception):
            bank_df_dedup.unpersist()
    logger.info(f"Reconciliation batch complete: {counts}")
    return counts


def maintain_tables(
    spark=None, tables: list[str] | None = None, retain_last: int | None = None
) -> dict:
    """Binpack + expire snapshots on Iceberg tables (prod parity with bench hygiene).

    Never raises: per-statement failures are logged and skipped so maintenance
    can never fail a batch. Opens its own session when spark is None.
    """
    import contextlib as _ctx

    from src.common.config import get_spark_session
    from src.common.settings import get_settings

    own_session = spark is None
    if own_session:
        spark = get_spark_session("Maintenance")
    try:
        settings = get_settings()
        targets = tables or [settings.webhook_table, settings.dlq_table]
        retain = retain_last or int(os.environ.get("MAINTAIN_RETAIN_LAST", "7"))
        out: dict = {}
        for raw in targets:
            tbl = _qualified_table(raw)
            parts = tbl.split(".")
            catalog, rest = parts[0], ".".join(parts[1:])
            entry: dict = {}
            stmts = [
                ("files_before", f"SELECT COUNT(*) AS n FROM {tbl}.files"),
                (
                    None,
                    f"CALL {catalog}.system.rewrite_data_files(table => '{rest}', strategy => 'binpack')",
                ),
                (
                    None,
                    f"CALL {catalog}.system.expire_snapshots(table => '{rest}', retain_last => {retain})",
                ),
                ("files_after", f"SELECT COUNT(*) AS n FROM {tbl}.files"),
            ]
            for key, sql in stmts:
                try:
                    res = spark.sql(sql)
                    if key:
                        entry[key] = res.collect()[0]["n"]
                except Exception as exc:
                    logger.warning(f"Maintenance: {key or 'statement'} failed for {tbl}: {exc}")
            out[tbl] = entry
        logger.info(f"Maintenance complete: {out}")
        return out
    finally:
        if own_session:
            with _ctx.suppress(Exception):
                spark.stop()


if __name__ == "__main__":
    run_reconciliation()
