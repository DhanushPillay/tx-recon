import glob
import logging
import os
import re

from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import get_spark_session
from src.common.schemas import (
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_LATE_UNRESOLVED,
    EXCEPTION_MISSING_BANK_STATEMENT,
    EXCEPTION_MISSING_WEBHOOK,
    INSTRUMENT_TYPES,
    MATCHED,
)
from src.common.settings import get_settings
from src.processing.fee_engine import get_fee_engine, gst_pct_to_bps

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
    """Wrap per-card SQL fragments in settlement_date WHENs (latest-first, ELSE earliest).

    WHENs test `{date_col} >= effective_from` in latest-first order, so the
    first match is the latest card with from <= date; dates before the first
    card fall to ELSE (earliest). This mirrors FeeEngine._card_for_date
    exactly (chosen-or-prior-or-first) for non-overlapping cards, including
    gap dates; overlapping ranges are rejected by FeeEngine._validate_config.
    NULL/malformed dates match no WHEN -> earliest (same as Python pre-first).
    """
    per = []
    for card in reversed(cards):
        eff_from_esc = str(card.get("effective_from", "1970-01-01")).replace("'", "''")
        card_eff = dict(card)
        card_eff["merchants"] = {**top_merchants, **(card.get("merchants", {}) or {})}
        cond = f"{date_col} >= '{eff_from_esc}'"
        per.append((cond, build(card_eff)))
    first_eff = dict(cards[0])
    first_eff["merchants"] = {**top_merchants, **(cards[0].get("merchants", {}) or {})}
    first = build(first_eff)
    latest_eff = dict(cards[-1])
    latest_eff["merchants"] = {**top_merchants, **(cards[-1].get("merchants", {}) or {})}
    latest = build(latest_eff)
    # NULL/empty dates -> latest, mirroring _card_for_date's falsy early-return.
    null_cond = f"({date_col} IS NULL OR {date_col} = '')"
    return tuple(
        "CASE "
        + f"WHEN {null_cond} THEN {latest[i]} "
        + " ".join(f"WHEN {cond} THEN {frag[i]}" for cond, frag in per)
        + f" ELSE {first[i]} END"
        for i in range(len(first))
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
    default_gst_bps = gst_pct_to_bps(default.get("gst_on_mdr", 18.0))
    default_tol = int(default.get("tolerance_paise", 1))

    def _triple(rate: dict) -> tuple[int, int, int]:
        mdr = int(rate.get("mdr_rate_bps", default_mdr))
        gst = gst_pct_to_bps(rate.get("gst_on_mdr", default.get("gst_on_mdr", 18.0)))
        return mdr, gst, int(rate.get("tolerance_paise", default_tol))

    # Merchant overrides first (most specific)
    merchants = card.get("merchants", {}) or {}
    for merch, inst_map in merchants.items():
        merch_esc = merch.replace("'", "''")
        for inst, rate in (inst_map or {}).items():
            mdr_bps, gst_bps, tol = _triple(rate)
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
        mdr_bps, gst_bps, tol = _triple(rate)
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


def _single_card(fee_engine) -> dict:
    """Single-card dict both CASE builders share (emitted SQL unchanged)."""
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) == 1:
        return cards[0]
    return {
        "default": fee_engine.get_default_rate(),
        "instruments": fee_engine.config.get("instruments", {}),
        "merchants": fee_engine.config.get("merchants", {}),
    }


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
    card = _single_card(fee_engine)
    fee_sql, gst_sql, _ = _build_single_card_fee_sql(card, amount_col, inst_col, merchant_col)
    return fee_sql, gst_sql


def build_tolerance_case_sql(
    fee_engine,
    inst_col="s.instrument_type",
    merchant_col="s.merchant_id",
    settlement_date_col="s.settlement_date",
    amount_col="t.amount_paise",
) -> str:
    """Build CASE that yields per-row tolerance_paise (merchant + instrument + version aware).

    amount_col threads through for signature consistency with
    build_fee_case_sql (the tolerance fragment itself is amount-independent).
    """
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) > 1:
        top_merchants = (getattr(fee_engine, "config", {}) or {}).get("merchants", {}) or {}
        return _versioned_wrap(
            cards,
            top_merchants,
            settlement_date_col,
            lambda c: _build_single_card_fee_sql(c, amount_col, inst_col, merchant_col)[2:],
        )[0]
    card = _single_card(fee_engine)
    _, _, tol_sql = _build_single_card_fee_sql(card, amount_col, inst_col, merchant_col)
    return tol_sql


def _settlement_source(data_dir: str, date_str: str | None) -> tuple[str, list[str]]:
    """Prefer curated (normalized) files from validate; fall back to raw.

    Curated files are written by validate_latest_settlement as
    curated_settlement_*.csv so PG INR->paise/date/instrument normalization
    survives into the MERGE. Uses settlement_*.csv (never *.csv) so
    quarantine_*.csv is never merged.
    """
    if date_str:
        # Digit-only date in YYYY-MM-DD or YYYYMMDD (pipeline uses dashed,
        # tests seed compact files). Anything else (.., /, glob chars) is
        # rejected so date_str can never escape data_dir.
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{8}", date_str):
            raise ValueError(f"Bad date_str (want YYYY-MM-DD): {date_str!r}")
        stem = date_str.replace("-", "")
        curated = os.path.join(data_dir, f"curated_settlement_{stem}.csv")
        if os.path.exists(curated):
            return curated.replace("\\", "/"), [curated]
        raw = os.path.join(data_dir, f"settlement_{stem}.csv")
        return raw.replace("\\", "/"), [raw]
    curated_files = sorted(glob.glob(os.path.join(data_dir, "curated_settlement_*.csv")))
    if curated_files:
        pattern = os.path.join(data_dir, "curated_settlement_*.csv")
        return pattern.replace("\\", "/"), curated_files
    pattern = os.path.join(data_dir, "settlement_*.csv")
    return pattern.replace("\\", "/"), sorted(glob.glob(pattern))


def _load_mart_sql(project_root: str, source_table: str, mart_table: str) -> tuple[str, str]:
    """Load the newest sql/marts/migrations/V*__fact_reconciliation.sql migration.

    Single source of truth for the mart DDL — the inline CTAS it replaces and
    the checked-in hand-run copy (sql/marts/fact_reconciliation.sql) both derive
    from these files, so ad-hoc DDL cannot drift. Returns (sql, version).
    Falls back to the V1 inline copy if the migrations dir is absent (tests).
    """
    files = sorted(glob.glob(os.path.join(project_root, "sql", "marts", "migrations", "V*.sql")))
    if files:
        path = files[-1]
        with open(path) as f:
            body = f.read()
        version = os.path.basename(path).split("__")[0]
        return (
            body.replace("__SOURCE_TABLE__", source_table).replace("__MART_TABLE__", mart_table),
            version,
        )
    return (
        f"CREATE OR REPLACE TABLE {mart_table} USING iceberg AS "
        "SELECT transaction_id, amount_paise, merchant_id, instrument_type, gateway_status, "
        "reconciliation_status AS status, bank_ref_id, "
        "CAST(timestamp_utc AS TIMESTAMP) AS transacted_at, "
        "CASE WHEN reconciliation_status = 'MATCHED' THEN amount_paise "
        f"ELSE NULL END AS matched_amount_paise FROM {source_table}",
        "inline",
    )


def run_reconciliation(date_str: str | None = None) -> dict[str, int | float]:
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
    if not date_str:
        # Fail closed: date-less runs merge every curated file (all history),
        # and cumulative counts then masquerade as batch health.
        raise ValueError("run_reconciliation requires date_str YYYY-MM-DD (no date-less runs)")
    settings = get_settings()
    if settings.strict_slo:
        logger.info("strict SLO mode: breach or stale mart fails the batch")
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
            StructField("provider", StringType(), True),
        ]
    )

    try:
        if settings.load_csv_on_driver:
            import pandas as pd

            # Per-file conversion + union: avoids one giant pd.concat on the driver.
            _frames = []
            for _f in settlement_files if not date_str else settlement_files[:1]:
                _pdf = pd.read_csv(_f)
                if len(_pdf) <= 50000:
                    _frames.append(spark.createDataFrame(_pdf))
                else:
                    _chunks = [
                        spark.createDataFrame(_pdf.iloc[i : i + 50000])
                        for i in range(0, len(_pdf), 50000)
                    ]
                    _sdf = _chunks[0]
                    for _c in _chunks[1:]:
                        _sdf = _sdf.union(_c)
                    _frames.append(_sdf)
            bank_df = _frames[0]
            for _frame in _frames[
                1:
            ]:  # unionByName takes one frame (2nd positional is a bool flag)
                bank_df = bank_df.unionByName(_frame)
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
    # Cache once: total + dedup counts share one CSV scan instead of two.
    # Repartition BEFORE the window on the same key: the window then needs no
    # exchange (was window-shuffle + repartition-shuffle = 2x on 13M rows).
    bank_df_valid = (
        bank_df.filter(F.col("transaction_id").isNotNull())
        .repartition(settings.spark_shuffle_partitions, "transaction_id")
        .cache()
    )
    settlement_rows_total = bank_df_valid.count()
    bank_df_dedup = (
        bank_df_valid.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    bank_df_dedup.cache()
    settlement_rows_deduped = bank_df_dedup.count()  # warm cache before MERGE; reuse below
    import contextlib

    with contextlib.suppress(Exception):
        bank_df_valid.unpersist()
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
    WHEN MATCHED AND (t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' OR t.reconciliation_status = '{EXCEPTION_LATE_UNRESOLVED}') THEN
        UPDATE SET
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN MATCHED AND t.amount_paise IS NULL THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN MATCHED THEN
        UPDATE SET
            t.reconciliation_status = CASE WHEN ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= ({tolerance_sql}) THEN '{MATCHED}' ELSE '{EXCEPTION_FEE_MISMATCH}' END,
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, reconciliation_status, bank_ref_id, instrument_type)
        VALUES (s.transaction_id, s.settled_amount_paise, 'UNKNOWN', s.settlement_date, COALESCE(s.merchant_id, 'UNKNOWN'), '{EXCEPTION_MISSING_WEBHOOK}', s.bank_ref_id, s.instrument_type)
    """

    logger.info("Executing MERGE INTO operation")
    try:
        spark.sql(merge_sql)
    except Exception as exc:
        import contextlib as _ctx

        with _ctx.suppress(Exception):
            bank_df_dedup.unpersist()
        raise RuntimeError(f"Reconciliation MERGE failed: {exc}") from exc

    # Mart: materialize a BI-ready snapshot table over the reconciled data so
    # Trino/Metabase scan a compact snapshot, not the full mutable table.
    # DDL comes from sql/marts/migrations (newest V* file); full rewrite per
    # batch is fine at this scale, graduate to incremental merge when batches grow.
    namespace = table.rsplit(".", 1)[0]
    try:
        _mart_sql, _mart_version = _load_mart_sql(
            project_root, table, f"{namespace}.fact_reconciliation"
        )
        spark.sql(_mart_sql)
        logger.info(f"mart {_mart_version} refreshed: {namespace}.fact_reconciliation")
    except Exception as exc:
        # A stale mart behind a "successful" batch is worse than a failed batch.
        msg = f"Could not refresh fact_reconciliation table: {exc}"
        if settings.strict_slo:
            raise RuntimeError(msg) from exc
        logger.warning(msg)

    # Observability: report outcome distribution (FAANG expects match-rate metrics).
    # NOTE: status counts below are table-level (cumulative); settlement_rows_deduped
    # scopes the batch so MATCHED can be judged per-run.
    # Fail closed: dedup count must succeed, else the batch is unknown (never return {}).
    counts: dict[str, int | float] = {"settlement_rows_total": settlement_rows_total}
    counts["settlement_rows_deduped"] = settlement_rows_deduped
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
        with contextlib.suppress(Exception):
            bank_df_valid.unpersist()
    logger.info(f"Reconciliation batch complete: {counts}")
    import os as _os

    def _env_float(name: str, default: float) -> float:
        try:
            return float(_os.environ.get(name, default))
        except (TypeError, ValueError):
            logger.warning(f"bad {name}={_os.environ.get(name)!r}: using {default}")
            return default

    def _env_int(name: str, default: int) -> int:
        try:
            return int(_os.environ.get(name, default))
        except (TypeError, ValueError):
            logger.warning(f"bad {name}={_os.environ.get(name)!r}: using {default}")
            return default

    _slo = _env_float("MATCH_RATE_SLO", 0.95)
    _bm, _bt = counts.get("batch_MATCHED", 0), counts.get("settlement_rows_deduped", 0)
    if _bt:
        _rate = _bm / _bt
        counts["batch_match_rate"] = round(_rate, 4)
        if _rate < _slo:
            msg = f"Match-rate SLO breach: {_rate:.2%} < {_slo:.0%} (batch)"
            if settings.strict_slo:
                raise RuntimeError(msg)
            logger.warning(msg)
    try:  # Late SLA per provider: MISSING older than N days -> LATE_UNRESOLVED.
        # Thresholds come from config/providers.yaml (fallback: global default);
        # LATE_SLA_DAYS env overrides every provider when set. One MERGE with a
        # CASE keeps this a single job however many providers the batch holds.
        from src.processing.batches import load_providers, provider_spec

        try:
            _pcfg = load_providers(os.path.join(project_root, "config", "providers.yaml"))
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(f"provider config unreadable ({exc}): using global late SLA")
            _pcfg = {"default": {}, "providers": {}}
        _known = sorted(
            {
                str(r["provider"])
                for r in spark.sql(
                    "SELECT DISTINCT provider FROM bank_settlements WHERE provider IS NOT NULL"
                ).collect()
            }
        )
        _global_days = (
            _env_int("LATE_SLA_DAYS", 7)
            if "LATE_SLA_DAYS" in _os.environ
            else int(provider_spec(_pcfg, None).get("late_sla_days", 7))
        )
        _whens = " ".join(
            f"WHEN '{p}' THEN {int(provider_spec(_pcfg, p).get('late_sla_days', _global_days))}"
            for p in _known
            if p != "None"
        )
        _days_case = f"CASE s.provider {_whens} ELSE {_global_days} END"
        _late_where = (
            f"t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' "
            f"AND to_date(t.timestamp_utc) < date_sub(current_date(), ({_days_case}))"
        )
        # Side-output counts first: the MERGE below returns no row count, and
        # the oncall alert (runbook) keys off late_unresolved_marked > 0.
        # One job: both gauges share the same join, split by conditional sums
        # (was two full join scans). Both read pre-MERGE state: the MERGE
        # below consumes MISSING rows, so the lag gauge must run before it.
        _lag_whens = " ".join(
            f"WHEN '{p}' THEN {int(provider_spec(_pcfg, p).get('lag_days', 2))}"
            for p in _known
            if p != "None"
        )
        _lag_case = f"CASE s.provider {_lag_whens} ELSE 2 END"
        _late_lag = spark.sql(
            f"SELECT "
            f"SUM(CASE WHEN {_late_where} THEN 1 ELSE 0 END) AS late_n, "
            f"SUM(CASE WHEN t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' "
            f"AND DATEDIFF(s.settlement_date, to_date(t.timestamp_utc)) "
            f"BETWEEN 0 AND ({_lag_case}) THEN 1 ELSE 0 END) AS lag_n "
            f"FROM {table} t "
            f"JOIN bank_settlements s ON s.transaction_id = t.transaction_id "
        ).collect()[0]
        counts["late_unresolved_marked"] = int(_late_lag["late_n"] or 0)
        # Within-lag gauge (missing-but-expected vs truly-missing triage).
        # NOT batch_-prefixed: it overlaps MISSING and must not enter the drift tile.
        counts["missing_within_lag"] = int(_late_lag["lag_n"] or 0)
        if counts["late_unresolved_marked"]:
            logger.warning(
                "late-SLA: %d placeholders past provider windows -> %s",
                counts["late_unresolved_marked"],
                EXCEPTION_LATE_UNRESOLVED,
            )
        if counts["late_unresolved_marked"]:
            spark.sql(
                f"MERGE INTO {table} t USING bank_settlements s "
                "ON t.transaction_id = s.transaction_id "
                f"WHEN MATCHED AND {_late_where} THEN UPDATE SET "
                f"t.reconciliation_status = '{EXCEPTION_LATE_UNRESOLVED}'"
            )
    except Exception as exc:
        logger.warning(f"Late-SLA marking skipped: {exc}")
    try:  # Ops depth gauges: one UNION job scans both tables in a single action.
        _dlq = _qualified_table(settings.dlq_table)
        for row in spark.sql(
            f"SELECT 'dlq' AS k, COUNT(*) AS n FROM {_dlq} "
            f"UNION ALL SELECT 'late' AS k, COUNT(*) AS n FROM {table} "
            f"WHERE reconciliation_status = '{EXCEPTION_LATE_UNRESOLVED}'"
        ).collect():
            if row["k"] == "dlq":
                counts["dlq_depth"] = row["n"]
            else:
                counts["late_unresolved_total"] = row["n"]
        counts.setdefault("dlq_depth", 0)
        counts.setdefault("late_unresolved_total", 0)
    except Exception as exc:
        logger.warning(f"Depth gauges skipped: {exc}")
    import json as _json

    logger.info(
        f"metrics run_date={date_str} "
        f"{_json.dumps({k: counts[k] for k in sorted(counts) if isinstance(counts[k], (int, float))})}"
    )
    return counts


def _bank_orphan_where(tol: int, lag_days: int) -> str:
    """Shared orphan core: bank rows matching no settlement (link or window)."""
    return (
        "FROM bank_statements b "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_settlements s "
        "WHERE s.transaction_id = b.link_tx "
        f"OR (ABS(b.amount_paise - s.settled_amount_paise) <= {tol} "
        f"AND ABS(DATEDIFF(b.value_date, s.settlement_date)) <= {int(lag_days)}))"
    )


def build_bank_leg_sql(
    table: str, lag_days: int = 2, tolerance_paise: int | None = None
) -> tuple[str, str]:
    """Third-leg SQL: bank evidence for MATCHED rows.

    Evidence = exact narration link OR (net within tolerance AND value date
    within lag). Tolerance defaults to the fee engine default (same rounding
    allowance the MERGE uses) instead of a hardcoded 1 paise. Rows without
    evidence demote to EXCEPTION_MISSING_BANK_STATEMENT; bank rows matching
    nothing are counted as orphans (logged, not statused).
    Pure builder (unit-tested); executed by reconcile_bank_leg.
    """
    table = _qualified_table(table)
    tol = (
        int(tolerance_paise)
        if tolerance_paise is not None
        else get_fee_engine().default_tolerance_paise
    )
    missing = (
        "SELECT s.transaction_id AS tid FROM bank_settlements s "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_statements b WHERE "
        "b.link_tx = s.transaction_id "
        "OR (b.link_tx IS NULL "
        f"AND ABS(b.amount_paise - s.settled_amount_paise) <= {tol} "
        f"AND ABS(DATEDIFF(b.value_date, s.settlement_date)) <= {int(lag_days)}))"
    )
    merge_sql = (
        f"MERGE INTO {table} t USING ({missing}) m "
        "ON t.transaction_id = m.tid "
        f"WHEN MATCHED AND t.reconciliation_status = '{MATCHED}' THEN "
        f"UPDATE SET t.reconciliation_status = '{EXCEPTION_MISSING_BANK_STATEMENT}'"
    )
    orphan_sql = f"SELECT COUNT(*) AS n {_bank_orphan_where(tol, lag_days)}"
    return merge_sql, orphan_sql


def reconcile_bank_leg(
    spark, table: str, bank_df, lag_days: int = 2, tolerance_paise: int | None = None
) -> dict:
    """Run the bank-statement leg. Returns {bank_evidence, bank_missing, bank_orphans}.

    bank_df: Spark frame with (bank_ref, amount_paise, value_date, link_tx).
    Requires the bank_settlements temp view from run_reconciliation.
    """
    table = _qualified_table(table)
    bank_df.createOrReplaceTempView("bank_statements")
    merge_sql, _ = build_bank_leg_sql(table, lag_days, tolerance_paise)
    # One action for both pre-MERGE gauges: before-MATCHED and orphan counts
    # share nothing but single-row shape, so UNION ALL collapses two full
    # scans into one (was 4 scans across the leg; the post-MERGE count must
    # stay separate — it reads post-write state).
    tol = (
        int(tolerance_paise)
        if tolerance_paise is not None
        else get_fee_engine().default_tolerance_paise
    )
    pre = {
        r["k"]: r["n"]
        for r in spark.sql(
            f"SELECT 'matched' AS k, COUNT(*) AS n FROM {table} "
            f"WHERE reconciliation_status = '{MATCHED}' "
            f"UNION ALL SELECT 'orphans' AS k, COUNT(*) AS n "
            f"{_bank_orphan_where(tol, lag_days)}"
        ).collect()
    }
    before, orphans = int(pre["matched"]), int(pre["orphans"])
    spark.sql(merge_sql)
    after = spark.sql(
        f"SELECT COUNT(*) AS n FROM {table} WHERE reconciliation_status = '{MATCHED}'"
    ).collect()[0]["n"]
    out = {
        "bank_evidence": int(after),
        "bank_missing": int(before - after),
        "bank_orphans": int(orphans),
    }
    logger.info(f"bank leg complete: {out}")
    return out


def maintain_tables(
    spark=None, tables: list[str] | None = None, retain_last: int | None = None
) -> dict:
    """Iceberg maintenance in dependency order (prod parity with bench hygiene).

    Order is load-bearing: expire snapshots -> remove orphans -> binpack ->
    rewrite manifests. Reversed, you compact files about to expire (wasted
    work) or delete files still referenced (corruption). Orphan removal keeps
    minAge 3d so in-flight WAP writes are never swept.

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
        try:
            retain = retain_last or int(os.environ.get("MAINTAIN_RETAIN_LAST", "7"))
        except (TypeError, ValueError):
            logger.warning(
                f"bad MAINTAIN_RETAIN_LAST={os.environ.get('MAINTAIN_RETAIN_LAST')!r}: using 7"
            )
            retain = retain_last or 7
        out: dict = {}
        # Explicit retention bound: without older_than, maintenance can sweep
        # files from in-flight writes (or never clean, depending on default).
        import datetime as _dt

        _orphan_older_than = (_dt.datetime.now(_dt.UTC) - _dt.timedelta(days=3)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        for raw in targets:
            tbl = _qualified_table(raw)
            parts = tbl.split(".")
            catalog, rest = parts[0], ".".join(parts[1:])
            entry: dict = {}
            stmts = [
                ("files_before", f"SELECT COUNT(*) AS n FROM {tbl}.files"),
                (
                    None,
                    f"CALL {catalog}.system.expire_snapshots(table => '{rest}', retain_last => {retain})",
                ),
                (
                    None,
                    f"CALL {catalog}.system.remove_orphan_files(table => '{rest}', "
                    f"older_than => TIMESTAMP '{_orphan_older_than}')",
                ),
                (
                    None,
                    f"CALL {catalog}.system.rewrite_data_files(table => '{rest}', strategy => 'binpack')",
                ),
                (
                    None,
                    f"CALL {catalog}.system.rewrite_manifests(table => '{rest}')",
                ),
                ("files_after", f"SELECT COUNT(*) AS n FROM {tbl}.files"),
            ]
            for key, sql in stmts:
                try:
                    res = spark.sql(sql)
                    if key:
                        entry[key] = res.collect()[0]["n"]
                except Exception as exc:
                    # Logged AND recorded: never-raise must not mean never-seen.
                    # entry["error"] is the oncall signal (runbook), not just a log line.
                    entry["status"] = "failed"
                    entry["error"] = f"{key or 'statement'}: {exc}"
                    logger.warning(f"Maintenance: {key or 'statement'} failed for {tbl}: {exc}")
            entry.setdefault("status", "ok")
            out[tbl] = entry
        logger.info(f"Maintenance complete: {out}")
        return out
    finally:
        if own_session:
            with _ctx.suppress(Exception):
                spark.stop()


if __name__ == "__main__":
    run_reconciliation()
