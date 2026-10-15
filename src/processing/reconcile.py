import glob
import logging
import os
import re

from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.common.config import get_spark_session
from src.common.schemas import (
    EXCEPTION_CURRENCY_MISMATCH,
    EXCEPTION_FEE_MISMATCH,
    EXCEPTION_LATE_UNRESOLVED,
    EXCEPTION_MISSING_BANK_STATEMENT,
    EXCEPTION_MISSING_WEBHOOK,
    INSTRUMENT_TYPES,
    MATCHED,
    PENDING_RECONCILIATION,
    PLACEHOLDER_STATUSES,
    TERMINAL_STATUSES,
)
from src.common.settings import get_settings
from src.processing.fee_engine import _merge_merchants, get_fee_engine, gst_pct_to_bps

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z0-9_.]+$")

# same 16 as the bucket count in ingest_webhooks, so both sides line up
_DEDUP_PARTITIONS = 16


def _qualified_table(name: str) -> str:
    if not _TABLE_RE.match(name):
        raise ValueError(f"Unsafe table identifier: {name!r}")
    return name


def _sql_str(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def known_fee_merchants(fee_engine) -> set[str]:
    """Merchant ids that have a negotiated rate on at least one rate card."""
    out: set[str] = set()
    for card in getattr(fee_engine, "rate_cards", None) or []:
        out.update(k for k in (card.get("merchants") or {}) if k)
    return out


def earliest_rate_card_date(fee_engine) -> str:
    dates = [
        c.get("effective_from")
        for c in (getattr(fee_engine, "rate_cards", None) or [])
        if c.get("effective_from")
    ]
    return min(dates) if dates else "1970-01-01"


# Every one of these used to be a silent fallback inside the fee CASE. A row
# that reached one still got a fee, so the batch looked clean while the amount
# had been priced off a rate that was never agreed.
FEE_FALLBACK_COUNT_KEYS = {
    "card_unparseable_date": "fee_card_unparseable_date_rows",
    "card_out_of_range": "fee_card_out_of_range_rows",
    "merchant_unknown": "fee_merchant_unknown_rows",
    "instrument_unknown": "fee_instrument_unknown_rows",
}


def fee_fallback_sql(
    fee_engine, date_col: str = "settlement_date", merchant_col: str = "merchant_id"
) -> str:
    """A CASE naming which silent fee fallback each settlement row would take.

    The predicates mirror _versioned_wrap and _build_single_card_fee_sql
    deliberately: a diagnostic that drifts from the expression it describes is
    worse than none, because it understates the problem.
    """
    known = ", ".join(f"'{_sql_str(m)}'" for m in sorted(known_fee_merchants(fee_engine)))
    known = known or "''"
    instruments = ", ".join(f"'{_sql_str(i)}'" for i in INSTRUMENT_TYPES)
    d = f"to_date({date_col}, 'yyyy-MM-dd')"
    first = _sql_str(earliest_rate_card_date(fee_engine))
    return (
        "CASE "
        f"WHEN {d} IS NULL THEN 'card_unparseable_date' "
        f"WHEN {d} < DATE '{first}' THEN 'card_out_of_range' "
        f"WHEN {merchant_col} IS NOT NULL AND {merchant_col} <> 'UNKNOWN' "
        f"AND {merchant_col} NOT IN ({known}) THEN 'merchant_unknown' "
        f"WHEN instrument_type IS NULL OR instrument_type NOT IN ({instruments}) "
        "THEN 'instrument_unknown' "
        "ELSE NULL END"
    )


def fold_fallback_rows(rows) -> dict[str, int]:
    """GROUP BY output of fee_fallback_sql into the four counters, all present."""
    seen = {str(r["reason"]): int(r["n"] or 0) for r in rows}
    return {key: seen.get(reason, 0) for reason, key in FEE_FALLBACK_COUNT_KEYS.items()}


def fold_settlements(bank_df):
    """Collapse redeliveries, then combine the instalments of a split payout.

    Returns (distinct_rows, one_row_per_transaction, partial_txn_count).

    The old behaviour took row_number() == 1 per transaction and dropped the
    rest. That is correct for a provider re-sending the same settlement line
    and wrong for a merchant paid in two instalments: the second instalment was
    discarded before the MERGE saw it, and whichever row sorted first became
    the whole settlement. A transaction that settled 400 then 600 was compared
    as 600 alone, and if the tolerance happened to cover it the row was marked
    MATCHED with 400 paise of settled revenue unrecorded and unflagged. The
    discarded rows were then reported as duplicate_settlement_rows, which is
    how an instalment plan got filed as a delivery problem.

    So the two cases are separated by content instead of by arrival:

      - rows identical on amount, date and bank reference are redeliveries and
        collapse to one;
      - whatever survives that is summed per transaction, with the latest date
        as the effective one, because that is the instalment that completed it.
    """
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    order = F.struct("settlement_date", "bank_ref_id", "settlement_id")
    redelivery_window = Window.partitionBy(
        "transaction_id", "settled_amount_paise", "settlement_date", "bank_ref_id"
    ).orderBy(order)
    distinct = (
        bank_df.withColumn("row_num", F.row_number().over(redelivery_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    # max_by on the same ordering the old tiebreak used, so a split payout with
    # two rows sharing a date still resolves to one of them deterministically.
    latest = F.max_by
    folded = distinct.groupBy("transaction_id").agg(
        F.sum("settled_amount_paise").alias("settled_amount_paise"),
        F.max("settlement_date").alias("settlement_date"),
        latest("bank_ref_id", order).alias("bank_ref_id"),
        F.first("instrument_type", ignorenulls=True).alias("instrument_type"),
        F.first("merchant_id", ignorenulls=True).alias("merchant_id"),
        F.first("currency", ignorenulls=True).alias("currency"),
        F.first("provider", ignorenulls=True).alias("provider"),
        F.sum("fee_paise").alias("fee_paise"),
        F.sum("gst_paise").alias("gst_paise"),
        latest("settlement_id", order).alias("settlement_id"),
        latest("utr", order).alias("utr"),
        latest("gross_amount_paise", order).alias("gross_amount_paise"),
        F.count("*").alias("installment_rows"),
    )
    partial = folded.filter(F.col("installment_rows") > 1).count()
    return distinct, folded.drop("installment_rows"), partial


def _bps_sql(value_col: str, bps: int) -> str:
    """value_col * bps / 10000 in paise, rounded half away from zero.

    Spark's `DIV` truncates toward zero, so the plain
    `(x * bps + 5000) DIV 10000` is round-half-up only while x is positive. On a
    negative x it lands a paisa short of the rule `fee_engine.bps_amount`
    applies in Python, and the two halves of the same formula then disagree by
    one paisa on almost every negative amount rather than on a rare tie.
    """
    return (
        f"CASE WHEN {value_col} < 0 THEN -(((-({value_col})) * {bps} + 5000) DIV 10000) "
        f"ELSE (({value_col} * {bps} + 5000) DIV 10000) END"
    )


def _append_leg(fee_cases, gst_cases, tol_cases, pred, amount_col, mdr_bps, gst_bps, tol):
    fee_sql = _bps_sql(amount_col, mdr_bps)
    fee_cases.append(f"WHEN {pred} THEN {fee_sql}")
    if gst_bps > 0:
        gst_cases.append(f"WHEN {pred} THEN {_bps_sql(fee_sql, gst_bps)}")
    else:
        gst_cases.append(f"WHEN {pred} THEN 0")
    tol_cases.append(f"WHEN {pred} THEN {tol}")


def _versioned_wrap(cards, top_merchants, date_col, build):
    per = []
    for card in reversed(cards):
        eff_from_esc = _sql_str(card.get("effective_from", "1970-01-01"))
        card_eff = dict(card)
        card_eff["merchants"] = _merge_merchants(top_merchants, card.get("merchants", {}))
        cond = f"{date_col} >= '{eff_from_esc}'"
        per.append((cond, build(card_eff)))
    first_eff = dict(cards[0])
    first_eff["merchants"] = _merge_merchants(top_merchants, cards[0].get("merchants", {}))
    first = build(first_eff)
    latest_eff = dict(cards[-1])
    latest_eff["merchants"] = _merge_merchants(top_merchants, cards[-1].get("merchants", {}))
    latest = build(latest_eff)
    null_cond = f"to_date({date_col}, 'yyyy-MM-dd') IS NULL"
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

    merchants = card.get("merchants", {}) or {}
    for merch, inst_map in merchants.items():
        if not merch:
            continue
        merch_esc = _sql_str(merch)
        for inst, rate in (inst_map or {}).items():
            mdr_bps, gst_bps, tol = _triple(rate)
            inst_esc = _sql_str(inst)
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
        inst_esc = _sql_str(inst)
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

    default_fee_sql = _bps_sql(amount_col, default_mdr)
    fee_sql = "CASE " + " ".join(fee_cases) + f" ELSE {default_fee_sql} END"
    if default_gst_bps > 0:
        gst_sql = (
            "CASE "
            + " ".join(gst_cases)
            + f" ELSE {_bps_sql(default_fee_sql, default_gst_bps)} END"
        )
    else:
        gst_sql = "CASE " + " ".join(gst_cases) + " ELSE 0 END"
    tol_sql = "CASE " + " ".join(tol_cases) + f" ELSE {default_tol} END"
    return fee_sql, gst_sql, tol_sql


def _single_card(fee_engine) -> dict:
    top_merchants = (getattr(fee_engine, "config", {}) or {}).get("merchants", {}) or {}
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) == 1:
        card = dict(cards[0])
    else:
        card = {
            "default": fee_engine.get_default_rate(),
            "instruments": fee_engine.config.get("instruments", {}),
            "merchants": {},
        }
    card["merchants"] = _merge_merchants(top_merchants, card.get("merchants", {}) or {})
    return card


def build_fee_case_sql(
    fee_engine,
    amount_col="t.amount_paise",
    inst_col="s.instrument_type",
    merchant_col="s.merchant_id",
    settlement_date_col="s.settlement_date",
):
    cards = getattr(fee_engine, "rate_cards", None)
    if cards and len(cards) > 1:
        top_merchants = (getattr(fee_engine, "config", {}) or {}).get("merchants", {}) or {}
        return _versioned_wrap(
            cards,
            top_merchants,
            settlement_date_col,
            lambda c: _build_single_card_fee_sql(c, amount_col, inst_col, merchant_col)[:2],
        )

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
    if date_str:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{8}", date_str):
            raise ValueError(f"Bad date_str (want YYYY-MM-DD): {date_str!r}")
        stem = date_str.replace("-", "")
        curated = os.path.join(data_dir, f"curated_settlement_{stem}.csv")
        if os.path.exists(curated):
            return curated.replace("\\", "/"), [curated]
        if os.environ.get("ALLOW_UNVALIDATED_SETTLEMENT", "") not in {"1", "true", "TRUE"}:
            # no curated file means stop, never merge the raw drop
            raise FileNotFoundError(
                f"curated settlement not found: {curated} -- run "
                "src.validation.validate_settlement.validate_latest_settlement first. "
                "Set ALLOW_UNVALIDATED_SETTLEMENT=1 to merge the raw drop anyway "
                "(unvalidated: no INR->paise normalization, no date or instrument checks)."
            )
        raw = os.path.join(data_dir, f"settlement_{stem}.csv")
        return raw.replace("\\", "/"), [raw]
    curated_files = sorted(glob.glob(os.path.join(data_dir, "curated_settlement_*.csv")))
    if curated_files:
        pattern = os.path.join(data_dir, "curated_settlement_*.csv")
        return pattern.replace("\\", "/"), curated_files
    pattern = os.path.join(data_dir, "settlement_*.csv")
    return pattern.replace("\\", "/"), sorted(glob.glob(pattern))


def _mart_table(source_table: str) -> str:
    return f"{source_table.rsplit('.', 1)[0]}.fact_reconciliation"


def refresh_mart(spark, settings) -> None:
    """Rebuild fact_reconciliation from whatever the webhook table holds now.

    The pipeline calls this only after the drift and SLO gates pass. It used to
    run inside run_reconciliation, which meant a batch that failed its gate had
    already replaced the mart -- and the mart is what every Trino and Metabase
    reader sees. WAP branched the webhook table but never this one.
    """
    table = _qualified_table(settings.webhook_table)
    mart_table = _mart_table(table)
    try:
        mart_sql, mart_version = _load_mart_sql(settings.project_root, table, mart_table)
        spark.sql(mart_sql)
        logger.info(f"mart {mart_version} refreshed: {mart_table}")
    except Exception as exc:
        msg = f"Could not refresh fact_reconciliation table: {exc}"
        if settings.strict_slo:
            raise RuntimeError(msg) from exc
        logger.warning(msg)


_REWRITE_TARGET_BYTES = 128 * 1024 * 1024
_REWRITE_MAX_GROUP_BYTES = 4 * 1024 * 1024 * 1024


def _load_mart_sql(project_root: str, source_table: str, mart_table: str) -> tuple[str, str]:
    files = glob.glob(os.path.join(project_root, "sql", "marts", "migrations", "V*.sql"))
    if files:

        def _ver(path: str) -> tuple[int, str]:
            m = re.match(r"V(\d+)", os.path.basename(path))
            return (int(m.group(1)) if m else -1, path)

        path = max(files, key=_ver)
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
        "TRY_CAST(timestamp_utc AS TIMESTAMP) AS transacted_at, "
        "CASE WHEN reconciliation_status = 'MATCHED' THEN amount_paise "
        f"ELSE NULL END AS matched_amount_paise FROM {source_table}",
        "inline",
    )


def fold_status_counts(rows) -> dict[str, int]:
    """Turn the scope-tagged counts query into a counts dict.

    Three scopes come back: `table` for every status in the warehouse, `batch`
    for the statuses of the rows this settlement file touched, and `dupmut` for
    the running total of transactions the gateway re-delivered with a different
    amount after they were already reconciled.
    """
    counts: dict[str, int] = {}
    for row in rows:
        if row["scope"] == "table":
            counts[row["st"]] = row["n"]
        elif row["scope"] == "dupmut":
            # SUM over no rows is NULL, and an absent key would read as a
            # missing measurement rather than a real zero.
            counts["duplicate_mutations"] = int(row["n"] or 0)
        else:
            counts[f"batch_{row['st']}"] = row["n"]
    return counts


def batch_match_rate(counts: dict) -> float | None:
    """MATCHED over the rows a settlement could legitimately account for.

    Shared with pipeline._fold_residual so the publish gate, the SLO breach
    warning and the metrics record cannot each derive a rate their own way.
    Returns None when no settlement row survived evaluation, since there is
    nothing to be a percentage of.
    """
    _bm = counts.get("batch_MATCHED", 0)
    _bt = max(
        0,
        int(counts.get("settlement_rows_evaluated", 0)) - int(counts.get("missing_within_lag", 0)),
    )
    return _bm / _bt if _bt else None


def run_reconciliation(date_str: str | None = None) -> dict[str, int | float]:
    if not date_str:
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
    from src.adapters.settlement import CANONICAL_COLS

    assert [f.name for f in bank_schema.fields] == CANONICAL_COLS, (
        "bank_schema drifted from CANONICAL_COLS"
    )

    try:
        if settings.load_csv_on_driver:
            import pandas as pd

            _frames = []
            for _f in settlement_files[:1]:
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
            for _frame in _frames[1:]:
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

    bank_df_valid = (
        bank_df.filter(F.col("transaction_id").isNotNull())
        .repartition(_DEDUP_PARTITIONS, "transaction_id")
        .cache()
    )
    settlement_rows_total = bank_df_valid.count()
    bank_df_distinct, bank_df_dedup, partial_settlement_txns = fold_settlements(bank_df_valid)
    bank_df_dedup.cache()
    settlement_rows_distinct = bank_df_distinct.count()
    settlement_rows_deduped = bank_df_dedup.count()
    settlement_rows_evaluated = bank_df_dedup.filter(
        F.col("settled_amount_paise").isNotNull()
    ).count()
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

    fee_fallbacks = fold_fallback_rows(
        spark.sql(
            f"SELECT reason, COUNT(*) AS n FROM ("
            f"SELECT {fee_fallback_sql(fee_engine)} AS reason FROM bank_settlements) "
            "WHERE reason IS NOT NULL GROUP BY reason"
        ).collect()
    )
    _fallback_total = sum(fee_fallbacks.values())
    if _fallback_total:
        logger.warning(
            "%d settlement row(s) would be priced off a rate that was never "
            "agreed for them (%s). Set STRICT_FEE_RATES=1 to refuse the batch "
            "instead of counting it.",
            _fallback_total,
            ", ".join(f"{k}={v}" for k, v in fee_fallbacks.items() if v),
        )
        if os.environ.get("STRICT_FEE_RATES", "0").strip().lower() in ("1", "true", "yes"):
            raise RuntimeError(
                f"STRICT_FEE_RATES=1 and {_fallback_total} row(s) hit a fee fallback: "
                + ", ".join(f"{k}={v}" for k, v in fee_fallbacks.items() if v)
            )

    table = _qualified_table(settings.webhook_table)
    _placeholder_in = " OR ".join(f"t.reconciliation_status = '{s}'" for s in PLACEHOLDER_STATUSES)
    _terminal_not_in = ", ".join(f"'{s}'" for s in TERMINAL_STATUSES)
    merge_sql = f"""
    MERGE INTO {table} t
    USING (
        SELECT
            s.transaction_id,
            s.bank_ref_id,
            s.settled_amount_paise,
            s.instrument_type,
            COALESCE(s.merchant_id, 'UNKNOWN') AS merchant_id,
            s.settlement_date,
            s.currency
        FROM bank_settlements s
        WHERE s.transaction_id IS NOT NULL AND s.settled_amount_paise IS NOT NULL
    ) s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND ({_placeholder_in}) THEN
        UPDATE SET
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN MATCHED AND t.amount_paise IS NULL THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_FEE_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN MATCHED AND t.reconciliation_status NOT IN ({_terminal_not_in})
             AND t.currency IS NOT NULL AND s.currency IS NOT NULL
             AND t.currency <> s.currency THEN
        UPDATE SET
            t.reconciliation_status = '{EXCEPTION_CURRENCY_MISMATCH}',
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN MATCHED AND t.reconciliation_status NOT IN ({_terminal_not_in}) THEN
        UPDATE SET
            t.reconciliation_status = CASE WHEN ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= ({tolerance_sql}) THEN '{MATCHED}' ELSE '{EXCEPTION_FEE_MISMATCH}' END,
            t.bank_ref_id = s.bank_ref_id,
            t.instrument_type = s.instrument_type
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, reconciliation_status, bank_ref_id, instrument_type, currency)
        VALUES (s.transaction_id, s.settled_amount_paise, 'UNKNOWN', s.settlement_date, COALESCE(s.merchant_id, 'UNKNOWN'), '{EXCEPTION_MISSING_WEBHOOK}', s.bank_ref_id, s.instrument_type, s.currency)
    """

    logger.info("Executing MERGE INTO operation")
    try:
        spark.sql(merge_sql)
    except Exception as exc:
        import contextlib as _ctx

        with _ctx.suppress(Exception):
            bank_df_dedup.unpersist()
        raise RuntimeError(f"Reconciliation MERGE failed: {exc}") from exc

    bank_out = _run_bank_leg(spark, table, data_dir, date_str, settings)

    # The mart is deliberately NOT refreshed here. run_daily calls
    # refresh_mart once the drift and SLO gates pass, so a rejected batch never
    # reaches the readers of fact_reconciliation.

    counts: dict[str, int | float] = {"settlement_rows_total": settlement_rows_total}
    counts["settlement_rows_deduped"] = settlement_rows_deduped
    counts["settlement_rows_evaluated"] = settlement_rows_evaluated
    counts["duplicate_settlement_rows"] = max(0, settlement_rows_total - settlement_rows_distinct)
    counts["settlement_rows_distinct"] = settlement_rows_distinct
    counts["partial_settlement_txns"] = partial_settlement_txns
    # A settlement that declares no currency cannot be proven comparable to a
    # webhook that declares one, and the fee tolerance has no meaning across
    # currencies. Counted, not assumed. Deliberately not batch_-prefixed: it is
    # a data-quality signal, not a tile of the batch status breakdown that
    # check_batch_drift sums.
    counts["settlement_currency_missing_rows"] = bank_df_dedup.filter(
        F.col("currency").isNull()
    ).count()
    # Not batch_-prefixed for the same reason as the currency counter: these
    # describe how the batch was priced, not how its rows are distributed, and
    # check_batch_drift sums every batch_ int.
    counts.update(fee_fallbacks)
    counts.update(bank_out)
    if counts["duplicate_settlement_rows"]:
        counts["batch_EXCEPTION_DUPLICATE_SETTLEMENT"] = counts["duplicate_settlement_rows"]
        logger.warning(
            "Duplicate settlements deduped: %d row(s) were byte-identical redeliveries "
            "of a settlement already in the batch (same amount, date and bank reference)",
            counts["duplicate_settlement_rows"],
        )
    if counts["partial_settlement_txns"]:
        logger.warning(
            "%d transaction(s) settled in more than one instalment; the instalments "
            "were summed rather than one of them being kept",
            counts["partial_settlement_txns"],
        )
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

    try:
        from src.processing.batches import load_providers, provider_spec

        try:
            _pcfg = load_providers(os.path.join(project_root, "config", "providers.yaml"))
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(f"provider config unreadable ({exc}): using global late SLA")
            _pcfg = {"default": {}, "providers": {}}
        _global_days = (
            _env_int("LATE_SLA_DAYS", 7)
            if "LATE_SLA_DAYS" in _os.environ
            else int(provider_spec(_pcfg, None).get("late_sla_days", 7))
        )
        _known = sorted(
            {
                str(r["provider"])
                for r in spark.sql(
                    "SELECT DISTINCT provider FROM bank_settlements WHERE provider IS NOT NULL"
                ).collect()
            }
        )
        _unresolved_in = " OR ".join(
            f"reconciliation_status = '{s}'"
            for s in (*PLACEHOLDER_STATUSES, PENDING_RECONCILIATION)
        )
        _late_pred = f"({_unresolved_in}) AND to_date(timestamp_utc) < date_sub(current_date(), {int(_global_days)})"
        _late_n = spark.sql(f"SELECT COUNT(*) AS n FROM {table} t WHERE {_late_pred}").collect()[0][
            "n"
        ]
        if _late_n:
            spark.sql(
                f"UPDATE {table} SET reconciliation_status = '{EXCEPTION_LATE_UNRESOLVED}' "
                f"WHERE {_late_pred}"
            )
            logger.warning(
                "late-SLA: %d unresolved rows past %d days -> %s",
                _late_n,
                int(_global_days),
                EXCEPTION_LATE_UNRESOLVED,
            )
        counts["late_unresolved_marked"] = int(_late_n or 0)
        _lag_whens = " ".join(
            f"WHEN '{_sql_str(p)}' THEN {int(provider_spec(_pcfg, p).get('lag_days', 2))}"
            for p in _known
            if p != "None"
        )
        _lag_case = f"CASE s.provider {_lag_whens} ELSE 2 END" if _lag_whens else "2"
        counts["missing_within_lag"] = int(
            spark.sql(
                f"SELECT COUNT(*) AS n FROM {table} t "
                f"JOIN bank_settlements s ON s.transaction_id = t.transaction_id "
                f"WHERE t.reconciliation_status = '{EXCEPTION_MISSING_WEBHOOK}' "
                f"AND DATEDIFF(s.settlement_date, to_date(t.timestamp_utc)) "
                f"BETWEEN 0 AND ({_lag_case})"
            ).collect()[0]["n"]
            or 0
        )
    except Exception as exc:
        counts["late_sla_failed"] = 1
        logger.error("Late-SLA sweep failed, unresolved rows NOT aged: %s", exc)

    try:
        # semi join so dup settlement rows cant double count the batch
        _rows = spark.sql(
            f"SELECT 'table' AS scope, reconciliation_status AS st, COUNT(*) AS n "
            f"FROM {table} GROUP BY reconciliation_status "
            f"UNION ALL "
            f"SELECT 'batch' AS scope, t.reconciliation_status AS st, COUNT(*) AS n "
            f"FROM {table} t "
            f"WHERE t.transaction_id IN (SELECT transaction_id FROM bank_settlements) "
            f"GROUP BY t.reconciliation_status "
            f"UNION ALL "
            # Whole table, so this ride-along costs one column on a scan the
            # query already makes.
            "SELECT 'dupmut' AS scope, 'duplicate_mutations' AS st, "
            f"COALESCE(SUM(duplicate_mutation_count), 0) AS n FROM {table}"
        ).collect()
        counts.update(fold_status_counts(_rows))
    except Exception as exc:
        raise RuntimeError(f"Could not fetch reconciliation counts: {exc}") from exc
    finally:
        import contextlib

        with contextlib.suppress(Exception):
            bank_df_dedup.unpersist()
        with contextlib.suppress(Exception):
            bank_df_valid.unpersist()
    if counts["settlement_currency_missing_rows"]:
        logger.warning(
            "%d settlement row(s) declare no currency, so they are compared against "
            "the webhook amount as if both were the same unit; see "
            "batch_EXCEPTION_CURRENCY_MISMATCH for the rows where both sides "
            "declared different currencies",
            counts["settlement_currency_missing_rows"],
        )
    _dup_mutations = counts.get("duplicate_mutations") or 0
    if _dup_mutations:
        logger.warning(
            "%d transaction(s) were re-delivered with a different amount after they "
            "were already reconciled; see duplicate_mutation_count on the webhook table",
            _dup_mutations,
        )
    logger.info(f"Reconciliation batch complete: {counts}")

    _rate = batch_match_rate(counts)
    if _rate is not None:
        # Full precision on purpose. The publish gate in pipeline._slo_ok reads
        # this same key, and a rate of 0.94997 rounded to 0.95 passed that gate
        # while the comparison here below correctly called it a breach.
        counts["batch_match_rate"] = _rate
        if _rate < _slo:
            msg = f"Match-rate SLO breach: {_rate:.2%} < {_slo:.0%} (batch)"
            if settings.strict_slo:
                raise RuntimeError(msg)
            logger.warning(msg)
    try:
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
    except Exception as exc:
        logger.warning(f"Depth gauges skipped: {exc}")
    counts.setdefault("dlq_depth", 0)
    counts.setdefault("late_unresolved_total", 0)
    import json as _json

    logger.info(
        f"metrics run_date={date_str} "
        f"{_json.dumps({k: counts[k] for k in sorted(counts) if isinstance(counts[k], (int, float))})}"
    )
    return counts


def _bank_orphan_where(tol_sql: str, lag_days: int) -> str:
    return (
        "FROM bank_statements b "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_settlements s "
        "WHERE s.transaction_id = b.link_tx "
        f"OR (ABS(b.amount_paise - s.settled_amount_paise) <= {tol_sql} "
        f"AND ABS(DATEDIFF(b.value_date, s.settlement_date)) <= {int(lag_days)}))"
    )


def _bank_tolerance_sql(fee_engine, inst_col: str, merchant_col: str, date_col: str) -> str:
    return build_tolerance_case_sql(
        fee_engine, inst_col=inst_col, merchant_col=merchant_col, settlement_date_col=date_col
    )


def build_bank_leg_sql(
    table: str, lag_days: int = 2, tolerance_paise: int | None = None
) -> tuple[str, str]:
    table = _qualified_table(table)
    fee_engine = get_fee_engine()
    tol = (
        str(int(tolerance_paise))
        if tolerance_paise is not None
        else _bank_tolerance_sql(
            fee_engine, "s.instrument_type", "s.merchant_id", "s.settlement_date"
        )
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
    table = _qualified_table(table)
    import pandas as pd

    if isinstance(bank_df, pd.DataFrame):
        bank_df = spark.createDataFrame(bank_df)
    bank_df.createOrReplaceTempView("bank_statements")
    merge_sql, _ = build_bank_leg_sql(table, lag_days, tolerance_paise)
    tol = (
        str(int(tolerance_paise))
        if tolerance_paise is not None
        else _bank_tolerance_sql(
            get_fee_engine(), "s.instrument_type", "s.merchant_id", "s.settlement_date"
        )
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


def leg_enabled(name: str, settings=None) -> bool:
    settings = settings or get_settings()
    raw = (getattr(settings, "wired_legs", "") or "").strip()
    if not raw:
        return True
    return name in {p.strip().lower() for p in raw.split(",") if p.strip()}


def _run_bank_leg(spark, table: str, data_dir: str, date_str: str, settings) -> dict:
    if not leg_enabled("bank", settings):
        logger.info("bank leg skipped: not in WIRED_LEGS")
        return {"bank_statement": 0}
    stem = date_str.replace("-", "")
    path = os.path.join(data_dir, f"statement_{stem}.sta")
    if not os.path.exists(path):
        logger.warning(
            "bank leg skipped: no statement file at %s; MATCHED rows keep "
            "settlement+webhook evidence only",
            path,
        )
        return {"bank_statement": 0}
    from src.adapters.bank_statement import parse_mt940
    from src.processing.batches import load_providers, provider_spec

    lag_days = provider_spec(
        load_providers(os.path.join(settings.project_root, "config", "providers.yaml")), None
    )["lag_days"]
    try:
        bank_df = parse_mt940(path)
        out = reconcile_bank_leg(spark, table, bank_df, lag_days=lag_days)
    except Exception as exc:
        msg = f"Bank leg failed: {exc}"
        if settings.strict_slo:
            raise RuntimeError(msg) from exc
        logger.warning(msg)
        return {"bank_statement": 0}
    return {"bank_statement": 1, **out}


def maintain_tables(
    spark=None, tables: list[str] | None = None, retain_last: int | None = None
) -> dict:
    from src.common.config import get_spark_session

    settings = get_settings()
    if not settings.maintain_after_merge:
        logger.info("Maintenance skipped: MAINTAIN_AFTER_MERGE=0")
        return {"status": "skipped", "reason": "MAINTAIN_AFTER_MERGE=0"}
    if spark is None:
        spark = get_spark_session("Maintenance")
    # never spark.stop() here: getOrCreate returns the shared singleton and
    # stopping it kills the caller's session mid-pipeline (pipeline.py, DAG).
    targets = tables or [settings.webhook_table, settings.dlq_table]
    mart = _mart_table(settings.webhook_table)
    if not tables and mart not in targets:
        targets = [*targets, mart]
    per_table_retain = {mart: 1}
    try:
        retain = retain_last or int(os.environ.get("MAINTAIN_RETAIN_LAST", "7"))
    except (TypeError, ValueError):
        logger.warning(
            f"bad MAINTAIN_RETAIN_LAST={os.environ.get('MAINTAIN_RETAIN_LAST')!r}: using 7"
        )
        retain = retain_last or 7
    out: dict = {}
    import datetime as _dt

    _orphan_older_than = (_dt.datetime.now(_dt.UTC) - _dt.timedelta(days=3)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    for raw in targets:
        tbl = _qualified_table(raw)
        parts = tbl.split(".")
        catalog, rest = parts[0], ".".join(parts[1:])
        entry: dict = {}
        _retain = per_table_retain.get(raw, retain)
        stmts = [
            ("files_before", f"SELECT COUNT(*) AS n FROM {tbl}.files"),
            (
                None,
                f"CALL {catalog}.system.expire_snapshots(table => '{rest}', retain_last => {_retain})",
            ),
            (
                None,
                f"CALL {catalog}.system.remove_orphan_files(table => '{rest}', "
                f"older_than => TIMESTAMP '{_orphan_older_than}')",
            ),
            (
                None,
                f"CALL {catalog}.system.rewrite_data_files(table => '{rest}', strategy => 'binpack', "
                f"options => map('target-file-size-bytes', '{_REWRITE_TARGET_BYTES}', "
                f"'max-file-group-size-bytes', '{_REWRITE_MAX_GROUP_BYTES}'))",
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
                entry["status"] = "failed"
                entry["error"] = f"{key or 'statement'}: {exc}"
                logger.warning(f"Maintenance: {key or 'statement'} failed for {tbl}: {exc}")
        entry.setdefault("status", "ok")
        out[tbl] = entry
    logger.info(f"Maintenance complete: {out}")
    return out


if __name__ == "__main__":
    run_reconciliation()
