"""Real-data loader: public transaction dumps -> canonical settlement CSV + webhook staging.

One leg real (amounts, dates, merchants), one leg derived: no public dataset
pairs gateway gross with PG net, so the loader applies an INDEPENDENT flat fee
schedule inline (never FeeEngine) and config/fee_rates.yaml v1 is calibrated
to the same numbers. MATCHED is then earned by the MERGE, not assumed.

PII strip at load: only tx id, amount, date, merchant/mcc, card type cross
the boundary. card_number, names, addresses, balances never leave parquet.
"""

import hashlib
import logging
import re

logger = logging.getLogger(__name__)

# Loader-side fee schedule. MUST equal fee_rates.yaml v1
# (CREDIT 200bps / DEBIT 100bps, 18% GST, tol 1). Kept as literals on purpose:
# importing FeeEngine here would make loader and matcher share code.
_LOADER_RATES = {
    "CREDIT_CARD": (200, 1800),
    "DEBIT_CARD": (100, 1800),
}
_LOADER_TOL = 1

THIRU_INSTRUMENT = {
    "Credit": "CREDIT_CARD",
    "Debit": "DEBIT_CARD",
    "Debit (Prepaid)": "DEBIT_CARD",
}

_FLUSH_EVERY = 200_000  # chunked CSV/parquet writes keep driver memory flat at 13M scale

CANONICAL_ORDER = [
    "bank_ref_id",
    "transaction_id",
    "settled_amount_paise",
    "settlement_date",
    "instrument_type",
    "merchant_id",
    "fee_paise",
    "gst_paise",
    "settlement_id",
    "utr",
    "currency",
    "gross_amount_paise",
]


def loader_net(gross_paise: int, instrument: str) -> tuple[int, int, int]:
    """Independent integer fee math: (mdr_fee, gst, net). Mirrors v1, shares no code."""
    mdr_bps, gst_bps = _LOADER_RATES[instrument]
    fee = (gross_paise * mdr_bps + 5000) // 10000
    gst = (fee * gst_bps + 5000) // 10000 if gst_bps > 0 else 0
    return fee, gst, gross_paise - fee - gst


def _bucket(tx: str, seed: int) -> int:
    """Stable 0-99 bucket per tx (deterministic across runs, no RNG state at scale)."""
    h = hashlib.sha256(f"{seed}|{tx}".encode()).digest()
    return int.from_bytes(h[:2], "big") % 100


def _noise_delta(tx: str, seed: int) -> int:
    """Rounding noise voted by hash: always exactly +-1 (inside tolerance 1)."""
    return 1 if _bucket(f"n|{tx}", seed) % 2 else -1


def build_real_files(
    source_parquet: str,
    out_settlement_csv: str,
    out_webhook_csv: str,
    dataset: str = "thiru",
    sample_pct: int = 100,
    seed: int = 7,
    spark=None,
) -> dict:
    """Read public parquet with Spark, write canonical settlement CSV (chunked) +
    webhook staging CSV. Returns counts. Never raises on dirty rows: they
    are counted (dropped_invalid) and skipped so the file always validates.
    """
    from src.common.config import get_spark_session

    spark = spark or get_spark_session("RealDataLoader")
    df = spark.read.parquet(source_parquet)
    if dataset != "thiru":
        raise ValueError(f"unknown dataset {dataset!r}")

    counts = {"rows": 0, "kept": 0, "dropped_invalid": 0, "orphans": 0, "dups": 0, "mismatch": 0}
    import pandas as pd

    first = True
    hook_rows: list = []
    rows: list = []
    hook_first = True
    hook_cols = ["transaction_id", "amount_paise", "day", "merchant_id", "instrument_type"]

    # toLocalIterator yields Row objects (one partition at a time, bounded
    # driver memory); fixed select order + positional access, no per-row asDict.
    cols = ["transaction_id", "amount", "merchant_id", "card_type", "day"]
    for r in df.select(*cols).toLocalIterator():
        counts["rows"] += 1
        tx = str(r[0])
        try:
            gross = int(round(float(r[1]) * 100))
        except (TypeError, ValueError):
            counts["dropped_invalid"] += 1
            continue
        if gross <= 0:  # refunds/chargebacks/zeros: kept out of the clean file
            counts["dropped_invalid"] += 1
            continue
        if sample_pct < 100 and _bucket(tx, seed) >= sample_pct:
            continue
        inst = THIRU_INSTRUMENT.get(r[3], "CREDIT_CARD")
        day = str(r[4])[:10]  # clean parquet carries YYYY-MM-DD already
        merch = str(r[2]) if r[2] is not None else None
        fee, gst, net = loader_net(gross, inst)
        b = _bucket(f"exc|{tx}", seed)
        settled, orphan = net, False
        if b < 5:  # genuine mismatch
            settled = net + 50 + (_bucket(f"m|{tx}", seed) % 4950)
            counts["mismatch"] += 1
        elif b < 7:  # rounding noise, still MATCHED (clamped: settled must stay > 0)
            settled = max(1, net + _noise_delta(tx, seed))
        elif b < 12:  # orphan: webhook withheld -> MISSING_WEBHOOK
            orphan = True
            counts["orphans"] += 1
        rows.append(
            (f"bnk_{tx}", tx, settled, day, inst, merch, fee, gst, f"set_{tx}", None, None, gross)
        )
        if b in (12, 13):  # duplicate settlement row, latest wins
            rows.append(
                (
                    f"bnk_{tx}b",
                    tx,
                    settled,
                    day,
                    inst,
                    merch,
                    fee,
                    gst,
                    f"set_{tx}b",
                    None,
                    None,
                    gross,
                )
            )
            counts["dups"] += 1
        if not orphan:
            hook_rows.append((tx, gross, day, merch, inst))
        if len(rows) >= _FLUSH_EVERY:  # flush CSV in bounded driver memory
            chunk = pd.DataFrame(rows, columns=CANONICAL_ORDER)
            chunk.to_csv(out_settlement_csv, mode="w" if first else "a", header=first, index=False)
            first = False
            counts["kept"] += len(rows)
            rows = []
        if len(hook_rows) > _FLUSH_EVERY:  # flush webhook staging in bounded memory
            hook_first = _flush_hooks_csv(hook_rows, hook_cols, out_webhook_csv, hook_first)
            hook_rows = []
    if rows:
        chunk = pd.DataFrame(rows, columns=CANONICAL_ORDER)
        chunk.to_csv(out_settlement_csv, mode="w" if first else "a", header=first, index=False)
        counts["kept"] += len(rows)
    if hook_rows:
        _flush_hooks_csv(hook_rows, hook_cols, out_webhook_csv, hook_first)
    logger.info(f"real-data build complete: {counts}")
    return counts


def _flush_hooks_csv(hook_rows: list, cols: list, out_csv: str, first: bool) -> bool:
    """Append hook rows to a staging CSV (pandas path: no Spark Python-worker on Windows)."""
    import pandas as pd

    pd.DataFrame(hook_rows, columns=cols).to_csv(
        out_csv, mode="w" if first else "a", header=first, index=False
    )
    return False


def seed_real_webhooks(spark, table: str, staging_csv: str, batch_date: str) -> int:
    """Spark-native seed from staging CSV (JVM read; no driver-side 13M list).

    Re-runnable: MERGE-DELETEs prior rows for the staged ids first. Destructive
    by design — requires allow_destructive_seed.
    """
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from src.common.settings import get_settings
    from src.ingestion.ingest_webhooks import ensure_webhook_table
    from src.processing.reconcile import _qualified_table

    if not get_settings().allow_destructive_seed:
        raise RuntimeError(
            "webhook seeding deletes existing rows: set ALLOW_DESTRUCTIVE_SEED=1 to confirm"
        )

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", batch_date):
        raise ValueError(f"bad batch_date {batch_date!r}: want YYYY-MM-DD")
    table = _qualified_table(table)
    ensure_webhook_table(spark, table)
    st = spark.read.csv(
        staging_csv,
        header=True,
        schema=StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("amount_paise", LongType(), True),
                StructField("day", StringType(), True),
                StructField("merchant_id", StringType(), True),
                StructField("instrument_type", StringType(), True),
            ]
        ),
    )
    st.createOrReplaceTempView("real_hooks")
    spark.sql(
        f"MERGE INTO {table} t USING real_hooks s "
        "ON t.transaction_id = s.transaction_id WHEN MATCHED THEN DELETE"
    )
    n = st.count()
    spark.sql(
        f"INSERT INTO {table} SELECT transaction_id, amount_paise, 'SUCCESS', "
        f"'{batch_date}T00:00:00', merchant_id, NULL, 'SUCCESS', NULL, "
        f"current_timestamp(), instrument_type FROM real_hooks"
    )
    return n
