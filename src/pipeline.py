"""Daily reconciliation pipeline: cron/systemd replacement for the Airflow DAG.

Usage:
    python -m src.pipeline --date 2026-09-04
    python -m src.pipeline --demo --num-records 500   # infra demo: seeds webhooks first so MERGE matches
    cron: 0 2 * * * .venv/bin/python -m src.pipeline --date $(date +%F)
"""

import argparse
import logging
import random
from datetime import UTC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEMO_MERCHANT = "merch_demo"


def _seed_demo_webhooks(
    spark,
    table: str,
    planned: list[tuple[str, int, str]] | list[tuple[str, int, str, str]],
    skip: set[str] | None = None,
    ts: str | None = None,
) -> int:
    """Bulk-seed one webhook row per planned triple; re-runnable (MERGE-DELETEs prior rows first).

    skip: tx ids to leave unseeded (messy orphans -> EXCEPTION_MISSING_WEBHOOK).
    ts: webhook event timestamp (per-batch date for multi-day trends); defaults to now.
    """
    from datetime import datetime

    # ponytail: DDL duplicated from src/ingestion/ingest_webhooks.py __main__; extract if it changes.
    *parts, _ = table.split(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(parts)}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table} (
            transaction_id string, amount_paise bigint, gateway_status string,
            timestamp_utc string, merchant_id string, processing_run_id string,
            reconciliation_status string, bank_ref_id string, ingested_at timestamp,
            instrument_type string
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'write.parquet.compression-codec' = 'zstd'
        )"""
    )
    now = datetime.now(UTC)
    event_ts = ts or now.isoformat()
    rows = []
    seen: set[str] = set()  # one webhook per tx: dup settlement rows are settlement-side only
    for item in planned:
        if len(item) == 4:  # type: ignore[arg-type]
            tx, amt, inst, merch = item  # type: ignore[misc]
        else:
            tx, amt, inst = item  # type: ignore[misc]
            merch = DEMO_MERCHANT
        if skip and tx in skip:
            continue
        if tx in seen:
            continue
        seen.add(tx)
        rows.append((tx, amt, "SUCCESS", event_ts, merch, None, "SUCCESS", None, now, inst))
    plan_df = spark.createDataFrame(
        rows,
        "transaction_id string, amount_paise long, gateway_status string, timestamp_utc string, "
        "merchant_id string, processing_run_id string, reconciliation_status string, "
        "bank_ref_id string, ingested_at timestamp, instrument_type string",
    )
    plan_df.createOrReplaceTempView("demo_plan")
    spark.sql(
        f"MERGE INTO {table} t USING demo_plan s "
        "ON t.transaction_id = s.transaction_id WHEN MATCHED THEN DELETE"
    )
    plan_df.writeTo(table).append()
    return len(planned)


def _build_demo_plan(num_records: int, seed: int = 42) -> list[tuple[str, int, str, str]]:
    from src.common.schemas import INSTRUMENT_TYPES

    rnd = random.Random(seed)  # noqa: S311 — deterministic demo data, not crypto
    planned: list[tuple[str, int, str, str]] = []
    seen: set[str] = set()
    while len(planned) < num_records:
        tx = f"tx_{rnd.getrandbits(48):012x}"
        if tx in seen:
            continue
        seen.add(tx)
        # 20% merch_001 to exercise merchant overrides, rest DEMO_MERCHANT
        merch = "merch_001" if rnd.random() < 0.2 else DEMO_MERCHANT
        planned.append((tx, rnd.randint(1000, 1000000), rnd.choice(INSTRUMENT_TYPES), merch))
    return planned


def _assign_mix(
    planned: list[tuple[str, int, str, str]], seed: int = 42, batch_date: str | None = None
) -> tuple[list, dict[str, list[dict]], set[str]]:
    """Assign one accuracy-mix scenario per planned tx (70/10/5/5/5/2.5/2.5).

    Returns (items, row_opts, orphans): items is planned plus duplicate rows for
    dup/late scenarios; row_opts maps tx -> per-occurrence {"delta","date"};
    orphans are tx ids the seeder must skip (no webhook -> MISSING placeholder).
    """
    from datetime import datetime, timedelta

    day = batch_date or datetime.now(UTC).strftime("%Y-%m-%d")
    base = datetime.strptime(day, "%Y-%m-%d")

    def ago(d: int) -> str:
        return (base - timedelta(days=d)).strftime("%Y-%m-%d")

    rnd = random.Random(seed)  # noqa: S311 — deterministic demo data, not crypto
    items: list = []
    row_opts: dict[str, list[dict]] = {}
    orphans: set[str] = set()
    for tx, amt, inst, merch in planned:
        row = (tx, amt, inst, merch)
        r = rnd.random()
        opts: list[dict]
        if r < 0.70:
            opts = [{}]
        elif r < 0.80:
            opts = [{"delta": rnd.choice((-1, 1))}]
        elif r < 0.85:
            opts = [{"delta": 50}]
        elif r < 0.90:
            opts = [{}]
            orphans.add(tx)
        elif r < 0.95:
            opts = [{}, {}]
            items.extend([row, row])
        elif r < 0.975:
            opts = [{"date": ago(3)}]
        else:
            opts = [{"date": ago(2)}, {"date": day}]
            items.extend([row, row])
        if r < 0.90 or (0.95 <= r < 0.975):
            items.append(row)
        row_opts[tx] = opts
    return items, row_opts, orphans


def check_batch_drift(counts: dict, ds: str | None = None) -> None:
    """Fail closed if batch statuses don't tile deduped rows (shared cron/DAG).

    batch_* counts join the DEDUPED settlement view 1:1 to the target, so they
    must tile settlement_rows_deduped exactly. batch_EXCEPTION_DUPLICATE_SETTLEMENT
    is a diagnostic (rows collapsed), not an outcome — never subtract it.
    """
    """Fail closed if batch statuses don't tile deduped rows (shared cron/DAG)."""
    batch_n = counts.get("settlement_rows_deduped", 0)
    batch_total = sum(
        v
        for k, v in counts.items()
        if k.startswith("batch_") and k != "batch_EXCEPTION_DUPLICATE_SETTLEMENT"
    )
    if batch_n and batch_total and batch_total != batch_n:
        raise RuntimeError(
            f"Batch drift{f' for ds={ds}' if ds else ''}: deduped {batch_n} "
            f"but batch statuses tile {batch_total} — investigate dedup/window."
        )


def run_daily(
    date_str: str | None = None,
    num_records: int = 500,
    seed: int = 42,
    demo: bool = False,
    messy: bool = False,
) -> dict:
    """Shared daily entrypoint for cron/main and Airflow DAG (single source of truth)."""
    from src.generators.settlement_generator import generate_settlement_file
    from src.processing.reconcile import maintain_tables, run_reconciliation
    from src.validation.validate_settlement import validate_latest_settlement

    planned = _build_demo_plan(num_records, seed=seed) if demo else None
    row_opts, orphans = None, None
    if planned is not None and messy:
        planned, row_opts, orphans = _assign_mix(planned, seed=seed, batch_date=date_str)
    spark = None
    if planned is not None:
        from src.common.config import get_spark_session
        from src.common.settings import get_settings
        from src.processing.reconcile import _qualified_table

        logger.info("Step 0/3: seeding demo webhooks (same plan as settlements)")
        spark = get_spark_session("DemoSeed")
        try:
            seeded = _seed_demo_webhooks(
                spark,
                _qualified_table(get_settings().webhook_table),
                planned,
                skip=orphans,
                ts=f"{date_str}T12:00:00+00:00" if date_str else None,
            )
        finally:
            spark.stop()
            spark = None
        logger.info(f"Seeded {seeded} demo webhooks")
    logger.info("Step 1/3: generating settlement file")
    generate_settlement_file(
        num_records=num_records,
        seed=seed,
        date_str=date_str,
        planned=planned,
        row_opts=row_opts,
    )
    logger.info("Step 2/3: validating settlement file")
    validate_latest_settlement(date_str=date_str)
    logger.info("Step 3/3: running reconciliation MERGE")
    counts: dict = run_reconciliation(date_str=date_str)
    logger.info(f"Pipeline done: {counts}")
    check_batch_drift(counts, ds=date_str)
    batch_n = counts.get("settlement_rows_deduped", 0)
    batch_matched = counts.get("batch_MATCHED", counts.get("MATCHED", 0))
    if demo and batch_n and not batch_matched:
        raise RuntimeError("Demo matched nothing — seeding and settlement plan diverged.")
    try:
        counts["maintenance"] = maintain_tables()
    except Exception as exc:
        logger.warning(f"Maintenance skipped: {exc}")
    return counts


def main() -> dict:
    parser = argparse.ArgumentParser(description="Daily tx-reconciliation pipeline")
    parser.add_argument("--date", default=None, help="Settlement date YYYY-MM-DD")
    parser.add_argument("--num-records", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Seed the webhook table from the same plan as the settlement CSV, so the demo MERGE matches.",
    )
    parser.add_argument(
        "--messy",
        action="store_true",
        help="With --demo: realistic status mix (mismatches, orphans, duplicates) instead of all-MATCHED.",
    )
    args = parser.parse_args()
    return run_daily(
        date_str=args.date,
        num_records=args.num_records,
        seed=args.seed,
        demo=args.demo,
        messy=args.messy,
    )


if __name__ == "__main__":
    main()
