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
    spark, table: str, planned: list[tuple[str, int, str]] | list[tuple[str, int, str, str]]
) -> int:
    """Bulk-seed one webhook row per planned triple; re-runnable (MERGE-DELETEs prior rows first)."""
    from datetime import datetime

    # ponytail: DDL duplicated from src/ingestion/ingest_webhooks.py __main__; extract if it changes.
    *parts, _ = table.split(".")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(parts)}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table} (
            transaction_id string, amount_paise bigint, gateway_status string,
            timestamp_utc string, merchant_id string, processing_run_id string,
            reconciliation_status string, bank_ref_id string, ingested_at timestamp
        ) USING iceberg"""
    )
    now = datetime.now(UTC)
    rows = []
    for item in planned:
        if len(item) == 4:  # type: ignore[arg-type]
            tx, amt, _, merch = item  # type: ignore[misc]
        else:
            tx, amt, _ = item  # type: ignore[misc]
            merch = DEMO_MERCHANT
        rows.append((tx, amt, "SUCCESS", now.isoformat(), merch, None, "SUCCESS", None, now))
    plan_df = spark.createDataFrame(
        rows,
        "transaction_id string, amount_paise long, gateway_status string, timestamp_utc string, "
        "merchant_id string, processing_run_id string, reconciliation_status string, "
        "bank_ref_id string, ingested_at timestamp",
    ).repartition(32)
    plan_df.createOrReplaceTempView("demo_plan")
    spark.sql(
        f"MERGE INTO {table} t USING demo_plan s "
        "ON t.transaction_id = s.transaction_id WHEN MATCHED THEN DELETE"
    )
    plan_df.writeTo(table).append()
    return len(planned)


def _build_demo_plan(num_records: int, seed: int = 42) -> list[tuple[str, int, str, str]]:
    from src.common.schemas import INSTRUMENT_TYPES

    rnd = random.Random(seed)
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


def main() -> dict:
    parser = argparse.ArgumentParser(description="Daily tx-reconciliation pipeline")
    parser.add_argument("--date", default=None, help="Settlement date YYYY-MM-DD")
    parser.add_argument("--num-records", type=int, default=500)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Seed the webhook table from the same plan as the settlement CSV, so the demo MERGE matches.",
    )
    args = parser.parse_args()

    from src.generators.settlement_generator import generate_settlement_file
    from src.processing.reconcile import run_reconciliation
    from src.validation.validate_settlement import validate_latest_settlement

    planned = _build_demo_plan(args.num_records) if args.demo else None
    spark = None
    if planned is not None:
        from src.common.config import get_spark_session
        from src.common.settings import get_settings
        from src.processing.reconcile import _qualified_table

        logger.info("Step 0/3: seeding demo webhooks (same plan as settlements)")
        spark = get_spark_session("DemoSeed")
        try:
            seeded = _seed_demo_webhooks(
                spark, _qualified_table(get_settings().webhook_table), planned
            )
        finally:
            spark.stop()
            spark = None
        logger.info(f"Seeded {seeded} demo webhooks")
    logger.info("Step 1/3: generating settlement file")
    generate_settlement_file(
        num_records=args.num_records, seed=42, date_str=args.date, planned=planned
    )
    logger.info("Step 2/3: validating settlement file")
    validate_latest_settlement(date_str=args.date)
    logger.info("Step 3/3: running reconciliation MERGE")
    counts = run_reconciliation(date_str=args.date)
    logger.info(f"Pipeline done: {counts}")
    batch_n = counts.get("settlement_rows_deduped", 0)
    batch_matched = counts.get("batch_MATCHED", counts.get("MATCHED", 0))
    if args.demo and batch_n and not batch_matched:
        logger.warning("Demo matched nothing — seeding and settlement plan diverged, investigate.")
    return counts


if __name__ == "__main__":
    main()
