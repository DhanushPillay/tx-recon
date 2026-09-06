"""Daily reconciliation pipeline: cron/systemd replacement for the Airflow DAG.

Usage:
    python -m src.pipeline --date 2026-09-04
    python -m src.pipeline --demo --num-records 500   # infra demo: seeds webhooks first so MERGE matches
    cron: 0 2 * * * .venv/bin/python -m src.pipeline --date $(date +%F)
"""

import argparse
import logging
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEMO_MERCHANT = "merch_demo"


def _seed_demo_webhooks(spark, table: str, planned: list[tuple[str, int, str]]) -> int:
    """Insert one webhook row per planned triple; re-runnable (deletes prior demo rows first)."""
    from datetime import datetime, timezone

    # ponytail: DDL duplicated from src/ingestion/ingest_webhooks.py __main__; extract if it changes.
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table} (
            transaction_id string, amount_paise bigint, gateway_status string,
            timestamp_utc string, merchant_id string, processing_run_id string,
            reconciliation_status string, bank_ref_id string, ingested_at timestamp
        ) USING iceberg"""
    )
    ids = ",".join(f"'{tx}'" for tx, _, _ in planned)
    spark.sql(f"DELETE FROM {table} WHERE transaction_id IN ({ids})")
    now = datetime.now(timezone.utc).isoformat()
    values = ",\n".join(
        f"('{tx}', {amt}, 'SUCCESS', '{now}', '{DEMO_MERCHANT}', NULL, 'SUCCESS', NULL, current_timestamp())"
        for tx, amt, _ in planned
    )
    spark.sql(
        f"""INSERT INTO {table}
            (transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id,
             processing_run_id, reconciliation_status, bank_ref_id, ingested_at)
            VALUES {values}"""
    )
    return len(planned)


def _build_demo_plan(num_records: int, seed: int = 42) -> list[tuple[str, int, str]]:
    from src.common.schemas import INSTRUMENT_TYPES

    rnd = random.Random(seed)
    planned, seen = [], set()
    while len(planned) < num_records:
        tx = f"tx_{rnd.getrandbits(48):012x}"
        if tx in seen:
            continue
        seen.add(tx)
        planned.append((tx, rnd.randint(1000, 1000000), rnd.choice(INSTRUMENT_TYPES)))
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
    if planned is not None:
        from src.common.config import get_spark_session
        from src.common.settings import get_settings
        from src.processing.reconcile import _qualified_table

        logger.info("Step 0/3: seeding demo webhooks (same plan as settlements)")
        spark = get_spark_session("DemoSeed")
        seeded = _seed_demo_webhooks(spark, _qualified_table(get_settings().webhook_table), planned)
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
    if args.demo and not counts.get("MATCHED"):
        logger.warning("Demo matched nothing — seeding and settlement plan diverged, investigate.")
    return counts


if __name__ == "__main__":
    main()
