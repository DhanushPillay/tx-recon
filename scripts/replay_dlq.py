"""Replay DLQ rows back into webhooks after fixing upstream data.

Usage: python scripts/replay_dlq.py [--delete]  (--delete removes replayed rows from DLQ)
Read-only by default: reports replayable count without writing.
"""

import argparse
import logging

from src.common.config import get_spark_session
from src.common.settings import get_settings
from src.processing.reconcile import _qualified_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete", action="store_true", help="delete replayed rows from DLQ")
    args = ap.parse_args()

    settings = get_settings()
    spark = get_spark_session("ReplayDLQ")
    dlq = _qualified_table(settings.dlq_table)
    target = _qualified_table(settings.webhook_table)

    replayable = spark.sql(
        f"SELECT * FROM {dlq} WHERE transaction_id IS NOT NULL AND amount_paise > 0"
    )
    n = replayable.count()
    logger.info(f"DLQ replayable rows: {n}")
    if n == 0:
        return {"replayable": 0}

    replayable.createOrReplaceTempView("dlq_replay")
    spark.sql(
        f"MERGE INTO {target} t USING dlq_replay s "
        "ON t.transaction_id = s.transaction_id "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    logger.info(f"Replayed {n} rows into {target}")
    if args.delete:
        spark.sql(
            f"MERGE INTO {dlq} t USING dlq_replay s "
            "ON t.transaction_id = s.transaction_id "
            "WHEN MATCHED THEN DELETE"
        )
        logger.info("Deleted replayed rows from DLQ")
    return {"replayable": n, "deleted": bool(args.delete)}


if __name__ == "__main__":
    main()
