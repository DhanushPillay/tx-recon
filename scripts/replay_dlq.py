"""Replay DLQ rows back into webhooks after fixing upstream data.

Usage: python scripts/replay_dlq.py [--delete --reason TICKET-1 --approved-by name]
Read-only by default: reports replayable count without writing.
--delete purges replayed DLQ rows and requires --reason + --approved-by
(maker-checker); the purge is recorded in data/corrections.jsonl.
"""

import argparse
import logging
import os

from src.common.config import get_spark_session
from src.common.settings import get_settings
from src.processing.corrections import journal_correction
from src.processing.reconcile import _qualified_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete", action="store_true", help="delete replayed rows from DLQ")
    ap.add_argument("--reason", default="", help="ticket/cause (required with --delete)")
    ap.add_argument("--approved-by", default="", help="second approver (required with --delete)")
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
        # Validate BEFORE the purge: journal_correction also refuses empty
        # reason/approver, but only after the DELETE already ran — rows would
        # be gone with no journal record. Fail fast instead.
        if not (args.reason or "").strip():
            raise ValueError("--delete requires --reason (ticket/cause)")
        if not (args.approved_by or "").strip():
            raise ValueError("--delete requires --approved-by (second pair of eyes)")
        spark.sql(
            f"MERGE INTO {dlq} t USING dlq_replay s "
            "ON t.transaction_id = s.transaction_id "
            "WHEN MATCHED THEN DELETE"
        )
        # Journal after the purge succeeds: the record must describe reality.
        journal_correction(
            os.path.join(settings.project_root, "data"),
            "dlq_purge",
            args.reason,
            args.approved_by,
            {"rows": n, "table": dlq},
        )
        logger.info("Deleted replayed rows from DLQ")
    return {"replayable": n, "deleted": bool(args.delete)}


if __name__ == "__main__":
    main()
