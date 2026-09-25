"""Daily reconciliation pipeline: cron/systemd replacement for the Airflow DAG.

Real-data only. Usage:
    python -m src.pipeline --date 2026-09-04
    cron: 0 2 * * * .venv/bin/python -m src.pipeline --date $(date +%F)

Fail-closed: merges a real PG settlement file, never fabricates one
(validate raises FileNotFoundError when absent).
"""

import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def check_batch_drift(counts: dict, ds: str | None = None) -> None:
    """Fail closed if batch statuses don't tile deduped rows (shared cron/DAG).

    batch_* counts join the DEDUPED settlement view 1:1 to the target, so they
    must tile settlement_rows_deduped exactly. batch_EXCEPTION_DUPLICATE_SETTLEMENT
    is a diagnostic (rows collapsed), not an outcome — never subtract it.
    """
    batch_n = counts.get("settlement_rows_deduped", 0)
    batch_total = sum(
        v
        for k, v in counts.items()
        if k.startswith("batch_")
        and k != "batch_EXCEPTION_DUPLICATE_SETTLEMENT"
        and isinstance(v, int)  # gauges like batch_match_rate (float) are not statuses
    )
    if batch_n and batch_total and batch_total != batch_n:
        raise RuntimeError(
            f"Batch drift{f' for ds={ds}' if ds else ''}: deduped {batch_n} "
            f"but batch statuses tile {batch_total} — investigate dedup/window."
        )


def _resolve_batch_date(validated_path: str, date_str: str | None) -> str:
    """Batch date for MERGE/drift: explicit --date wins, else the validated filename.

    Fail closed: reconcile must never silently merge every curated file when the
    date is unknown (date-less runs re-merge all history).
    """
    import re

    if date_str:
        return date_str
    m = re.search(r"(\d{8})", os.path.basename(validated_path))
    if not m:
        raise ValueError(
            f"cannot resolve batch date from {validated_path!r}: pass --date YYYY-MM-DD"
        )
    stem = m.group(1)
    return f"{stem[:4]}-{stem[4:6]}-{stem[6:]}"


def run_daily(date_str: str | None = None) -> dict:
    """Shared daily entrypoint for cron/main and Airflow DAG (single source of truth).

    Real-data only: merges the PG settlement file for date_str, never synthesizes.
    """
    import json

    from src.processing.reconcile import maintain_tables, run_reconciliation
    from src.validation.validate_settlement import validate_latest_settlement

    logger.info("Step 1/2: validating real PG settlement file")
    curated_path = validate_latest_settlement(date_str=date_str)
    if not curated_path:
        raise RuntimeError("validation returned no curated file")
    batch_date = _resolve_batch_date(curated_path, date_str)
    logger.info("Step 2/2: running reconciliation MERGE")
    counts: dict = run_reconciliation(date_str=batch_date)
    logger.info(f"Pipeline done: {counts}")
    check_batch_drift(counts, ds=batch_date)
    try:  # machine-readable ops record next to the curated file
        metrics_path = os.path.join(
            os.path.dirname(curated_path), f"metrics_{batch_date.replace('-', '')}.json"
        )
        with open(metrics_path, "w", encoding="utf-8") as fh:
            json.dump({"batch_date": batch_date, "counts": counts}, fh, default=str)
    except OSError as exc:
        logger.warning(f"metrics file skipped: {exc}")
    try:
        counts["maintenance"] = maintain_tables()
    except Exception as exc:
        logger.warning(f"Maintenance skipped: {exc}")
    return counts


def main() -> dict:
    parser = argparse.ArgumentParser(
        description="Daily tx-reconciliation pipeline (real data only)"
    )
    parser.add_argument("--date", default=None, help="Settlement date YYYY-MM-DD")
    args = parser.parse_args()
    return run_daily(date_str=args.date)


if __name__ == "__main__":
    main()
