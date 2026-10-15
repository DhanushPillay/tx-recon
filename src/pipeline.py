import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def check_batch_drift(counts: dict, ds: str | None = None) -> None:
    # A real 0 must stay 0. Only an absent key falls back to the deduped count:
    # reconciliation reports 0 evaluated whenever every settled_amount_paise is
    # NULL (reachable with ALLOW_UNVALIDATED_SETTLEMENT=1), and reading that as
    # "missing" turned a degenerate batch into a bogus drift error.
    batch_n = counts.get("settlement_rows_evaluated")
    if batch_n is None:
        batch_n = counts.get("settlement_rows_deduped", 0)
    batch_total = sum(
        v
        for k, v in counts.items()
        if k.startswith("batch_")
        and k != "batch_EXCEPTION_DUPLICATE_SETTLEMENT"
        and isinstance(v, int)
    )
    if batch_n == 0:
        logger.warning(
            "Empty settlement batch%s: 0 rows evaluated, no drift or SLO check possible.",
            f" for ds={ds}" if ds else "",
        )
        return
    if batch_total != batch_n:
        raise RuntimeError(
            f"Batch drift{f' for ds={ds}' if ds else ''}: evaluated {batch_n} "
            f"but batch statuses tile {batch_total} — investigate dedup/window."
        )


def _resolve_batch_date(validated_path: str, date_str: str | None) -> str:
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


def _run_residual(counts: dict) -> bool:
    from src.common.config import get_spark_session
    from src.common.settings import get_settings
    from src.processing.reconcile import leg_enabled
    from src.processing.residual import apply_residual

    settings = get_settings()
    if not leg_enabled("residual", settings):
        logger.info("residual pass skipped: not in WIRED_LEGS")
        return True
    try:
        demoted = apply_residual(get_spark_session("residual"), settings.webhook_table)
        counts["residual"] = demoted
        _fold_residual(counts, demoted)
    except Exception as exc:
        msg = f"Residual pass failed: {exc}"
        if settings.strict_slo:
            raise RuntimeError(msg) from exc
        logger.warning(msg)
        return False
    return True


def _for_metrics(counts: dict) -> dict:
    """Round the rate for the record, not for the gate.

    ``batch_match_rate`` is kept full precision so the publish gate compares the
    rate the batch actually produced. The metrics file is a human artifact, so
    it gets the four decimals the runbook quotes.
    """
    rate = counts.get("batch_match_rate")
    if isinstance(rate, float):
        return {**counts, "batch_match_rate": round(rate, 4)}
    return counts


def _slo_ok(counts: dict) -> bool:
    try:
        slo = float(os.environ.get("MATCH_RATE_SLO", 0.95))
    except (TypeError, ValueError):
        slo = 0.95
    rate = counts.get("batch_match_rate")
    if rate is None:
        return True
    try:
        return float(rate) >= slo
    except (TypeError, ValueError):
        return False


def _fold_residual(counts: dict, demoted: int) -> None:
    """Fold the residual pass's demotions back into the batch counts.

    run_reconciliation counts statuses and computes batch_match_rate before the
    residual runs, and apply_residual then moves MATCHED rows to
    EXCEPTION_FEE_MISMATCH. Nothing recomputed the counts afterwards, so the
    publish gate judged the rate the batch had before a second opinion acted on
    it.

    The status *sum* is unchanged by a demotion, which is why check_batch_drift
    kept passing and nobody noticed the rate going stale. Only the rate and the
    two buckets move here.
    """
    from src.processing.reconcile import batch_match_rate

    if demoted <= 0:
        return
    counts["batch_MATCHED"] = max(0, counts.get("batch_MATCHED", 0) - demoted)
    counts["batch_EXCEPTION_FEE_MISMATCH"] = counts.get("batch_EXCEPTION_FEE_MISMATCH", 0) + demoted
    _rate = batch_match_rate(counts)
    if _rate is not None:
        counts["batch_match_rate"] = _rate


def run_daily(date_str: str | None = None) -> dict:
    import json

    from src.common.config import get_spark_session
    from src.common.settings import get_settings
    from src.processing.reconcile import (
        drop_batch_temp_views,
        leg_enabled,
        maintain_tables,
        refresh_mart,
        run_reconciliation,
    )
    from src.processing.wap import (
        branch_name,
        create_branch,
        drop_branch,
        enable_wap,
        publish_branch,
        use_branch,
    )
    from src.validation.validate_settlement import validate_latest_settlement

    settings = get_settings()
    logger.info("Step 1/5: validating real PG settlement file")
    curated_path = validate_latest_settlement(date_str=date_str)
    if not curated_path:
        raise RuntimeError("validation returned no curated file")
    batch_date = _resolve_batch_date(curated_path, date_str)

    wap_on = leg_enabled("wap", settings)
    branch = branch_name(batch_date)
    if wap_on:
        spark = get_spark_session("pipeline")
        enable_wap(spark, settings.webhook_table)
        drop_branch(spark, settings.webhook_table, branch)
        create_branch(spark, settings.webhook_table, branch)
        use_branch(spark, branch)

    published = False
    try:
        logger.info("Step 2/5: running reconciliation MERGE")
        counts: dict = run_reconciliation(date_str=batch_date)
        logger.info("Step 3/5: running residual second opinion")
        residual_ok = _run_residual(counts)
        logger.info("Step 4/5: verifying batch")
        check_batch_drift(counts, ds=batch_date)
        if not residual_ok:
            raise RuntimeError("publish gate failed: residual second opinion failed")
        if not _slo_ok(counts):
            raise RuntimeError(
                f"publish gate failed: batch_match_rate {counts.get('batch_match_rate')} "
                f"< SLO {os.environ.get('MATCH_RATE_SLO', 0.95)}"
            )
        # Last, once the batch is known good: the mart is a published artifact
        # that WAP does not branch, so rebuilding it before the gates would
        # hand a rejected batch to every Trino and Metabase reader.
        logger.info("Step 5/5: refreshing fact_reconciliation and publishing")
        refresh_mart(spark if wap_on else get_spark_session("pipeline"), settings)
        published = True
    finally:
        if wap_on:
            try:
                if published:
                    publish_branch(spark, settings.webhook_table, branch, validated=True)
                    drop_branch(spark, settings.webhook_table, branch)
                else:
                    drop_branch(spark, settings.webhook_table, branch)
            except Exception as exc:
                logger.warning(f"WAP finalize failed: {exc}")
                if published:
                    raise
            use_branch(spark, None)
        # Last. The residual pass above reads bank_settlements, so the batch
        # owns that view until every gate has run -- including a gate that raised.
        drop_batch_temp_views(spark)
    logger.info(f"Pipeline done: {counts}")
    try:
        metrics_path = os.path.join(
            os.path.dirname(curated_path), f"metrics_{batch_date.replace('-', '')}.json"
        )
        with open(metrics_path, "w", encoding="utf-8") as fh:
            json.dump({"batch_date": batch_date, "counts": _for_metrics(counts)}, fh, default=str)
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
