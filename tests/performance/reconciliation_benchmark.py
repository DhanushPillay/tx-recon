import argparse
import json
import logging
import os
import sys
import time

# Single node: executors run in-process, always use the current interpreter.
os.environ["PYSPARK_PYTHON"] = sys.executable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from hardware import get_hardware_info  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import get_spark_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Canonical fee math (must mirror src/processing/reconcile.py build_fee_case_sql
# for the default 150bps + 18% GST rate card). Integer DIV only — Spark DIV
# rejects float operands. Tolerance-aware: |expected_net - settled| <= 1.
# Single WHEN MATCHED with CASE keeps one ABS eval per row (was 2x).
def _build_merge_sql(table: str, fee_case_sql: str, gst_case_sql: str, tol_case_sql: str) -> str:
    return f"""
MERGE INTO {table} t
USING bank_settlements s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        t.reconciliation_status = CASE WHEN ABS((t.amount_paise - ({fee_case_sql}) - ({gst_case_sql})) - s.settled_amount_paise) <= ({tol_case_sql}) THEN 'MATCHED' ELSE 'EXCEPTION_FEE_MISMATCH' END,
        t.bank_ref_id = s.bank_ref_id
"""


REAL_SCALES = [1_000_000, 5_000_000, 12_000_000]
SAMPLE_SCALES = [10_000]

# Real-data inputs (thiru loader outputs). Settlement nets are loader-computed
# (independent schedule = v1 card), so clean rows MATCH and the ~5% injected
# mismatch slice reports as mismatched instead of a synthetic 0%.
REAL_HOOKS_CSV = os.path.join(os.path.dirname(__file__), "../../data/real_thiru_full_hooks.csv")
REAL_SETTLEMENT_CSV = os.path.join(
    os.path.dirname(__file__), "../../data/real_thiru_full_settlement.csv"
)
SAMPLE_HOOKS_CSV = os.path.join(os.path.dirname(__file__), "../../data/samples/real_10k_hooks.csv")
SAMPLE_SETTLEMENT_CSV = os.path.join(
    os.path.dirname(__file__), "../../data/samples/real_10k_settlement.csv"
)


def _latest_snapshot_summary(spark, table_name):
    """Latest snapshot id + summary map (Iceberg snapshots metadata table).

    Each MERGE commit's own summary carries that commit's added-data-files,
    added-delete-files, added-records etc — the per-MERGE diagnostics that
    rows/sec alone cannot show (LST-Bench rule: report physical cost).
    """
    rows = spark.sql(
        f"SELECT snapshot_id, summary FROM {table_name}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return None, {}
    return rows[0][0], dict(rows[0][1] or {})


def _hygiene(spark, catalog, table_name):
    """Binpack + expire between repeats (untimed).

    Without this, MoR delete-debt accumulates across repeats and the median
    measures degrading runs. With it, each timed MERGE starts from a
    maintained steady state — labeled 'maintained' in results.
    """
    spark.sql(
        f"CALL {catalog}.system.rewrite_data_files(table => '{table_name}', strategy => 'binpack')"
    )
    spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{table_name}', retain_last => 1)")


def _bench_ddl(spark, table_name, merge_mode="mor"):
    """Shared scratch-table DDL (all real-only runs measure the same layout)."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    mode = "merge-on-read" if merge_mode == "mor" else "copy-on-write"
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            transaction_id string,
            amount_paise bigint,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '67108864',
            'write.parquet.compression-codec' = 'snappy',
            'write.distribution-mode' = 'hash',
            'write.merge.mode' = '{mode}',
            'write.delete.mode' = '{mode}',
            'write.update.mode' = '{mode}',
            'write.fanout.enabled' = 'false',
            -- Scratch table: dropped at run end, no branches/tags reference old
            -- snapshots, so GC is safe. Required: Nessie disables GC by
            -- default and expire_snapshots is refused without it.
            'gc.enabled' = 'true'
        )
    """
    )


def create_table_real(spark, table_name, num_rows, merge_mode="mor", hooks_csv=None):
    """Bench target from real hook rows: JVM CSV read (no driver list), same DDL.

    ORDER BY makes the slice deterministic; setup is untimed so sort cost is free.
    hooks_csv overrides the default full file (e.g. data/samples/ for smoke runs).
    """
    from pyspark.sql import functions as F

    src = hooks_csv or REAL_HOOKS_CSV
    logger.info(f"Creating {table_name} with {num_rows:,} real rows from {src}...")
    _bench_ddl(spark, table_name, merge_mode)
    hooks = spark.read.csv(
        src,
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
    hooks.orderBy("transaction_id").limit(num_rows).select(
        "transaction_id",
        "amount_paise",
        F.lit("SUCCESS").alias("gateway_status"),
        F.concat(F.col("day"), F.lit("T00:00:00")).alias("timestamp_utc"),
        "merchant_id",
        F.lit("PENDING_SETTLEMENT").alias("reconciliation_status"),
        F.lit(None).cast("string").alias("bank_ref_id"),
        F.current_timestamp().alias("ingested_at"),
    ).writeTo(table_name).append()
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    logger.info(f"Table {table_name} created with {count:,} real rows")
    return count


_REAL_RAW_CACHED = False


def _real_settlement_raw(spark):
    """Read + persist the 1GB settlement CSV once per process.

    Both update fractions reuse it instead of re-scanning 12.9M rows each
    (~1 min saved per scale). Untimed setup; warmed before timing.
    """
    global _REAL_RAW_CACHED

    if _REAL_RAW_CACHED:
        return
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
        ]
    )
    raw = spark.read.csv(REAL_SETTLEMENT_CSV, header=True, schema=bank_schema)
    raw.persist()
    raw.createOrReplaceTempView("settlement_raw_real")
    raw.count()
    _REAL_RAW_CACHED = True


def create_settlement_data_real(spark, table_name, update_fraction):
    """Settlement slice = cached raw CSV semi-joined to the target ids.

    Same temp view + persist + warm-count contract as before.
    """
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    settlement_count = int(count * update_fraction)
    logger.info(
        f"Creating real settlement data: {settlement_count:,} rows "
        f"({update_fraction * 100:.0f}% of {count:,})"
    )
    _real_settlement_raw(spark)
    target_ids = spark.sql(
        f"SELECT transaction_id FROM {table_name} ORDER BY transaction_id LIMIT {settlement_count}"
    )
    settlement_df = (
        spark.sql("SELECT * FROM settlement_raw_real")
        .join(target_ids, "transaction_id")
        # Real files carry dup settlement rows (dedup-keep-latest is prod behavior);
        # MERGE demands one source row per target. Untimed setup.
        .dropDuplicates(["transaction_id"])
        .select(
            "bank_ref_id",
            "transaction_id",
            "settled_amount_paise",
            "settlement_date",
            "instrument_type",
            "merchant_id",
        )
    )
    settlement_df = settlement_df.persist()
    settlement_df.createOrReplaceTempView("bank_settlements")
    settlement_df.count()
    return settlement_count


def measure_merge(spark, catalog, table_name, update_fraction, merge_mode, repeats=3):
    import statistics

    create_settlement_data_real(spark, table_name, update_fraction)

    files_before = spark.sql(f"SELECT COUNT(*) FROM {table_name}.files").collect()[0][0]

    from src.processing.fee_engine import get_fee_engine  # noqa: E402
    from src.processing.reconcile import (  # noqa: E402
        build_fee_case_sql,
        build_tolerance_case_sql,
    )

    fee_engine = get_fee_engine()
    fee_case_sql, gst_case_sql = build_fee_case_sql(
        fee_engine, amount_col="t.amount_paise", inst_col="s.instrument_type"
    )
    tol_case_sql = build_tolerance_case_sql(fee_engine)
    merge_sql = _build_merge_sql(table_name, fee_case_sql, gst_case_sql, tol_case_sql)

    # Join strategy from the plan: broadcast (no target shuffle) vs sort-merge.
    plan = "\n".join(r[0] for r in spark.sql(f"EXPLAIN {merge_sql}").collect())
    if "BroadcastHashJoin" in plan:
        join_type = "broadcast"
    elif "SortMergeJoin" in plan:
        join_type = "sortmerge"
    else:
        join_type = "unknown"

    merge_runs = []
    reset_sql = f"""UPDATE {table_name}
                SET reconciliation_status = 'PENDING_SETTLEMENT',
                    bank_ref_id = NULL"""
    # Warmup (untimed): cold JVM/S3/JIT inflates the first MERGE 2-3x, which
    # would drag the median. One throwaway MERGE + reset before timing.
    spark.sql(merge_sql)
    spark.sql(reset_sql)
    for _ in range(repeats):
        _hygiene(spark, catalog, table_name)  # untimed steady-state reset
        start = time.perf_counter()
        spark.sql(merge_sql)
        elapsed = time.perf_counter() - start
        _, summary = _latest_snapshot_summary(spark, table_name)
        merge_runs.append(
            {
                "time_sec": round(elapsed, 2),
                "added_data_files": int(summary.get("added-data-files", 0) or 0),
                "added_delete_files": int(summary.get("added-delete-files", 0) or 0),
                "added_records": int(summary.get("added-records", 0) or 0),
                "added_data_files_size": int(summary.get("added-files-size", 0) or 0),
            }
        )
        # Reset state between repeats so each timing measures the same work.
        spark.sql(reset_sql)
    # One final MERGE leaves the table matched for the count queries below.
    start = time.perf_counter()
    spark.sql(merge_sql)
    write_time = time.perf_counter() - start
    merge_runs.append(
        {
            "time_sec": round(write_time, 2),
            "added_data_files": 0,
            "added_delete_files": 0,
            "added_records": 0,
            "added_data_files_size": 0,
            "note": "final state-setting MERGE, no diagnostics",
        }
    )
    write_time = statistics.median(r["time_sec"] for r in merge_runs)

    files_after = spark.sql(f"SELECT COUNT(*) FROM {table_name}.files").collect()[0][0]

    matched = spark.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE reconciliation_status = 'MATCHED'"
    ).collect()[0][0]
    mismatched = spark.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'"
    ).collect()[0][0]

    start = time.perf_counter()
    spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
    read_time = time.perf_counter() - start

    # Release settlement cache
    try:
        spark.catalog.uncacheTable("bank_settlements")
    except Exception:
        pass
    try:
        spark.sql("CLEAR CACHE")
    except Exception:
        pass

    return {
        "merge_mode": merge_mode,
        "join_type": join_type,
        "state": "maintained (binpack+expire between repeats)",
        "write_time_sec": round(write_time, 2),
        "write_time_median_sec": round(write_time, 2),
        "write_time_repeats": repeats + 1,
        "merge_runs": merge_runs,
        "read_time_sec": round(read_time, 2),
        "rows_per_sec": round((matched + mismatched) / write_time, 1) if write_time > 0 else 0,
        "files_before": files_before,
        "files_after": files_after,
        "matched": matched,
        "mismatched": mismatched,
        "match_rate": round(matched / (matched + mismatched), 4) if matched + mismatched else 0,
        "healthy": bool(matched + mismatched > 0),
    }


def run_benchmark(scale=None, catalog="nessie", merge_mode="mor", cluster=False, hooks_csv=None):
    from hardware import fingerprint

    hw = {**get_hardware_info(), "fingerprint": fingerprint()}
    logger.info(
        f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}"
    )

    spark = get_spark_session("ReconciliationBenchmark")

    # Adaptive query execution — tuned for single-node local[12] 32 partitions
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "32")
    spark.conf.set("spark.sql.adaptive.optimizeSkewsInReorderedPartitions.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.default.parallelism", "32")
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    if hooks_csv:
        hooks_src, settle_src = hooks_csv, REAL_SETTLEMENT_CSV
    elif os.path.exists(REAL_HOOKS_CSV) and os.path.exists(REAL_SETTLEMENT_CSV):
        hooks_src, settle_src = REAL_HOOKS_CSV, REAL_SETTLEMENT_CSV
    else:
        hooks_src, settle_src = SAMPLE_HOOKS_CSV, SAMPLE_SETTLEMENT_CSV
    for req in (hooks_src, settle_src):
        if not os.path.exists(req):
            raise FileNotFoundError(
                f"real source needs {req}: build data/samples/ or data/real_thiru_full_*.csv"
            )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.db")

    row_counts = (
        [scale] if scale else (REAL_SCALES if hooks_src == REAL_HOOKS_CSV else SAMPLE_SCALES)
    )
    results = {}
    output = {"hardware": hw, "benchmarks": results, "catalog": catalog}

    # Single node: nessie -> results_iceberg_real.json (s3a).
    # CoW runs get their own file so they never overwrite the cited MoR record.
    if merge_mode == "cow":
        out_name = "results_iceberg_cow.json"
    else:
        out_name = "results_iceberg_real.json"
    out_path = os.path.join(os.path.dirname(__file__), out_name)

    def _dump():
        # Incremental: a kill loses at most the in-flight scale, never completed ones.
        with open(out_path, "w") as f:
            json.dump(output, f, indent=2)

    for num_rows in row_counts:
        table_name = f"{catalog}.db.webhooks_bench_{num_rows // 1000}k_real"
        if num_rows >= 5_000_000:
            # 12M-row joins OOM at 32 partitions on a 16GB box (measured);
            # smaller partitions keep per-task heap flat. Runtime conf only.
            spark.conf.set("spark.sql.shuffle.partitions", "128")
            spark.conf.set("spark.default.parallelism", "128")
        create_table_real(spark, table_name, num_rows, merge_mode=merge_mode, hooks_csv=hooks_src)
        if cluster:
            # One-variable experiment: sort on the join key. Untimed setup.
            logger.info("Clustering table on transaction_id (sort rewrite)...")
            spark.sql(
                f"CALL {catalog}.system.rewrite_data_files("
                f"table => '{table_name}', strategy => 'sort', "
                f"sort_order => 'transaction_id ASC')"
            )

        for update_pct in [10, 50]:
            update_frac = update_pct / 100
            label = f"{num_rows // 1000}k_rows_{update_pct}pct_update_{merge_mode}_real"
            if cluster:
                label += "_clustered"
            logger.info(f"\n=== Benchmark: {label} ===")

            result = measure_merge(spark, catalog, table_name, update_frac, merge_mode)
            results[label] = result

            logger.info(
                f"  MERGE write: {result['write_time_sec']}s, "
                f"read: {result['read_time_sec']}s, "
                f"matched: {result['matched']:,}, "
                f"mismatched: {result['mismatched']:,}, "
                f"match_rate: {result['match_rate']:.2%}"
            )
            if result["match_rate"] < 0.85:
                raise RuntimeError(
                    f"real-data health gate: match_rate {result['match_rate']:.2%} < 85% "
                    f"(loader mix targets ~90%; fee calibration or data drift suspect)"
                )
            logger.info(f"  Files: {result['files_before']} -> {result['files_after']}")
            logger.info(f"  Join: {result['join_type']}, mode: {merge_mode}")

            spark.sql(
                f"""UPDATE {table_name}
                    SET reconciliation_status = 'PENDING_SETTLEMENT',
                        bank_ref_id = NULL"""
            )
        _dump()
        logger.info(f"  checkpoint: scale {num_rows:,} results flushed to {out_name}")

    for num_rows in row_counts:
        table_name = f"{catalog}.db.webhooks_bench_{num_rows // 1000}k_real"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    spark.stop()

    _dump()
    logger.info(f"\nResults written to {out_path} (catalog={catalog})")

    return output


def main():
    parser = argparse.ArgumentParser(description="Reconciliation Benchmark (real data only)")
    parser.add_argument("--scale", type=int, default=None)
    parser.add_argument("--catalog", type=str, default="nessie", choices=["nessie"])
    parser.add_argument("--merge-mode", type=str, default="mor", choices=["mor", "cow"])
    parser.add_argument(
        "--hooks-csv",
        type=str,
        default=None,
        help="override hooks CSV (default: full real file, sample fallback)",
    )
    parser.add_argument(
        "--cluster", action="store_true", help="sort-rewrite on join key before measuring"
    )
    args = parser.parse_args()
    allowed = REAL_SCALES + SAMPLE_SCALES
    if args.scale is not None and args.scale not in allowed:
        raise SystemExit(f"--scale {args.scale} not in {allowed} (real-only)")
    run_benchmark(args.scale, args.catalog, args.merge_mode, args.cluster, args.hooks_csv)


if __name__ == "__main__":
    main()
