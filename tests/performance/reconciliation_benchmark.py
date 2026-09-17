import argparse
import json
import logging
import os
import random
import sys
import time
import uuid

# YARN executors run in Linux containers; Windows venv path with spaces fails there.
# Use python3 for YARN (containers must have python3), venv python for local.
if os.environ.get("SPARK_MODE") == "yarn" or os.environ.get("SPARK_MASTER") == "yarn":
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
else:
    os.environ["PYSPARK_PYTHON"] = sys.executable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from hardware import get_hardware_info  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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


SCALE_OPTIONS = [100_000, 500_000, 1_000_000, 2_000_000, 5_000_000]


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


def create_table(spark, table_name, num_rows, seed=7, merge_mode="mor"):
    logger.info(f"Creating {table_name} with {num_rows:,} rows...")
    rnd = random.Random(seed)

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

    batch_size = 50000
    schema = StructType(
        [
            StructField("transaction_id", StringType()),
            StructField("amount_paise", LongType()),
            StructField("gateway_status", StringType()),
            StructField("timestamp_utc", StringType()),
            StructField("merchant_id", StringType()),
            StructField("reconciliation_status", StringType()),
            StructField("bank_ref_id", StringType()),
            StructField("ingested_at", TimestampType()),
        ]
    )
    for offset in range(0, num_rows, batch_size):
        current_batch = min(batch_size, num_rows - offset)
        data = [
            (
                f"tx_{uuid.UUID(int=rnd.getrandbits(128)).hex[:12]}",
                rnd.randint(1000, 1000000),
                "SUCCESS",
                "2024-01-15T10:00:00Z",
                "merch_12345",
                "PENDING_SETTLEMENT",
                None,
                None,
            )
            for _ in range(current_batch)
        ]
        df = spark.createDataFrame(data, schema)
        # 4 partitions: 500k in 50k batches -> 40 files, the Sep-15 baseline
        # layout. 8 partitions doubled it to 80 small files and the MERGE scan
        # regressed ~20-45% (measured 16 Sep). File size is tuned via the
        # 64MB TBLPROPERTY, not partition count.
        df.repartition(4).writeTo(table_name).append()

    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    logger.info(f"Table {table_name} created with {count:,} rows")
    return count


def create_settlement_data(spark, table_name, update_fraction):
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    settlement_count = int(count * update_fraction)
    logger.info(
        f"Creating settlement data: {settlement_count:,} rows ({update_fraction * 100:.0f}% of {count:,})"
    )

    # ORDER BY makes sampling deterministic for the seeded table (bare LIMIT
    # is nondeterministic across runs); setup is untimed so sort cost is free.
    ids_df = spark.sql(
        f"SELECT transaction_id, amount_paise, merchant_id FROM {table_name} "
        f"ORDER BY transaction_id LIMIT {settlement_count}"
    )

    from pyspark.sql.functions import col, lit

    # Fee-accurate settlement net: mirror FeeEngine integer paise math.
    # Settlement generation uses gateway amount -> net via compute_fee;
    # benchmark derives settled from t.amount_paise similarly for measurement.
    # For speed we use SQL-equivalent integer math via FeeEngine loop in Python
    # per row is slow, so keep SQL expression for ids_df, but use correct DIV.
    # Keep simple default 150bps/18% mirror for bench table creation.
    settlement_df = (
        ids_df.withColumn(
            "settled_amount_paise",
            col("amount_paise")
            - ((col("amount_paise") * lit(150) + lit(5000)) / lit(10000)).cast("long")
            - (
                (
                    ((col("amount_paise") * lit(150) + lit(5000)) / lit(10000)).cast("long")
                    * lit(1800)
                    + lit(5000)
                )
                / lit(10000)
            ).cast("long"),
        )
        .withColumn("bank_ref_id", col("transaction_id"))
        .withColumn("settlement_date", lit("2024-01-16"))
        .withColumn("instrument_type", lit("WALLET"))
        .select(
            "bank_ref_id",
            "transaction_id",
            "settled_amount_paise",
            "settlement_date",
            "instrument_type",
            "merchant_id",
        )
    )

    # Persist avoids re-evaluating the LIMIT + fee math 4x in measure_merge loop
    settlement_df = settlement_df.persist()
    settlement_df.createOrReplaceTempView("bank_settlements")
    # Warm cache before timing
    settlement_df.count()
    return settlement_count


def measure_merge(spark, catalog, table_name, update_fraction, merge_mode, repeats=3):
    import statistics

    create_settlement_data(spark, table_name, update_fraction)

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
        start = time.time()
        spark.sql(merge_sql)
        elapsed = time.time() - start
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
    start = time.time()
    spark.sql(merge_sql)
    write_time = time.time() - start
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

    start = time.time()
    spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
    read_time = time.time() - start

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
        "healthy": bool(matched + mismatched > 0),
    }


def run_benchmark(scale=None, catalog="nessie", merge_mode="mor", cluster=False):
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
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")
    spark.conf.set("spark.default.parallelism", "32")
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.db")

    row_counts = [scale] if scale else SCALE_OPTIONS
    results = {}

    for num_rows in row_counts:
        table_name = f"{catalog}.db.webhooks_bench_{num_rows // 1000}k"
        create_table(spark, table_name, num_rows, merge_mode=merge_mode)
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
            label = f"{num_rows // 1000}k_rows_{update_pct}pct_update_{merge_mode}"
            if cluster:
                label += "_clustered"
            logger.info(f"\n=== Benchmark: {label} ===")

            result = measure_merge(spark, catalog, table_name, update_frac, merge_mode)
            results[label] = result

            logger.info(
                f"  MERGE write: {result['write_time_sec']}s, "
                f"read: {result['read_time_sec']}s, "
                f"matched: {result['matched']:,}, "
                f"mismatched: {result['mismatched']:,}"
            )
            logger.info(f"  Files: {result['files_before']} -> {result['files_after']}")
            logger.info(f"  Join: {result['join_type']}, mode: {merge_mode}")

            spark.sql(
                f"""UPDATE {table_name}
                    SET reconciliation_status = 'PENDING_SETTLEMENT',
                        bank_ref_id = NULL"""
            )

    for num_rows in row_counts:
        table_name = f"{catalog}.db.webhooks_bench_{num_rows // 1000}k"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    spark.stop()

    output = {"hardware": hw, "benchmarks": results, "catalog": catalog}

    # Dual-catalog: nessie -> results_iceberg.json (s3a), nessie_hdfs -> results_iceberg_yarn_hdfs.json (hdfs).
    # CoW runs get their own file so they never overwrite the cited MoR record.
    if catalog == "nessie_hdfs":
        out_name = "results_iceberg_yarn_hdfs.json"
    elif merge_mode == "cow":
        out_name = "results_iceberg_cow.json"
    else:
        out_name = "results_iceberg.json"
    out_path = os.path.join(os.path.dirname(__file__), out_name)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"\nResults written to {out_path} (catalog={catalog})")

    return output


def main():
    parser = argparse.ArgumentParser(description="Reconciliation Benchmark")
    parser.add_argument("--scale", type=int, default=None, choices=SCALE_OPTIONS)
    parser.add_argument("--catalog", type=str, default="nessie", choices=["nessie", "nessie_hdfs"])
    parser.add_argument("--merge-mode", type=str, default="mor", choices=["mor", "cow"])
    parser.add_argument(
        "--cluster", action="store_true", help="sort-rewrite on join key before measuring"
    )
    args = parser.parse_args()
    run_benchmark(args.scale, args.catalog, args.merge_mode, args.cluster)


if __name__ == "__main__":
    main()
