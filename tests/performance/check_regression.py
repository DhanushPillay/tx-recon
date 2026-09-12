"""ServiceLab-lite regression gate: fail a PR if perf moved vs baseline.

Compares tests/performance/results.json against a baseline JSON file:
  - throughput_msgs_sec must not drop more than 15%
  - ack p99 must not rise more than 20%
  - iceberg rows_per_sec (if present both sides) must not drop more than 15%

Usage:
    python tests/performance/check_regression.py --baseline baseline_results.json
    python tests/performance/check_regression.py  # compares nothing, prints current
"""

import argparse
import json
import os
import sys

TP_DROP_PCT = 15.0
P99_RISE_PCT = 20.0


def _pct_change(new, old):
    return (new - old) / old * 100 if old else 0.0


def check(current, baseline):
    failures = []
    warn = []
    ch = current.get("hardware", {}).get("fingerprint")
    bh = baseline.get("hardware", {}).get("fingerprint")
    if ch and bh and ch != bh:
        warn.append(f"hardware changed since baseline ({bh} -> {ch}): treat deltas as noisy")
    elif ch and not bh:
        warn.append("baseline has no hardware fingerprint: deltas may not be comparable")
    ck, bk = current.get("kafka", {}), baseline.get("kafka", {})
    if "throughput_msgs_sec" in ck and "throughput_msgs_sec" in bk:
        d = _pct_change(ck["throughput_msgs_sec"], bk["throughput_msgs_sec"])
        if d < -TP_DROP_PCT:
            failures.append(f"kafka throughput dropped {d:.1f}% (>{TP_DROP_PCT}%)")
    cl, bl = ck.get("ack_latency", {}), bk.get("ack_latency", {})
    if "p99" in cl and "p99" in bl:
        d = _pct_change(cl["p99"], bl["p99"])
        if d > P99_RISE_PCT:
            failures.append(f"kafka p99 rose {d:.1f}% (>{P99_RISE_PCT}%)")
    ci, bi = current.get("iceberg", {}), baseline.get("iceberg", {})
    for bench, vals in ci.get("benchmarks", {}).items():
        if vals.get("healthy") is False:
            failures.append(f"iceberg {bench} matched nothing (healthy=false)")
        if bench in bi.get("benchmarks", {}):
            old, new = bi["benchmarks"][bench], vals
            if "rows_per_sec" in new and "rows_per_sec" in old:
                d = _pct_change(new["rows_per_sec"], old["rows_per_sec"])
                if d < -TP_DROP_PCT:
                    failures.append(f"iceberg {bench} rows/sec dropped {d:.1f}%")
    acc = current.get("accuracy", {})
    if acc.get("min_f1", 1.0) < 1.0 or acc.get("max_false_positives", 0) > 0:
        failures.append("accuracy gate failed (min_f1 < 1.0 or FP > 0)")
    return failures, warn


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(os.path.dirname(__file__), "results.json"))
    ap.add_argument("--baseline", default=None)
    args = ap.parse_args()
    with open(args.results) as f:
        current = json.load(f)
    if not args.baseline:
        print(
            "No baseline given -- nothing to compare. Store this run as baseline for the next PR."
        )
        sys.exit(0)
    with open(args.baseline) as f:
        baseline = json.load(f)
    failures, warn = check(current, baseline)
    for w in warn:
        print(f"WARNING: {w}")
    if failures:
        print("REGRESSION DETECTED:")
        for f_ in failures:
            print(f"  - {f_}")
        sys.exit(1)
    print("No regression vs baseline.")
