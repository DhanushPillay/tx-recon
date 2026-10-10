import argparse
import json
import os
import sys

TP_DROP_PCT = 15.0
P99_RISE_PCT = 20.0


def _pct_change(new, old):
    return (new - old) / old * 100 if old else 0.0


def _comparable(results):
    found = []
    k = results.get("kafka", {})
    if "throughput_msgs_sec" in k:
        found.append("kafka.throughput_msgs_sec")
    if "p99" in k.get("ack_latency", {}):
        found.append("kafka.ack_latency.p99")
    for bench, vals in results.get("iceberg", {}).get("benchmarks", {}).items():
        if "rows_per_sec" in vals:
            found.append(f"iceberg.benchmarks.{bench}.rows_per_sec")
    return found


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
        if vals.get("match_rate", 1.0) < 0.85:
            failures.append(f"iceberg {bench} match_rate < 85% (real-data drift?)")
        if bench in bi.get("benchmarks", {}):
            old, new = bi["benchmarks"][bench], vals
            if "rows_per_sec" in new and "rows_per_sec" in old:
                d = _pct_change(new["rows_per_sec"], old["rows_per_sec"])
                if d < -TP_DROP_PCT:
                    failures.append(f"iceberg {bench} rows/sec dropped {d:.1f}%")
    acc = current.get("accuracy", {})
    if acc and acc.get("match_rate", 1.0) < 0.85:
        failures.append("real accuracy gate failed (match_rate < 85%)")
    compared = set(_comparable(current)) & set(_comparable(baseline))
    for name in sorted(set(_comparable(baseline)) - compared):
        warn.append(f"baseline measures {name} but this run did not, so no comparison ran for it")
    return failures, warn, compared


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--results", default=os.path.join(os.path.dirname(__file__), "results_real.json")
    )
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
    failures, warn, compared = check(current, baseline)
    for w in warn:
        print(f"WARNING: {w}")
    if failures:
        print("REGRESSION DETECTED:")
        for f_ in failures:
            print(f"  - {f_}")
        sys.exit(1)
    print("No regression vs baseline.")
    print(f"Compared throughput metrics: {', '.join(sorted(compared)) or 'none'}")
