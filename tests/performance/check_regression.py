"""Compare a benchmark run against a checked-in baseline and fail on regressions.

The failure mode this replaces: a missing metric used to land in `warn` and exit
0. CI passed an accuracy-only artifact, so throughput, p99 and every Iceberg
metric were never measured and the step still reported success. Anyone reading
the log saw a green gate covering metrics it had not looked at.

Two changes close that:

- `--require` names the metric families the gate covers. A required family the
  run does not supply is a FAILURE, so the log and the exit code agree.
- `_pct_change` returns None for an unusable baseline value instead of 0.0%, so
  a missing or zero baseline cannot read as "no change".

Usage:
    python tests/performance/check_regression.py \\
        --results tests/performance/results_accuracy.json \\
        --baseline tests/performance/baseline_real.json \\
        --require accuracy
"""

import argparse
import json
import os
import sys

TP_DROP_PCT = 15.0
P99_RISE_PCT = 20.0
P95_RISE_PCT = 20.0
MATCH_RATE_FLOOR = 0.85

#: Families whose metrics are compared against the baseline. Accuracy has its
#: own absolute floor rather than a delta, so it is required but not compared.
THROUGHPUT_FAMILIES = ("kafka", "kafka-latency", "iceberg")

FAMILIES = {
    "accuracy": lambda r: (
        ["accuracy.match_rate"] if r.get("accuracy", {}).get("match_rate") is not None else []
    ),
    **{
        name: probe
        for name, probe in (
            (
                "kafka",
                lambda r: (
                    ["kafka.throughput_msgs_sec"]
                    if "throughput_msgs_sec" in r.get("kafka", {})
                    else []
                ),
            ),
            (
                "kafka-latency",
                lambda r: (
                    [
                        f"kafka.ack_latency.{q}"
                        for q in ("p95", "p99")
                        if q in r.get("kafka", {}).get("ack_latency", {})
                    ]
                ),
            ),
            (
                "iceberg",
                lambda r: [
                    f"iceberg.benchmarks.{b}.rows_per_sec"
                    for b, v in r.get("iceberg", {}).get("benchmarks", {}).items()
                    if "rows_per_sec" in v
                ],
            ),
        )
    },
}


def _pct_change(new, old):
    """Percent change from old to new, or None when the baseline cannot support it.

    None means uncomparable. The old version returned 0.0, which read as
    "unchanged" for a missing or zero baseline.
    """
    if not old:
        return 0.0 if not new else None
    return (new - old) / old * 100


def _comparable(results):
    """Throughput metric names in `results` that the gate can compare."""
    found = []
    for family in THROUGHPUT_FAMILIES:
        found.extend(FAMILIES[family](results))
    return sorted(found)


def _require_failures(current, baseline, require, failures):
    """A required family this run did not measure is a failure, not a warning."""
    for family in require:
        probe = FAMILIES.get(family)
        if probe is None:
            failures.append(
                f"unknown metric family {family!r}; known: {', '.join(sorted(FAMILIES))}"
            )
            continue
        missing = probe(current)
        if not missing:
            failures.append(f"--require {family} but this run measured no {family} metric")

    # Workload mismatch: a throughput number from a different message count is
    # not a regression, it is a different experiment.
    if "kafka" in require:
        cc = current.get("kafka", {}).get("config", {}).get("count")
        bc = baseline.get("kafka", {}).get("config", {}).get("count")
        if cc is not None and bc is not None and cc != bc:
            failures.append(
                f"kafka message count changed ({bc} -> {cc}): throughput is not comparable"
            )

    # Iceberg scale is encoded in the benchmark label, so a changed scale drops
    # the key and used to drop the comparison with only a warning. Only a gate
    # that actually claims to cover iceberg should care.
    if "iceberg" in require:
        cur_labels = set(current.get("iceberg", {}).get("benchmarks", {}))
        base_labels = set(baseline.get("iceberg", {}).get("benchmarks", {}))
        if not cur_labels:
            failures.append("--require iceberg but this run measured no iceberg benchmark")
        for label in sorted(base_labels - cur_labels):
            failures.append(
                f"baseline has iceberg benchmark {label} but this run does not; "
                "either the scale changed or the benchmark did not run"
            )


def check(current, baseline, require=(), hardware_policy="warn"):
    failures = []
    warn = []
    ch = current.get("hardware", {}).get("fingerprint")
    bh = baseline.get("hardware", {}).get("fingerprint")
    if ch and bh and ch != bh:
        msg = f"hardware changed since baseline ({bh} -> {ch}): treat deltas as noisy"
        (failures if hardware_policy == "fail" else warn).append(msg)
    elif ch and not bh:
        warn.append("baseline has no hardware fingerprint: deltas may not be comparable")

    ck, bk = current.get("kafka", {}), baseline.get("kafka", {})
    if "throughput_msgs_sec" in ck and "throughput_msgs_sec" in bk:
        d = _pct_change(ck["throughput_msgs_sec"], bk["throughput_msgs_sec"])
        if d is None:
            failures.append(
                f"kafka throughput baseline is {bk['throughput_msgs_sec']}, cannot compute a change"
            )
        elif d < -TP_DROP_PCT:
            failures.append(f"kafka throughput dropped {d:.1f}% (>{TP_DROP_PCT}%)")

    cl, bl = ck.get("ack_latency", {}), bk.get("ack_latency", {})
    for q, limit, label in (("p95", P95_RISE_PCT, "p95"), ("p99", P99_RISE_PCT, "p99")):
        if q in cl and q in bl:
            d = _pct_change(cl[q], bl[q])
            if d is None:
                failures.append(f"kafka {label} baseline is {bl[q]}, cannot compute a change")
            elif d > limit:
                failures.append(f"kafka {label} rose {d:.1f}% (>{limit}%)")

    ci, bi = current.get("iceberg", {}), baseline.get("iceberg", {})
    for bench, vals in ci.get("benchmarks", {}).items():
        if vals.get("healthy") is False:
            failures.append(f"iceberg {bench} matched nothing (healthy=false)")
        if "match_rate" not in vals:
            failures.append(f"iceberg {bench} has no match_rate; cannot judge it")
        elif vals["match_rate"] < MATCH_RATE_FLOOR:
            failures.append(
                f"iceberg {bench} match_rate {vals['match_rate']:.2%} < {MATCH_RATE_FLOOR:.0%} (real-data drift?)"
            )
        old = bi.get("benchmarks", {}).get(bench)
        if old and "rows_per_sec" in vals and "rows_per_sec" in old:
            d = _pct_change(vals["rows_per_sec"], old["rows_per_sec"])
            if d is None:
                failures.append(
                    f"iceberg {bench} baseline rows_per_sec is {old['rows_per_sec']}, cannot compute a change"
                )
            elif d < -TP_DROP_PCT:
                failures.append(f"iceberg {bench} rows/sec dropped {d:.1f}%")

    acc = current.get("accuracy", {})
    if "match_rate" not in acc:
        if "accuracy" in require:
            failures.append("accuracy artifact has no match_rate")
    elif acc["match_rate"] < MATCH_RATE_FLOOR:
        failures.append(
            f"real accuracy gate failed (match_rate {acc['match_rate']:.2%} < {MATCH_RATE_FLOOR:.0%})"
        )

    compared = set(_comparable(current)) & set(_comparable(baseline))
    for name in sorted(set(_comparable(baseline)) - compared):
        warn.append(f"baseline measures {name} but this run did not, so no comparison ran for it")

    _require_failures(current, baseline, require, failures)
    return failures, warn, compared


def _parse_require(value):
    if not value:
        return []
    return [f.strip() for f in value.split(",") if f.strip()]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--results", default=os.path.join(os.path.dirname(__file__), "results_real.json")
    )
    ap.add_argument("--baseline", default=None)
    ap.add_argument(
        "--require",
        default="",
        help="comma-separated metric families this gate covers "
        f"({', '.join(sorted(FAMILIES))}); required when --baseline is given",
    )
    ap.add_argument(
        "--hardware-policy",
        choices=("warn", "fail"),
        default="warn",
        help="what a hardware fingerprint change does (default: warn)",
    )
    args = ap.parse_args()

    if not args.baseline:
        print(
            "No baseline given -- nothing to compare. Store this run as baseline for the next PR."
        )
        sys.exit(0)

    require = _parse_require(args.require)
    if not require:
        # Refusing here is the point: a gate that does not say what it covers
        # is the exact failure this rewrite exists to stop.
        print("FAIL: --baseline given without --require.")
        print(f"Say which families this run measures, e.g. --require {','.join(sorted(FAMILIES))}.")
        print("Without it, missing metrics warn and the gate reports success anyway.")
        sys.exit(2)

    with open(args.results) as f:
        current = json.load(f)
    with open(args.baseline) as f:
        baseline = json.load(f)

    failures, warn, compared = check(current, baseline, require, args.hardware_policy)
    for w in warn:
        print(f"WARNING: {w}")
    print(f"Gate covers: {', '.join(require)}")
    if failures:
        print("REGRESSION DETECTED:")
        for f_ in failures:
            print(f"  - {f_}")
        sys.exit(1)
    print("No regression vs baseline.")
    print(f"Compared throughput metrics: {', '.join(sorted(compared)) or 'none'}")
