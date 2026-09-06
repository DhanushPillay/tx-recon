"""Infra-free perf quick gate: sealed accuracy harness + machine context.

Runs the correctness oracle (no Spark, no Kafka, no Java) and writes a
results JSON carrying the hardware fingerprint, so check_regression.py
can tell a real slowdown from a different machine.

Usage:
    python tests/performance/quick_perf.py [--out results.json]
"""

import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from hardware import fingerprint, get_hardware_info
from recon_accuracy import run


def main(out):
    reps = [run(n=2000, seed=s) for s in (7, 42, 123)]
    ok = all(r["f1"] == 1.0 and r["false_positives"] == 0 for r in reps)
    results = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "hardware": {**get_hardware_info(), "fingerprint": fingerprint()},
        "accuracy": {
            "min_f1": min(r["f1"] for r in reps),
            "max_false_positives": max(r["false_positives"] for r in reps),
        },
    }
    print(f"accuracy_min_f1={results['accuracy']['min_f1']:.4f}")
    print(f"accuracy_max_fp={results['accuracy']['max_false_positives']}")
    print(f"hardware_fingerprint={results['hardware']['fingerprint']}")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"wrote {out}")
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results.json"))
    sys.exit(main(ap.parse_args().out))
