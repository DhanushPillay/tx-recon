"""Infra-free accuracy quick gate on real data (no Spark/Kafka/Java).

Runs the real-data accuracy gate and writes a results JSON carrying the
hardware fingerprint, so check_regression.py can tell real drift from a
different machine.

Usage:
    python tests/performance/quick_perf.py [--out results_accuracy.json]
"""

import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from hardware import fingerprint, get_hardware_info
from recon_accuracy import MATCH_GATE, run


def main(out):
    rep = run()
    ok = rep["match_rate"] >= MATCH_GATE
    results = {
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "hardware": {**get_hardware_info(), "fingerprint": fingerprint()},
        "accuracy": {
            "match_rate": rep["match_rate"],
            "matched": rep["matched"],
            "mismatched": rep["mismatched"],
            "orphans": rep["orphans"],
            "n": rep["n"],
        },
    }
    print(f"accuracy_match_rate={rep['match_rate']:.2%}")
    print(f"hardware_fingerprint={results['hardware']['fingerprint']}")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"wrote {out}")
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out", default=os.path.join(os.path.dirname(__file__), "results_accuracy.json")
    )
    sys.exit(main(ap.parse_args().out))
