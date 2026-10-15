import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from hardware import fingerprint, get_hardware_info
from recon_accuracy import MATCH_GATE, mix_drift, run


def main(out):
    rep = run()
    drift = mix_drift(rep)
    if drift:
        for line in drift:
            print(f"mix drift: {line}", file=sys.stderr)
    ok = rep["match_rate"] >= MATCH_GATE and not drift
    results = {
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "hardware": {**get_hardware_info(), "fingerprint": fingerprint()},
        "mix": {
            "match_rate": rep["match_rate"],
            "matched": rep["matched"],
            "mismatched": rep["mismatched"],
            "orphans": rep["orphans"],
            "n": rep["n"],
            "mismatch_delta_abs": rep["mismatch_delta_abs"],
            "measures": rep["measures"],
            "is_not": rep["is_not"],
        },
    }
    print(f"sample_mix_match_rate={rep['match_rate']:.2%}")
    print(f"(not an accuracy measurement: {rep['is_not']})")
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
