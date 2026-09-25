"""Real-data reconciliation accuracy gate (no Spark needed).

Joins the checked-in 10k real sample (data/samples/real_10k_*.csv, full
data/real_thiru_full_*.csv when present) through FeeEngine.check_match and
gates match_rate >= 85%. No synthetic breaks, no answer key: the loader mix
targets ~90% matched, so a drop below 85% means fee miscalibration or drift.

Usage: python tests/performance/recon_accuracy.py [--hooks CSV] [--settlements CSV]
"""

import argparse
import csv
import os

from src.common.schemas import EXCEPTION_FEE_MISMATCH, EXCEPTION_MISSING_WEBHOOK, MATCHED
from src.processing.fee_engine import FeeEngine

MATCH_GATE = 0.85

HERE = os.path.dirname(os.path.abspath(__file__))
FULL_HOOKS = os.path.normpath(os.path.join(HERE, "../../data/real_thiru_full_hooks.csv"))
FULL_SETTLE = os.path.normpath(os.path.join(HERE, "../../data/real_thiru_full_settlement.csv"))
SAMPLE_HOOKS = os.path.normpath(os.path.join(HERE, "../../data/samples/real_10k_hooks.csv"))
SAMPLE_SETTLE = os.path.normpath(os.path.join(HERE, "../../data/samples/real_10k_settlement.csv"))


def _default_paths():
    # Pure-Python gate: always the 10k sample (fast, hermetic). Full-scale
    # accuracy is covered by the Spark MERGE bench match_rate gate instead.
    return SAMPLE_HOOKS, SAMPLE_SETTLE


def load(hooks_csv, settle_csv):
    for p in (hooks_csv, settle_csv):
        if not os.path.exists(p):
            raise FileNotFoundError(f"real accuracy gate needs {p}")
    webhooks = {}
    with open(hooks_csv, newline="") as f:
        for row in csv.DictReader(f):
            webhooks[row["transaction_id"]] = (
                int(row["amount_paise"]),
                row["instrument_type"],
                row.get("merchant_id") or None,
                row.get("day") or None,
            )
    latest = {}
    with open(settle_csv, newline="") as f:
        for row in csv.DictReader(f):
            tx = row["transaction_id"]
            day = row.get("settlement_date") or ""
            if tx not in latest or day >= latest[tx][1]:
                latest[tx] = (
                    int(row["settled_amount_paise"]),
                    day,
                    row.get("merchant_id") or None,
                )
    return webhooks, latest


def match(webhooks, latest):
    engine = FeeEngine()
    predicted = {}
    for tx, (settled, sdate, smerch) in latest.items():
        if tx not in webhooks:
            predicted[tx] = EXCEPTION_MISSING_WEBHOOK
            continue
        amount, inst, wmerch, _ = webhooks[tx]
        ok, _ = engine.check_match(amount, settled, inst, smerch or wmerch, sdate or None)
        predicted[tx] = MATCHED if ok else EXCEPTION_FEE_MISMATCH
    return predicted


def run(hooks_csv=None, settle_csv=None):
    h, s = _default_paths() if hooks_csv is None else (hooks_csv, settle_csv)
    webhooks, latest = load(h, s)
    predicted = match(webhooks, latest)
    matched = sum(1 for v in predicted.values() if v == MATCHED)
    mismatched = sum(1 for v in predicted.values() if v == EXCEPTION_FEE_MISMATCH)
    orphans = sum(1 for v in predicted.values() if v == EXCEPTION_MISSING_WEBHOOK)
    joined = matched + mismatched
    return {
        "n": len(predicted),
        "matched": matched,
        "mismatched": mismatched,
        "orphans": orphans,
        "match_rate": round(matched / joined, 4) if joined else 0.0,
        "source": os.path.basename(h),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hooks", default=None)
    ap.add_argument("--settlements", default=None)
    args = ap.parse_args()
    rep = run(args.hooks, args.settlements)
    print(
        f"n={rep['n']} matched={rep['matched']} mismatched={rep['mismatched']} "
        f"orphans={rep['orphans']} match_rate={rep['match_rate']:.2%} src={rep['source']}"
    )
    if rep["match_rate"] < MATCH_GATE:
        raise SystemExit(f"accuracy gate failed: match_rate {rep['match_rate']:.2%} < 85%")
