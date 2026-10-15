"""Scale and mix check over the checked-in 10k sample.

What this measures: that the pipeline can read a ten-thousand-row batch, decide
every row, and produce the same numbers every run. That is a real check and it
has caught regressions.

What this does not measure: reconciliation accuracy. ``run`` calls
``FeeEngine.check_match`` and then scores its own output, so there is no
labelled answer anywhere in this file or in the CSVs it reads. The number it
prints reproduces exactly as ``(196 + 8617) / (9805 - 492)`` -- the arithmetic
of ``src/adapters/real_data.py``'s ``sha256(seed|tx) % 100`` bucket plan, over a
settlement leg that generator wrote from a hardcoded two-instrument fee table
rather than from a bank. A fee engine that works and one that is completely
broken produce the same 94.63%, and the gate sits ten points below it where
only a total collapse could reach.

So the gate is not dressed up as correctness. It is a floor on the mix and a
smoke test at scale, and ``EXPECTED_MIX`` below pins the counts it can actually
detect drift in. Correctness against hand-written answers lives in
``tests/fixtures/recon_cases.yaml`` and ``tests/test_recon_cases.py``.
"""

import argparse
import csv
import os
import time

from src.common.schemas import EXCEPTION_FEE_MISMATCH, EXCEPTION_MISSING_WEBHOOK, MATCHED
from src.processing.fee_engine import FeeEngine

MATCH_GATE = 0.85

# The checked-in sample is deterministic: src/adapters/real_data.py buckets by
# sha256(seed|tx) % 100 with seed 7, and nothing else is random or clock-driven.
# These counts are therefore properties of the generator plus the rate card, not
# of a measurement. They move when one of those changes, which is exactly when
# someone wants to be told.
EXPECTED_MIX = {"n": 9805, "matched": 8813, "mismatched": 500, "orphans": 492}

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE_HOOKS = os.path.normpath(os.path.join(HERE, "../../data/samples/real_10k_hooks.csv"))
SAMPLE_SETTLE = os.path.normpath(os.path.join(HERE, "../../data/samples/real_10k_settlement.csv"))


def _default_paths():
    return SAMPLE_HOOKS, SAMPLE_SETTLE


def load(hooks_csv, settle_csv):
    for p in (hooks_csv, settle_csv):
        if not os.path.exists(p):
            raise FileNotFoundError(f"scale and mix check needs {p}")
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
    started = time.perf_counter()
    webhooks, latest = load(h, s)
    predicted = match(webhooks, latest)
    matched = sum(1 for v in predicted.values() if v == MATCHED)
    mismatched = sum(1 for v in predicted.values() if v == EXCEPTION_FEE_MISMATCH)
    orphans = sum(1 for v in predicted.values() if v == EXCEPTION_MISSING_WEBHOOK)
    joined = matched + mismatched
    rep = {
        "n": len(predicted),
        "matched": matched,
        "mismatched": mismatched,
        "orphans": orphans,
        # Orphan settlements are excluded from the denominator, which is why
        # this reads 94.63% where the end-to-end batch reports 89.98%. Both are
        # the same run measured two ways; docs/BENCHMARKS.md names them.
        "match_rate": round(matched / joined, 4) if joined else 0.0,
        "source": os.path.basename(h),
        "seconds": round(time.perf_counter() - started, 1),
        "measures": "scale and mix over the checked-in 10k sample",
        "is_not": "reconciliation accuracy; nothing here is independently labelled",
    }
    rep["mismatch_delta_abs"] = _mismatch_spread(webhooks, latest, predicted)
    return rep


def mix_drift(rep):
    """What moved in the mix, or an empty list.

    Only meaningful on the checked-in sample; a caller that passed --hooks is
    looking at different data and gets an empty list back rather than a
    confident "nothing changed".
    """
    if rep.get("source") != os.path.basename(SAMPLE_HOOKS):
        return []
    return [
        f"{key}: expected {want}, got {rep.get(key)}"
        for key, want in EXPECTED_MIX.items()
        if rep.get(key) != want
    ]


def _mismatch_spread(webhooks, latest, predicted):
    engine = FeeEngine()
    gaps = []
    for tx, status in predicted.items():
        if status != EXCEPTION_FEE_MISMATCH:
            continue
        settled, sdate, smerch = latest[tx]
        amount, inst, wmerch, _ = webhooks[tx]
        _, res = engine.check_match(amount, settled, inst, smerch or wmerch, sdate or None)
        gaps.append(abs(res.net_paise - settled))
    if not gaps:
        return {"p50": 0, "p90": 0, "max": 0}
    ordered = sorted(gaps)
    return {
        "p50": ordered[len(ordered) // 2],
        "p90": ordered[max(0, int(len(ordered) * 0.9) - 1)],
        "max": ordered[-1],
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hooks", default=None)
    ap.add_argument("--settlements", default=None)
    args = ap.parse_args()
    rep = run(args.hooks, args.settlements)
    print(
        f"n={rep['n']} matched={rep['matched']} mismatched={rep['mismatched']} "
        f"orphans={rep['orphans']} match_rate={rep['match_rate']:.2%} "
        f"src={rep['source']} in {rep['seconds']}s"
    )
    print(f"(scale and mix check, not accuracy: {rep['is_not']})")

    drift = mix_drift(rep)
    if drift:
        raise SystemExit("the checked-in sample's mix moved:\n  " + "\n  ".join(drift))
    if rep["match_rate"] < MATCH_GATE:
        raise SystemExit(
            f"mix floor failed: match_rate {rep['match_rate']:.2%} < {MATCH_GATE:.0%}. "
            "This is the loader's injected mix, not a correctness measurement -- "
            "see the module docstring."
        )
