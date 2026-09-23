"""Sealed-key reconciliation accuracy harness (pure Python, no Spark needed).

Mirrors the MERGE semantics in src/processing/reconcile.py:
  1. dedup settlements by transaction_id (latest settlement_date wins —
     every row carries a unique day offset so "latest" is unambiguous,
     exactly like WINDOW row_number() ORDER BY settlement_date DESC),
  2. MATCHED iff |webhook_net - settled| <= tolerance (FeeEngine.check_match),
  3. no webhook -> EXCEPTION_MISSING_WEBHOOK.

The answer key is built at injection time and NEVER passed to the matcher,
so precision/recall/F1 are falsifiable (a wrong match counts against you).
Break mix follows ledger-recon-engine taxonomy: EXACT / ROUNDING /
FEE_MISMATCH / ORPHAN / DUPLICATE / OUT_OF_ORDER / LATE_CORRECTION.

Usage: python tests/performance/recon_accuracy.py --n 2000 --seed 7
"""

import argparse
import os
import random

from src.common.schemas import EXCEPTION_FEE_MISMATCH, EXCEPTION_MISSING_WEBHOOK, MATCHED
from src.processing.fee_engine import FeeEngine

INSTRUMENTS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]


def _load_rate_card():
    import yaml

    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    with open(os.path.join(root, "config", "fee_rates.yaml")) as f:
        return yaml.safe_load(f)


_RATE_CARD = _load_rate_card()


def _expected_net_raw(amount_paise, instrument_type, merchant_id=None, settlement_date=None):
    """Independent oracle: reads the YAML rate card directly and does integer
    math inline, never touching FeeEngine. Ground truth must not share code
    with the matcher, or a FeeEngine bug scores a perfect 1.0 on both sides.
    Version + merchant aware: picks the history card whose
    [effective_from, effective_to] contains settlement_date, then
    merchant+instrument > instrument > default (mirrors FeeEngine)."""
    from datetime import date as _date

    def _iso(s):
        try:
            return _date.fromisoformat(str(s))
        except Exception:
            return None

    card = _RATE_CARD
    history = card.get("history") or card.get("rate_card_history") or card.get("rate_cards")
    if history and settlement_date:
        d = _iso(settlement_date)
        chosen = prior = None
        cards = sorted(
            history, key=lambda c: _iso(str(c.get("effective_from", "1970-01-01"))) or _date.min
        )
        for c in cards:
            eff_from = _iso(str(c.get("effective_from", "1970-01-01")))
            if not eff_from or (d is not None and eff_from > d):
                continue
            prior = c
            eff_to = _iso(str(c["effective_to"])) if c.get("effective_to") else None
            if d is None or eff_to is None or d <= eff_to:
                chosen = c
        card = chosen or prior or cards[0]
    rate = dict(card.get("default", {}))
    for k, v in card.get("instruments", {}).get(instrument_type, {}).items():
        if k in ("mdr_rate_bps", "gst_on_mdr", "tolerance_paise"):
            rate[k] = v
    merch_map = (card.get("merchants", {}) or {}).get(merchant_id or "", {}) if merchant_id else {}
    if merchant_id and not merch_map:
        # Top-level merchants apply to all cards unless a card overrides that
        # merchant key (mirrors FeeEngine _build_rate_cards + _lookup).
        merch_map = (_RATE_CARD.get("merchants", {}) or {}).get(merchant_id, {}) or {}
    for k, v in merch_map.get(instrument_type, {}).items():
        if k in ("mdr_rate_bps", "gst_on_mdr", "tolerance_paise"):
            rate[k] = v
    mdr_bps = int(rate.get("mdr_rate_bps", 150))
    gst_bps = int(round(float(rate.get("gst_on_mdr", 0)) * 100))
    fee = (amount_paise * mdr_bps + 5000) // 10000
    gst = (fee * gst_bps + 5000) // 10000 if gst_bps > 0 else 0
    return amount_paise - fee - gst


def build_case(n=2000, seed=7):
    """Return (webhooks, settlements, answer) with a known break mix.

    settlements rows are (tx, settled_paise, day_offset); day_offset is a
    unique global sequence so dedup-by-latest-date mirrors the SQL
    WINDOW row_number() ORDER BY settlement_date DESC exactly.
    """
    rnd = random.Random(seed)
    webhooks, settlements, answer = {}, [], {}
    seen_ids = set()
    seq = 0

    def add(tx, settled, merchant=None, sdate=None):
        nonlocal seq
        settlements.append((tx, settled, seq, merchant, sdate))
        seq += 1

    for _ in range(n):
        tx = f"tx_{rnd.getrandbits(48):012x}"
        while tx in seen_ids:  # getrandbits can repeat; a silent dict-merge would fake the mix
            tx = f"tx_{rnd.getrandbits(48):012x}"
        seen_ids.add(tx)
        amount = rnd.randint(1000, 1000000)
        inst = rnd.choice(INSTRUMENTS)
        # Merchant + version coverage: 20% merch_001, 10% merch_demo, dates split v1/v2.
        _mr = rnd.random()
        merchant = "merch_001" if _mr < 0.20 else ("merch_demo" if _mr < 0.30 else None)
        sdate = "2025-01-15" if rnd.random() < 0.5 else "2025-06-15"
        webhooks[tx] = (amount, inst, merchant, sdate)
        net = _expected_net_raw(amount, inst, merchant, sdate)
        r = rnd.random()
        if r < 0.70:  # exact
            add(tx, net, merchant, sdate)
            answer[tx] = MATCHED
        elif r < 0.80:  # rounding noise, still within tolerance
            add(tx, net + rnd.choice([-1, 1]), merchant, sdate)
            answer[tx] = MATCHED
        elif r < 0.85:  # genuine fee mismatch
            add(tx, net + rnd.randint(50, 5000), merchant, sdate)
            answer[tx] = EXCEPTION_FEE_MISMATCH
        elif r < 0.90:  # orphan settlement, webhook withheld
            del webhooks[tx]
            add(tx, net, merchant, sdate)
            answer[tx] = EXCEPTION_MISSING_WEBHOOK
        elif r < 0.95:  # duplicate settlement row (dedup keeps latest = correct net)
            add(tx, net + rnd.randint(50, 5000), merchant, sdate)
            add(tx, net, merchant, sdate)
            answer[tx] = MATCHED
        elif r < 0.975:  # out-of-order redelivery: stale dup arrives between, correct net is latest
            add(tx, net + rnd.randint(50, 5000), merchant, sdate)
            add(tx, net + rnd.randint(50, 5000), merchant, sdate)
            add(tx, net, merchant, sdate)
            answer[tx] = MATCHED
        else:  # late correction: wrong net posted first, corrective row converges to net
            add(tx, net + rnd.randint(50, 5000), merchant, sdate)
            add(tx, net, merchant, sdate)
            answer[tx] = MATCHED
    return webhooks, settlements, answer


def match(webhooks, settlements):
    """Matcher under test: sees ONLY webhooks + settlements, never the answer."""
    engine = FeeEngine()
    latest = {}
    for row in settlements:  # max day wins = WINDOW row_number() dedup
        tx, settled, day = row[0], row[1], row[2]
        merch, sdate = (row[3], row[4]) if len(row) > 4 else (None, None)
        if tx not in latest or day > latest[tx][1]:
            latest[tx] = (settled, day, merch, sdate)
    predicted = {}
    for tx, (settled, _, merch, sdate) in latest.items():
        if tx not in webhooks:
            predicted[tx] = EXCEPTION_MISSING_WEBHOOK
            continue
        wh = webhooks[tx]
        amount, inst = wh[0], wh[1]
        wmerch = wh[2] if len(wh) > 2 else None
        wsdate = wh[3] if len(wh) > 3 else None
        # Settlement-side merchant/date win (they are the observed leg);
        # webhook values are the fallback for old 2-tuple harnesses.
        ok, _ = engine.check_match(amount, settled, inst, merch or wmerch, sdate or wsdate)
        predicted[tx] = MATCHED if ok else EXCEPTION_FEE_MISMATCH
    return predicted


def score(answer, predicted):
    """Precision/recall/F1 on the MATCHED decision + per-class recall + FP count."""
    tp = sum(1 for tx, y in answer.items() if y == MATCHED and predicted.get(tx) == MATCHED)
    fp = sum(1 for tx, y in answer.items() if y != MATCHED and predicted.get(tx) == MATCHED)
    fn = sum(1 for tx, y in answer.items() if y == MATCHED and predicted.get(tx) != MATCHED)
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    per_class = {}
    for cls in (MATCHED, EXCEPTION_FEE_MISMATCH, EXCEPTION_MISSING_WEBHOOK):
        idx = [tx for tx, y in answer.items() if y == cls]
        per_class[cls] = sum(1 for tx in idx if predicted.get(tx) == cls) / len(idx) if idx else 1.0
    return {
        "n": len(answer),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_positives": fp,
        "per_class_recall": {k: round(v, 4) for k, v in per_class.items()},
    }


def run(n=2000, seed=7):
    from collections import Counter

    webhooks, settlements, answer = build_case(n, seed)
    predicted = match(webhooks, settlements)
    report = score(answer, predicted)
    # Real fanout check: one prediction per distinct settlement tx, keys tile the answer.
    # (Old check compared a dict against itself, always true.)
    assert set(predicted) == set(answer), "prediction keys diverged from answer keys"
    dup_max = max(Counter(s[0] for s in settlements).values(), default=1)
    assert len(predicted) == len(answer), f"fanout: {len(predicted)} preds vs {len(answer)} answers"
    report["fanout_max"] = 1
    report["settlement_dup_max"] = dup_max
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    rep = run(args.n, args.seed)
    print(
        f"n={rep['n']} precision={rep['precision']} recall={rep['recall']} "
        f"f1={rep['f1']} FP={rep['false_positives']} per_class={rep['per_class_recall']}"
    )
