"""Paper-ready figures from the archived 10k sample. No extra deps (stdlib only).

Reads:
  data/metrics_20260904.json      -> match mix + KPIs (exact reconciled counts)
  data/samples/real_10k_settlement.csv -> instrument mix, daily volume, top merchants

Writes (vector SVG, no Metabase chrome):
  docs/fig/fig1_match_mix.svg
  docs/fig/fig2_volume_by_instrument.svg
  docs/fig/fig3_daily_settlement_volume.svg
  docs/fig/fig4_top_merchants_settlement.svg

Usage: python scripts/make_figures.py  (or: make figures)
"""

import csv
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
METRICS = ROOT / "data" / "metrics_20260904.json"
SETTLEMENT = ROOT / "data" / "samples" / "real_10k_settlement.csv"
OUT = ROOT / "docs" / "fig"

C_MATCHED = "#E8B93E"
C_FEE = "#7BAF4B"
C_LATE = "#9B7EBD"
C_BAR_A = "#7FBFBF"
C_BAR_B = "#E0787A"
INK = "#2B2B2B"
GREY = "#8A8A8A"


def _svg_open(w: int, h: int, title: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}" font-family="Inter, Arial, sans-serif">',
        f"<title>{title}</title>",
    ]


def _footer(p: list[str], w: int, h: int, text: str) -> None:
    p.append(
        f'<text x="{w - 12}" y="{h - 10}" font-size="11" fill="{GREY}" '
        f'text-anchor="end">{text}</text>'
    )


def fig1_match_mix(matched: int, fee: int, late: int) -> str:
    w, h = 640, 360
    total = matched + fee + late
    # donut via stroke-dasharray on circles
    cx, cy, r = 200, 170, 95
    circ = 2 * 3.14159265 * r
    segs = [(matched, C_MATCHED), (fee, C_FEE), (late, C_LATE)]
    p = _svg_open(w, h, "Match mix")
    p.append(
        f'<text x="24" y="36" font-size="20" font-weight="700" fill="{INK}">'
        "Match mix — 10k real sample</text>"
    )
    p.append(
        f'<text x="24" y="58" font-size="13" fill="{GREY}">'
        f"n={total:,} deduped settlement rows · batch 2026-09-04</text>"
    )
    offset = 0.0
    for n, col in segs:
        frac = n / total
        dash = frac * circ
        # rotate -90deg: use stroke-dashoffset and transform
        p.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{col}" '
            f'stroke-width="52" stroke-dasharray="{dash:.1f} {circ - dash:.1f}" '
            f'stroke-dashoffset="{-offset:.1f}" transform="rotate(-90 {cx} {cy})"/>'
        )
        offset += dash
    p.append(
        f'<text x="{cx}" y="{cy - 4}" font-size="30" font-weight="700" '
        f'fill="{INK}" text-anchor="middle">{total:,}</text>'
    )
    p.append(
        f'<text x="{cx}" y="{cy + 18}" font-size="12" fill="{GREY}" '
        'text-anchor="middle">TOTAL</text>'
    )
    labels = [
        ("MATCHED", matched, C_MATCHED),
        ("EXCEPTION_FEE_MISMATCH", fee, C_FEE),
        ("EXCEPTION_LATE_UNRESOLVED", late, C_LATE),
    ]
    y = 120
    for name, n, col in labels:
        pct = 100.0 * n / total
        p.append(f'<rect x="360" y="{y - 12}" width="14" height="14" fill="{col}"/>')
        p.append(f'<text x="382" y="{y}" font-size="14" fill="{INK}">{name}</text>')
        p.append(
            f'<text x="600" y="{y}" font-size="14" fill="{INK}" '
            f'text-anchor="end">{n:,} · {pct:.2f}%</text>'
        )
        y += 34
    p.append(
        f'<text x="24" y="330" font-size="12" fill="{GREY}">'
        "MATCHED rows reconciled gateway↔settlement within fee tolerance; "
        "LATE = missing webhook past SLA.</text>"
    )
    _footer(p, w, h, "tx-recon · data/metrics_20260904.json")
    p.append("</svg>")
    return "\n".join(p)


def _bars(
    p: list[str],
    items: list[tuple[str, float]],
    x0: int,
    y0: int,
    bw: int,
    gap: int,
    scale: float,
    color: str,
    fmt,
) -> None:
    x = x0
    for name, val in items:
        bh = max(2, int(val * scale))
        p.append(f'<rect x="{x}" y="{y0 - bh}" width="{bw}" height="{bh}" fill="{color}"/>')
        p.append(
            f'<text x="{x + bw // 2}" y="{y0 - bh - 8}" font-size="13" '
            f'fill="{INK}" text-anchor="middle">{fmt(val)}</text>'
        )
        p.append(
            f'<text x="{x + bw // 2}" y="{y0 + 20}" font-size="13" '
            f'fill="{INK}" text-anchor="middle">{name}</text>'
        )
        x += bw + gap


def fig2_volume(inst_counts: Counter, inst_rupees: dict) -> str:
    w, h = 640, 380
    p = _svg_open(w, h, "Volume by instrument")
    p.append(
        f'<text x="24" y="36" font-size="20" font-weight="700" fill="{INK}">'
        "Volume by instrument — settlement sample</text>"
    )
    p.append(
        f'<text x="24" y="58" font-size="13" fill="{GREY}">'
        "10,000 settlement rows · real card_type mapping (only 2 methods exist)</text>"
    )
    y0 = 300
    items_n = sorted(inst_counts.items(), key=lambda kv: -kv[1])
    max_n = max(inst_counts.values())
    _bars(p, items_n, 80, y0, 110, 120, 200 / max_n, C_BAR_A, lambda v: f"{int(v):,}")
    p.append(f'<text x="24" y="150" font-size="12" fill="{GREY}">rows</text>')
    # rupees line under
    y = 340
    parts = " · ".join(
        f"{k}: Rs {v:,.0f}" for k, v in sorted(inst_rupees.items(), key=lambda kv: -kv[1])
    )
    p.append(f'<text x="24" y="{y}" font-size="13" fill="{INK}">{parts}</text>')
    _footer(p, w, h, "tx-recon · data/samples/real_10k_settlement.csv")
    p.append("</svg>")
    return "\n".join(p)


def fig3_daily(day_counts: Counter, day_rupees: dict) -> str:
    w, h = 640, 380
    p = _svg_open(w, h, "Daily settlement volume")
    p.append(
        f'<text x="24" y="36" font-size="20" font-weight="700" fill="{INK}">'
        "Daily settlement volume — sample window</text>"
    )
    days = sorted(day_counts)
    p.append(
        f'<text x="24" y="58" font-size="13" fill="{GREY}">'
        f"{days[0]} → {days[-1]} · {sum(day_counts.values()):,} rows</text>"
    )
    y0 = 300
    items = [(d, day_counts[d]) for d in days]
    _bars(
        p,
        [(d[5:], v) for d, v in items],
        60,
        y0,
        90,
        40,
        200 / max(day_counts.values()),
        C_BAR_B,
        lambda v: f"{int(v):,}",
    )
    p.append(
        f'<text x="24" y="340" font-size="13" fill="{INK}">'
        + " · ".join(f"{d}: Rs {day_rupees[d]:,.0f}" for d in days)
        + "</text>"
    )
    _footer(p, w, h, "tx-recon · data/samples/real_10k_settlement.csv")
    p.append("</svg>")
    return "\n".join(p)


def fig4_top_merchants(top: list[tuple[str, int]]) -> str:
    w, h = 640, 400
    p = _svg_open(w, h, "Top merchants by settlement rows")
    p.append(
        f'<text x="24" y="36" font-size="20" font-weight="700" fill="{INK}">'
        "Top merchants — settlement rows (worklist proxy)</text>"
    )
    p.append(
        f'<text x="24" y="58" font-size="13" fill="{GREY}">'
        "Fee-mismatch table needs reconciled output; this shows where volume sits.</text>"
    )
    max_n = top[0][1]
    y = 92
    for mid, n in top:
        bw = int(380 * n / max_n)
        p.append(f'<text x="24" y="{y + 14}" font-size="13" fill="{INK}">{mid}</text>')
        p.append(f'<rect x="120" y="{y}" width="{bw}" height="20" fill="{C_BAR_A}"/>')
        p.append(f'<text x="{130 + bw}" y="{y + 15}" font-size="13" fill="{INK}">{n}</text>')
        y += 30
    _footer(p, w, h, "tx-recon · data/samples/real_10k_settlement.csv")
    p.append("</svg>")
    return "\n".join(p)


def main() -> None:
    counts = json.loads(METRICS.read_text())["counts"]
    matched = counts["MATCHED"]
    fee = counts["EXCEPTION_FEE_MISMATCH"]
    late = counts["EXCEPTION_LATE_UNRESOLVED"]

    inst_counts: Counter = Counter()
    inst_rupees: dict = {}
    day_counts: Counter = Counter()
    day_rupees: dict = {}
    merch: Counter = Counter()
    with SETTLEMENT.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            inst = row["instrument_type"]
            day = row["settlement_date"]
            gross = int(row["gross_amount_paise"]) / 100.0
            inst_counts[inst] += 1
            inst_rupees[inst] = inst_rupees.get(inst, 0.0) + gross
            day_counts[day] += 1
            day_rupees[day] = day_rupees.get(day, 0.0) + gross
            merch[row["merchant_id"]] += 1

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "fig1_match_mix.svg").write_text(fig1_match_mix(matched, fee, late), encoding="utf-8")
    (OUT / "fig2_volume_by_instrument.svg").write_text(
        fig2_volume(inst_counts, inst_rupees), encoding="utf-8"
    )
    (OUT / "fig3_daily_settlement_volume.svg").write_text(
        fig3_daily(day_counts, day_rupees), encoding="utf-8"
    )
    (OUT / "fig4_top_merchants_settlement.svg").write_text(
        fig4_top_merchants(merch.most_common(10)), encoding="utf-8"
    )
    total = matched + fee + late
    print(f"mix {matched}/{fee}/{late} on {total} ({100.0 * matched / total:.2f}% matched)")
    print(f"instruments {dict(inst_counts)}")
    print(f"wrote 4 SVGs -> {OUT}")


if __name__ == "__main__":
    main()
