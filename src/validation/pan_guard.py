"""No-PAN gate: card numbers must never enter the recon pipeline.

One PAN in a webhook/CSV/log puts the whole lake in PCI-DSS scope.
Fail the batch on detection; store last-4 + issuer only.
"""

import logging
import re

logger = logging.getLogger(__name__)

# 13-19 digit runs, allowing spaces/dashes inside (stripped before Luhn).
_PAN_RUN = re.compile(r"\d(?:[ \-]?\d){12,18}")

# Same 13-19 digit run for the JVM side (Java regex compatible).
_SPARK_PAN_PATTERN = r"\d(?:[ \-]?\d){12,18}"


def spark_pan_candidate_filter(df, cols: list[str]):
    """Distributed prefilter: rows where any listed column holds a PAN-like run.

    Luhn verification still runs on the driver (count_spark_pan_hits), so the
    collect is bounded by candidate rows (normally zero).
    """
    from pyspark.sql import functions as _F

    cond = None
    for c in cols:
        if c not in df.columns:
            continue
        f = _F.col(c).cast("string").rlike(_SPARK_PAN_PATTERN)
        cond = f if cond is None else (cond | f)
    return df.filter(cond) if cond is not None else df.limit(0)


def count_spark_pan_hits(df, cols: list[str]) -> int:
    """Luhn-verified PAN row count in a Spark frame (fail-closed gate input)."""
    present = [c for c in cols if c in df.columns]
    cands = spark_pan_candidate_filter(df, cols).select(*present).collect()
    n = 0
    for row in cands:
        for v in row:
            if v is not None and find_pans(str(v)):
                n += 1
                break
    return n


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = ord(ch) - 48
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def find_pans(text: str) -> list[str]:
    """Card-number candidates in free text (Luhn-verified, digits only)."""
    hits = []
    for m in _PAN_RUN.finditer(text or ""):
        digits = re.sub(r"[ \-]", "", m.group(0))
        if 13 <= len(digits) <= 19 and _luhn_ok(digits):
            hits.append(digits)
    return hits


def scan_frame(df, columns: list[str] | None = None) -> dict[str, int]:
    """Count PAN hits per column in a pandas frame. Empty dict = clean.

    columns=None scans every object-dtype column (a PAN in any free-text
    field enters the lake otherwise). Vectorized prefilter: str.contains
    (C-level) selects candidate rows, Luhn verification runs only on those,
    so clean batches never materialize full Python lists.
    """
    if columns is None:
        columns = [c for c in df.columns if str(df[c].dtype) == "object"]
    found: dict[str, int] = {}
    for col in columns:
        if col not in df.columns:
            continue
        s = df[col].dropna().astype(str)
        if s.empty:
            continue
        cands = s[s.str.contains(_PAN_RUN, regex=True, na=False)]
        if cands.empty:
            continue
        n = sum(1 for v in cands.tolist() if find_pans(v))
        if n:
            found[col] = n
    return found
