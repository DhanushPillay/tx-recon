"""No-PAN gate: card numbers must never enter the recon pipeline.

One PAN in a webhook/CSV/log puts the whole lake in PCI-DSS scope.
Fail the batch on detection; store last-4 + issuer only.
"""

import logging
import re

logger = logging.getLogger(__name__)

# 13-19 digit runs, allowing spaces/dashes inside (stripped before Luhn).
_PAN_RUN = re.compile(r"\d(?:[ \-]?\d){12,18}")


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


def scan_frame(df, columns: list[str]) -> dict[str, int]:
    """Count PAN hits per column in a pandas frame. Empty dict = clean."""
    found: dict[str, int] = {}
    for col in columns:
        if col not in df.columns:
            continue
        n = 0
        for v in df[col].dropna().astype(str).tolist():
            if find_pans(v):
                n += 1
        if n:
            found[col] = n
    return found
