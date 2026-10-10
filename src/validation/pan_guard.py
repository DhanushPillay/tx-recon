import logging
import re

logger = logging.getLogger(__name__)

_IIN = r"(?:4\d{3}|5[1-5]\d{2}|2[2-7]\d{2}|3[47]\d{2}|6[0-5]\d{2}|8[1-9]\d{2})"

# needs the card prefix or plain numbers like utrs trip it
_PAN_RUN = re.compile(rf"(?<!\d){_IIN}[ \-]?\d(?:[ \-]?\d){{8,14}}(?!\d)")

_SPARK_PAN_PATTERN = rf"(?<!\d){_IIN}[ \-]?\d(?:[ \-]?\d){{8,14}}(?!\d)"


def _text_columns(df, cols: list[str] | None) -> list[str]:
    if cols is not None:
        return [c for c in cols if c in df.columns]
    from pyspark.sql.types import StringType

    return [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]


def spark_pan_candidate_filter(df, cols: list[str] | None = None):
    from pyspark.sql import functions as _F

    cond = None
    for c in _text_columns(df, cols):
        f = _F.col(c).rlike(_SPARK_PAN_PATTERN)
        cond = f if cond is None else (cond | f)
    return df.filter(cond) if cond is not None else df.limit(0)


def count_spark_pan_hits(df, cols: list[str] | None = None) -> int:
    present = _text_columns(df, cols)
    cands = spark_pan_candidate_filter(df, present).select(*present).collect()
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
    hits = []
    for m in _PAN_RUN.finditer(text or ""):
        digits = re.sub(r"[ \-]", "", m.group(0))
        if 13 <= len(digits) <= 19 and _luhn_ok(digits):
            hits.append(digits)
    return hits


def scan_frame(df, columns: list[str] | None = None) -> dict[str, int]:
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
