"""PG settlement adapters — Razorpay / Cashfree / PayU -> canonical tx-recon."""

from __future__ import annotations

import re

import pandas as pd

CANONICAL_COLS = [
    # Order matches bank_schema in src/processing/reconcile.py: Spark's CSV
    # reader maps positionally under an explicit schema, so curated files
    # must be bank_ref-first or tx/bank_ref silently swap on read.
    "bank_ref_id",
    "transaction_id",
    "settled_amount_paise",
    "settlement_date",
    "instrument_type",
    "merchant_id",
    "fee_paise",
    "gst_paise",
    "settlement_id",
    "utr",
    "currency",
    "gross_amount_paise",
    # Provenance for batch-model recon (provider batch/cutoff config) and
    # per-provider late SLA. Stamped by _finalize from the handling adapter.
    "provider",
]

# Mapping raw PG method strings -> canonical INSTRUMENT_TYPES
_INSTRUMENT_ALIASES: dict[str, str] = {
    "upi": "UPI",
    "vpa": "UPI",
    "card": "CREDIT_CARD",
    "credit": "CREDIT_CARD",
    "credit_card": "CREDIT_CARD",
    "credit card": "CREDIT_CARD",
    "debit": "DEBIT_CARD",
    "debit_card": "DEBIT_CARD",
    "debit card": "DEBIT_CARD",
    "netbanking": "NETBANKING",
    "net banking": "NETBANKING",
    "nb": "NETBANKING",
    "wallet": "WALLET",
    "international": "INTERNATIONAL",
    "intl": "INTERNATIONAL",
    "emi": "CREDIT_CARD",
    "paylater": "WALLET",
    "cardless_emi": "CREDIT_CARD",
}


def _map_instrument(raw: str | None) -> str:
    """Map a PG method string to a canonical instrument.

    Unknown strings map to "UNKNOWN" (not a real instrument): the Pandera
    isin check quarantines the row. Silently defaulting to CREDIT_CARD would
    price the row at the wrong MDR and MATCH it.
    """
    if not raw:
        return "UNKNOWN"
    key = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
    if key in _INSTRUMENT_ALIASES:
        return _INSTRUMENT_ALIASES[key]
    # substring match for compound strings like "card - visa credit"
    for alias, canonical in _INSTRUMENT_ALIASES.items():
        if alias in key:
            return canonical
    return "UNKNOWN"


def _to_iso_date(value) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    # Already YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # DD-MM-YYYY or DD/MM/YYYY
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", s)
    if m:
        d, mo, y = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    # ISO timestamp -> date part
    # e.g. 2025-04-01T10:00:00Z or 2025-04-01 10:00:00
    m2 = re.match(r"^(\d{4}-\d{2}-\d{2})[T ]", s)
    if m2:
        return m2.group(1)
    try:
        dt = pd.to_datetime(s, errors="coerce", utc=True)
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _to_iso_series(s: pd.Series) -> pd.Series:
    # ponytail: vectorized date parse — single pd.to_datetime over series, not per-row apply
    if s.empty:
        return s
    str_s = s.astype(str).str.strip()
    if str_s.str.match(r"^\d{4}-\d{2}-\d{2}$").all():
        return str_s
    out = pd.to_datetime(str_s, errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    mask = out.isna() & s.notna() & (str_s != "nan") & (str_s != "None")
    if mask.any():
        fallback = s.loc[mask].apply(_to_iso_date)
        out = out.copy()
        out.loc[mask] = fallback
    return out


def _paise_inr_series(s: pd.Series) -> pd.Series:
    # ponytail: vectorized INR->paise, HALF_UP without per-row Decimal.
    # floor(x + 0.5) is exact half-up on the non-negative domain (amounts are
    # validated > 0 downstream; negative halves would differ but those rows
    # fail validation regardless). The 1e-9 absorbs float64 representation
    # error (e.g. 2.675 stored as 2.67499999...); it is smaller than the ulp
    # of large values so it can never push a genuine non-half over.
    if s.empty:
        return s
    import numpy as np

    cleaned = s.astype(str).str.replace(r"[₹,\s]", "", regex=True).str.strip()
    cleaned = cleaned.replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})
    nums = pd.to_numeric(cleaned, errors="coerce")
    return np.floor(nums * 100 + 0.5 + 1e-9).astype("Int64")


def _map_instrument_series(s: pd.Series) -> pd.Series:
    # ponytail: vectorized map, fallback to row-wise substring only for unmapped
    if s.empty:
        return s
    lower = (
        s.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("-", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )
    mapped = lower.map(_INSTRUMENT_ALIASES)
    # substring fallback where direct miss
    missing = mapped.isna()
    if missing.any():
        mapped.loc[missing] = s.loc[missing].apply(_map_instrument)
    return mapped.fillna("UNKNOWN")


def _lc(df: pd.DataFrame) -> dict[str, str]:
    """Lowercased header lookup with separators normalized to underscores.

    'Settlement Amount' / 'settlement-amount' / 'settlement_amount' all map
    to 'settlement_amount', so every adapter looks keys up underscore-style.
    """
    return {re.sub(r"[\s\-]+", "_", c.strip().lower()): c for c in df.columns}


def _col(df: pd.DataFrame, lc: dict[str, str], name: str) -> pd.Series:
    return df[lc[name]] if name in lc else pd.Series([pd.NA] * len(df), index=df.index)


def _paise_col(df: pd.DataFrame, lc: dict[str, str], *names: str) -> pd.Series:
    for n in names:
        if n in lc:
            return _paise_inr_series(df[lc[n]])
    return pd.Series([pd.NA] * len(df), index=df.index)


def _iso_col(df: pd.DataFrame, lc: dict[str, str], *names: str) -> pd.Series:
    for n in names:
        if n in lc:
            return _to_iso_series(df[lc[n]])
    return pd.Series([pd.NA] * len(df), index=df.index)


def _date_with_fallback(
    df: pd.DataFrame, lc: dict[str, str], primary: tuple[str, ...], fallbacks: tuple[str, ...]
) -> pd.Series:
    out = _iso_col(df, lc, *primary)
    for fb in fallbacks:
        if fb in lc:
            out = out.fillna(_iso_col(df, lc, fb))
    return out


def _merchant(df: pd.DataFrame, lc: dict[str, str]) -> pd.Series:
    for cand in ("merchant_id", "account_id"):
        if cand in lc:
            s = df[lc[cand]].astype(str).str.strip()
            break
    else:
        return pd.Series(["UNKNOWN"] * len(df), index=df.index)
    return s.fillna("UNKNOWN").replace("nan", "UNKNOWN").replace("", "UNKNOWN")


def _currency(df: pd.DataFrame, lc: dict[str, str]) -> pd.Series:
    # fillna before str cast: pd.NA astype(str) is "<NA>", not NaN, and would dodge fillna
    cur = _col(df, lc, "currency").fillna("INR").astype(str).str.strip().str.upper()
    return cur.replace("NAN", "INR").replace("", "INR")


def _bank_ref(
    df: pd.DataFrame, lc: dict[str, str], candidates: tuple[str, ...], fallback: pd.Series
) -> pd.Series:
    for cand in candidates:
        if cand in lc:
            return df[lc[cand]].astype(str).str.strip().replace("nan", pd.NA)
    return fallback


class BaseSettlementAdapter:
    pg_name: str = "base"

    def can_handle(self, columns: list[str]) -> bool:
        raise NotImplementedError

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure canonical columns exist, fill missing with NA, order.
        # Provenance stamp: which adapter (PG) produced this row — but an
        # existing provider (e.g. re-validated curated file) always wins.
        stamped = "provider" in df.columns
        for col in CANONICAL_COLS:
            if col not in df.columns:
                df[col] = pd.NA
        if not stamped:
            df["provider"] = self.pg_name
        # Coerce types for required cols
        df["settled_amount_paise"] = pd.to_numeric(
            df["settled_amount_paise"], errors="coerce"
        ).astype("Int64")
        for c in ("fee_paise", "gst_paise", "gross_amount_paise"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        return df[CANONICAL_COLS].copy()


class GenericAdapter(BaseSettlementAdapter):
    """Already-canonical 5-col file: bank_ref_id, transaction_id, settled_amount_paise, settlement_date, instrument_type."""

    pg_name = "generic"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return {"transaction_id", "bank_ref_id", "settled_amount_paise"}.issubset(cols)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # Column names are canonical; just normalize aliases and fill optional cols
        df = df.copy()
        # lower -> canonical for case-insensitive files
        lc = _lc(df)
        # Ensure canonical names (if file used Transaction_ID etc.)
        rename = {}
        for want in CANONICAL_COLS:
            if want not in df.columns and want.lower() in lc:
                rename[lc[want.lower()]] = want
        if rename:
            df = df.rename(columns=rename)
            lc = _lc(df)
        df["instrument_type"] = _map_instrument_series(df["instrument_type"])
        df["settlement_date"] = _to_iso_series(df["settlement_date"])
        df["merchant_id"] = _merchant(df, lc)
        df["currency"] = _currency(df, lc)
        # settled_amount_paise is already paise in generic
        df["settled_amount_paise"] = pd.to_numeric(df["settled_amount_paise"], errors="coerce")
        if "utr" not in df.columns and "bank_ref_id" in df.columns:
            df["utr"] = df["bank_ref_id"]
        if "settlement_id" not in df.columns:
            df["settlement_id"] = pd.NA
        return self._finalize(df)


class RazorpayAdapter(BaseSettlementAdapter):
    """Razorpay settlement CSV -> canonical.

    Expected Razorpay headers (case-insensitive, spaces):
    Payment Id, Order Id, Settlement Id, Settlement UTR, Payment Method,
    Amount, Fee, Tax, Settlement Amount, Currency, Created At, Settlement Date, Status
    """

    pg_name = "razorpay"

    # Lowercased required columns to detect (utr optional for tests)
    _SIGNATURE = {"payment id", "settlement id"}

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return self._SIGNATURE.issubset(cols)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = _lc(df)

        out = pd.DataFrame()
        out["transaction_id"] = _col(df, lc, "payment_id").astype(str).str.strip()
        out["settlement_id"] = _col(df, lc, "settlement_id").astype(str).str.strip()
        out["bank_ref_id"] = (
            _col(df, lc, "settlement_utr").astype(str).str.strip().replace("nan", pd.NA)
        )
        out["utr"] = out["bank_ref_id"]
        # Settlement Amount is INR decimal -> paise
        out["settled_amount_paise"] = _paise_col(df, lc, "settlement_amount")
        # Fallback to Amount - Fee - Tax if settlement amount missing
        # gross
        out["gross_amount_paise"] = _paise_col(df, lc, "amount")
        out["fee_paise"] = _paise_col(df, lc, "fee")
        out["gst_paise"] = _paise_col(df, lc, "tax")
        # Dates: prefer Settlement Date, fallback Created At
        out["settlement_date"] = _date_with_fallback(df, lc, ("settlement_date",), ("created_at",))
        out["instrument_type"] = _map_instrument_series(_col(df, lc, "payment_method"))
        # Merchant: Razorpay files are per-account; column may not exist
        out["merchant_id"] = _merchant(df, lc)
        out["currency"] = _currency(df, lc)
        return self._finalize(out)


class CashfreeAdapter(BaseSettlementAdapter):
    """Cashfree settlement CSV -> canonical.

    Headers: payment_id, order_id, settlement_id, settlement_amount, fee, tax, utr,
    payment_method, settlement_date, currency, status, merchant_id
    """

    pg_name = "cashfree"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        # cashfree uses underscores, needs payment_id + settlement_id
        return {"payment_id", "settlement_id", "settlement_amount"}.issubset(
            cols
        ) and "settlement utr" not in cols

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = _lc(df)

        out = pd.DataFrame()
        out["transaction_id"] = _col(df, lc, "payment_id").astype(str).str.strip()
        out["settlement_id"] = _col(df, lc, "settlement_id").astype(str).str.strip()
        # UTR may be 'utr' or 'settlement_utr'
        out["bank_ref_id"] = _bank_ref(
            df, lc, ("utr", "settlement_utr"), _col(df, lc, "settlement_id").astype(str)
        )
        out["utr"] = out["bank_ref_id"]
        out["settled_amount_paise"] = _paise_col(df, lc, "settlement_amount")
        out["gross_amount_paise"] = _paise_col(df, lc, "amount")
        out["fee_paise"] = _paise_col(df, lc, "fee")
        out["gst_paise"] = _paise_col(df, lc, "tax")
        out["settlement_date"] = _date_with_fallback(df, lc, ("settlement_date",), ("created_at",))
        out["instrument_type"] = _map_instrument_series(_col(df, lc, "payment_method"))
        out["merchant_id"] = _merchant(df, lc)
        out["currency"] = _currency(df, lc)
        return self._finalize(out)


class PayUAdapter(BaseSettlementAdapter):
    """PayU settlement CSV -> canonical.

    Headers: mihpayid, transaction_id|txnid, settlementId, settlementAmount, surcharge|fee, tax, utr, paymentMode, settlementDate
    """

    pg_name = "payu"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return "mihpayid" in cols

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = _lc(df)

        out = pd.DataFrame()
        # PayU primary id is mihpayid
        tx = _col(df, lc, "mihpayid").astype(str).str.strip()
        # txnid alias fallback
        if tx.isna().all() or (tx == "nan").all():
            for cand in ("transaction_id", "txnid", "txnid_"):
                if cand in lc:
                    tx = df[lc[cand]].astype(str).str.strip()
                    break
        out["transaction_id"] = tx
        sid = _col(df, lc, "settlementid").astype(str).str.strip()
        if ("settlementid" not in lc) or sid.isna().all() or (sid == "nan").all():
            sid = _col(df, lc, "settlement_id").astype(str).str.strip()
        out["settlement_id"] = sid
        out["bank_ref_id"] = _bank_ref(df, lc, ("utr", "settlement_utr"), sid)
        out["utr"] = out["bank_ref_id"]
        out["settled_amount_paise"] = _paise_col(df, lc, "settlementamount", "settlement_amount")
        # Fee: surcharge or fee
        out["fee_paise"] = _paise_col(df, lc, "surcharge", "fee")
        out["gst_paise"] = _paise_col(df, lc, "tax")
        # Gross: amount
        out["gross_amount_paise"] = _paise_col(df, lc, "amount", "gross_amount")
        # Date
        out["settlement_date"] = _date_with_fallback(
            df, lc, ("settlementdate", "settlement_date"), ("created_at",)
        )
        # paymentMode variations
        pm = next(
            (
                lc[cand]
                for cand in ("paymentmode", "payment_mode", "paymentmethod", "payment_method")
                if cand in lc
            ),
            None,
        )
        out["instrument_type"] = _map_instrument_series(df[pm]) if pm else "UNKNOWN"
        out["merchant_id"] = _merchant(df, lc)
        out["currency"] = _currency(df, lc)
        return self._finalize(out)


_ADAPTERS: list[BaseSettlementAdapter] = [
    RazorpayAdapter(),
    CashfreeAdapter(),
    PayUAdapter(),
    GenericAdapter(),  # must be last — generic is most permissive
]


def detect_adapter(df: pd.DataFrame) -> BaseSettlementAdapter:
    cols = list(df.columns)
    for adapter in _ADAPTERS:
        if adapter.can_handle(cols):
            return adapter
    return GenericAdapter()


def normalize_settlement_df(
    df: pd.DataFrame, pg_hint: str | None = None
) -> tuple[pd.DataFrame, str]:
    """Normalize df to canonical. Returns (canonical_df, pg_name)."""
    if pg_hint:
        hint = pg_hint.strip().lower()
        for a in _ADAPTERS:
            if a.pg_name == hint and a.can_handle(list(df.columns)):
                return a.normalize(df), a.pg_name
        # Fallback: try by hint name even if signature not matched
        for a in _ADAPTERS:
            if a.pg_name == hint:
                return a.normalize(df), a.pg_name
    adapter = detect_adapter(df)
    return adapter.normalize(df), adapter.pg_name


# Null spellings shared by both readers: the chunked (>256MB) and direct
# paths must tokenize the same bytes identically (was: chunked passed none).
_CSV_NA_VALUES = ["", "NA", "null", "NULL"]


def load_settlement_csv(path: str, pg_hint: str | None = None) -> tuple[pd.DataFrame, str]:
    """Read CSV at path and normalize. Returns (canonical_df, pg_name)."""
    # sep=None sniffs comma vs semicolon PG dialects in one read.
    df = pd.read_csv(
        path,
        dtype=str,
        sep=None,
        engine="python",
        keep_default_na=False,
        na_values=_CSV_NA_VALUES,
    )
    # Replace empty strings with NA for normalization
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    canonical, pg_name = normalize_settlement_df(df, pg_hint=pg_hint)
    return canonical, pg_name
