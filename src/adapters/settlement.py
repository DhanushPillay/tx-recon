"""PG settlement adapters — Razorpay / Cashfree / PayU -> canonical tx-recon."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass

CANONICAL_COLS = [
    "transaction_id",
    "bank_ref_id",
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
    if not raw:
        return "CREDIT_CARD"
    key = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
    if key in _INSTRUMENT_ALIASES:
        return _INSTRUMENT_ALIASES[key]
    # substring match for compound strings like "card - visa credit"
    for alias, canonical in _INSTRUMENT_ALIASES.items():
        if alias in key:
            return canonical
    return "CREDIT_CARD"


def _to_paise(value) -> int | None:
    """Parse INR amount (int paise, decimal string, '₹1,000.00') -> paise int."""
    if pd.isna(value):
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        # Heuristic: if value > 1e7 assume already paise? No — PG files are INR.
        # Generic adapter passes paise through; PG adapters pass INR decimal.
        # We treat int as paise only when called from Generic path (handled separately).
        return value
    s = str(value).strip().replace("₹", "").replace(",", "").replace(" ", "")
    if not s:
        return None
    try:
        # Use Decimal to avoid float drift: "976.40" -> 97640 exactly
        d = Decimal(s)
        # If value looks like paise already (no decimal point and > 100000 and from generic)
        # caller handles this; here we always treat as INR decimal.
        return int((d * 100).to_integral_value())
    except (InvalidOperation, ValueError, AttributeError):
        return None


def _to_paise_inr(value) -> int | None:
    """PG files: INR decimal -> paise. Always treat as INR."""
    if pd.isna(value):
        return None
    s = str(value).strip().replace("₹", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return int((Decimal(s) * 100).to_integral_value())
    except (InvalidOperation, ValueError):
        return None


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


class BaseSettlementAdapter:
    pg_name: str = "base"

    def can_handle(self, columns: list[str]) -> bool:
        raise NotImplementedError

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure canonical columns exist, fill missing with NA, order
        for col in CANONICAL_COLS:
            if col not in df.columns:
                df[col] = pd.NA
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
        lower_map = {c.lower(): c for c in df.columns}
        # Ensure canonical names (if file used Transaction_ID etc.)
        rename = {}
        for want in CANONICAL_COLS:
            if want not in df.columns and want.lower() in lower_map:
                rename[lower_map[want.lower()]] = want
        if rename:
            df = df.rename(columns=rename)
        df["instrument_type"] = df["instrument_type"].apply(_map_instrument)
        df["settlement_date"] = df["settlement_date"].apply(_to_iso_date)
        if "merchant_id" not in df.columns:
            df["merchant_id"] = "UNKNOWN"
        df["merchant_id"] = df["merchant_id"].fillna("UNKNOWN").replace("", "UNKNOWN")
        df["currency"] = (
            df.get("currency", pd.Series([pd.NA] * len(df))).fillna("INR").replace("", "INR")
        )
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

    # Lowercased required columns to detect
    _SIGNATURE = {"payment id", "settlement id", "settlement utr"}

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return self._SIGNATURE.issubset(cols)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = {c.strip().lower(): c for c in df.columns}

        def col(name: str) -> pd.Series:
            return df[lc[name]] if name in lc else pd.Series([pd.NA] * len(df))

        out = pd.DataFrame()
        out["transaction_id"] = col("payment id").astype(str).str.strip()
        out["settlement_id"] = col("settlement id").astype(str).str.strip()
        out["bank_ref_id"] = col("settlement utr").astype(str).str.strip().replace("nan", pd.NA)
        out["utr"] = out["bank_ref_id"]
        # Settlement Amount is INR decimal -> paise
        out["settled_amount_paise"] = col("settlement amount").apply(_to_paise_inr)
        # Fallback to Amount - Fee - Tax if settlement amount missing
        # gross
        out["gross_amount_paise"] = col("amount").apply(_to_paise_inr)
        out["fee_paise"] = col("fee").apply(_to_paise_inr)
        out["gst_paise"] = col("tax").apply(_to_paise_inr)
        # Dates: prefer Settlement Date, fallback Created At
        sd = col("settlement date").apply(_to_iso_date)
        ca = col("created at").apply(_to_iso_date)
        out["settlement_date"] = sd.fillna(ca)
        out["instrument_type"] = col("payment method").apply(_map_instrument)
        # Merchant: Razorpay files are per-account; column may not exist
        mid_col = None
        for cand in ("merchant id", "merchant_id", "account id"):
            if cand in lc:
                mid_col = lc[cand]
                break
        out["merchant_id"] = df[mid_col].astype(str).str.strip() if mid_col else "UNKNOWN"
        out["merchant_id"] = (
            out["merchant_id"].fillna("UNKNOWN").replace("nan", "UNKNOWN").replace("", "UNKNOWN")
        )
        cur = col("currency").astype(str).str.strip().str.upper()
        out["currency"] = cur.replace("NAN", pd.NA).fillna("INR").replace("", "INR")
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
        lc = {c.strip().lower(): c for c in df.columns}

        def col(name: str) -> pd.Series:
            return df[lc[name]] if name in lc else pd.Series([pd.NA] * len(df))

        out = pd.DataFrame()
        out["transaction_id"] = col("payment_id").astype(str).str.strip()
        out["settlement_id"] = col("settlement_id").astype(str).str.strip()
        # UTR may be 'utr' or 'settlement_utr'
        utr_col = "utr" if "utr" in lc else ("settlement_utr" if "settlement_utr" in lc else None)
        out["bank_ref_id"] = (
            col(utr_col).astype(str).str.strip().replace("nan", pd.NA)
            if utr_col
            else col("settlement_id").astype(str)
        )
        out["utr"] = out["bank_ref_id"]
        out["settled_amount_paise"] = col("settlement_amount").apply(_to_paise_inr)
        out["gross_amount_paise"] = (
            col("amount").apply(_to_paise_inr) if "amount" in lc else pd.Series([pd.NA] * len(df))
        )
        out["fee_paise"] = col("fee").apply(_to_paise_inr)
        out["gst_paise"] = col("tax").apply(_to_paise_inr)
        out["settlement_date"] = col("settlement_date").apply(_to_iso_date)
        if out["settlement_date"].isna().all() and "created_at" in lc:
            out["settlement_date"] = col("created_at").apply(_to_iso_date)
        out["instrument_type"] = col("payment_method").apply(_map_instrument)
        out["merchant_id"] = (
            col("merchant_id").astype(str).str.strip() if "merchant_id" in lc else "UNKNOWN"
        )
        out["merchant_id"] = (
            out["merchant_id"].fillna("UNKNOWN").replace("nan", "UNKNOWN").replace("", "UNKNOWN")
        )
        out["currency"] = (
            col("currency").astype(str).str.strip().str.upper().replace("NAN", pd.NA).fillna("INR")
        )
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
        lc = {c.strip().lower(): c for c in df.columns}

        def col(name: str) -> pd.Series:
            return df[lc[name]] if name in lc else pd.Series([pd.NA] * len(df))

        out = pd.DataFrame()
        # PayU primary id is mihpayid
        tx = col("mihpayid").astype(str).str.strip()
        # txnid alias fallback
        if tx.isna().all() or (tx == "nan").all():
            for cand in ("transaction_id", "txnid", "txnid_"):
                if cand in lc:
                    tx = df[lc[cand]].astype(str).str.strip()
                    break
        out["transaction_id"] = tx
        sid = (
            col("settlementid").astype(str).str.strip()
            if "settlementid" in lc
            else col("settlement_id").astype(str).str.strip()
        )
        out["settlement_id"] = sid
        utr_col = "utr" if "utr" in lc else ("settlement_utr" if "settlement_utr" in lc else None)
        out["bank_ref_id"] = (
            col(utr_col).astype(str).str.strip().replace("nan", pd.NA) if utr_col else sid
        )
        out["utr"] = out["bank_ref_id"]
        amt_col = (
            "settlementamount"
            if "settlementamount" in lc
            else ("settlement_amount" if "settlement_amount" in lc else None)
        )
        out["settled_amount_paise"] = (
            col(amt_col).apply(_to_paise_inr) if amt_col else pd.Series([pd.NA] * len(df))
        )
        # Fee: surcharge or fee
        fee_col = "surcharge" if "surcharge" in lc else ("fee" if "fee" in lc else None)
        out["fee_paise"] = (
            col(fee_col).apply(_to_paise_inr) if fee_col else pd.Series([pd.NA] * len(df))
        )
        out["gst_paise"] = col("tax").apply(_to_paise_inr)
        # Gross: amount
        gross_col = (
            "amount" if "amount" in lc else ("gross_amount" if "gross_amount" in lc else None)
        )
        out["gross_amount_paise"] = (
            col(gross_col).apply(_to_paise_inr) if gross_col else pd.Series([pd.NA] * len(df))
        )
        # Date
        date_col = (
            "settlementdate"
            if "settlementdate" in lc
            else ("settlement_date" if "settlement_date" in lc else None)
        )
        out["settlement_date"] = (
            col(date_col).apply(_to_iso_date) if date_col else pd.Series([pd.NA] * len(df))
        )
        if out["settlement_date"].isna().all() and "created_at" in lc:
            out["settlement_date"] = col("created_at").apply(_to_iso_date)
        # paymentMode variations
        pm = None
        for cand in ("paymentmode", "payment_mode", "paymentmethod", "payment_method"):
            if cand in lc:
                pm = lc[cand]
                break
        out["instrument_type"] = df[pm].apply(_map_instrument) if pm else "CREDIT_CARD"
        out["merchant_id"] = (
            col("merchant_id").astype(str).str.strip() if "merchant_id" in lc else "UNKNOWN"
        )
        out["merchant_id"] = (
            out["merchant_id"].fillna("UNKNOWN").replace("nan", "UNKNOWN").replace("", "UNKNOWN")
        )
        out["currency"] = (
            col("currency").astype(str).str.strip().str.upper().replace("NAN", pd.NA).fillna("INR")
        )
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


def load_settlement_csv(path: str, pg_hint: str | None = None) -> tuple[pd.DataFrame, str]:
    """Read CSV at path and normalize. Returns (canonical_df, pg_name)."""
    # Detect delimiter: PG files may be comma or semicolon; pandas handles comma default.
    # Try reading; if single column, try semicolon.
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=["", "NA", "null", "NULL"])
    # Re-read with na handling if needed — keep_default_na True for empty strings
    # Replace empty strings with NA for normalization
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    # If reading produced single column with commas, already handled; if semicolon, retry
    if len(df.columns) == 1 and ";" in str(df.columns[0]):
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, na_values=["", "NA"])
        df = df.replace(r"^\s*$", pd.NA, regex=True)
    canonical, pg_name = normalize_settlement_df(df, pg_hint=pg_hint)
    # Attach raw payload for audit (json of original row limited to 2k chars)
    try:
        raw = df.to_json(orient="records")
        # Store first row sample? Instead per-row raw is too heavy; keep column not needed now.
        _ = raw  # keep for future
    except Exception:
        pass
    return canonical, pg_name
