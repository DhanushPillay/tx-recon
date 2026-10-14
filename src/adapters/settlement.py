from __future__ import annotations

import csv
import re

import pandas as pd

CANONICAL_COLS = [
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
    "provider",
]

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
    # pd.NA breaks a plain if-not so check it directly
    if raw is None or (not isinstance(raw, str) and pd.isna(raw)):
        return "UNKNOWN"
    key = raw.strip().lower().replace("-", "_").replace(" ", "_")
    if not key:
        return "UNKNOWN"
    if key in _INSTRUMENT_ALIASES:
        return _INSTRUMENT_ALIASES[key]
    # longest alias first so "international_card" hits "international", not "card"
    for alias, canonical in sorted(_INSTRUMENT_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if alias in key:
            return canonical
    return "UNKNOWN"


def _to_iso_date(value) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", s)
    if m:
        d, mo, y = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
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
    # Ambiguous slashes ("02/04/2025") are DAY-first here, matching _to_iso_date.
    # The vectorized fast path only fires for uniform ISO input; anything else
    # falls through to the row-wise fallback below, which is also day-first,
    # so both paths agree (pinned by test_adapters_extra date tests).
    if s.empty:
        return s
    str_s = s.astype(str).str.strip()
    if str_s.str.match(r"^\d{4}-\d{2}-\d{2}$").all():
        return str_s
    # strict ISO on the vector path (dayfirst=True mangles ISO in pandas 2.x);
    # every non-ISO value falls through to the day-first row fallback below.
    out = pd.to_datetime(str_s, errors="coerce", utc=True, format="%Y-%m-%d").dt.strftime(
        "%Y-%m-%d"
    )
    mask = out.isna() & s.notna() & (str_s != "nan") & (str_s != "None")
    if mask.any():
        fallback = s.loc[mask].apply(_to_iso_date)
        out = out.copy()
        out.loc[mask] = fallback
    return out


def _paise_inr_series(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    import numpy as np

    cleaned = s.astype(str).str.replace(r"[₹,\s]", "", regex=True).str.strip()
    cleaned = cleaned.replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})
    nums = pd.to_numeric(cleaned, errors="coerce")
    # Round half away from zero on the magnitude. `floor(x + 0.5)` alone sends
    # ties towards +infinity, so -100.50 came out as -10050 where the MT940
    # reader's Decimal HALF_UP gives -10051. The 1e-9 keeps a decimal that is
    # a hair under a tie (0.145 -> 14.499999999999998) from losing the
    # half-paisa to float representation.
    return (np.sign(nums) * np.floor(np.abs(nums) * 100 + 0.5 + 1e-9)).astype("Int64")


def _map_instrument_series(s: pd.Series) -> pd.Series:
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
    missing = mapped.isna() & s.notna()
    if missing.any():
        mapped.loc[missing] = s.loc[missing].apply(_map_instrument)
    return mapped.fillna("UNKNOWN")


def _lc(df: pd.DataFrame) -> dict[str, str]:
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
    # astype(str) stringifies missing markers, so map every spelling of
    # missing back to UNKNOWN: None->"None", np.nan->"nan", pd.NA->"<NA>".
    return s.fillna("UNKNOWN").replace(
        {"nan": "UNKNOWN", "None": "UNKNOWN", "<NA>": "UNKNOWN", "": "UNKNOWN"}
    )


def _currency(df: pd.DataFrame, lc: dict[str, str]) -> pd.Series:
    cur = _col(df, lc, "currency").fillna("INR").astype(str).str.strip().str.upper()
    return cur.replace("NAN", "INR").replace("", "INR")


def _bank_ref(
    df: pd.DataFrame, lc: dict[str, str], candidates: tuple[str, ...], fallback: pd.Series
) -> pd.Series:
    for cand in candidates:
        if cand in lc:
            return (
                df[lc[cand]]
                .astype(str)
                .str.strip()
                .replace({"nan": pd.NA, "None": pd.NA, "<NA>": pd.NA, "": pd.NA})
            )
    return fallback


class BaseSettlementAdapter:
    pg_name: str = "base"

    def can_handle(self, columns: list[str]) -> bool:
        raise NotImplementedError

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        stamped = "provider" in df.columns
        for col in CANONICAL_COLS:
            if col not in df.columns:
                df[col] = pd.NA
        if not stamped:
            df["provider"] = self.pg_name
        df["settled_amount_paise"] = pd.to_numeric(
            df["settled_amount_paise"], errors="coerce"
        ).astype("Int64")
        for c in ("fee_paise", "gst_paise", "gross_amount_paise"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        return df[CANONICAL_COLS].copy()


class GenericAdapter(BaseSettlementAdapter):
    pg_name = "generic"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return {"transaction_id", "bank_ref_id", "settled_amount_paise"}.issubset(cols)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        lc = _lc(df)
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
        df["settled_amount_paise"] = pd.to_numeric(df["settled_amount_paise"], errors="coerce")
        if "utr" not in df.columns and "bank_ref_id" in df.columns:
            df["utr"] = df["bank_ref_id"]
        if "settlement_id" not in df.columns:
            df["settlement_id"] = pd.NA
        return self._finalize(df)


class RazorpayAdapter(BaseSettlementAdapter):
    pg_name = "razorpay"

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
        out["settled_amount_paise"] = _paise_col(df, lc, "settlement_amount")
        out["gross_amount_paise"] = _paise_col(df, lc, "amount")
        out["fee_paise"] = _paise_col(df, lc, "fee")
        out["gst_paise"] = _paise_col(df, lc, "tax")
        out["settlement_date"] = _date_with_fallback(df, lc, ("settlement_date",), ("created_at",))
        out["instrument_type"] = _map_instrument_series(_col(df, lc, "payment_method"))
        out["merchant_id"] = _merchant(df, lc)
        out["currency"] = _currency(df, lc)
        return self._finalize(out)


class CashfreeAdapter(BaseSettlementAdapter):
    pg_name = "cashfree"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return {"payment_id", "settlement_id", "settlement_amount"}.issubset(
            cols
        ) and "settlement utr" not in cols

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = _lc(df)

        out = pd.DataFrame()
        out["transaction_id"] = _col(df, lc, "payment_id").astype(str).str.strip()
        out["settlement_id"] = _col(df, lc, "settlement_id").astype(str).str.strip()
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
    pg_name = "payu"

    def can_handle(self, columns: list[str]) -> bool:
        cols = {c.strip().lower() for c in columns}
        return "mihpayid" in cols

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        lc = _lc(df)

        out = pd.DataFrame()
        tx = _col(df, lc, "mihpayid").astype(str).str.strip()
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
        out["fee_paise"] = _paise_col(df, lc, "surcharge", "fee")
        out["gst_paise"] = _paise_col(df, lc, "tax")
        out["gross_amount_paise"] = _paise_col(df, lc, "amount", "gross_amount")
        out["settlement_date"] = _date_with_fallback(
            df, lc, ("settlementdate", "settlement_date"), ("created_at",)
        )
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
    GenericAdapter(),
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
    if pg_hint:
        hint = pg_hint.strip().lower()
        for a in _ADAPTERS:
            if a.pg_name == hint and a.can_handle(list(df.columns)):
                return a.normalize(df), a.pg_name
        for a in _ADAPTERS:
            if a.pg_name == hint:
                return a.normalize(df), a.pg_name
    adapter = detect_adapter(df)
    return adapter.normalize(df), adapter.pg_name


_CSV_NA_VALUES = ["", "NA", "null", "NULL"]

_CSV_DELIMITERS = ",;\t|"


def blank_to_na(df: pd.DataFrame) -> pd.DataFrame:
    mask = pd.DataFrame(False, index=df.index, columns=df.columns)
    for col in df.columns:
        if df[col].dtype == object:
            mask[col] = df[col].str.isspace().eq(True) | df[col].eq("")
    return df.mask(mask, pd.NA) if mask.to_numpy().any() else df


def sniff_csv_delimiter(path: str, default: str = ",") -> str:
    try:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            header = fh.readline()
        dialect = csv.Sniffer().sniff(header, delimiters=_CSV_DELIMITERS)
    except (OSError, UnicodeDecodeError, csv.Error):
        return default
    return dialect.delimiter or default


def load_settlement_csv(path: str, pg_hint: str | None = None) -> tuple[pd.DataFrame, str]:
    df = pd.read_csv(
        path,
        dtype=str,
        sep=sniff_csv_delimiter(path),
        engine="c",
        keep_default_na=False,
        na_values=_CSV_NA_VALUES,
    )
    df = blank_to_na(df)
    canonical, pg_name = normalize_settlement_df(df, pg_hint=pg_hint)
    return canonical, pg_name
